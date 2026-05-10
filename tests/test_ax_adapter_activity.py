import asyncio
import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "ax_adapter_for_test",
    Path(__file__).resolve().parents[1] / "plugins" / "platforms" / "ax" / "adapter.py",
)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
try:
    _SPEC.loader.exec_module(_MODULE)
except ModuleNotFoundError as exc:
    # The adapter imports gateway.config / gateway.platforms.base from the
    # hermes-agent install. In the ax-gateway repo's own venv (ruff / pytest
    # under uv) those modules aren't on sys.path. Skip cleanly so the
    # repo-level test run doesn't fail on contributors without a local
    # hermes-agent checkout. Run under hermes-agent's venv to exercise the
    # adapter's runtime contract.
    if "gateway" in str(exc) or "hermes" in str(exc):
        pytest.skip(
            f"hermes-agent not importable from this venv: {exc}",
            allow_module_level=True,
        )
    raise
AxAdapter = _MODULE.AxAdapter


def _adapter() -> AxAdapter:
    adapter = AxAdapter.__new__(AxAdapter)
    adapter.agent_name = "nova"
    adapter.agent_id = "agent-123"
    adapter.space_id = "space-123"
    import re as _re
    adapter._mention_pattern = _re.compile(
        rf"(?<!\w)@{_re.escape(adapter.agent_name)}(?!\w)",
        _re.IGNORECASE,
    )
    return adapter


def test_ax_adapter_uses_activity_status_and_final_only_messages():
    assert AxAdapter.SUPPORTS_ACTIVITY_STATUS is True
    assert AxAdapter.SUPPORTS_MESSAGE_EDITING is False


def test_send_typing_forwards_activity_metadata_to_processing_status(monkeypatch):
    adapter = _adapter()
    calls = []

    async def fake_post(message_id, status, *, activity=None):
        calls.append({"message_id": message_id, "status": status, "activity": activity})

    monkeypatch.setattr(adapter, "_post_processing_status", fake_post)

    asyncio.run(
        adapter.send_typing(
            "msg-1",
            metadata={"status": "tool_call", "activity": "🔍 read_file: adapter.py"},
        )
    )

    assert calls == [
        {
            "message_id": "msg-1",
            "status": "tool_call",
            "activity": "🔍 read_file: adapter.py",
        }
    ]


def test_mention_match_uses_word_boundaries():
    """Substring matching would treat @nova2 or email@nova.com as a hit for
    @nova; the word-boundary regex must reject those while still matching
    real mentions in any sane punctuation context."""
    adapter = _adapter()

    accepts = [
        "@nova hi",
        "hey @nova, ping",
        "  @nova  ",
        "@nova.",
        "@nova!\n",
        "(@nova)",
        "yo @NOVA",  # case-insensitive
    ]
    rejects = [
        "@nova2 hi",
        "@novanaut",
        "email@nova.com",
        "send to alice@nova",
        "no mention here",
        "@nov",
    ]
    for text in accepts:
        assert adapter._is_for_me({"content": text}), f"should match: {text!r}"
    for text in rejects:
        assert not adapter._is_for_me({"content": text}), f"should not match: {text!r}"


def test_mention_match_prefers_structured_mentions_list():
    """Structured mentions list bypasses the regex and matches by name."""
    adapter = _adapter()
    assert adapter._is_for_me({"mentions": ["nova"], "content": "no @ here"})
    assert adapter._is_for_me({"mentions": [{"name": "nova"}], "content": ""})
    assert not adapter._is_for_me({"mentions": ["nova2"], "content": "no @ here"})


def test_dispatch_inbound_uses_stable_thread_chat_type(monkeypatch):
    """chat_type must be 'thread' for both the first mention and follow-up
    replies. build_session_key bakes chat_type into the session key, so a
    "channel"-then-"thread" flip would split one logical thread into two
    Hermes sessions and break continuity / the active-session guard."""
    from types import SimpleNamespace
    adapter = _adapter()
    # SessionSource.platform.value is the only platform attribute touched on
    # the dispatch path; a duck-typed stub avoids depending on hermes-agent's
    # plugin-registry scan picking up "ax" in this test process.
    adapter.platform = SimpleNamespace(value="ax")
    captured: list = []

    async def fake_handle_message(event):
        captured.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle_message)

    first_mention = {
        "id": "msg-root",
        "content": "@nova start",
        "sender": "alice",
        "sender_id": "u-1",
        "parent_id": None,
    }
    follow_up = {
        "id": "msg-2",
        "content": "@nova continue",
        "sender": "alice",
        "sender_id": "u-1",
        "parent_id": "msg-root",
    }

    asyncio.run(adapter._dispatch_inbound(first_mention))
    asyncio.run(adapter._dispatch_inbound(follow_up))

    assert len(captured) == 2
    assert captured[0].source.chat_type == "thread"
    assert captured[1].source.chat_type == "thread"
    # Same thread root → same chat_id → same session key.
    assert captured[0].source.chat_id == captured[1].source.chat_id == "msg-root"


def test_stop_typing_marks_processing_status_completed(monkeypatch):
    adapter = _adapter()
    calls = []

    async def fake_post(message_id, status, *, activity=None):
        calls.append({"message_id": message_id, "status": status, "activity": activity})

    monkeypatch.setattr(adapter, "_post_processing_status", fake_post)

    asyncio.run(adapter.stop_typing("msg-1"))

    assert calls == [{"message_id": "msg-1", "status": "completed", "activity": None}]
