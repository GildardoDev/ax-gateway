import asyncio
import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "ax_adapter_for_test",
    Path(__file__).resolve().parents[1] / "ax_cli" / "plugins" / "platforms" / "ax" / "adapter.py",
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
    from collections import OrderedDict
    adapter = AxAdapter.__new__(AxAdapter)
    adapter.agent_name = "nova"
    adapter.agent_id = "agent-123"
    adapter.space_id = "space-123"
    adapter.local_gateway_url = "http://127.0.0.1:8765"
    import re as _re
    adapter._mention_pattern = _re.compile(
        rf"(?<!\w)@{_re.escape(adapter.agent_name)}(?!\w)",
        _re.IGNORECASE,
    )
    adapter._seen_message_ids = OrderedDict()
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


def test_dispatch_inbound_dedupes_double_event(monkeypatch):
    """aX SSE emits both `event: message` and `event: mention` for any
    mention — same message_id, two events. Without dedup the second hits
    the active-session guard and fires the "⚡ Interrupting current task"
    template even though the agent is idle. Dedup must let the first
    through and silently drop the second."""
    from types import SimpleNamespace
    adapter = _adapter()
    adapter.platform = SimpleNamespace(value="ax")
    captured: list = []

    async def fake_handle_message(event):
        captured.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle_message)

    payload = {
        "id": "msg-aaa",
        "content": "@nova hi",
        "sender": "alice",
        "sender_id": "u-1",
        "parent_id": None,
    }
    asyncio.run(adapter._dispatch_inbound(payload))
    asyncio.run(adapter._dispatch_inbound(payload))  # SSE redelivery

    assert len(captured) == 1, "second SSE event for same message_id must be dropped"


def test_dispatch_inbound_strips_leading_agent_mention_before_command(monkeypatch):
    """Telegram strips its own bot trigger before dispatch so `@bot /cmd`
    still reaches Hermes as a slash command. aX should do the same; Hermes
    command detection is text.startswith('/'), so leaving the leading mention
    would route control commands through the normal busy/interrupt path."""
    from types import SimpleNamespace
    adapter = _adapter()
    adapter.platform = SimpleNamespace(value="ax")
    captured: list = []

    async def fake_handle_message(event):
        captured.append(event)

    monkeypatch.setattr(adapter, "handle_message", fake_handle_message)

    asyncio.run(
        adapter._dispatch_inbound(
            {
                "id": "msg-cmd",
                "content": "@nova: /busy status",
                "sender": "alice",
                "sender_id": "u-1",
                "parent_id": None,
            }
        )
    )

    assert len(captured) == 1
    assert captured[0].text == "/busy status"
    assert captured[0].is_command()


def test_ax_adapter_requires_agent_pat_for_runtime_identity():
    """The runtime adapter should fail closed to an agent PAT. Letting a
    user PAT exchange to user_access would make Hermes runtime actions appear
    to come from the bootstrap user instead of the bound aX agent identity."""
    config = _MODULE.PlatformConfig(
        token="axp_u_not_for_runtime",
        extra={
            "space_id": "space-123",
            "agent_name": "nova",
            "agent_id": "agent-123",
        },
    )

    with pytest.raises(ValueError, match="agent PAT"):
        AxAdapter(config)


def test_ax_adapter_does_not_advertise_chat_edit_streaming():
    """aX progress belongs on the processing-status activity stream, not a
    mutable chat bubble. If SUPPORTS_MESSAGE_EDITING is false, the adapter
    should inherit the base edit_message stub rather than advertising a
    half-supported chat edit path."""
    assert AxAdapter.edit_message is _MODULE.BasePlatformAdapter.edit_message


def test_seen_message_lru_evicts_oldest(monkeypatch):
    """The dedup LRU has a fixed cap; oldest entries get evicted so a long-
    lived adapter does not leak memory. After the bound is exceeded, the
    earliest message_id should no longer be considered seen."""
    monkeypatch.setattr(_MODULE, "SEEN_MESSAGE_LRU_MAX", 4)
    adapter = _adapter()
    for n in range(5):
        adapter._seen_or_record(f"msg-{n}")
    assert "msg-0" not in adapter._seen_message_ids
    assert "msg-4" in adapter._seen_message_ids
    assert len(adapter._seen_message_ids) == 4


def test_stop_typing_marks_processing_status_completed(monkeypatch):
    adapter = _adapter()
    calls = []

    async def fake_post(message_id, status, *, activity=None):
        calls.append({"message_id": message_id, "status": status, "activity": activity})

    monkeypatch.setattr(adapter, "_post_processing_status", fake_post)

    asyncio.run(adapter.stop_typing("msg-1"))

    assert calls == [{"message_id": "msg-1", "status": "completed", "activity": None}]


def test_send_omits_parent_id_for_space_level_home_channel(monkeypatch):
    adapter = _adapter()
    posts = []

    async def fake_get_jwt():
        return "jwt-1"

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, **kwargs):
            posts.append({"url": url, **kwargs})
            return type(
                "Response",
                (),
                {
                    "status_code": 201,
                    "headers": {"content-type": "application/json"},
                    "text": "",
                    "json": lambda self: {"id": "msg-out"},
                },
            )()

    monkeypatch.setattr(adapter, "_get_jwt", fake_get_jwt)
    monkeypatch.setattr(_MODULE.httpx, "AsyncClient", FakeAsyncClient)

    result = asyncio.run(adapter.send(adapter.space_id, "proactive note"))

    assert result.success is True
    assert posts[0]["json"] == {"content": "proactive note", "space_id": "space-123"}


def test_send_keeps_parent_id_for_thread_or_reply_anchor(monkeypatch):
    adapter = _adapter()
    posts = []

    async def fake_get_jwt():
        return "jwt-1"

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, **kwargs):
            posts.append({"url": url, **kwargs})
            return type(
                "Response",
                (),
                {
                    "status_code": 201,
                    "headers": {"content-type": "application/json"},
                    "text": "",
                    "json": lambda self: {"id": "msg-out"},
                },
            )()

    monkeypatch.setattr(adapter, "_get_jwt", fake_get_jwt)
    monkeypatch.setattr(_MODULE.httpx, "AsyncClient", FakeAsyncClient)

    result = asyncio.run(adapter.send("msg-root", "thread note"))
    reply_result = asyncio.run(adapter.send(adapter.space_id, "reply note", reply_to="msg-parent"))

    assert result.success is True
    assert reply_result.success is True
    assert posts[0]["json"]["parent_id"] == "msg-root"
    assert posts[1]["json"]["parent_id"] == "msg-parent"


def test_announce_local_gateway_posts_external_runtime_state(monkeypatch):
    adapter = _adapter()
    posts = []

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, **kwargs):
            posts.append({"url": url, **kwargs})
            return type("Response", (), {"status_code": 200})()

    monkeypatch.setattr(_MODULE.httpx, "AsyncClient", FakeAsyncClient)

    asyncio.run(
        adapter._announce_local_gateway(
            "thinking",
            activity="Using search_docs",
            message_id="msg-1",
            current_tool="search_docs",
        )
    )

    assert posts[0]["url"] == "http://127.0.0.1:8765/api/agents/nova/external-runtime-announce"
    assert posts[0]["json"]["runtime_kind"] == "hermes_plugin"
    assert posts[0]["json"]["status"] == "thinking"
    assert posts[0]["json"]["agent_id"] == "agent-123"
    assert posts[0]["json"]["activity"] == "Using search_docs"
    assert posts[0]["json"]["message_id"] == "msg-1"
    assert posts[0]["json"]["current_tool"] == "search_docs"
