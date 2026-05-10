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


def test_stop_typing_marks_processing_status_completed(monkeypatch):
    adapter = _adapter()
    calls = []

    async def fake_post(message_id, status, *, activity=None):
        calls.append({"message_id": message_id, "status": status, "activity": activity})

    monkeypatch.setattr(adapter, "_post_processing_status", fake_post)

    asyncio.run(adapter.stop_typing("msg-1"))

    assert calls == [{"message_id": "msg-1", "status": "completed", "activity": None}]
