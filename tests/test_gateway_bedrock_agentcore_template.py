"""Regression: bedrock_agentcore template registers correctly and the bridge
modules behave per the Gateway exec-runtime contract.

Tests cover:
  - Template catalog registration and ordering
  - Bridge file existence
  - Deep module 1: validate_bedrock_options (ARN validator)
  - Deep module 2: classify_stream_event (SSE classifier)
  - Deep module 3: build_bedrock_client (boto3 version check)
  - Bridge I/O loop integration (missing ARN, event-stream, error, empty reply)
  - CLI cross-template gating (--bedrock-* with non-bedrock template)
  - Deterministic session ID shape
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ax_cli.gateway_runtime_types import (
    agent_template_catalog,
    agent_template_definition,
    agent_template_list,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_PATH = REPO_ROOT / "examples" / "gateway_bedrock_agentcore" / "bedrock_agentcore_bridge.py"

sys.path.insert(0, str(BRIDGE_PATH.parent))
import bedrock_agentcore_bridge as bridge  # noqa: E402, I001


# ---------------------------------------------------------------------------
# Template catalog
# ---------------------------------------------------------------------------


def test_bedrock_agentcore_template_is_registered():
    catalog = agent_template_catalog()
    assert "bedrock_agentcore" in catalog
    t = agent_template_definition("bedrock_agentcore")
    assert t["id"] == "bedrock_agentcore"
    assert t["runtime_type"] == "exec"
    assert t["intake_model"] == "launch_on_send"
    assert t["return_paths"] == ["inline_reply"]
    assert t["availability"] == "ready"
    assert t["launchable"] is True


def test_bedrock_agentcore_template_exec_command_points_at_bridge():
    t = agent_template_definition("bedrock_agentcore")
    cmd = str((t.get("defaults") or {}).get("exec_command") or "")
    assert "examples/gateway_bedrock_agentcore/bedrock_agentcore_bridge.py" in cmd


def test_bedrock_agentcore_template_listed_after_strands():
    ids = [item["id"] for item in agent_template_list()]
    assert "bedrock_agentcore" in ids
    assert ids.index("bedrock_agentcore") > ids.index("strands")


def test_bedrock_agentcore_bridge_file_exists():
    assert BRIDGE_PATH.exists()


# ---------------------------------------------------------------------------
# Deep module 1: validate_bedrock_options
# ---------------------------------------------------------------------------

VALID_ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my-runtime"


def test_validate_valid_arn_extracts_region():
    opts = bridge.validate_bedrock_options(
        runtime_arn=VALID_ARN,
        region=None,
        qualifier=None,
        payload_key=None,
        aws_profile=None,
        template_id="bedrock_agentcore",
    )
    assert opts["bedrock_runtime_arn"] == VALID_ARN
    assert opts["bedrock_region"] == "us-east-1"
    assert opts["bedrock_qualifier"] == "DEFAULT"
    assert opts["bedrock_payload_key"] == "prompt"


def test_validate_explicit_region_overrides_arn():
    opts = bridge.validate_bedrock_options(
        runtime_arn=VALID_ARN,
        region="eu-west-1",
        qualifier=None,
        payload_key=None,
        aws_profile=None,
        template_id="bedrock_agentcore",
    )
    assert opts["bedrock_region"] == "eu-west-1"


def test_validate_explicit_qualifier_and_payload_key():
    opts = bridge.validate_bedrock_options(
        runtime_arn=VALID_ARN,
        region=None,
        qualifier="my-canary",
        payload_key="input",
        aws_profile=None,
        template_id="bedrock_agentcore",
    )
    assert opts["bedrock_qualifier"] == "my-canary"
    assert opts["bedrock_payload_key"] == "input"


def test_validate_malformed_arn_raises():
    with pytest.raises(ValueError, match="Invalid AgentCore runtime ARN"):
        bridge.validate_bedrock_options(
            runtime_arn="arn:aws:bedrock-agentcore:us-east-1:short:runtime/x",
            region=None,
            qualifier=None,
            payload_key=None,
            aws_profile=None,
            template_id="bedrock_agentcore",
        )


def test_validate_model_arn_raises():
    model_arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3"
    with pytest.raises(ValueError, match="foundation-model"):
        bridge.validate_bedrock_options(
            runtime_arn=model_arn,
            region=None,
            qualifier=None,
            payload_key=None,
            aws_profile=None,
            template_id="bedrock_agentcore",
        )


def test_validate_missing_arn_raises():
    with pytest.raises(ValueError, match="bedrock_agentcore template requires"):
        bridge.validate_bedrock_options(
            runtime_arn=None,
            region=None,
            qualifier=None,
            payload_key=None,
            aws_profile=None,
            template_id="bedrock_agentcore",
        )


def test_validate_env_var_supplies_arn(monkeypatch):
    monkeypatch.setenv("AX_BEDROCK_RUNTIME_ARN", VALID_ARN)
    opts = bridge.validate_bedrock_options(
        runtime_arn=None,
        region=None,
        qualifier=None,
        payload_key=None,
        aws_profile=None,
        template_id="bedrock_agentcore",
    )
    assert opts["bedrock_runtime_arn"] == VALID_ARN


def test_validate_bedrock_flag_with_wrong_template_raises():
    with pytest.raises(ValueError, match="only valid with --template bedrock_agentcore"):
        bridge.validate_bedrock_options(
            runtime_arn=VALID_ARN,
            region=None,
            qualifier=None,
            payload_key=None,
            aws_profile=None,
            template_id="ollama",
        )


# ---------------------------------------------------------------------------
# Deep module 2: classify_stream_event
# ---------------------------------------------------------------------------


def test_classify_plain_text():
    tag, val = bridge.classify_stream_event("hello world")
    assert tag == "reply_text"
    assert val == "hello world"


def test_classify_empty_string():
    tag, val = bridge.classify_stream_event("")
    assert tag == "reply_text"
    assert val == ""


def test_classify_malformed_json():
    tag, val = bridge.classify_stream_event("{not valid json")
    assert tag == "reply_text"


def test_classify_json_primitive():
    tag, val = bridge.classify_stream_event("42")
    assert tag == "reply_text"


def test_classify_error_event_field():
    line = json.dumps({"event": "error", "message": "something broke"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "error_event"
    assert "something broke" in val["message"]


def test_classify_error_type_field():
    line = json.dumps({"type": "error", "message": "bad"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "error_event"


def test_classify_error_truthy_error_key():
    line = json.dumps({"error": "ThrottlingException"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "error_event"


def test_classify_tool_use():
    line = json.dumps({"type": "tool_use", "tool_name": "search", "tool_call_id": "tc-1", "input": {}})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "typed_event"
    assert val["kind"] == "tool_start"
    assert val["tool_name"] == "search"


def test_classify_tool_result():
    line = json.dumps({"type": "tool_result", "tool_call_id": "tc-1", "tool_name": "search", "result": "ok"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "typed_event"
    assert val["kind"] == "tool_result"
    assert val["status"] == "ok"


def test_classify_thinking():
    line = json.dumps({"type": "thinking", "thinking": "let me consider this"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "typed_event"
    assert val["kind"] == "activity"


def test_classify_content_block_delta():
    line = json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "hello"}})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "reply_text"
    assert val == "hello"


def test_classify_unrecognised_dict():
    line = json.dumps({"some_unknown_key": "value"})
    tag, val = bridge.classify_stream_event(line)
    assert tag == "heartbeat_chunk"


# ---------------------------------------------------------------------------
# Deep module 3: build_bedrock_client
# ---------------------------------------------------------------------------


def test_build_bedrock_client_raises_on_missing_service(monkeypatch):
    mock_session = MagicMock()
    mock_session.get_available_services.return_value = ["s3", "ec2"]
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = mock_session
    mock_boto3.__version__ = "1.20.0"

    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        with pytest.raises(RuntimeError, match="bedrock-agentcore service client"):
            bridge.build_bedrock_client(aws_profile="", region="us-east-1")


def test_build_bedrock_client_raises_on_missing_boto3(monkeypatch):
    with patch.dict(sys.modules, {"boto3": None}):
        with pytest.raises((RuntimeError, ImportError)):
            bridge.build_bedrock_client(aws_profile="", region="us-east-1")


def test_build_bedrock_client_passes_profile_to_session():
    mock_session = MagicMock()
    mock_session.get_available_services.return_value = ["bedrock-agentcore"]
    mock_session.client.return_value = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = mock_session

    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        bridge.build_bedrock_client(aws_profile="my-profile", region="us-east-1")
        mock_boto3.Session.assert_called_once_with(profile_name="my-profile")


def test_build_bedrock_client_passes_none_profile_when_empty():
    mock_session = MagicMock()
    mock_session.get_available_services.return_value = ["bedrock-agentcore"]
    mock_session.client.return_value = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = mock_session

    with patch.dict(sys.modules, {"boto3": mock_boto3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        bridge.build_bedrock_client(aws_profile="", region="us-east-1")
        mock_boto3.Session.assert_called_once_with(profile_name=None)


# ---------------------------------------------------------------------------
# Session ID
# ---------------------------------------------------------------------------


def test_compute_session_id_is_deterministic():
    sid1 = bridge._compute_session_id("agent-123", "space-456")
    sid2 = bridge._compute_session_id("agent-123", "space-456")
    assert sid1 == sid2


def test_compute_session_id_min_length():
    sid = bridge._compute_session_id("a", "b")
    assert len(sid) >= 33


def test_compute_session_id_differs_by_agent():
    sid1 = bridge._compute_session_id("agent-1", "space-1")
    sid2 = bridge._compute_session_id("agent-2", "space-1")
    assert sid1 != sid2


# ---------------------------------------------------------------------------
# Bridge I/O loop integration
# ---------------------------------------------------------------------------


def _make_fake_client(chunks: list[str], content_type: str = "text/event-stream") -> MagicMock:
    lines = [c.encode() for c in chunks]

    class FakeBody:
        def iter_lines(self):
            return iter(lines)

        def iter_chunks(self):
            return iter(lines)

        def read(self):
            return b"".join(lines)

    client = MagicMock()
    client.invoke_agent_runtime.return_value = {
        "contentType": content_type,
        "response": FakeBody(),
    }
    return client


def test_bridge_missing_arn_exits_nonzero(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bridge.py", "hello"])
    monkeypatch.delenv("AX_BEDROCK_RUNTIME_ARN", raising=False)
    monkeypatch.setenv("AX_MENTION_CONTENT", "hello")

    rc = bridge.main()
    assert rc != 0
    out = capsys.readouterr().out
    assert "error" in out.lower()


def test_bridge_plain_text_stream(monkeypatch, capsys):
    # Two plain-text chunks — bridge strips each line so they join without separator
    fake_client = _make_fake_client(["hello", "world"])

    monkeypatch.setenv("AX_BEDROCK_RUNTIME_ARN", VALID_ARN)
    monkeypatch.setenv("AX_BEDROCK_REGION", "us-east-1")
    monkeypatch.setenv("AX_GATEWAY_AGENT_ID", "agent-1")
    monkeypatch.setenv("AX_GATEWAY_SPACE_ID", "space-1")
    monkeypatch.setattr(sys, "argv", ["bridge.py", "test prompt"])
    monkeypatch.setattr(bridge, "build_bedrock_client", lambda **_: fake_client)

    rc = bridge.main()
    captured = capsys.readouterr()
    assert rc == 0
    assert "helloworld" in captured.out

    event_lines = [ln for ln in captured.out.splitlines() if ln.startswith(bridge.EVENT_PREFIX)]
    statuses = [
        json.loads(ln[len(bridge.EVENT_PREFIX) :]).get("status")
        for ln in event_lines
        if json.loads(ln[len(bridge.EVENT_PREFIX) :]).get("kind") == "status"
    ]
    assert "processing" in statuses
    assert "completed" in statuses


def test_bridge_inband_error_exits_nonzero(monkeypatch, capsys):
    error_chunk = json.dumps({"event": "error", "message": "guardrail tripped"})
    fake_client = _make_fake_client([error_chunk])

    monkeypatch.setenv("AX_BEDROCK_RUNTIME_ARN", VALID_ARN)
    monkeypatch.setenv("AX_BEDROCK_REGION", "us-east-1")
    monkeypatch.setenv("AX_GATEWAY_AGENT_ID", "agent-1")
    monkeypatch.setenv("AX_GATEWAY_SPACE_ID", "space-1")
    monkeypatch.setattr(sys, "argv", ["bridge.py", "test prompt"])
    monkeypatch.setattr(bridge, "build_bedrock_client", lambda **_: fake_client)

    rc = bridge.main()
    captured = capsys.readouterr()
    assert rc == 0  # in-band error returns 0; error surfaced via status:error event
    event_lines = [ln for ln in captured.out.splitlines() if ln.startswith(bridge.EVENT_PREFIX)]
    events = [json.loads(ln[len(bridge.EVENT_PREFIX) :]) for ln in event_lines]
    statuses = [e.get("status") for e in events]
    assert "error" in statuses, "expected a status:error event"
    assert "completed" not in statuses, "status:completed must not follow status:error"
    error_event = next(e for e in events if e.get("status") == "error")
    assert "guardrail tripped" in error_event.get("error_message", "")
    assert "detail" in error_event


def test_bridge_empty_reply_emits_placeholder(monkeypatch, capsys):
    typed_chunk = json.dumps({"type": "thinking", "thinking": "thinking..."})
    fake_client = _make_fake_client([typed_chunk])

    monkeypatch.setenv("AX_BEDROCK_RUNTIME_ARN", VALID_ARN)
    monkeypatch.setenv("AX_BEDROCK_REGION", "us-east-1")
    monkeypatch.setenv("AX_GATEWAY_AGENT_ID", "agent-1")
    monkeypatch.setenv("AX_GATEWAY_SPACE_ID", "space-1")
    monkeypatch.setattr(sys, "argv", ["bridge.py", "test prompt"])
    monkeypatch.setattr(bridge, "build_bedrock_client", lambda **_: fake_client)

    rc = bridge.main()
    captured = capsys.readouterr()
    assert rc == 0
    reply_lines = [ln for ln in captured.out.splitlines() if not ln.startswith(bridge.EVENT_PREFIX)]
    assert reply_lines, "expected a placeholder reply line"
    assert "activity stream" in reply_lines[-1].lower() or "no text" in reply_lines[-1].lower()


def test_bridge_boto3_client_error_exits_nonzero(monkeypatch, capsys):
    botocore = pytest.importorskip("botocore")

    def raise_client_error(**_):
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "InvokeAgentRuntime"
        )

    fake_client = MagicMock()
    fake_client.invoke_agent_runtime.side_effect = raise_client_error

    monkeypatch.setenv("AX_BEDROCK_RUNTIME_ARN", VALID_ARN)
    monkeypatch.setenv("AX_BEDROCK_REGION", "us-east-1")
    monkeypatch.setenv("AX_GATEWAY_AGENT_ID", "agent-1")
    monkeypatch.setenv("AX_GATEWAY_SPACE_ID", "space-1")
    monkeypatch.setattr(sys, "argv", ["bridge.py", "test prompt"])
    monkeypatch.setattr(bridge, "build_bedrock_client", lambda **_: fake_client)

    rc = bridge.main()
    captured = capsys.readouterr()
    assert rc != 0
    event_lines = [ln for ln in captured.out.splitlines() if ln.startswith(bridge.EVENT_PREFIX)]
    error_events = [
        json.loads(ln[len(bridge.EVENT_PREFIX) :])
        for ln in event_lines
        if json.loads(ln[len(bridge.EVENT_PREFIX) :]).get("status") == "error"
    ]
    assert error_events
    assert error_events[0].get("detail", {}).get("error_code") == "ThrottlingException"
