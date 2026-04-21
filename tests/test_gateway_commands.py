import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from typer.testing import CliRunner

from ax_cli import gateway as gateway_core
from ax_cli.commands import gateway as gateway_cmd
from ax_cli.main import app

runner = CliRunner()


class _FakeTokenExchanger:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token

    def get_token(self, *args, **kwargs):
        return "jwt-test"


class _FakeLoginClient:
    def __init__(self, *args, **kwargs):
        self.base_url = kwargs["base_url"]
        self.token = kwargs["token"]

    def whoami(self):
        return {"username": "madtank", "email": "madtank@example.com"}

    def list_spaces(self):
        return {"spaces": [{"id": "space-1", "name": "Workspace", "is_default": True}]}


class _FakeUserClient:
    def update_agent(self, *args, **kwargs):
        return {"ok": True}


class _FakeSseResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        self.closed = True

    def iter_lines(self):
        yield "event: connected"
        yield "data: {}"
        yield ""
        yield "event: message"
        yield f"data: {json.dumps(self.payload)}"
        yield ""


class _SharedRuntimeClient:
    def __init__(self, payload):
        self.payload = payload
        self.sent = []
        self.processing = []
        self.tool_calls = []
        self.connect_calls = 0

    def connect_sse(self, *, space_id, timeout=None):
        self.connect_calls += 1
        if self.connect_calls > 1:
            raise ConnectionError("test done")
        return _FakeSseResponse(self.payload)

    def send_message(self, space_id, content, *, agent_id=None, parent_id=None, **kwargs):
        self.sent.append(
            {
                "space_id": space_id,
                "content": content,
                "agent_id": agent_id,
                "parent_id": parent_id,
                "metadata": kwargs.get("metadata"),
            }
        )
        return {"message": {"id": "reply-1"}}

    def set_agent_processing_status(self, message_id, status, *, agent_name=None, space_id=None, **kwargs):
        payload = {
            "message_id": message_id,
            "status": status,
            "agent_name": agent_name,
            "space_id": space_id,
        }
        payload.update(kwargs)
        self.processing.append(payload)
        return {"ok": True}

    def record_tool_call(self, **payload):
        self.tool_calls.append(payload)
        return {"ok": True, "tool_call_id": payload["tool_call_id"]}

    def close(self):
        return None


class _FakeManagedSendClient:
    def __init__(self, *args, **kwargs):
        self.base_url = kwargs["base_url"]
        self.token = kwargs["token"]
        self.agent_name = kwargs.get("agent_name")
        self.agent_id = kwargs.get("agent_id")

    def send_message(self, space_id, content, *, agent_id=None, parent_id=None, metadata=None):
        return {
            "message": {
                "id": "msg-sent-1",
                "space_id": space_id,
                "content": content,
                "agent_id": agent_id,
                "parent_id": parent_id,
                "metadata": metadata,
            }
        }


def test_gateway_login_saves_gateway_session(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    monkeypatch.setattr("ax_cli.token_cache.TokenExchanger", _FakeTokenExchanger)
    monkeypatch.setattr(gateway_cmd, "AxClient", _FakeLoginClient)

    result = runner.invoke(
        app,
        ["gateway", "login", "--token", "axp_u_test.token", "--url", "https://paxai.app", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["base_url"] == "https://paxai.app"
    assert payload["space_id"] == "space-1"
    session = gateway_core.load_gateway_session()
    assert session["token"] == "axp_u_test.token"
    assert session["base_url"] == "https://paxai.app"
    assert not (config_dir / "user.toml").exists()
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] == "gateway_login"
    assert recent[-1]["username"] == "madtank"


def test_gateway_run_refuses_second_live_daemon(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "madtank",
        }
    )
    gateway_core.write_gateway_pid(4242)
    monkeypatch.setattr(gateway_core, "_pid_alive", lambda pid: pid == 4242)

    result = runner.invoke(app, ["gateway", "run", "--once"])

    assert result.exit_code == 1, result.output
    assert "Gateway already running (pid 4242)." in result.output
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] == "gateway_start_blocked"
    assert recent[-1]["existing_pid"] == 4242


def test_gateway_run_refuses_process_table_daemon_when_pid_file_missing(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "madtank",
        }
    )
    monkeypatch.setattr(gateway_core, "_scan_gateway_process_pids", lambda: [5514])

    result = runner.invoke(app, ["gateway", "run", "--once"])

    assert result.exit_code == 1, result.output
    assert "Gateway already running (pid 5514)." in result.output
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] == "gateway_start_blocked"
    assert recent[-1]["existing_pids"] == [5514]


def test_clear_gateway_pid_keeps_newer_owner(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.write_gateway_pid(22179)

    gateway_core.clear_gateway_pid(5514)

    assert gateway_core.pid_path().exists()
    assert gateway_core.pid_path().read_text().strip() == "22179"


def test_scan_gateway_process_pids_ignores_current_parent_wrapper(monkeypatch):
    monkeypatch.setattr(gateway_core.os, "getpid", lambda: 22179)
    monkeypatch.setattr(gateway_core.os, "getppid", lambda: 22178)
    monkeypatch.setattr(gateway_core, "_pid_alive", lambda pid: True)
    monkeypatch.setattr(
        gateway_core.subprocess,
        "check_output",
        lambda *args, **kwargs: "\n".join(
            [
                "22178 uv run ax gateway run",
                "22179 /Users/jacob/claude_home/ax-cli/.venv/bin/python3 /Users/jacob/claude_home/ax-cli/.venv/bin/ax gateway run",
                "5514 /Users/jacob/claude_home/ax-cli/.venv/bin/python3 /Users/jacob/claude_home/ax-cli/.venv/bin/ax gateway run",
            ]
        ),
    )

    assert gateway_core._scan_gateway_process_pids() == [5514]


def test_gateway_agents_add_mints_token_and_writes_registry(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "madtank",
        }
    )
    monkeypatch.setattr(gateway_cmd, "_load_gateway_user_client", lambda: _FakeUserClient())
    monkeypatch.setattr(gateway_cmd, "_find_agent_in_space", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gateway_cmd,
        "_create_agent_in_space",
        lambda *args, **kwargs: {"id": "agent-1", "name": "echo-bot"},
    )
    monkeypatch.setattr(gateway_cmd, "_polish_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway_cmd, "_mint_agent_pat", lambda *args, **kwargs: ("axp_a_agent.secret", "mgmt"))

    result = runner.invoke(app, ["gateway", "agents", "add", "echo-bot", "--type", "echo", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["name"] == "echo-bot"
    assert payload["runtime_type"] == "echo"
    assert payload["desired_state"] == "running"
    assert payload["credential_source"] == "gateway"
    assert payload["transport"] == "gateway"
    registry = gateway_core.load_gateway_registry()
    assert registry["agents"][0]["name"] == "echo-bot"
    token_file = Path(registry["agents"][0]["token_file"])
    assert token_file.exists()
    assert token_file.read_text().strip() == "axp_a_agent.secret"
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] == "managed_agent_added"
    assert recent[-1]["agent_name"] == "echo-bot"


def test_sanitize_exec_env_strips_ax_credentials(monkeypatch):
    monkeypatch.setenv("AX_TOKEN", "secret-token")
    monkeypatch.setenv("AX_USER_TOKEN", "secret-user")
    monkeypatch.setenv("AX_BASE_URL", "https://paxai.app")
    monkeypatch.setenv("AX_AGENT_NAME", "orion")
    monkeypatch.setenv("OPENAI_API_KEY", "keep-me")

    env = gateway_core.sanitize_exec_env("hello", {"name": "echo-bot", "agent_id": "agent-1", "runtime_type": "exec"})

    assert "AX_TOKEN" not in env
    assert "AX_USER_TOKEN" not in env
    assert "AX_BASE_URL" not in env
    assert "AX_AGENT_NAME" not in env
    assert env["AX_MENTION_CONTENT"] == "hello"
    assert env["AX_GATEWAY_AGENT_NAME"] == "echo-bot"
    assert env["OPENAI_API_KEY"] == "keep-me"


def test_managed_echo_runtime_processes_message(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    token_file = tmp_path / "token"
    token_file.write_text("axp_a_agent.secret")
    payload = {
        "id": "msg-1",
        "content": "@echo-bot ping",
        "author": {"id": "user-1", "name": "madtank", "type": "user"},
        "mentions": ["echo-bot"],
    }
    shared = _SharedRuntimeClient(payload)

    runtime = gateway_core.ManagedAgentRuntime(
        {
            "name": "echo-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "base_url": "https://paxai.app",
            "runtime_type": "echo",
            "token_file": str(token_file),
        },
        client_factory=lambda **kwargs: shared,
    )

    runtime.start()
    deadline = time.time() + 2.0
    while time.time() < deadline and not shared.sent:
        time.sleep(0.05)
    runtime.stop()

    assert shared.sent, "echo runtime should have replied"
    assert shared.sent[0]["content"] == "Echo: ping"
    assert shared.sent[0]["parent_id"] == "msg-1"
    assert shared.sent[0]["agent_id"] == "agent-1"
    assert shared.sent[0]["metadata"]["control_plane"] == "gateway"
    assert shared.sent[0]["metadata"]["gateway"]["managed"] is True
    assert shared.sent[0]["metadata"]["gateway"]["agent_name"] == "echo-bot"
    assert [row["status"] for row in shared.processing] == ["started", "processing", "completed"]
    assert shared.processing[0]["activity"] == "Picked up by Gateway"
    assert shared.processing[0]["detail"] == {"backlog_depth": 1, "pickup_state": "claimed"}
    assert shared.processing[1]["activity"] == "Composing echo reply"
    recent = gateway_core.load_recent_gateway_activity()
    event_names = [row["event"] for row in recent]
    assert "message_received" in event_names
    assert "message_claimed" in event_names
    assert "reply_sent" in event_names


def test_managed_exec_runtime_parses_gateway_progress_events(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    token_file = tmp_path / "token"
    token_file.write_text("axp_a_agent.secret")
    script = tmp_path / "bridge.py"
    script.write_text(
        """
import json
import sys

prefix = "AX_GATEWAY_EVENT "
print(prefix + json.dumps({"kind": "status", "status": "working", "message": "warming up"}), flush=True)
print(prefix + json.dumps({"kind": "status", "status": "working", "message": "warming up", "progress": {"current": 1, "total": 3, "unit": "steps"}}), flush=True)
print(prefix + json.dumps({"kind": "tool_start", "tool_name": "sleep", "tool_call_id": "tool-1", "arguments": {"seconds": 1}}), flush=True)
print(prefix + json.dumps({"kind": "tool_result", "tool_name": "sleep", "tool_call_id": "tool-1", "arguments": {"seconds": 1}, "initial_data": {"slept_seconds": 1}, "status": "success"}), flush=True)
print("done", flush=True)
""".strip()
    )
    payload = {
        "id": "msg-1",
        "content": "@exec-bot pause 1s",
        "author": {"id": "user-1", "name": "madtank", "type": "user"},
        "mentions": ["exec-bot"],
    }
    shared = _SharedRuntimeClient(payload)

    runtime = gateway_core.ManagedAgentRuntime(
        {
            "name": "exec-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "base_url": "https://paxai.app",
            "runtime_type": "exec",
            "exec_command": f"{sys.executable} {script}",
            "token_file": str(token_file),
        },
        client_factory=lambda **kwargs: shared,
    )

    runtime.start()
    deadline = time.time() + 3.0
    while time.time() < deadline and not shared.sent:
        time.sleep(0.05)
    snapshot = runtime.snapshot()
    runtime.stop()

    assert shared.sent, "exec runtime should have replied"
    assert shared.sent[0]["content"] == "done"
    assert [row["status"] for row in shared.processing] == [
        "started",
        "processing",
        "working",
        "working",
        "tool_call",
        "tool_complete",
        "completed",
    ]
    assert shared.processing[0]["activity"] == "Picked up by Gateway"
    assert shared.processing[0]["detail"] == {"backlog_depth": 1, "pickup_state": "claimed"}
    assert shared.processing[1]["activity"] == "Preparing runtime"
    assert shared.processing[2]["activity"] == "warming up"
    assert shared.processing[3]["activity"] == "warming up"
    assert shared.processing[3]["progress"] == {"current": 1, "total": 3, "unit": "steps"}
    assert shared.processing[4]["tool_name"] == "sleep"
    assert shared.processing[4]["activity"] == "Using sleep"
    assert shared.processing[5]["tool_name"] == "sleep"
    assert shared.processing[5]["detail"] == {"slept_seconds": 1}
    assert shared.tool_calls
    assert shared.tool_calls[0]["tool_name"] == "sleep"
    assert shared.tool_calls[0]["message_id"] == "msg-1"
    assert snapshot["current_activity"] in {None, "warming up"}
    recent = gateway_core.load_recent_gateway_activity()
    events = [row["event"] for row in recent]
    assert "message_claimed" in events
    assert "tool_started" in events
    assert "tool_finished" in events


def test_managed_inbox_runtime_queues_message_without_reply(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    token_file = tmp_path / "token"
    token_file.write_text("axp_a_agent.secret")
    payload = {
        "id": "msg-1",
        "content": "@inbox-bot hello there",
        "author": {"id": "user-1", "name": "madtank", "type": "user"},
        "mentions": ["inbox-bot"],
    }
    shared = _SharedRuntimeClient(payload)

    runtime = gateway_core.ManagedAgentRuntime(
        {
            "name": "inbox-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "base_url": "https://paxai.app",
            "runtime_type": "inbox",
            "token_file": str(token_file),
        },
        client_factory=lambda **kwargs: shared,
    )

    runtime.start()
    deadline = time.time() + 2.0
    snapshot = runtime.snapshot()
    while time.time() < deadline and snapshot["backlog_depth"] < 1:
        time.sleep(0.05)
        snapshot = runtime.snapshot()
    runtime.stop()

    assert not shared.sent
    assert snapshot["backlog_depth"] >= 1
    assert [row["status"] for row in shared.processing] == ["queued"]
    assert shared.processing[0]["activity"] == "Queued in Gateway"
    assert shared.processing[0]["detail"] == {"backlog_depth": 1, "pickup_state": "queued"}
    recent = gateway_core.load_recent_gateway_activity()
    events = [row["event"] for row in recent]
    assert "message_received" in events
    assert "message_queued" in events


def test_annotate_runtime_health_marks_stale_after_missed_heartbeat():
    old_seen = (datetime.now(timezone.utc) - timedelta(seconds=gateway_core.RUNTIME_STALE_AFTER_SECONDS + 5)).isoformat()

    snapshot = gateway_core.annotate_runtime_health(
        {
            "effective_state": "running",
            "last_seen_at": old_seen,
        }
    )

    assert snapshot["effective_state"] == "stale"
    assert snapshot["connected"] is False
    assert snapshot["last_seen_age_seconds"] >= gateway_core.RUNTIME_STALE_AFTER_SECONDS


def test_listener_timeout_enters_reconnecting_state(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    token_file = tmp_path / "token"
    token_file.write_text("axp_a_agent.secret")

    class _TimeoutRuntimeClient:
        def __init__(self):
            self.timeout = None

        def connect_sse(self, *, space_id, timeout=None):
            self.timeout = timeout
            raise httpx.ReadTimeout("boom", request=httpx.Request("GET", "https://paxai.app/api/v1/sse/messages"))

        def close(self):
            return None

    shared = _TimeoutRuntimeClient()
    runtime = gateway_core.ManagedAgentRuntime(
        {
            "name": "echo-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "base_url": "https://paxai.app",
            "runtime_type": "echo",
            "token_file": str(token_file),
        },
        client_factory=lambda **kwargs: shared,
    )

    runtime.start()
    deadline = time.time() + 1.0
    snapshot = runtime.snapshot()
    while time.time() < deadline and snapshot["effective_state"] != "reconnecting":
        time.sleep(0.05)
        snapshot = runtime.snapshot()
    runtime.stop()

    assert shared.timeout is not None
    assert shared.timeout.read == gateway_core.SSE_IDLE_TIMEOUT_SECONDS
    assert snapshot["effective_state"] == "reconnecting"
    assert snapshot["last_error"] == "idle timeout after 45s without SSE heartbeat"
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] in {"runtime_stopped", "listener_timeout"}
    assert any(row["event"] == "listener_timeout" for row in recent)


def test_gateway_watch_once_renders_dashboard(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "codex",
        }
    )
    registry = gateway_core.load_gateway_registry()
    registry["gateway"].update(
        {
            "gateway_id": "gw-12345678",
            "desired_state": "running",
            "effective_state": "running",
            "last_reconcile_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    registry["agents"] = [
        {
            "name": "echo-bot",
            "runtime_type": "echo",
            "desired_state": "running",
            "effective_state": "running",
            "backlog_depth": 2,
            "processed_count": 7,
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
            "last_reply_preview": "Echo: ping",
        }
    ]
    gateway_core.save_gateway_registry(registry)
    gateway_core.record_gateway_activity("message_received", entry=registry["agents"][0], message_id="msg-1")

    result = runner.invoke(app, ["gateway", "watch", "--once"])

    assert result.exit_code == 0, result.output
    assert "Gateway Overview" in result.output
    assert "Managed Agents" in result.output
    assert "@echo-bot" in result.output
    assert "Recent Activity" in result.output


def test_gateway_agents_show_json_filters_activity(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "codex",
        }
    )
    registry = gateway_core.load_gateway_registry()
    registry["agents"] = [
        {
            "name": "echo-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "runtime_type": "echo",
            "desired_state": "running",
            "effective_state": "running",
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
            "last_reply_preview": "Echo: ping",
            "token_file": "/tmp/echo-token",
        },
        {
            "name": "other-bot",
            "agent_id": "agent-2",
            "space_id": "space-1",
            "runtime_type": "exec",
            "desired_state": "running",
            "effective_state": "running",
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
            "token_file": "/tmp/other-token",
        },
    ]
    gateway_core.save_gateway_registry(registry)
    gateway_core.record_gateway_activity("reply_sent", entry=registry["agents"][0], reply_preview="Echo: ping")
    gateway_core.record_gateway_activity("reply_sent", entry=registry["agents"][1], reply_preview="Other reply")

    result = runner.invoke(app, ["gateway", "agents", "show", "echo-bot", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["agent"]["name"] == "echo-bot"
    assert payload["recent_activity"]
    assert all(row["agent_name"] == "echo-bot" for row in payload["recent_activity"])


def test_gateway_agents_send_uses_managed_identity(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    monkeypatch.setenv("AX_CONFIG_DIR", str(config_dir))
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "codex",
        }
    )
    token_file = tmp_path / "sender.token"
    token_file.write_text("axp_a_agent.secret")
    registry = gateway_core.load_gateway_registry()
    registry["agents"] = [
        {
            "name": "sender-bot",
            "agent_id": "agent-1",
            "space_id": "space-1",
            "base_url": "https://paxai.app",
            "runtime_type": "inbox",
            "desired_state": "running",
            "effective_state": "running",
            "token_file": str(token_file),
            "transport": "gateway",
            "credential_source": "gateway",
        }
    ]
    gateway_core.save_gateway_registry(registry)
    monkeypatch.setattr(gateway_cmd, "AxClient", _FakeManagedSendClient)

    result = runner.invoke(app, ["gateway", "agents", "send", "sender-bot", "hello there", "--to", "codex", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["agent"] == "sender-bot"
    assert payload["content"] == "@codex hello there"
    assert payload["message"]["metadata"]["gateway"]["sent_via"] == "gateway_cli"
    recent = gateway_core.load_recent_gateway_activity()
    assert recent[-1]["event"] == "manual_message_sent"
