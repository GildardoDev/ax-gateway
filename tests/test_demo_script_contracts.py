"""JSON-shape contracts for the commands listed in docs/gateway-demo-script.md.

aX task 33384a22 acceptance: "Demo commands have expected JSON checks."

The demo script's "CLI Verification Path" section lists five commands an
operator runs before a live demo to prove behavior end-to-end:

    ax gateway status --json
    ax gateway agents list --json
    ax gateway agents show gemma4 --json
    ax gateway agents test gemma4 --json
    ax gateway local inbox --agent codex-pass-through --json

The prose calls out specific things the operator checks against — pending
approval count, target agent space_id, mailbox activity for pass-through.
This file pins each command's JSON output to the keys the demo prose
relies on, so a future refactor that drops or renames one of those keys
breaks the test before it breaks the demo.

These are structural contracts, not state assertions. They don't say
"pending_approvals must equal zero" — that's an operator state check the
demo prose already documents. They DO say "the response must contain a
`pending_approvals` field of type int" so the operator's grep/jq still
finds it.

When the demo script changes, update these tests in lockstep.
"""

from __future__ import annotations

import json

from typer.testing import CliRunner

from ax_cli import gateway as gateway_core
from ax_cli.commands import gateway as gateway_cmd
from ax_cli.main import app

runner = CliRunner()


def _isolate_gateway(tmp_path, monkeypatch):
    monkeypatch.setenv("AX_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("AX_GATEWAY_DIR", str(tmp_path / "gw"))


def _seed_registry(tmp_path, *, agents=None, approvals=None):
    """Write a minimal registry with the given agents/approvals."""
    registry = {"agents": list(agents or []), "approvals": list(approvals or [])}
    gateway_core.save_gateway_registry(registry)


def _seed_session():
    gateway_core.save_gateway_session(
        {
            "token": "axp_u_test.token",
            "base_url": "https://paxai.app",
            "space_id": "space-1",
            "username": "operator",
        }
    )


# --- ax gateway status --json -----------------------------------------------


def test_demo_status_json_contains_pending_approvals_summary(monkeypatch, tmp_path):
    """Demo line 34: `ax gateway status --json`. The operator checks
    `pending_approvals` to confirm a clean state before the demo."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    _seed_registry(tmp_path)

    result = runner.invoke(app, ["gateway", "status", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)

    # Top-level keys the demo + alerts panel + UI render off.
    for required in ("connected", "agents", "approvals", "summary", "alerts", "recent_activity"):
        assert required in payload, f"missing required top-level key {required!r}"

    # The specific field the demo prose names.
    assert "pending_approvals" in payload["summary"], "summary.pending_approvals is the demo's clean-state check"
    assert isinstance(payload["summary"]["pending_approvals"], int)

    # Sibling counts the operator may also grep.
    for counter in ("managed_agents", "live_agents", "inbox_agents", "connected_agents"):
        assert counter in payload["summary"]
        assert isinstance(payload["summary"][counter], int)


# --- ax gateway agents list --json ------------------------------------------


def test_demo_agents_list_json_returns_agents_array(monkeypatch, tmp_path):
    """Demo line 36 + 193: `ax gateway agents list --json`. Each agent row
    must carry the keys the demo prose checks (`name`, `space_id`)."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    _seed_registry(
        tmp_path,
        agents=[
            {
                "name": "gemma4",
                "agent_id": "agent-gemma",
                "template_id": "ollama",
                "runtime_type": "ollama_bridge",
                "space_id": "space-1",
                "effective_state": "running",
                "desired_state": "running",
            }
        ],
    )

    result = runner.invoke(app, ["gateway", "agents", "list", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert "agents" in payload and isinstance(payload["agents"], list)
    assert "count" in payload and isinstance(payload["count"], int)
    assert payload["count"] == 1
    agent = payload["agents"][0]
    # The exact two fields demo prose names: "the target agent should have
    # the expected space_id" implies both name and space_id are present.
    assert agent.get("name") == "gemma4"
    assert agent.get("space_id") == "space-1"


# --- ax gateway agents show <name> --json -----------------------------------


def test_demo_agents_show_json_carries_agent_and_gateway_blocks(monkeypatch, tmp_path):
    """Demo line 194: `ax gateway agents show gemma4 --json`. The output
    must split `agent` (the row) from `gateway` (daemon/session context)
    so the operator can ${{.agent.space_id}} or ${{.gateway.space_id}}
    cleanly with jq."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    _seed_registry(
        tmp_path,
        agents=[
            {
                "name": "gemma4",
                "agent_id": "agent-gemma",
                "template_id": "ollama",
                "runtime_type": "ollama_bridge",
                "space_id": "space-1",
                "effective_state": "running",
                "desired_state": "running",
            }
        ],
    )

    result = runner.invoke(app, ["gateway", "agents", "show", "gemma4", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    for required in ("agent", "gateway", "recent_activity"):
        assert required in payload, f"missing required key {required!r}"
    assert payload["agent"].get("name") == "gemma4"
    assert payload["agent"].get("space_id") == "space-1"
    # gateway block carries connection state for the demo's "is the daemon up" check
    for required in ("connected", "base_url", "space_id"):
        assert required in payload["gateway"]


def test_demo_agents_show_unknown_name_exits_one(monkeypatch, tmp_path):
    """The demo prose assumes `gemma4` exists; if it doesn't, the command
    must exit 1 with an error (operator catches the typo before the demo)."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    _seed_registry(tmp_path)

    result = runner.invoke(app, ["gateway", "agents", "show", "ghost", "--json"])

    assert result.exit_code == 1
    assert "not found" in result.output.lower()


# --- ax gateway approvals cleanup --json ------------------------------------


def test_demo_approvals_cleanup_json_returns_archive_summary(monkeypatch, tmp_path):
    """Demo line 35: `ax gateway approvals cleanup`. The JSON must include
    `archived_count` and `remaining_pending` so the operator's pre-demo
    assert (`remaining_pending == 0`) has stable keys to read."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    # Pre-seed an orphaned approval (no matching agent/install).
    _seed_registry(
        tmp_path,
        approvals=[
            {
                "approval_id": "approval-orphan",
                "asset_id": "missing-agent",
                "install_id": "missing-install",
                "candidate_signature": "sha256:missing",
                "approval_kind": "binding_drift",
                "status": "pending",
                "requested_at": "2026-04-27T12:00:00+00:00",
            }
        ],
    )

    result = runner.invoke(app, ["gateway", "approvals", "cleanup", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    for required in ("archived_count", "remaining_pending"):
        assert required in payload, f"cleanup JSON must include {required!r}"
        assert isinstance(payload[required], int)
    # The orphan was archived → cleanup proves the contract works end-to-end.
    assert payload["archived_count"] == 1
    assert payload["remaining_pending"] == 0


# --- ax gateway local inbox --agent <name> --json ---------------------------


def test_demo_local_inbox_json_returns_agent_messages_envelope(monkeypatch, tmp_path):
    """Demo line 196: `ax gateway local inbox --agent codex-pass-through --json`.
    Operator's check is "mailbox activity rather than live listener status."
    The JSON envelope must carry `agent` and `messages` so the operator can
    distinguish a pass-through inbox response from a live-listener one."""
    _isolate_gateway(tmp_path, monkeypatch)

    # The local inbox path goes through the gateway daemon's HTTP API. Since
    # the daemon isn't running in this test, we patch the helpers the CLI
    # calls directly to avoid the network round-trip.
    def fake_resolve_session(**_kwargs):
        return ("axgw_s_test.session", {"status": "approved"})

    def fake_poll(**_kwargs):
        return {
            "agent": "codex-pass-through",
            "messages": [],
            "unread_count": 0,
            "marked_read_count": 0,
        }

    monkeypatch.setattr(gateway_cmd, "_resolve_local_gateway_session", fake_resolve_session)
    monkeypatch.setattr(gateway_cmd, "_poll_local_inbox_over_http", fake_poll)

    result = runner.invoke(app, ["gateway", "local", "inbox", "--agent", "codex-pass-through", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    for required in ("agent", "messages", "unread_count"):
        assert required in payload, f"local inbox JSON must include {required!r}"
    assert payload["agent"] == "codex-pass-through"
    assert isinstance(payload["messages"], list)
    assert isinstance(payload["unread_count"], int)


# --- Cross-command consistency ---------------------------------------------


def test_demo_status_and_agents_list_agree_on_visible_agents(monkeypatch, tmp_path):
    """Demo runs `status --json` and `agents list --json` back-to-back. Both
    surfaces consume the same `_status_payload` internally; this test pins
    them to remain consistent so the operator never sees one report 3
    agents while the other reports 2."""
    _isolate_gateway(tmp_path, monkeypatch)
    _seed_session()
    _seed_registry(
        tmp_path,
        agents=[
            {
                "name": "gemma4",
                "agent_id": "agent-gemma",
                "template_id": "ollama",
                "runtime_type": "ollama_bridge",
                "space_id": "space-1",
                "effective_state": "running",
                "desired_state": "running",
            },
            {
                "name": "demo-hermes",
                "agent_id": "agent-hermes",
                "template_id": "hermes",
                "runtime_type": "hermes_sentinel",
                "space_id": "space-1",
                "effective_state": "running",
                "desired_state": "running",
            },
        ],
    )

    status_result = runner.invoke(app, ["gateway", "status", "--json"])
    list_result = runner.invoke(app, ["gateway", "agents", "list", "--json"])

    status_payload = json.loads(status_result.stdout)
    list_payload = json.loads(list_result.stdout)

    status_names = sorted(a["name"] for a in status_payload["agents"])
    list_names = sorted(a["name"] for a in list_payload["agents"])
    assert status_names == list_names == ["demo-hermes", "gemma4"]
    assert status_payload["summary"]["managed_agents"] == list_payload["count"]
