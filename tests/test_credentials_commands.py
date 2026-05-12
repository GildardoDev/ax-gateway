import json

from typer.testing import CliRunner

from ax_cli.commands.credentials import build_credential_audit
from ax_cli.main import app

runner = CliRunner()


def _credential(
    agent_id: str,
    credential_id: str,
    *,
    state: str = "active",
    created_at: str = "2026-04-15T00:00:00Z",
    expires_at: str = "2027-01-01T00:00:00Z",
):
    return {
        "credential_id": credential_id,
        "key_id": f"key-{credential_id}",
        "name": f"credential {credential_id}",
        "bound_agent_id": agent_id,
        "audience": "both",
        "lifecycle_state": state,
        "created_at": created_at,
        "expires_at": expires_at,
        "last_used_at": None,
    }


def test_build_credential_audit_classifies_active_agent_pat_counts():
    report = build_credential_audit(
        [
            _credential("agent-ok", "ok-1"),
            _credential("agent-rotate", "rotate-1", created_at="2026-04-14T00:00:00Z"),
            _credential("agent-rotate", "rotate-2", created_at="2026-04-15T00:00:00Z"),
            _credential("agent-cleanup", "cleanup-1"),
            _credential("agent-cleanup", "cleanup-2"),
            _credential("agent-cleanup", "cleanup-3"),
            _credential("agent-cleanup", "revoked-ignored", state="revoked"),
            {"credential_id": "user-credential", "lifecycle_state": "active", "bound_agent_id": None},
        ]
    )

    by_agent = {agent["agent_id"]: agent for agent in report["agents"]}
    assert by_agent["agent-ok"]["status"] == "ok"
    assert by_agent["agent-rotate"]["status"] == "rotation_window"
    assert by_agent["agent-rotate"]["severity"] == "warning"
    assert by_agent["agent-cleanup"]["status"] == "cleanup_required"
    assert by_agent["agent-cleanup"]["severity"] == "violation"
    assert report["summary"] == {
        "agents_checked": 3,
        "ok": 1,
        "expiring_soon": 0,
        "rotation_windows": 1,
        "cleanup_required": 1,
    }


def test_credentials_audit_json_reports_rotation_and_cleanup(monkeypatch):
    class FakeClient:
        def mgmt_list_credentials(self):
            return [
                _credential("agent-rotate", "rotate-1"),
                _credential("agent-rotate", "rotate-2"),
                _credential("agent-cleanup", "cleanup-1"),
                _credential("agent-cleanup", "cleanup-2"),
                _credential("agent-cleanup", "cleanup-3"),
            ]

    monkeypatch.setattr("ax_cli.commands.credentials.get_client", lambda: FakeClient())

    result = runner.invoke(app, ["credentials", "audit", "--json"])

    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    assert report["policy"]["max_active_agent_pats"] == 2
    assert report["summary"]["rotation_windows"] == 1
    assert report["summary"]["cleanup_required"] == 1


def test_credentials_audit_strict_passes_for_rotation_window(monkeypatch):
    class RotationWindowClient:
        def mgmt_list_credentials(self):
            return [_credential("agent-rotate", "rotate-1"), _credential("agent-rotate", "rotate-2")]

    monkeypatch.setattr("ax_cli.commands.credentials.get_client", lambda: RotationWindowClient())

    rotation_result = runner.invoke(app, ["credentials", "audit", "--strict", "--json"])
    assert rotation_result.exit_code == 0, rotation_result.output

    class CleanupClient:
        def mgmt_list_credentials(self):
            return [
                _credential("agent-cleanup", "cleanup-1"),
                _credential("agent-cleanup", "cleanup-2"),
                _credential("agent-cleanup", "cleanup-3"),
            ]

    monkeypatch.setattr("ax_cli.commands.credentials.get_client", lambda: CleanupClient())

    cleanup_result = runner.invoke(app, ["credentials", "audit", "--strict", "--json"])
    assert cleanup_result.exit_code == 2, cleanup_result.output


def test_build_credential_audit_flags_expiring_soon():
    report = build_credential_audit(
        [
            _credential("agent-ok", "ok-1"),
            _credential("agent-expiring", "exp-1", expires_at="2026-05-15T00:00:00Z"),
            _credential("agent-boundary", "boundary-1", expires_at="2026-05-26T00:00:00Z"),
        ]
    )
    by_agent = {a["agent_id"]: a for a in report["agents"]}
    assert not by_agent["agent-ok"]["expiring_soon"]
    assert by_agent["agent-expiring"]["expiring_soon"]
    assert by_agent["agent-boundary"]["expiring_soon"]  # exactly 14 days — included
    assert by_agent["agent-expiring"]["severity"] == "warning"
    assert "rotate" in by_agent["agent-expiring"]["recommendation"]
    assert report["summary"]["expiring_soon"] == 2
    assert report["summary"]["ok"] == 1


def test_credentials_audit_strict_fails_for_expiring_soon(monkeypatch):
    class ExpiringClient:
        def mgmt_list_credentials(self):
            return [_credential("agent-expiring", "exp-1", expires_at="2026-05-15T00:00:00Z")]

    monkeypatch.setattr("ax_cli.commands.credentials.get_client", lambda: ExpiringClient())

    result = runner.invoke(app, ["credentials", "audit", "--strict", "--json"])
    assert result.exit_code == 2, result.output
    report = json.loads(result.output)
    assert report["summary"]["expiring_soon"] == 1


def test_credentials_audit_expiring_soon_shown_in_human_output(monkeypatch):
    class ExpiringClient:
        def mgmt_list_credentials(self):
            return [_credential("agent-expiring", "exp-1", expires_at="2026-05-15T00:00:00Z")]

    monkeypatch.setattr("ax_cli.commands.credentials.get_client", lambda: ExpiringClient())

    result = runner.invoke(app, ["credentials", "audit"])
    assert result.exit_code == 0, result.output
    assert "expiring_soon=1" in result.output
