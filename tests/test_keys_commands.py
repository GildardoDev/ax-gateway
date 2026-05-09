import json

from typer.testing import CliRunner

from ax_cli.main import app

runner = CliRunner()


def test_keys_create_passes_bound_agent_id(monkeypatch):
    captured = {}

    class FakeClient:
        def create_key(self, name, *, allowed_agent_ids=None, bound_agent_id=None):
            captured["name"] = name
            captured["allowed_agent_ids"] = allowed_agent_ids
            captured["bound_agent_id"] = bound_agent_id
            return {"credential_id": "c1", "token": "axp_a_test"}

    monkeypatch.setattr("ax_cli.commands.keys.get_client", lambda: FakeClient())

    uuid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    result = runner.invoke(
        app,
        ["keys", "create", "--name", "orion", "--bound-agent-id", uuid, "--json"],
    )
    assert result.exit_code == 0, result.output
    assert captured == {
        "name": "orion",
        "allowed_agent_ids": None,
        "bound_agent_id": uuid,
    }
    payload = json.loads(result.output)
    assert payload["credential_id"] == "c1"
    assert payload["token"] == "axp_a_test"


def test_keys_create_passes_scope_and_bound_agent_id(monkeypatch):
    captured = {}

    class FakeClient:
        def create_key(self, name, *, allowed_agent_ids=None, bound_agent_id=None):
            captured["allowed_agent_ids"] = allowed_agent_ids
            captured["bound_agent_id"] = bound_agent_id
            return {"credential_id": "c2"}

    monkeypatch.setattr("ax_cli.commands.keys.get_client", lambda: FakeClient())

    aid = "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22"
    result = runner.invoke(
        app,
        [
            "keys",
            "create",
            "--name",
            "combo",
            "--scope-to-agent",
            aid,
            "--bound-agent-id",
            aid,
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["allowed_agent_ids"] == [aid]
    assert captured["bound_agent_id"] == aid
