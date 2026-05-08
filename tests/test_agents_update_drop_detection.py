"""Tests for silent-drop detection on `ax agents update` — see issue #76.

When the server returns HTTP 200 but omits bio/specialization from the
response, the CLI must warn on stderr so the user doesn't get a false-
success signal.
"""

from __future__ import annotations

import json as _json

from typer.testing import CliRunner

from ax_cli.commands import agents as agents_cmd
from ax_cli.commands import bootstrap as bootstrap_cmd
from ax_cli.commands.agents import _warn_if_fields_dropped
from ax_cli.main import app

runner = CliRunner()


# ── helpers ─────────────────────────────────────────────────────────────────


class _EchoClient:
    """Fake client that echoes all sent fields back — simulates a fixed server."""

    def update_agent(self, identifier, **fields):
        return {"id": "agent-1", "name": identifier, **fields}


class _DroppingClient:
    """Fake client that drops bio/specialization — simulates current server."""

    def update_agent(self, identifier, **fields):
        kept = {k: v for k, v in fields.items() if k not in ("bio", "specialization")}
        return {"id": "agent-1", "name": identifier, **kept}


# ── unit tests for _warn_if_fields_dropped ──────────────────────────────────


def test_no_warning_when_fields_echoed(capsys):
    sent = {"bio": "scout", "specialization": "recon"}
    response = {"id": "1", "name": "x", "bio": "scout", "specialization": "recon"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == []
    assert "Warning" not in capsys.readouterr().err


def test_warning_when_bio_dropped(capsys):
    sent = {"bio": "scout"}
    response = {"id": "1", "name": "x"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == ["--bio"]


def test_warning_when_specialization_dropped(capsys):
    sent = {"specialization": "recon"}
    response = {"id": "1", "name": "x"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == ["--specialization"]


def test_warning_when_both_dropped(capsys):
    sent = {"bio": "scout", "specialization": "recon"}
    response = {"id": "1", "name": "x"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert "--bio" in dropped
    assert "--specialization" in dropped


def test_no_warning_for_description_only():
    sent = {"description": "updated desc"}
    response = {"id": "1", "name": "x", "description": "updated desc"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == []


def test_no_warning_when_bio_not_sent():
    sent = {"description": "hi"}
    response = {"id": "1", "name": "x", "description": "hi"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == []


def test_warning_detects_value_mismatch():
    sent = {"bio": "scout"}
    response = {"id": "1", "name": "x", "bio": "different"}
    dropped = _warn_if_fields_dropped(sent, response)
    assert dropped == ["--bio"]


# ── integration tests: `ax agents update` command ──────────────────────────


def test_update_no_warning_when_server_echoes(monkeypatch):
    monkeypatch.setattr(agents_cmd, "get_client", lambda: _EchoClient())

    result = runner.invoke(app, ["agents", "update", "axolotl", "--bio", "hi"])
    assert result.exit_code == 0, result.stderr
    assert "Updated agent" in result.stdout
    assert "Warning" not in result.stderr


def test_update_warns_on_stderr_when_server_drops_bio(monkeypatch):
    monkeypatch.setattr(agents_cmd, "get_client", lambda: _DroppingClient())

    result = runner.invoke(app, ["agents", "update", "axolotl", "--bio", "hi"])
    assert result.exit_code == 0
    assert "Warning" in result.stderr
    assert "--bio" in result.stderr
    assert "issues/76" in result.stderr


def test_update_warns_on_stderr_when_server_drops_specialization(monkeypatch):
    monkeypatch.setattr(agents_cmd, "get_client", lambda: _DroppingClient())

    result = runner.invoke(app, ["agents", "update", "axolotl", "--specialization", "recon"])
    assert result.exit_code == 0
    assert "--specialization" in result.stderr
    assert "issues/76" in result.stderr


def test_update_json_stdout_stays_clean_when_warning_fires(monkeypatch):
    monkeypatch.setattr(agents_cmd, "get_client", lambda: _DroppingClient())

    result = runner.invoke(app, ["agents", "update", "axolotl", "--bio", "hi", "--json"])
    assert result.exit_code == 0
    parsed = _json.loads(result.stdout)
    assert parsed["name"] == "axolotl"
    assert "bio" not in parsed
    assert "Warning" in result.stderr
    assert "--bio" in result.stderr


def test_update_no_warning_when_description_only(monkeypatch):
    monkeypatch.setattr(agents_cmd, "get_client", lambda: _DroppingClient())

    result = runner.invoke(app, ["agents", "update", "axolotl", "--description", "new desc"])
    assert result.exit_code == 0
    assert "Warning" not in result.stderr


# ── integration tests: bootstrap _polish_metadata ───────────────────────────


def test_polish_metadata_warns_when_server_drops(capsys):
    bootstrap_cmd._polish_metadata(
        _DroppingClient(),
        name="axolotl",
        bio="scout",
        specialization="recon",
        system_prompt=None,
    )
    captured = capsys.readouterr()
    assert "--bio" in captured.err or "--bio" in captured.out
    assert "--specialization" in captured.err or "--specialization" in captured.out


def test_polish_metadata_no_warning_when_echoed(capsys):
    bootstrap_cmd._polish_metadata(
        _EchoClient(),
        name="axolotl",
        bio="scout",
        specialization="recon",
        system_prompt=None,
    )
    captured = capsys.readouterr()
    assert "Warning" not in captured.err
    assert "Warning" not in captured.out


def test_polish_metadata_skips_when_only_system_prompt(capsys):
    bootstrap_cmd._polish_metadata(
        _EchoClient(),
        name="axolotl",
        bio=None,
        specialization=None,
        system_prompt="You are helpful.",
    )
    captured = capsys.readouterr()
    assert "Warning" not in captured.err
    assert "Warning" not in captured.out
