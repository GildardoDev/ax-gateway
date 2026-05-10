"""Tests for the Gateway-supervised Hermes plugin runtime.

Covers the scaffolding + spawn helpers (no live subprocess). Specifically
asserts the trust-boundary property: the raw AX_TOKEN never lands in any
file under the workspace; it only appears in the subprocess env that
Gateway builds at spawn time from the Gateway-owned token file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ax_cli import gateway as gateway_core
from ax_cli.gateway_runtime_types import (
    agent_template_definition,
    runtime_type_definition,
    runtime_type_list,
)


def _base_entry(tmp_path: Path) -> dict:
    workdir = tmp_path / "wiki"
    return {
        "name": "mnemo",
        "agent_id": "11111111-1111-1111-1111-111111111111",
        "space_id": "22222222-2222-2222-2222-222222222222",
        "base_url": "https://paxai.app",
        "workdir": str(workdir),
        "runtime_type": "hermes_plugin",
        "template_id": "hermes",
    }


def test_hermes_plugin_runtime_in_catalog():
    plugin_def = runtime_type_definition("hermes_plugin")
    assert plugin_def["id"] == "hermes_plugin"
    assert plugin_def["kind"] == "supervised_process"
    assert plugin_def.get("deprecated") is not True
    ids = [r["id"] for r in runtime_type_list()]
    assert "hermes_plugin" in ids
    # hermes template now defaults to plugin runtime
    hermes_template = agent_template_definition("hermes")
    assert hermes_template["runtime_type"] == "hermes_plugin"
    assert hermes_template["defaults"]["runtime_type"] == "hermes_plugin"


def test_hermes_sentinel_marked_deprecated_but_still_resolvable():
    legacy = runtime_type_definition("hermes_sentinel")
    assert legacy["id"] == "hermes_sentinel"
    assert legacy.get("deprecated") is True


def test_is_predicates():
    assert gateway_core._is_hermes_plugin_runtime("hermes_plugin")
    assert not gateway_core._is_hermes_plugin_runtime("hermes_sentinel")
    assert gateway_core._is_supervised_subprocess_runtime("hermes_plugin")
    assert gateway_core._is_supervised_subprocess_runtime("hermes_sentinel")
    assert not gateway_core._is_supervised_subprocess_runtime("echo")
    assert not gateway_core._is_supervised_subprocess_runtime("exec")


def test_hermes_plugin_workdir_and_home(tmp_path):
    entry = _base_entry(tmp_path)
    workdir = gateway_core._hermes_plugin_workdir(entry)
    assert workdir == tmp_path / "wiki"
    home = gateway_core._hermes_plugin_home(entry)
    assert home == workdir / ".hermes"
    entry["hermes_home"] = str(tmp_path / "elsewhere")
    assert gateway_core._hermes_plugin_home(entry) == tmp_path / "elsewhere"


def test_scaffold_creates_dir_plugin_link_and_dotenv(tmp_path):
    entry = _base_entry(tmp_path)
    home = gateway_core._scaffold_hermes_plugin_home(entry)
    assert home.is_dir()
    plugin_link = home / "plugins" / "ax"
    assert plugin_link.is_symlink() or (plugin_link / "plugin.yaml").exists()
    assert plugin_link.resolve() == gateway_core._plugin_source_dir().resolve()
    dotenv = (home / ".env").read_text()
    assert "AX_AGENT_NAME=mnemo" in dotenv
    assert entry["agent_id"] in dotenv
    assert entry["space_id"] in dotenv
    assert "AX_BASE_URL=https://paxai.app" in dotenv
    # The .env can mention AX_TOKEN in a comment, but must never assign it.
    assert not any(
        line.strip().startswith("AX_TOKEN=") for line in dotenv.splitlines()
    ), "Identity .env must not assign AX_TOKEN"


def test_scaffold_is_idempotent(tmp_path):
    entry = _base_entry(tmp_path)
    home_first = gateway_core._scaffold_hermes_plugin_home(entry)
    plugin_link = home_first / "plugins" / "ax"
    first_target = plugin_link.resolve()
    home_second = gateway_core._scaffold_hermes_plugin_home(entry)
    assert home_first == home_second
    assert (home_second / "plugins" / "ax").resolve() == first_target


def test_scaffold_inherits_operator_auth_when_present(tmp_path, monkeypatch):
    fake_home = tmp_path / "operator-home"
    operator_hermes = fake_home / ".hermes"
    operator_hermes.mkdir(parents=True)
    (operator_hermes / "auth.json").write_text("{}")
    (operator_hermes / "config.yaml").write_text("providers: {}")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: fake_home))

    entry = _base_entry(tmp_path)
    home = gateway_core._scaffold_hermes_plugin_home(entry)
    auth = home / "auth.json"
    cfg = home / "config.yaml"
    assert auth.is_symlink()
    assert cfg.is_symlink()
    assert auth.resolve() == (operator_hermes / "auth.json").resolve()


def test_build_cmd_uses_hermes_gateway_run(tmp_path, monkeypatch):
    entry = _base_entry(tmp_path)
    monkeypatch.setenv("HERMES_BIN", "/usr/local/bin/hermes-test")
    cmd = gateway_core._build_hermes_plugin_cmd(entry)
    assert cmd == ["/usr/local/bin/hermes-test", "gateway", "run"]


def test_build_env_injects_token_from_gateway_file(tmp_path, monkeypatch):
    entry = _base_entry(tmp_path)
    token_file = tmp_path / "token"
    token_file.write_text("axp_a_TEST_TOKEN_NOT_REAL_xxxxx")
    entry["token_file"] = str(token_file)

    # Strip any inherited AX_TOKEN that would mask the per-agent value.
    for var in ("AX_TOKEN", "AX_AGENT_NAME", "AX_AGENT_ID", "AX_SPACE_ID"):
        monkeypatch.delenv(var, raising=False)

    env = gateway_core._build_hermes_plugin_env(entry)
    assert env["AX_TOKEN"] == "axp_a_TEST_TOKEN_NOT_REAL_xxxxx"
    assert env["AX_AGENT_NAME"] == "mnemo"
    assert env["AX_AGENT_ID"] == entry["agent_id"]
    assert env["AX_SPACE_ID"] == entry["space_id"]
    assert env["AX_BASE_URL"] == "https://paxai.app"
    home = gateway_core._hermes_plugin_home(entry)
    assert env["HERMES_HOME"] == str(home)
    # ENV_DENYLIST stripping: callers should not leak Gateway's own AX_TOKEN
    # if it happened to be set when Gateway started.
    monkeypatch.setenv("AX_TOKEN", "axp_u_GATEWAY_USER_TOKEN_LEAK")
    env_again = gateway_core._build_hermes_plugin_env(entry)
    assert env_again["AX_TOKEN"] == "axp_a_TEST_TOKEN_NOT_REAL_xxxxx"


def test_hermes_bin_prefers_entry_override(tmp_path, monkeypatch):
    entry = _base_entry(tmp_path)
    entry["hermes_bin"] = "/custom/bin/hermes"
    monkeypatch.setenv("HERMES_BIN", "/env/bin/hermes")
    assert gateway_core._hermes_bin(entry) == "/custom/bin/hermes"


def test_hermes_bin_falls_back_to_env(tmp_path, monkeypatch):
    entry = _base_entry(tmp_path)
    monkeypatch.setenv("HERMES_BIN", "/env/bin/hermes")
    assert gateway_core._hermes_bin(entry) == "/env/bin/hermes"


def test_hermes_bin_raises_when_unresolvable(tmp_path, monkeypatch):
    entry = _base_entry(tmp_path)
    monkeypatch.delenv("HERMES_BIN", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "nope"))
    monkeypatch.setattr(gateway_core.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="hermes CLI not found"):
        gateway_core._hermes_bin(entry)
