from pathlib import Path

DEMO_HTML = Path(__file__).resolve().parents[1] / "ax_cli" / "static" / "demo.html"


def test_agent_row_type_helpers_present() -> None:
    """Public-role helpers must exist; the row falls back to them when no
    specific runtime template is matched."""
    source = DEMO_HTML.read_text()

    assert "function publicAgentTypeLabel(agent)" in source
    assert "function publicAgentTypeMeta(label)" in source
    assert 'normalized.includes("on-demand")' in source


def test_agent_row_type_prefers_specific_runtime_over_public_role() -> None:
    """Specific runtime templates (Ollama, Hermes, Claude Code, ...) win
    over the public role abstraction (Live Listener / Pass-through). This
    keeps the row label informative when multiple agents share a role —
    the user wants to see "HERMES" vs "CLAUDE CODE" on Live Listener
    agents, not just the abstract "Live Listener" for both.

    Regression guard: an earlier rev called publicAgentTypeMeta first and
    flattened all managed agents to the public-role label.
    """
    source = DEMO_HTML.read_text()

    template_pos = source.index('if (template === "ollama")')
    public_type_pos = source.index("const publicType = publicAgentTypeMeta")
    assert template_pos < public_type_pos


def test_agent_row_type_falls_back_to_intake_model_when_no_template_match() -> None:
    """Custom / unknown templates without a specific meta still resolve to
    a useful label via intake_model (live_listener / launch_on_send /
    polling_mailbox). The fallback runs after the template-specific block."""
    source = DEMO_HTML.read_text()

    template_pos = source.index('if (template === "ollama")')
    launch_on_send_pos = source.index('intake === "launch_on_send"')
    live_listener_pos = source.index('intake === "live_listener"')

    assert template_pos < launch_on_send_pos
    assert template_pos < live_listener_pos


def test_agent_row_type_carries_tooltip_combining_runtime_and_role() -> None:
    """Operator hovering the type icon should see both the specific
    runtime and the public role (e.g. "Hermes · Live Listener") so the
    abstraction stays discoverable without taking row space."""
    source = DEMO_HTML.read_text()

    assert "tooltip" in source
    assert "${resolved.label} · ${publicLabel}" in source


def test_friendly_status_surfaces_external_plugin_attach_state_before_stopped() -> None:
    """Externally managed Hermes agents need a fresh plugin heartbeat; a stale
    reply alone should not read as connected."""
    source = DEMO_HTML.read_text()

    plugin_pos = source.index("Plugin not attached")
    stopped_pos = source.index('if (desired === "stopped")')
    assert plugin_pos < stopped_pos
    assert "external_runtime_managed" in source
    assert "fresh Gateway heartbeat" in source
