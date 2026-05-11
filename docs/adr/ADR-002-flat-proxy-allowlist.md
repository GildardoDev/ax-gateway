# ADR-002: Proxy Uses a Flat Allowlist, Not Per-Agent ACLs

**Status:** Accepted (Phase 4 will replace with `use`/`admin` tiers)

## Context

Gateway proxies API calls from local agent sessions through the `/local/proxy`
endpoint. The proxy needs to control which `AxClient` methods an agent can
invoke — unrestricted proxy access would let any local agent session perform
admin operations using the operator's credentials.

Two approaches were considered:

1. **Per-agent ACLs** — each agent registration declares which methods it may
   call. The proxy checks the agent's ACL before dispatching.
2. **Flat allowlist** — a single `_LOCAL_PROXY_METHODS` dict shared by all
   agents. Any method not in the list is rejected.

## Decision

Use a flat allowlist (`_LOCAL_PROXY_METHODS` in `ax_cli/commands/gateway.py`,
line 540). All agent sessions share the same allowed methods.

Current allowlist includes both `use`-tier read operations (`whoami`,
`list_spaces`, `list_agents`, `list_agents_availability`, `list_context`,
`get_context`, `list_messages`, `get_message`, `search_messages`, `list_tasks`,
`get_task`) and `admin`-tier write operations (`update_task`, `upload_file`).

`send_message` and `create_task` go through dedicated endpoints (`/local/send`,
`/local/tasks`) with additional validation. `upload_file` is in the allowlist
but sandboxed to the agent's workdir (see `commands/gateway.py:833-840`).

## Consequences

- **Positive:** Simple to understand and audit. One dict, one check.
- **Positive:** No per-agent configuration surface to get wrong.
- **Negative:** No granularity — an echo-test agent has the same proxy access as
  a coding sentinel. An inbox agent can call `update_task` even if it should
  only read messages.
- **Negative:** Adding a sensitive method to the allowlist grants it to all
  agents at that tier level. `upload_file` is admin-tiered and sandboxed to
  the agent workdir, but any agent with admin tier can invoke it.

## Replacement Plan

Issue #146 proposes a `use`/`admin` tier model. Each proxy method gets a tier
annotation. Agent registrations declare their tier. The proxy checks
`agent_tier >= method_tier` before dispatching. This preserves the simplicity
of a central list while adding per-agent granularity.
