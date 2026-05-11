# ADR-006: `use`/`admin` Tier Model for Proxy Methods

**Status:** Partially implemented (tier annotations landed in PR #215; per-agent
tier enforcement is proposed in issue #146)

## Context

ADR-002 established a flat proxy allowlist. All agent sessions share the same
set of allowed methods. This works for the current agent population but does
not scale to mixed-trust environments where some agents should have broader
access than others.

Specific gaps:

- An inbox agent that only needs `list_messages` and `get_message` currently
  also has access to `update_task`, `list_agents`, and `search_messages`.
- `upload_file` is now in the proxy allowlist with `tier: "admin"` and workdir
  sandboxing, but without per-agent tier enforcement all agents can invoke it.
- The operator cannot express "this agent is trusted for write operations"
  vs. "this agent only reads its inbox" without modifying the proxy source.

## Decision (Proposed)

Replace the flat allowlist with a two-tier model:

| Tier | Access | Example Agents |
| --- | --- | --- |
| `use` | Read operations: `whoami`, `list_*`, `get_*`, `search_*` | Inbox agents, echo bots, monitors |
| `admin` | Read + write operations: `update_task`, `upload_file`, `send_message` | Coding sentinels, automation agents |

Each entry in `_LOCAL_PROXY_METHODS` gets a `tier` annotation:

```python
_LOCAL_PROXY_METHODS = {
    "whoami": {"tier": "use"},
    "list_messages": {"tier": "use", "kwargs": [...]},
    "update_task": {"tier": "admin", "args": ["task_id"], "kwargs": [...]},
    "upload_file": {"tier": "admin", "args": ["file_path"]},
}
```

Agent registrations declare their tier (default: `use`). The proxy checks
`agent_tier >= method_tier` before dispatching.

## Consequences

- **Positive:** Operators can grant broader access to trusted agents without
  modifying source code.
- **Positive:** `upload_file` and `send_message` are already annotated with
  `tier: "admin"`. Per-agent tier enforcement will restrict them to
  admin-tier agents only.
- **Positive:** The allowlist remains a single dict — easy to audit. The tier
  annotation adds one field per entry, not a separate ACL system.
- **Negative:** Two tiers may not be enough. Future requirements may need
  finer granularity (per-space, per-method-argument). The two-tier model is
  a stepping stone, not a final access control system.
- **Negative:** Tier assignment at registration time is static. An agent
  cannot be promoted or demoted without re-registration. This matches the
  current "stop, modify, restart" operational model.

## Implementation Notes

- Add `tier` field to agent registration entries in `registry.json`.
- Default to `use` tier for existing agents (backward compatible).
- Tier annotations on `_LOCAL_PROXY_METHODS` entries are already in place
  (PR #215). Remaining work: per-agent tier enforcement at proxy dispatch.
- Proxy check: `if entry_tier < method_tier: reject`.
