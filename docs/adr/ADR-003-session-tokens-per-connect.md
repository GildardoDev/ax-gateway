# ADR-003: Session Tokens Are Short-Lived and Per-Connect

**Status:** Accepted

## Context

When a local agent connects to Gateway via `/local/connect`, it receives a
session token (`axgw_s_<payload>.<signature>`). This token authorizes subsequent
API calls through Gateway's local endpoints (`/local/send`, `/local/inbox`,
`/local/proxy`).

The question was whether session tokens should be cached and reused across
connections or issued fresh each time.

## Decision

Session tokens are short-lived and per-connect. Each `/local/connect` call
issues a new token. Tokens are HMAC-SHA256 signed with the secret at
`~/.ax/gateway/local_secret.bin` (see `local_secret_path()` and
`issue_local_session()` in `ax_cli/gateway.py`). Tokens are not cached on
disk or reused across agent restarts.

## Consequences

- **Positive:** A leaked session token has limited blast radius — it expires
  via TTL. There is no active session-end invalidation; the primary mitigation
  is short token lifetimes.
- **Positive:** Token compromise has a bounded window — tokens expire via TTL.
  There is no restart-based invalidation; the primary protection is short expiry.
- **Positive:** Simpler token lifecycle — no cache invalidation, no stale token
  bugs, no need for a revocation list.
- **Negative:** Every agent connect requires a Gateway round-trip to obtain a
  fresh token. This is negligible for local-machine communication.
- **Negative:** Long-running agents that lose their session (Gateway restart,
  secret rotation) must reconnect. The reconcile loop handles this
  automatically for managed agents.
