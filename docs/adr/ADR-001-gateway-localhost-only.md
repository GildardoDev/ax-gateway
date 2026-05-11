# ADR-001: Gateway Binds to 127.0.0.1 Only

**Status:** Accepted

## Context

Gateway serves an HTTP API and operator UI on a local port. During early
development, the question arose whether Gateway should bind to `0.0.0.0`
(accessible from the LAN) or `127.0.0.1` (localhost only).

Gateway manages agent credentials, proxies authenticated API calls, and can
start/stop local agent runtimes. Exposing these capabilities to the network
would create a large attack surface — any device on the local network could
register agents, read inboxes, and proxy API calls using the operator's
credentials.

## Decision

Gateway defaults to `127.0.0.1:8765`. The `--host` flag on `ax gateway start`
and `ax gateway ui` allows an explicit override (e.g. for SSH tunnel or reverse
proxy setups), but the default enforces localhost-only binding.

## Consequences

- **Positive:** The trust boundary is the local machine. Only processes running
  on the same host can reach Gateway. This matches the threat model: Gateway
  trusts the local operator and their local agent processes.
- **Positive:** No need for authentication on the Gateway HTTP API itself — the
  OS network stack provides the access control.
- **Negative:** Remote operators cannot access the Gateway UI without SSH
  tunneling or a reverse proxy. This is acceptable because Gateway is a
  local-development tool, not a production service.
- **Negative:** Multi-machine agent deployments need one Gateway per host. There
  is no centralized Gateway that manages agents across machines.

## Notes

Host header validation middleware is a recommended hardening measure (see issue
backlog) to prevent DNS rebinding attacks against the localhost endpoint.
