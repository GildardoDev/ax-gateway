# ADR-004: Space State Lives in session.json, Not registry.json Gateway Block

**Status:** Accepted (PR #172)

## Context

Gateway tracks which space each agent is bound to. Initially, the active space
was stored in the `gateway` block of `registry.json` alongside static agent
config (name, template, workdir, credentials).

This caused problems:

1. The reconcile loop writes `registry.json` every cycle. Space state changes
   (operator switches space) were mixed with lifecycle state changes (agent
   starts/stops), making it hard to reason about which writes were
   operator-initiated vs. system-initiated.
2. The auto-migration that strips stale `space_id`/`space_name` from the
   gateway block could race with an operator space switch, silently reverting
   the operator's choice.
3. Backup and restore of `registry.json` carried space state, which is
   ephemeral and environment-dependent — restoring a registry from staging
   onto a prod Gateway would bind agents to the wrong space.

## Decision

Move active space state to `session.json`. The registry gateway block retains
only static config (agent identity, template, workdir, credentials).
`session.json` holds ephemeral runtime state including active space, session
tokens, and presence information.

Implemented in PR #172.

## Consequences

- **Positive:** Clean separation of static config (registry) vs. runtime state
  (session). Operators can back up registry without carrying ephemeral state.
- **Positive:** The reconcile loop no longer races with space switches — session
  writes are independent of registry writes.
- **Positive:** Space auto-migration only needs to touch session, not registry.
- **Negative:** Two files to read when debugging space issues. During the
  migration, identity bindings in `registry.json` still carry `active_space_id`
  and `default_space_id` fields. Operators should check both `session.json` and
  `registry.json` identity bindings until the migration is complete.
- **Negative:** Gateway startup must load both files and reconcile any
  inconsistencies between them.
