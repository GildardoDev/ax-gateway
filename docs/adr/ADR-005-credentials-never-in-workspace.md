# ADR-005: Agent Credentials Are Brokered, Never Copied to Workspace Config

**Status:** Accepted

## Context

When Gateway mints a managed agent credential, the credential needs to be
available to the agent runtime. Two approaches:

1. **Copy the credential** into the agent's workspace config
   (`.ax/config.toml`) so the agent can read it directly.
2. **Broker the credential** through Gateway — the agent authenticates with a
   session token and Gateway attaches the real credential to proxied API calls.

Copying credentials to workspace config creates multiple risks:

- The credential is on disk in a file that may be committed to git, copied to
  another machine, or read by another process in the same workspace.
- Multiple agents sharing a workspace directory would share or overwrite each
  other's credentials.
- Credential rotation requires updating every workspace config file that
  contains the old credential.
- Logs, error messages, and diagnostic output may accidentally include the
  credential if it's a local config value.

## Decision

Agent credentials are brokered by Gateway. The raw credential lives only in
Gateway's state directory (`~/.ax/gateway/agents/{name}/token`). The agent
runtime authenticates to Gateway with a short-lived session token and Gateway
proxies API calls using the real credential.

Credentials must not appear in:
- `.ax/config.toml` (workspace config)
- Log files or diagnostic output
- Messages, PR comments, or generated docs
- Environment variables visible to child processes (except when explicitly
  injected by Gateway for a supervised runtime launch)

## Consequences

- **Positive:** Single source of truth for credentials — Gateway's state
  directory. Rotation updates one file.
- **Positive:** Workspace directories are safe to commit, share, and inspect.
  No credential leakage through git, logs, or copy operations.
- **Positive:** Multiple agents in the same workspace directory maintain
  distinct identities — their credentials are in Gateway, not in a shared
  `.ax/config.toml`.
- **Negative:** Agents cannot operate independently of Gateway. If Gateway is
  down, agents cannot authenticate. This is acceptable because Gateway is the
  management plane — agents without their management plane are intentionally
  non-functional.
- **Negative:** Advanced/legacy setups that use direct token profiles bypass
  this brokering. Those setups accept the credential-on-disk risk.
