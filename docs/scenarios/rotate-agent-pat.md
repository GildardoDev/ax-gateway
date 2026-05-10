# Rotate an Agent PAT

Replace an agent's PAT without downtime by minting the new token first,
verifying it works, then revoking the old one.

> **Scope:** this page documents the current PAT-based rotation path for
> existing agent credentials. Gateway's intended direction is OAuth/device
> login plus Gateway-brokered credentials — normal onboarding should not
> require operators or agents to copy PATs manually. See
> [DEVICE-TRUST-001](../../specs/DEVICE-TRUST-001/spec.md) and
> [GATEWAY-AUTH-TIERS-001](../../specs/GATEWAY-AUTH-TIERS-001/spec.md) for
> the trust-boundary direction.

Background: [`docs/credential-security.md`](../credential-security.md#agent-pat-rotation)
and [`docs/agent-authentication.md`](../agent-authentication.md#rotation-with-existing-cli-commands)
explain the policy. This page is the operator runbook.

## Goal

Cut over `<agent>` to a fresh PAT with no failed requests in between. The
old credential keeps working until the new one verifies; only then do you
revoke it.

## Prerequisites

- A user bootstrap login — `axctl auth whoami` returns your user, not the
  agent.
- The agent already exists and you know its name and audience (`cli`,
  `mcp`, or `both`).
- The agent's existing profile is already registered. If not, register it
  first with `axctl profile add` so you have a baseline to compare against.

## Steps

1. **Inventory current credentials.**

   ```bash
   axctl credentials list --json
   axctl credentials audit
   ```

   Note the `credential_id` of the PAT you intend to replace and confirm
   the agent has exactly one active PAT today. In automation use
   `axctl credentials audit --strict` to make policy violations fail loudly.

2. **Mint the replacement.** Match the audience and expiry of the old
   token. Save to a separate directory so the old token file is untouched.

   ```bash
   axctl token mint <agent> \
       --audience <cli|mcp|both> \
       --expires <days> \
       --save-to <new-token-dir> \
       --profile <new-profile> \
       --no-print-token
   ```

   `--save-to` takes a directory (typically the agent's `.ax/` directory,
   e.g. `/home/agent/.ax`); the token file and `config.toml` are written
   inside it. `--no-print-token` keeps the secret out of your shell
   history. The token file is mode `0600`, and `<new-profile>` is
   registered against it.

3. **Verify the new profile.**

   ```bash
   axctl profile verify <new-profile>
   axctl auth whoami --json
   ```

   `profile verify` re-checks the token fingerprint, hostname, and workdir
   hash. `auth whoami` proves the new PAT actually exchanges for a JWT
   end-to-end. Both must succeed before continuing.

4. **Revoke the old credential.** Use the `credential_id` recorded in
   step 1, not the new one.

   ```bash
   axctl credentials revoke <old-credential-id>
   ```

5. **Confirm cleanup.**

   ```bash
   axctl credentials list --json
   ```

   The old `credential_id` should now show `lifecycle_state: revoked`. The
   agent should have exactly one active PAT — the replacement.

## Verify

After the rotation:

- `axctl credentials list` shows one active PAT for the agent.
- `axctl credentials audit` reports no policy violations.
- The agent's normal workflow (send / listen / mcp call, whatever it does
  in production) succeeds with the new profile.
- The old token file can be deleted.

## What can go wrong

| Symptom | Cause | Fix |
|---|---|---|
| Agent fails right after step 4. | Revoked the old credential before the new one was actually serving traffic. | Re-run step 2 to mint another replacement, verify, then revoke. The originally-revoked id stays revoked. |
| `credentials list` shows 3+ active PATs for the agent. | A previous rotation aborted between mint and revoke. | Stop. Identify each `credential_id`, decide which is current, and revoke the rest before issuing more. More than two active PATs per agent is a security hygiene issue. |
| `profile verify` fails on the new profile. | Token file moved, host/workdir changed since `axctl token mint` ran, or the file was tampered with. | Inspect the token file inside `<new-token-dir>` (permissions and contents). If intentional (e.g. rotated to a new host), re-run `axctl profile add` from the new location. |
| `auth whoami` returns the user, not the agent. | Wrong audience minted, or the new profile isn't active yet. | Check that step 2 used the same audience as the old PAT, and that `axctl profile use <new-profile>` (or the appropriate env vars) selected the agent profile. |
| Honeypot / fingerprint alert fires during step 3. | The new token was used from a different machine or workspace than where it was minted. | Treat as a security event, not a rotation problem. See [`docs/credential-security.md`](../credential-security.md). |
