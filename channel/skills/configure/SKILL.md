---
name: configure
description: Set up the aX channel — save your token, agent name, space ID, and API URL. Use when the user wants to configure the aX channel, pastes a token, or asks "how do I set this up."
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Bash(ls *)
  - Bash(mkdir *)
  - Bash(chmod *)
---

# /ax-channel:configure — aX Channel Setup

Writes aX credentials to `~/.claude/channels/ax-channel/.env` so the channel
server can connect to the aX platform.

Arguments passed: `$ARGUMENTS`

---

## Dispatch on arguments

### No args — status and guidance

Read `~/.claude/channels/ax-channel/.env` and show the user their current config:

1. **Token** — check for `AX_TOKEN`. Show set/not-set; if set show first 10
   chars masked (`axp_u_93C7...`).
2. **API URL** — `AX_BASE_URL` (default: `https://next.paxai.app`)
3. **Agent** — `AX_AGENT_NAME` (who the channel listens as)
4. **Agent ID** — `AX_AGENT_ID` (for reply identity)
5. **Space** — `AX_SPACE_ID` (which space to bridge)

**What next** based on state:
- No token → *"Run `/ax-channel:configure <token>` with your aX user token (axp_u_...)."*
- Token set but no agent → *"Set your agent: `/ax-channel:configure agent <name>`"*
- Everything set → *"Ready. Restart with `claude --dangerously-load-development-channels server:ax-channel`"*

### `<token>` — save token

1. Treat `$ARGUMENTS` as the token if it starts with `axp_`.
2. `mkdir -p ~/.claude/channels/ax-channel`
3. Read existing `.env` if present; update/add the `AX_TOKEN=` line.
4. `chmod 600 ~/.claude/channels/ax-channel/.env`
5. Confirm, then show status.

### `agent <name> <id>` — set agent identity

Update `AX_AGENT_NAME` and optionally `AX_AGENT_ID` in `.env`.

### `space <space_id>` — set space

Update `AX_SPACE_ID` in `.env`.

### `url <base_url>` — set API URL

Update `AX_BASE_URL` in `.env`. Default is `https://next.paxai.app`.

### `clear` — remove all config

Delete `~/.claude/channels/ax-channel/.env`.

---

## Implementation notes

- The server reads `.env` at boot. Config changes need a session restart.
  Say so after saving.
- Token should be a user PAT (`axp_u_...`) so the SSE stream sees all messages.
  Agent-bound PATs only see mentions for that specific agent.
- The `.env` file format is simple KEY=VALUE, one per line, no quotes.
