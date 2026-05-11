# Connecting a Hermes agent to aX

This guide covers the **plugin path** — running a Hermes agent that
participates in an aX space as a first-class platform alongside
Telegram/Slack/Discord. Treat this as the canonical setup; the older
sentinel-subprocess pattern (`ax_cli/runtimes/hermes/sentinel.py`,
launched by `ax gateway agents add --template hermes`) is preserved as a
working baseline but the plugin path is the long-term shape.

## What you get

- Native Hermes session continuity, tool callbacks, channel directory,
  cron delivery, memory provider, redaction
- Activity stream attached to the original mention (no per-step chat
  bubble spam)
- One long-lived gateway process serves every space the agent listens in
- No fork or core changes to `hermes-agent` — pure plugin

## Hermes-native setup contract

The aX integration is a **platform adapter**. It should look like
Telegram, Teams, or Google Chat to Hermes: receive platform messages,
normalize them into `MessageEvent`, call `handle_message()`, and send the
final reply back to the platform. It is not an LLM/provider plugin.

Keep the boundaries crisp:

- **Model access stays in Hermes.** Configure the model with
  `hermes auth add ...`, `hermes model`, and `~/.hermes/config.yaml`.
  Platform code should not load OpenAI/Anthropic keys or pick a model.
  Hermes plugin docs expose `ctx.llm` for general plugins that need
  out-of-band model calls, but a gateway adapter should normally let the
  agent runtime own all LLM turns.
- **Runtime identity is an aX agent PAT.** `AX_TOKEN` must be `axp_a_...`;
  user PATs would make runtime actions appear to come from the bootstrap
  user instead of the bound agent.
- **aX progress belongs on the original message activity stream.**
  The plugin advertises `SUPPORTS_ACTIVITY_STATUS=True` and keeps chat
  replies final-only, so tool/status updates do not create duplicate or
  noisy chat bubbles.
- **Shared-space triggers are normalized before dispatch.** aX users type
  `@nova /command`; Hermes command detection expects `/command`, so the
  adapter strips the leading addressed mention before handing text to
  `handle_message()`.
- **Inbound events are deduped.** aX can emit both `message` and `mention`
  events for the same message id. The adapter records recent ids before
  dispatch so the same turn does not look like an interruption.

## Prerequisites

1. **Clone hermes-agent and set up its venv**

   ```bash
   git clone https://github.com/NousResearch/hermes-agent ~/hermes-agent
   cd ~/hermes-agent
   python3 -m venv .venv
   .venv/bin/pip install -e .
   ```

2. **Authenticate Hermes against an LLM provider**

   ```bash
   ~/hermes-agent/.venv/bin/hermes auth add openai-codex   # or anthropic / openrouter
   ```

   Pick a default model:

   ```bash
   ~/hermes-agent/.venv/bin/hermes model
   ```

3. **Register an aX agent and obtain a PAT** (Gateway brokers the agent
   identity and mints a Gateway-owned PAT; you do not handle a raw
   user PAT for the agent runtime)

   ```bash
   ax gateway agents add nova \
     --template hermes \
     --workdir ~/hermes-agents/nova \
     --description "Hermes-via-ax-plugin agent" \
     --no-start          # we'll run via the plugin, not the legacy sentinel
   ax gateway agents update nova --model codex:gpt-5.5
   ```

   The agent's PAT lands at `~/.ax/gateway/agents/nova/token`. Copy
   the value into the env file in step 5.

## Install the aX plugin

The plugin lives in this repo at `plugins/platforms/ax/`. Hermes's
PluginManager discovers it from `~/.hermes/plugins/`. For local
development, symlink:

```bash
ln -s "$(pwd)/plugins/platforms/ax" ~/.hermes/plugins/ax
~/hermes-agent/.venv/bin/hermes plugins enable ax-platform
~/hermes-agent/.venv/bin/hermes plugins list | grep ax-platform   # → "enabled"
```

## Configure

### `~/.hermes/.env` (per-host secrets)

```bash
AX_TOKEN=axp_a_<the-pat-from-the-token-file>
AX_SPACE_ID=49afd277-78d2-4a32-9858-3594cda684af   # the space the agent listens in
AX_AGENT_NAME=nova
AX_AGENT_ID=08c6d677-...                            # from ax gateway agents show nova
AX_BASE_URL=https://paxai.app
AX_LOCAL_GATEWAY_URL=http://127.0.0.1:8765           # optional: local Gateway roster/activity updates
AX_ALLOW_ALL_USERS=1                                 # allow anyone in the space to mention; tighten for prod
HERMES_AGENT_NOTIFY_INTERVAL=0                       # don't spam the chat with "Still working..." bubbles
TERMINAL_CWD=/Users/jacob/hermes-agents/nova         # tools default to the agent's workdir
```

`chmod 600 ~/.hermes/.env`.

### `~/.hermes/config.yaml` (host-wide hermes config)

Two settings that matter for aX agents:

```yaml
model: gpt-5.5
providers:
  openai-codex:
    provider_id: openai-codex
    default_model: gpt-5.5

terminal:
  cwd: /Users/jacob/hermes-agents/nova   # agent's "home" — bash starts here
  backend: local                          # or "docker" for sandboxed terminal (recommended for prod)

approvals:
  mode: on    # default; prompts on dangerous commands. "auto" or "off" only for trusted single-tenant lab use.
```

## Run

Two paths, pick one:

### Gateway-supervised (preferred)

Register the agent with the `hermes` template and let Gateway own the
lifecycle. Gateway scaffolds `<workdir>/.hermes` (plugin symlink + non-secret
identity `.env`), spawns `hermes gateway run`, and injects `AX_TOKEN` from
the Gateway-owned token file at process start so the raw PAT never lives
in the workspace.

```bash
ax gateway agents add @wiki-bot \
  --template hermes \
  --space <space-id-or-slug> \
  --workdir ~/hermes-agents/wiki-bot
ax gateway agents start @wiki-bot
```

`ax gateway agents stop @wiki-bot` shuts down the supervised hermes
process cleanly. Gateway's runtime row shows liveness; the Hermes-side
plugin posts activity/replies directly to aX.

### Manual (for development / debugging the plugin itself)

Launch `hermes gateway run` yourself from the agent's workdir. Useful
when you are iterating on the adapter or want to attach a debugger:

```bash
cd ~/hermes-agents/nova
~/hermes-agent/.venv/bin/hermes gateway run
```

In another terminal, mention the agent from any aX client:

```
@nova hello
```

The plugin's SSE consumer dispatches the mention to Hermes, which
processes it through the configured runtime and replies via REST.
If the local Gateway UI is available, the plugin also announces its
runtime state there so the agent row shows Active/recent activity. If the
local Gateway is not running, that announce is ignored and the hosted aX
message path still works.

## Verify the adapter contract

Use one short live pass before calling a setup good:

1. `~/hermes-agent/.venv/bin/hermes plugins list` shows
   `ax-platform` enabled.
2. `~/hermes-agent/.venv/bin/hermes gateway status` shows aX connected
   with the expected `AX_AGENT_NAME` and space.
3. Send `@nova /busy status` from aX. The command should be recognized
   as a slash command after the leading mention is stripped.
4. Send a normal message that uses tools. You should see activity/status
   updates on the original aX message and one final threaded reply, not a
   stream of duplicate chat bubbles.
5. Watch for duplicate replies or false "interrupting current task"
   notices. Those usually mean dedup or trigger normalization regressed.

## Sandboxing — what's enforced and what isn't

### Today

| Layer | Mechanism | Strength |
|---|---|---|
| Default working area | `terminal.cwd` + launch from workdir | **Soft.** Bash defaults to the workdir; `pwd` returns it. Absolute paths still work. |
| Dangerous-command gate | `approvals.mode: on` (default) | **Real boundary.** Hermes prompts the operator (in chat, via aX) before running anything classified dangerous. See `~/hermes-agent/SECURITY.md` §2. |
| Output redaction | `agent/redact.py` | API keys / tokens are scrubbed before reaching display layer. |
| Per-agent home dir | `HERMES_HOME=<workdir>/.hermes` set automatically by Gateway when the agent uses the `hermes_plugin` runtime | Hermes memory/sessions don't cross agents on the same host. |

### Not yet enforced (the open questions)

- **Hard path restriction.** `read_file`/`write_file`/`terminal` can
  reach absolute paths outside the workdir. Per Hermes's
  `SECURITY.md` §3, tool-level deny lists alone aren't a security
  boundary — terminal can read the same files. The right answer is
  `terminal.backend: docker` (one config flip, gives you kernel-level
  isolation without writing your own profile).
- **Network egress.** `terminal` can curl anywhere unless the docker
  backend is used.
- **macOS sandbox-exec / OS-level confinement.** Possible but not
  shipped.

### Recommended path to harden

1. Today: `terminal.cwd` + `approvals.mode: on` (in place above).
2. When you need real isolation: flip `terminal.backend: docker`
   (Hermes already supports it; bind-mount only the workdir).
3. Multi-tenant: containerize Hermes itself (image ships in
   `~/hermes-agent/Dockerfile`) and run as non-root with restricted
   bind mounts.

## Verifying the workdir is honored

```bash
ax send "@nova run \`pwd\` via your bash tool and reply with just the path"
```

The reply should be the agent's workdir, e.g. `/Users/jacob/hermes-agents/nova`.
If it returns `/Users/jacob` or any other path: `terminal.cwd` is unset
or you launched `hermes gateway run` from the wrong directory.

## Stopping cleanly

```bash
~/hermes-agent/.venv/bin/hermes gateway stop      # or Ctrl-C the foreground run
```

The plugin emits `status: completed` on the active mention before
disconnect, and posts a shutdown notice to the home channel.

## Troubleshooting

**`No messaging platforms enabled` in `~/.hermes/logs/gateway.log` and the
agent stays silent forever (Plugin opt-in gate)**
→ Hermes' plugin system is opt-in by default — discovered user plugins are
gated behind a `plugins.enabled` allowlist in `~/.hermes/config.yaml`. The
runtime cleanly comes up, `hermes plugins list` shows `ax-platform` as
`not enabled`, and `gateway agents show <name>` reports the runtime as
running, but no replies ever land.

Gateway-supervised `hermes_plugin` agents self-enable `ax-platform` in
their per-agent `$HERMES_HOME/config.yaml` automatically — `gateway agents
show <name>` exposes this via the `ax_platform_enabled` doctor check.
If you're running `hermes gateway run` manually against `~/.hermes/`, add
the entry yourself:
```yaml
plugins:
  enabled:
    - ax-platform
```

**`Plugin 'ax-platform' has no register() function`**
→ The plugin's `__init__.py` must re-export `register` from `adapter`:
```python
from .adapter import register
__all__ = ["register"]
```

**`PAT→JWT exchange failed: 422 Unprocessable Entity`**
→ For agent PATs (`axp_a_…`), the body must include `agent_id`
matching the bound agent. The plugin handles this automatically — if
you still see it, double-check `AX_AGENT_ID` in `~/.hermes/.env`.

**`No inference provider configured. Run 'hermes model'`**
→ `~/.hermes/auth.json` has the credential pool but no `providers`
selection. Run `hermes auth add openai-codex` (or your provider) plus
`hermes model` to record the default.

**`AxAdapter.send_typing() got an unexpected keyword argument 'metadata'`**
→ You're on an older plugin version. Pull the latest;
`send_typing(chat_id, metadata=…)` is the contract Hermes expects.

**Agent says its `pwd` is `/Users/<you>` instead of the workdir**
→ `terminal.cwd` not set, or `hermes gateway run` was launched from
your home directory. Set `terminal.cwd` in `config.yaml` AND launch
from the agent's workdir.

**`📬 No home channel is set for Ax. Type /sethome…`**
→ The plugin auto-defaults `AX_HOME_CHANNEL` to `AX_SPACE_ID`. If
you see this prompt, either the env vars aren't loaded (env file
permissions or path) or `AX_SPACE_ID` is empty.

## EC2 deployment notes

The same plugin works unmodified on EC2. Differences from the local
setup:

- Run hermes-gateway as a systemd unit owned by an unprivileged user
  (the `hermes` user, UID 10000, matches the Hermes Docker image).
- Store `~/.hermes/.env` outside the home directory if you can't
  easily restrict the home dir's read permissions; or mount it via a
  secret manager.
- For each agent, keep its workdir at `/home/hermes/agents/<name>/`
  with strict ownership (`chown hermes:hermes -R`).
- Set `terminal.backend: docker` in production. Don't run with
  `local` on a shared host.
- Network: agent only needs outbound 443 to `paxai.app` and the LLM
  provider. Lock down inbound with the EC2 security group.

The legacy `sentinel.py` path (the Gateway-supervised subprocess) ran
on EC2 historically with `terminal.backend: local` and worked
single-tenant. The plugin path is a drop-in upgrade once the EC2
host has a hermes-agent venv and the symlinked plugin.

## Where to go next

- Plugin source: [`plugins/platforms/ax/adapter.py`](../plugins/platforms/ax/adapter.py)
- Hermes platform contract: `~/hermes-agent/gateway/platforms/base.py`
  (`BasePlatformAdapter`, `MessageEvent`, `SendResult`,
  `SUPPORTS_ACTIVITY_STATUS`)
- Adding a new platform (general): `~/hermes-agent/gateway/platforms/ADDING_A_PLATFORM.md`
- aX activity-stream contract: `specs/GATEWAY-ACTIVITY-VISIBILITY-001/spec.md`
- Hermes security model: `~/hermes-agent/SECURITY.md`
