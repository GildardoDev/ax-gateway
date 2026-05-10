# Handoff — Hermes aX platform plugin

**Branch:** `feat/hermes-ax-platform-plugin` (pushed; PR not yet opened)
**Status:** MVP done, locally verified end-to-end. Five commits on top of `main`.

## TL;DR

We replaced the per-mention sentinel-subprocess pattern with a first-class
**Hermes platform plugin** that registers aX as a native messaging platform
alongside Telegram/Slack/Discord. Hermes runs as a single long-lived
`hermes gateway run` process; aX mentions arrive via SSE; replies post
via REST and thread under the original.

The plugin is at `plugins/platforms/ax/`. Symlink it to
`~/.hermes/plugins/ax/` and Hermes discovers it on startup.

## Commits on this branch

```
9acdaba  feat: periodic heartbeat (fixes "nova showing offline")
0a82b55  feat: route progress through aX activity stream, not chat bubbles
38f142b  fix: home channel auto-default, edit_message, typing kwargs
d11941a  fix: agent PAT exchange shape (AUTH-SPEC-001 §13)
d6aeb20  feat: MVP Hermes platform adapter for aX network
```

## What's running locally right now

```bash
$ pgrep -af "hermes gateway run|sentinel.py"
<pid>  axiom — sentinel.py --agent axiom (Gateway-supervised, legacy path)
<pid>  nova  — hermes gateway run        (plugin path — this branch's work)
```

Both alive, both sandboxed to `~/hermes-agents/<name>/` (`terminal.cwd`),
both replying to mentions. axiom is the working baseline; nova is the
new plugin path.

## Verifying the plugin still works in a new session

```bash
# 1. Plugin discovery
~/hermes-agent/.venv/bin/hermes plugins list | grep ax-platform   # → enabled

# 2. nova process still running?
pgrep -af "hermes gateway run"

# 3. If not, restart from the agent's workdir (sandbox starts here):
cd ~/hermes-agents/nova && ~/hermes-agent/.venv/bin/hermes gateway run

# 4. Smoke test from the ax-gateway shell
ax send "@nova hi from $(whoami) on $(hostname)"
# expect a thread reply within ~10s; activity bubble shows tool calls live
```

`docs/SETUP-HERMES.md` has the full operator guide (install, env, sandboxing,
troubleshooting). Read that first if anything below is unclear.

## Architecture (one-liner)

```
aX UI / agents
      │
      ▼  SSE /api/v1/sse/messages    REST POST /api/v1/messages
┌──────────────────┐                 ▲
│ AxAdapter        │─── sends ───────┘
│ plugins/.../ax/  │
└────────┬─────────┘
         │ MessageEvent          reply text
         ▼                       ▲
┌──────────────────┐             │
│ Hermes gateway   │─── runs ────┘
│ AIAgent + tools  │
└──────────────────┘
```

Class flags that drive Hermes-side behavior:

```python
SUPPORTS_MESSAGE_EDITING = False   # don't stream edits to a chat bubble
SUPPORTS_ACTIVITY_STATUS = True    # route tool/activity to the original
                                   # mention's processing-status stream
```

## Identity model

One `AxAdapter` instance = one aX agent identity bound to one space.

| Setting | Source |
|---|---|
| `AX_TOKEN` | agent PAT (`axp_a_…`) minted by `ax gateway agents add` |
| `AX_SPACE_ID` | UUID of the space the agent listens in |
| `AX_AGENT_NAME` | `@name` (without `@`) |
| `AX_AGENT_ID` | UUID — required for agent_access PAT exchange and heartbeats |

Lives in `~/.hermes/.env` (chmod 600). `_env_enablement` in `adapter.py`
auto-defaults `AX_HOME_CHANNEL=AX_SPACE_ID` so the
`📬 No home channel set` prompt doesn't fire on first mention.

## Confirmed working end-to-end

- Inbound @-mention via SSE → MessageEvent → Hermes runtime → reply ✓
- Tool-call activity renders on the original message's activity bubble
  (`🖥 terminal: "..."`, `🔍 read_file: ...`, etc.) ✓
- Sandboxed to workdir — `pwd` returns `/Users/jacob/hermes-agents/nova`,
  not `/Users/jacob` ✓
- Heartbeat every 30s → online dot turns green in aX UI ✓
- Approval gate fires on dangerous bash (`bash -lc ...`) — Hermes default ✓
- Auto-summary / context compression works (warnings cleared after
  setting `providers.openai-codex.default_model`) ✓

## Known imperfections (tracked as aX tasks 13–17)

| # | Issue | Severity |
|---|---|---|
| 13 | "final stream delivery not confirmed" warning still logs | low — chat reply still lands |
| 14 | Workdir not shown on the agent row in Gateway UI | UX nice-to-have, ~30 lines |
| 15 | `terminal.cwd` is soft confinement; abs paths still work | medium — needs `terminal.backend: docker` for prod |
| 16 | axiom (legacy sentinel) has same path-guard hole | medium — pair with #15 |
| 17 | Gateway UI Start/Stop buttons no-op for plugin agents | UX clarity needed |

Plus the legacy aX task **`4bb409ff`** (vendored `tools/` shim collision) —
this is what the plugin path *avoids* by going around the sentinel.
The plugin path is partly a workaround for that bug; the bug itself is
still real for axiom.

## Files to know

```
plugins/platforms/ax/
  __init__.py          re-exports register
  adapter.py           AxAdapter + register(ctx) + _env_enablement
  plugin.yaml          plugin manifest (name, env vars, label)
  README.md            install + usage

docs/SETUP-HERMES.md   full operator guide (install, sandbox, troubleshoot)
docs/HANDOFF-hermes-plugin.md   this file

tests/test_ax_adapter_activity.py
                       3 unit tests; skips cleanly when hermes-agent
                       isn't on sys.path (run under hermes venv to
                       exercise: ~/hermes-agent/.venv/bin/python3 -m pytest)
```

## Resume points

1. **Open the PR** — branch is pushed; URL printed at
   https://github.com/ax-platform/ax-gateway/pull/new/feat/hermes-ax-platform-plugin
2. **Pick the next task** — task #14 (workdir on row) is small and
   user-visible; task #15 (`terminal.backend: docker`) is the right
   next security step.
3. **EC2 deploy** — see SETUP-HERMES.md "EC2 deployment notes"; not yet
   started. Same plugin works unmodified; differences are systemd unit,
   non-root user, secret manager.
4. **Upstream PR to NousResearch/hermes-agent** — once polished, drop
   `plugins/platforms/ax/` into hermes-agent's `plugins/platforms/`
   alongside `irc/`, `teams/`, `google_chat/`. We're the first
   multi-agent platform adapter — worth highlighting.

## Working-tree warnings for whoever picks up

The branch was clean at commit time, but the working tree has
**uncommitted changes that aren't this branch's work**:

```
M  ax_cli/commands/gateway.py        ┐ ~239 lines from another session
M  ax_cli/static/demo.html           │ (localStorage persistence for
M  tests/test_gateway_commands.py    │  space filter + system-agent
M  tests/test_gateway_host_header.py │  toggles + matching tests).
M  tests/test_gateway_ui_static.py   ┘ Should be committed separately,
                                      not bundled into the plugin PR.

??  docs/pr-review-tracker*          PR-review tooling, unrelated
??  scripts/build_pr_review_tracker_*
??  oss-library-source-review.session-notes.md
```

Triage these before opening the plugin PR so the diff is clean.

## Critical reminders

- **`~/.hermes/.env` contains a real agent PAT** (`AX_TOKEN=axp_a_…`).
  Do not commit it. The doc uses placeholders only.
- **Plugin agents (nova) are NOT controlled by Gateway UI's Start/Stop**
  buttons. They run in an external `hermes gateway run` process.
  Until task #17 lands, this is a known UX gap — operators must
  start/stop hermes-gateway from the terminal.
- **Sandboxing is soft** — `terminal.cwd` is a default working area,
  not a hard boundary. Per Hermes `SECURITY.md` §3, the canonical
  hardening path is `terminal.backend: docker`. Don't use this in
  multi-tenant or untrusted-prompt contexts without that flip.

## State of the conversation when we paused

Last user direction: "this hermes plugin is good, can you give a
handoff summary so we can pick up in a new session?"

Last system state: branch pushed, both agents online and sandboxed,
all 5 plugin commits landed, this handoff doc to be written next.
