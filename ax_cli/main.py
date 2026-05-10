"""aX Platform CLI — Typer app with subcommand registration."""

import sys
from typing import Optional


def _reconfigure_stdio_to_utf8() -> None:
    """Force UTF-8 on stdout/stderr at CLI entry so Rich and our own prints
    don't crash on Windows consoles defaulting to cp1252.

    The classic symptom is ``UnicodeEncodeError: 'charmap' codec can't encode
    character '\\u2192'`` (or any of the table-drawing / arrow / check-mark
    glyphs Rich emits). The fix has to run *before* any module-level code
    that initializes a Rich Console (``ax_cli.output.console`` is the main
    one) — Rich snapshots the stream's encoding when it builds its renderer.

    Uses ``errors='replace'`` so a truly un-encodable codepoint that slips
    through can never crash a CLI run; the user sees a replacement char
    instead of a traceback. Streams that don't expose ``reconfigure``
    (StringIO in tests, redirected pipes that wrap a non-text buffer) are
    left alone.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            # Stream may already be detached, write-only, or refuse the
            # encoding change; nothing actionable we can do at startup.
            pass


_reconfigure_stdio_to_utf8()

import httpx  # noqa: E402  — must follow stdio reconfig
import typer  # noqa: E402

from .commands import (  # noqa: E402
    agents,
    alerts,
    apps,
    auth,
    bootstrap,
    channel,
    context,
    credentials,
    events,
    gateway,
    handoff,
    heartbeat,
    keys,
    listen,
    messages,
    mint,
    profile,
    qa,
    reminders,
    spaces,
    tasks,
    upload,
    watch,
)

app = typer.Typer(name="ax", help="aX Platform CLI", no_args_is_help=True)
app.add_typer(auth.app, name="auth")
app.add_typer(keys.app, name="keys")
app.add_typer(credentials.app, name="credentials")
app.add_typer(agents.app, name="agents")
app.add_typer(apps.app, name="apps")
app.add_typer(messages.app, name="messages")
app.add_typer(alerts.app, name="alerts")
app.add_typer(reminders.app, name="reminders")
app.add_typer(heartbeat.app, name="heartbeat")
app.add_typer(tasks.app, name="tasks")
app.add_typer(events.app, name="events")
app.add_typer(listen.app, name="listen")
app.add_typer(gateway.app, name="gateway")
app.add_typer(context.app, name="context")
app.add_typer(watch.app, name="watch")
app.add_typer(upload.app, name="upload")
app.add_typer(profile.app, name="profile")
app.add_typer(spaces.app, name="spaces")
app.add_typer(channel.app, name="channel")
app.add_typer(mint.app, name="token")
app.add_typer(qa.app, name="qa")
app.command("bootstrap-agent")(bootstrap.bootstrap_agent)
app.command("handoff")(handoff.run)


@app.command("login")
def login(
    token: str = typer.Option(None, "--token", "-t", help="PAT token (prompted securely if omitted)"),
    base_url: str = typer.Option(auth.DEFAULT_LOGIN_BASE_URL, "--url", "-u", help="API base URL"),
    env_name: str = typer.Option(
        None,
        "--env",
        "-e",
        help="Named user-login environment (e.g. dev, next, prod, customer-a)",
    ),
    agent: str = typer.Option(None, "--agent", "-a", help="Agent name or ID (auto-detected if not set)"),
    space_id: str = typer.Option(None, "--space-id", "-s", help="Optional default space ID"),
):
    """Log in to aX. Prompts for a token securely when --token is omitted."""
    auth.login_user(token=token, base_url=base_url, agent=agent, space_id=space_id, env_name=env_name)


@app.command("send")
def send_shortcut(
    content: str = typer.Argument(..., help="Message to send"),
    wait: bool = typer.Option(
        True,
        "--wait/--no-wait",
        "-w",
        help="Wait for a reply after sending. Use --no-wait for intentional notify-only sends.",
    ),
    skip_ax: bool = typer.Option(False, "--skip-ax", help="Deprecated alias for --no-wait.", hidden=True),
    timeout: int = typer.Option(60, "--timeout", "-t", help="Max seconds to wait"),
    reply_to: Optional[str] = typer.Option(None, "--reply-to", "--parent", "-r", help="Reply to message ID (thread)"),
    to: Optional[str] = typer.Option(None, "--to", help="@mention another agent by name"),
    ask_ax: bool = typer.Option(False, "--ask-ax", help="Route this message to aX by prepending @aX"),
    act_as: Optional[str] = typer.Option(
        None, "--act-as", help="Impersonate: send as a different agent. Requires scoped token."
    ),
    files: Optional[list[str]] = typer.Option(
        None,
        "--file",
        "-f",
        help="Attach a local file to this message; creates a transcript preview backed by context metadata (repeatable)",
    ),
    space_id: Optional[str] = typer.Option(None, "--space", "--space-id", "-s", help="Target space id, slug, or name"),
    as_json: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Send a message and wait for a reply by default.

    Use --to for a simple agent mention/intercom. Use `ax handoff` for
    delegated agent work that needs task ownership, response waiting, and
    evidence.

    Use --file when the primary intent is a chat message with an attachment
    preview. Use `ax upload file` when adding the artifact to context is the
    primary event and the transcript should show a compact context signal.
    """
    messages.send(
        content=content,
        wait=False if skip_ax else wait,
        skip_ax=False,
        timeout=timeout,
        to=to,
        ask_ax=ask_ax,
        act_as=act_as,
        files=files,
        channel="main",
        parent=reply_to,
        space_id=space_id,
        as_json=as_json,
    )


def main():
    """Entry point with global error handling."""
    try:
        app()
    except httpx.ConnectError:
        typer.echo("Error: cannot reach aX API. Is the server running?", err=True)
        sys.exit(1)
