"""ax gateway — local Gateway control plane."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import typer
from rich import box
from rich.columns import Columns
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..client import AxClient
from ..commands import auth as auth_cmd
from ..commands.bootstrap import (
    _create_agent_in_space,
    _find_agent_in_space,
    _mint_agent_pat,
    _polish_metadata,
)
from ..config import resolve_user_base_url, resolve_user_token
from ..gateway import (
    GatewayDaemon,
    agent_token_path,
    annotate_runtime_health,
    daemon_status,
    find_agent_entry,
    gateway_dir,
    load_gateway_registry,
    load_gateway_session,
    load_recent_gateway_activity,
    record_gateway_activity,
    remove_agent_entry,
    save_gateway_registry,
    save_gateway_session,
    upsert_agent_entry,
)
from ..output import JSON_OPTION, console, err_console, print_json, print_table

app = typer.Typer(name="gateway", help="Run the local Gateway control plane", no_args_is_help=True)
agents_app = typer.Typer(name="agents", help="Manage Gateway-controlled agents", no_args_is_help=True)
app.add_typer(agents_app, name="agents")

_STATE_STYLES = {
    "running": "green",
    "starting": "cyan",
    "reconnecting": "yellow",
    "stale": "yellow",
    "error": "red",
    "stopped": "dim",
}
_STATE_ORDER = {
    "running": 0,
    "starting": 1,
    "reconnecting": 2,
    "stale": 3,
    "error": 4,
    "stopped": 5,
}


def _resolve_gateway_login_token(explicit_token: str | None) -> str:
    if explicit_token and explicit_token.strip():
        return auth_cmd._resolve_login_token(explicit_token)
    existing = resolve_user_token()
    if existing:
        err_console.print("[cyan]Using existing axctl user login for Gateway bootstrap.[/cyan]")
        return existing
    return auth_cmd._resolve_login_token(None)


def _load_gateway_user_client() -> AxClient:
    session = load_gateway_session()
    if not session:
        err_console.print("[red]Gateway is not logged in.[/red] Run `ax gateway login` first.")
        raise typer.Exit(1)
    token = str(session.get("token") or "")
    if not token:
        err_console.print("[red]Gateway session is missing its bootstrap token.[/red]")
        raise typer.Exit(1)
    if not token.startswith("axp_u_"):
        err_console.print("[red]Gateway bootstrap currently requires a user PAT (axp_u_).[/red]")
        raise typer.Exit(1)
    return AxClient(base_url=str(session.get("base_url") or auth_cmd.DEFAULT_LOGIN_BASE_URL), token=token)


def _load_gateway_session_or_exit() -> dict:
    session = load_gateway_session()
    if not session:
        err_console.print("[red]Gateway is not logged in.[/red] Run `ax gateway login` first.")
        raise typer.Exit(1)
    return session


def _save_agent_token(name: str, token: str) -> Path:
    token_path = agent_token_path(name)
    token_path.write_text(token.strip() + "\n")
    token_path.chmod(0o600)
    return token_path


def _load_managed_agent_or_exit(name: str) -> dict:
    registry = load_gateway_registry()
    entry = find_agent_entry(registry, name)
    if not entry:
        err_console.print(f"[red]Managed agent not found:[/red] {name}")
        raise typer.Exit(1)
    return entry


def _load_managed_agent_client(entry: dict) -> AxClient:
    token_file = Path(str(entry.get("token_file") or "")).expanduser()
    if not token_file.exists():
        err_console.print(f"[red]Managed agent token is missing:[/red] {token_file}")
        raise typer.Exit(1)
    token = token_file.read_text().strip()
    if not token:
        err_console.print(f"[red]Managed agent token file is empty:[/red] {token_file}")
        raise typer.Exit(1)
    return AxClient(
        base_url=str(entry.get("base_url") or ""),
        token=token,
        agent_name=str(entry.get("name") or ""),
        agent_id=str(entry.get("agent_id") or "") or None,
    )


def _status_payload(*, activity_limit: int = 10) -> dict:
    daemon = daemon_status()
    session = load_gateway_session()
    registry = daemon["registry"]
    agents = [annotate_runtime_health(agent) for agent in registry.get("agents", [])]
    running_agents = [a for a in agents if str(a.get("effective_state")) == "running"]
    connected_agents = [a for a in agents if bool(a.get("connected"))]
    stale_agents = [a for a in agents if str(a.get("effective_state")) == "stale"]
    errored_agents = [a for a in agents if str(a.get("effective_state")) == "error"]
    gateway = dict(registry.get("gateway", {}))
    if not daemon["running"]:
        gateway["effective_state"] = "stopped"
        gateway["pid"] = None
    return {
        "gateway_dir": str(gateway_dir()),
        "connected": bool(session),
        "base_url": session.get("base_url") if session else None,
        "space_id": session.get("space_id") if session else None,
        "user": session.get("username") if session else None,
        "daemon": {
            "running": daemon["running"],
            "pid": daemon["pid"],
        },
        "gateway": gateway,
        "agents": agents,
        "recent_activity": load_recent_gateway_activity(limit=activity_limit),
        "summary": {
            "managed_agents": len(agents),
            "running_agents": len(running_agents),
            "connected_agents": len(connected_agents),
            "stale_agents": len(stale_agents),
            "errored_agents": len(errored_agents),
        },
    }


def _parse_iso8601(value: object) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _age_seconds(value: object) -> int | None:
    parsed = _parse_iso8601(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()))


def _format_age(seconds: object) -> str:
    if seconds is None:
        return "-"
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return "-"
    if total < 60:
        return f"{total}s"
    minutes, seconds = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours:02d}h"


def _format_timestamp(value: object) -> str:
    return _format_age(_age_seconds(value))


def _state_text(state: object) -> Text:
    label = str(state or "unknown").lower()
    style = _STATE_STYLES.get(label, "white")
    return Text(f"● {label}", style=style)


def _metric_panel(label: str, value: object, *, tone: str = "cyan", subtitle: str | None = None) -> Panel:
    body = Text()
    body.append(str(value), style=f"bold {tone}")
    body.append(f"\n{label}", style="dim")
    if subtitle:
        body.append(f"\n{subtitle}", style="dim")
    return Panel(body, border_style=tone, padding=(1, 2))


def _sorted_agents(agents: list[dict]) -> list[dict]:
    return sorted(
        agents,
        key=lambda agent: (
            _STATE_ORDER.get(str(agent.get("effective_state") or "").lower(), 99),
            str(agent.get("name") or "").lower(),
        ),
    )


def _render_gateway_overview(payload: dict) -> Panel:
    gateway = payload.get("gateway") or {}
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column(ratio=2)
    grid.add_column(style="bold")
    grid.add_column(ratio=2)
    grid.add_row("Gateway", str(gateway.get("gateway_id") or "-")[:8], "Daemon", "running" if payload["daemon"]["running"] else "stopped")
    grid.add_row("User", str(payload.get("user") or "-"), "Base URL", str(payload.get("base_url") or "-"))
    grid.add_row("Space", str(payload.get("space_id") or "-"), "PID", str(payload["daemon"].get("pid") or "-"))
    grid.add_row(
        "Session",
        "connected" if payload.get("connected") else "disconnected",
        "Last Reconcile",
        _format_timestamp(gateway.get("last_reconcile_at")),
    )
    return Panel(grid, title="Gateway Overview", border_style="cyan")


def _render_agent_table(agents: list[dict]) -> Table:
    table = Table(expand=True, box=box.SIMPLE_HEAVY)
    table.add_column("Agent", style="bold")
    table.add_column("Type")
    table.add_column("State")
    table.add_column("Phase")
    table.add_column("Queue", justify="right")
    table.add_column("Seen", justify="right")
    table.add_column("Processed", justify="right")
    table.add_column("Activity", overflow="fold")
    if not agents:
        table.add_row("No managed agents", "-", Text("● stopped", style="dim"), "-", "0", "-", "0", "-")
        return table
    for agent in _sorted_agents(agents):
        activity = str(agent.get("current_activity") or agent.get("current_tool") or agent.get("last_reply_preview") or "-")
        table.add_row(
            f"@{agent.get('name')}",
            str(agent.get("runtime_type") or "-"),
            _state_text(agent.get("effective_state")),
            str(agent.get("current_status") or "-"),
            str(agent.get("backlog_depth") or 0),
            _format_age(agent.get("last_seen_age_seconds")),
            str(agent.get("processed_count") or 0),
            activity,
        )
    return table


def _render_activity_table(activity: list[dict]) -> Table:
    table = Table(expand=True, box=box.SIMPLE_HEAVY)
    table.add_column("When", justify="right", no_wrap=True)
    table.add_column("Event", no_wrap=True)
    table.add_column("Agent", no_wrap=True)
    table.add_column("Detail", overflow="fold")
    if not activity:
        table.add_row("-", "idle", "-", "No activity yet")
        return table
    for item in activity:
        detail = (
            item.get("activity_message")
            or item.get("reply_preview")
            or item.get("tool_name")
            or item.get("error")
            or item.get("message_id")
            or "-"
        )
        agent_name = item.get("agent_name")
        table.add_row(
            _format_timestamp(item.get("ts")),
            str(item.get("event") or "-"),
            f"@{agent_name}" if agent_name else "-",
            str(detail),
        )
    return table


def _render_gateway_dashboard(payload: dict) -> Group:
    agents = payload.get("agents", [])
    summary = payload.get("summary", {})
    queue_depth = sum(int(agent.get("backlog_depth") or 0) for agent in agents)
    metrics = Columns(
        [
            _metric_panel("managed agents", summary.get("managed_agents", 0), tone="cyan"),
            _metric_panel("connected", summary.get("connected_agents", 0), tone="green"),
            _metric_panel("stale", summary.get("stale_agents", 0), tone="yellow"),
            _metric_panel("errors", summary.get("errored_agents", 0), tone="red"),
            _metric_panel("queue depth", queue_depth, tone="blue"),
        ],
        expand=True,
        equal=True,
    )
    return Group(
        _render_gateway_overview(payload),
        metrics,
        Panel(_render_agent_table(agents), title="Managed Agents", border_style="green"),
        Panel(_render_activity_table(payload.get("recent_activity", [])), title="Recent Activity", border_style="magenta"),
    )


def _render_agent_detail(entry: dict, *, activity: list[dict]) -> Group:
    overview = Table.grid(expand=True, padding=(0, 2))
    overview.add_column(style="bold")
    overview.add_column(ratio=2)
    overview.add_column(style="bold")
    overview.add_column(ratio=2)
    overview.add_row("Agent", f"@{entry.get('name')}", "Runtime", str(entry.get("runtime_type") or "-"))
    overview.add_row("Desired", str(entry.get("desired_state") or "-"), "Effective", str(entry.get("effective_state") or "-"))
    overview.add_row("Connected", "yes" if entry.get("connected") else "no", "Queue", str(entry.get("backlog_depth") or 0))
    overview.add_row("Seen", _format_age(entry.get("last_seen_age_seconds")), "Reconnect", _format_age(entry.get("reconnect_backoff_seconds")))
    overview.add_row("Processed", str(entry.get("processed_count") or 0), "Dropped", str(entry.get("dropped_count") or 0))
    overview.add_row("Last Work", _format_timestamp(entry.get("last_work_received_at")), "Completed", _format_timestamp(entry.get("last_work_completed_at")))
    overview.add_row("Phase", str(entry.get("current_status") or "-"), "Activity", str(entry.get("current_activity") or "-"))
    overview.add_row("Tool", str(entry.get("current_tool") or "-"), "Transport", str(entry.get("transport") or "-"))
    overview.add_row("Cred Source", str(entry.get("credential_source") or "-"), "Space", str(entry.get("space_id") or "-"))
    overview.add_row("Agent ID", str(entry.get("agent_id") or "-"), "Last Reply", str(entry.get("last_reply_preview") or "-"))
    overview.add_row("Last Error", str(entry.get("last_error") or "-"), "", "")

    paths = Table.grid(expand=True, padding=(0, 2))
    paths.add_column(style="bold")
    paths.add_column(ratio=3)
    paths.add_row("Token File", str(entry.get("token_file") or "-"))
    paths.add_row("Workdir", str(entry.get("workdir") or "-"))
    paths.add_row("Exec", str(entry.get("exec_command") or "-"))
    paths.add_row("Added", _format_timestamp(entry.get("added_at")))

    return Group(
        Panel(overview, title=f"Managed Agent · @{entry.get('name')}", border_style="cyan"),
        Panel(paths, title="Runtime Details", border_style="blue"),
        Panel(_render_activity_table(activity), title="Recent Agent Activity", border_style="magenta"),
    )


@app.command("login")
def login(
    token: str = typer.Option(None, "--token", "-t", help="User PAT (prompted or reused from axctl login when omitted)"),
    base_url: str = typer.Option(None, "--url", "-u", help="API base URL (defaults to existing axctl login or paxai.app)"),
    space_id: str = typer.Option(None, "--space-id", "-s", help="Optional default space for managed agents"),
    as_json: bool = JSON_OPTION,
):
    """Store the Gateway bootstrap session.

    The Gateway keeps the user PAT centrally and uses it to mint agent PATs for
    managed runtimes. Managed runtimes themselves never receive the PAT or JWT.
    """
    resolved_token = _resolve_gateway_login_token(token)
    if not resolved_token.startswith("axp_u_"):
        err_console.print("[red]Gateway bootstrap requires a user PAT (axp_u_).[/red]")
        raise typer.Exit(1)
    resolved_base_url = base_url or resolve_user_base_url() or auth_cmd.DEFAULT_LOGIN_BASE_URL

    err_console.print(f"[cyan]Verifying Gateway login against {resolved_base_url}...[/cyan]")
    from ..token_cache import TokenExchanger

    try:
        exchanger = TokenExchanger(resolved_base_url, resolved_token)
        exchanger.get_token(
            "user_access",
            scope="messages tasks context agents spaces search",
            force_refresh=True,
        )
        client = AxClient(base_url=resolved_base_url, token=resolved_token)
        me = client.whoami()
    except Exception as exc:
        err_console.print(f"[red]Gateway login failed:[/red] {exc}")
        raise typer.Exit(1)

    selected_space = space_id
    if not selected_space:
        try:
            spaces = client.list_spaces()
            space_list = spaces.get("spaces", spaces) if isinstance(spaces, dict) else spaces
            selected = auth_cmd._select_login_space([s for s in space_list if isinstance(s, dict)])
            if selected:
                selected_space = auth_cmd._candidate_space_id(selected)
        except Exception:
            selected_space = None

    payload = {
        "token": resolved_token,
        "base_url": resolved_base_url,
        "principal_type": "user",
        "space_id": selected_space,
        "username": me.get("username"),
        "email": me.get("email"),
        "saved_at": None,
    }
    path = save_gateway_session(payload)
    registry = load_gateway_registry()
    registry.setdefault("gateway", {})
    registry["gateway"]["session_connected"] = True
    save_gateway_registry(registry)
    record_gateway_activity("gateway_login", username=me.get("username"), base_url=resolved_base_url, space_id=selected_space)

    result = {
        "session_path": str(path),
        "base_url": resolved_base_url,
        "space_id": selected_space,
        "username": me.get("username"),
        "email": me.get("email"),
    }
    if as_json:
        print_json(result)
    else:
        err_console.print(f"[green]Gateway login saved:[/green] {path}")
        for key, value in result.items():
            err_console.print(f"  {key} = {value}")


@app.command("status")
def status(as_json: bool = JSON_OPTION):
    """Show Gateway status, daemon state, and managed runtimes."""
    payload = _status_payload()
    if as_json:
        print_json(payload)
        return

    err_console.print("[bold]ax gateway status[/bold]")
    err_console.print(f"  gateway_dir = {payload['gateway_dir']}")
    err_console.print(f"  connected   = {payload['connected']}")
    err_console.print(f"  daemon      = {'running' if payload['daemon']['running'] else 'stopped'}")
    if payload["daemon"]["pid"]:
        err_console.print(f"  pid         = {payload['daemon']['pid']}")
    err_console.print(f"  base_url    = {payload['base_url']}")
    err_console.print(f"  space_id    = {payload['space_id']}")
    err_console.print(f"  user        = {payload['user']}")
    err_console.print(f"  agents      = {payload['summary']['managed_agents']}")
    err_console.print(f"  connected   = {payload['summary']['connected_agents']}")
    if payload["agents"]:
        print_table(
            ["Agent", "Type", "Desired", "Effective", "Phase", "Seen", "Backlog", "Activity", "Last Error"],
            payload["agents"],
            keys=[
                "name",
                "runtime_type",
                "desired_state",
                "effective_state",
                "current_status",
                "last_seen_age_seconds",
                "backlog_depth",
                "current_activity",
                "last_error",
            ],
        )
    if payload["recent_activity"]:
        print_table(
            ["Time", "Event", "Agent", "Message", "Preview"],
            payload["recent_activity"],
            keys=["ts", "event", "agent_name", "message_id", "reply_preview"],
        )


@app.command("watch")
def watch(
    interval: float = typer.Option(2.0, "--interval", "-n", help="Dashboard refresh interval in seconds"),
    activity_limit: int = typer.Option(8, "--activity-limit", help="Number of recent events to display"),
    once: bool = typer.Option(False, "--once", help="Render one dashboard frame and exit"),
):
    """Watch the Gateway in a live terminal dashboard."""

    def render_dashboard() -> Group:
        return _render_gateway_dashboard(_status_payload(activity_limit=activity_limit))

    if once:
        console.print(render_dashboard())
        return

    try:
        with Live(render_dashboard(), console=console, screen=True, auto_refresh=False) as live:
            while True:
                live.update(render_dashboard(), refresh=True)
                time.sleep(interval)
    except KeyboardInterrupt:
        err_console.print("[yellow]Gateway watch stopped.[/yellow]")


@app.command("run")
def run(
    poll_interval: float = typer.Option(1.0, "--poll-interval", help="Registry reconcile interval in seconds"),
    once: bool = typer.Option(False, "--once", help="Run one reconcile pass and exit"),
):
    """Run the foreground Gateway supervisor."""
    _load_gateway_session_or_exit()
    err_console.print("[bold]ax gateway[/bold] — local control plane")
    err_console.print(f"  state_dir = {gateway_dir()}")
    err_console.print(f"  interval  = {poll_interval}s")
    err_console.print(f"  mode      = {'single-pass' if once else 'foreground'}")
    daemon = GatewayDaemon(logger=lambda msg: err_console.print(f"[dim]{msg}[/dim]"), poll_interval=poll_interval)
    try:
        daemon.run(once=once)
    except RuntimeError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    except KeyboardInterrupt:
        daemon.stop()
        err_console.print("[yellow]Gateway stopped.[/yellow]")


@agents_app.command("add")
def add_agent(
    name: str = typer.Argument(..., help="Managed agent name"),
    runtime_type: str = typer.Option("echo", "--type", help="Runtime type: echo | exec | inbox"),
    exec_cmd: str = typer.Option(None, "--exec", help="Per-mention command for exec runtimes"),
    workdir: str = typer.Option(None, "--workdir", help="Working directory for exec runtimes"),
    space_id: str = typer.Option(None, "--space-id", help="Target space (defaults to gateway session)"),
    audience: str = typer.Option("both", "--audience", help="Minted PAT audience"),
    description: str = typer.Option(None, "--description", help="Create/update description"),
    model: str = typer.Option(None, "--model", help="Create/update model"),
    start: bool = typer.Option(True, "--start/--no-start", help="Desired running state after registration"),
    as_json: bool = JSON_OPTION,
):
    """Register a managed agent and mint a Gateway-owned PAT for it."""
    runtime_type = runtime_type.lower().strip()
    if runtime_type not in {"echo", "exec", "command", "inbox"}:
        err_console.print("[red]Unsupported runtime type.[/red] Use echo, exec, or inbox.")
        raise typer.Exit(1)
    if runtime_type in {"exec", "command"} and not exec_cmd:
        err_console.print("[red]Exec runtimes require --exec.[/red]")
        raise typer.Exit(1)
    if runtime_type in {"echo", "inbox"} and exec_cmd:
        err_console.print("[red]Echo and inbox runtimes do not accept --exec.[/red]")
        raise typer.Exit(1)

    session = _load_gateway_session_or_exit()
    selected_space = space_id or session.get("space_id")
    if not selected_space:
        err_console.print("[red]No space selected.[/red] Use --space-id or re-run `ax gateway login` with one.")
        raise typer.Exit(1)

    client = _load_gateway_user_client()
    existing = _find_agent_in_space(client, name, selected_space)
    if existing:
        agent = existing
        if description or model:
            client.update_agent(name, **{k: v for k, v in {"description": description, "model": model}.items() if v})
    else:
        agent = _create_agent_in_space(
            client,
            name=name,
            space_id=selected_space,
            description=description,
            model=model,
        )
    _polish_metadata(client, name=name, bio=None, specialization=None, system_prompt=None)

    agent_id = str(agent.get("id") or agent.get("agent_id") or "")
    token, pat_source = _mint_agent_pat(
        client,
        agent_id=agent_id,
        agent_name=name,
        audience=audience,
        expires_in_days=90,
        pat_name=f"gateway-{name}",
        space_id=selected_space,
    )
    token_file = _save_agent_token(name, token)

    registry = load_gateway_registry()
    entry = upsert_agent_entry(
        registry,
        {
            "name": name,
            "agent_id": agent_id,
            "space_id": selected_space,
            "base_url": session["base_url"],
            "runtime_type": "exec" if runtime_type == "command" else runtime_type,
            "exec_command": exec_cmd,
            "workdir": workdir,
            "token_file": str(token_file),
            "desired_state": "running" if start else "stopped",
            "effective_state": "stopped",
            "transport": "gateway",
            "credential_source": "gateway",
            "last_error": None,
            "backlog_depth": 0,
            "processed_count": 0,
            "dropped_count": 0,
            "pat_source": pat_source,
            "added_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        },
    )
    save_gateway_registry(registry)
    record_gateway_activity(
        "managed_agent_added",
        entry=entry,
        space_id=selected_space,
        token_file=str(token_file),
    )

    if as_json:
        print_json(entry)
    else:
        err_console.print(f"[green]Managed agent ready:[/green] @{name}")
        err_console.print(f"  runtime_type = {entry['runtime_type']}")
        err_console.print(f"  desired_state = {entry['desired_state']}")
        err_console.print(f"  token_file = {token_file}")


@agents_app.command("list")
def list_agents(as_json: bool = JSON_OPTION):
    """List Gateway-managed agents."""
    agents = _status_payload()["agents"]
    if as_json:
        print_json({"agents": agents, "count": len(agents)})
        return
    print_table(
        ["Agent", "Type", "Desired", "Effective", "Space"],
        agents,
        keys=["name", "runtime_type", "desired_state", "effective_state", "space_id"],
    )


@agents_app.command("show")
def show_agent(
    name: str = typer.Argument(..., help="Managed agent name"),
    activity_limit: int = typer.Option(12, "--activity-limit", help="Number of recent agent events to display"),
    as_json: bool = JSON_OPTION,
):
    """Show one managed agent in detail."""
    payload = _status_payload(activity_limit=activity_limit)
    entry = next((agent for agent in payload["agents"] if str(agent.get("name") or "").lower() == name.lower()), None)
    if not entry:
        err_console.print(f"[red]Managed agent not found:[/red] {name}")
        raise typer.Exit(1)
    activity = load_recent_gateway_activity(limit=activity_limit, agent_name=name)
    result = {
        "gateway": {
            "connected": payload["connected"],
            "base_url": payload["base_url"],
            "space_id": payload["space_id"],
            "daemon": payload["daemon"],
        },
        "agent": entry,
        "recent_activity": activity,
    }
    if as_json:
        print_json(result)
        return
    console.print(_render_agent_detail(entry, activity=activity))


@agents_app.command("send")
def send_as_agent(
    name: str = typer.Argument(..., help="Managed agent name to send as"),
    content: str = typer.Argument(..., help="Message content"),
    to: str = typer.Option(None, "--to", help="Prepend a mention like @codex automatically"),
    parent_id: str = typer.Option(None, "--parent-id", help="Reply inside an existing thread"),
    as_json: bool = JSON_OPTION,
):
    """Send a message as a Gateway-managed agent."""
    entry = _load_managed_agent_or_exit(name)
    client = _load_managed_agent_client(entry)
    space_id = str(entry.get("space_id") or "")
    if not space_id:
        err_console.print(f"[red]Managed agent is missing a space id:[/red] @{name}")
        raise typer.Exit(1)

    message_content = content.strip()
    mention = str(to or "").strip().lstrip("@")
    if mention:
        prefix = f"@{mention}"
        if not message_content.startswith(prefix):
            message_content = f"{prefix} {message_content}".strip()

    metadata = {
        "control_plane": "gateway",
        "gateway": {
            "managed": True,
            "agent_name": entry.get("name"),
            "agent_id": entry.get("agent_id"),
            "runtime_type": entry.get("runtime_type"),
            "transport": entry.get("transport", "gateway"),
            "credential_source": entry.get("credential_source", "gateway"),
            "sent_via": "gateway_cli",
        },
    }
    result = client.send_message(
        space_id,
        message_content,
        agent_id=str(entry.get("agent_id") or "") or None,
        parent_id=parent_id or None,
        metadata=metadata,
    )
    payload = result.get("message", result) if isinstance(result, dict) else result
    if isinstance(payload, dict):
        record_gateway_activity(
            "manual_message_sent",
            entry=entry,
            message_id=payload.get("id"),
            reply_preview=message_content[:120] or None,
        )
    if as_json:
        print_json({"agent": entry.get("name"), "message": payload, "content": message_content})
        return
    err_console.print(f"[green]Sent as managed agent:[/green] @{entry.get('name')}")
    if isinstance(payload, dict) and payload.get("id"):
        err_console.print(f"  id = {payload['id']}")
    err_console.print(f"  content = {message_content}")


@agents_app.command("start")
def start_agent(name: str = typer.Argument(..., help="Managed agent name")):
    """Set a managed agent's desired state to running."""
    registry = load_gateway_registry()
    entry = find_agent_entry(registry, name)
    if not entry:
        err_console.print(f"[red]Managed agent not found:[/red] {name}")
        raise typer.Exit(1)
    entry["desired_state"] = "running"
    save_gateway_registry(registry)
    record_gateway_activity("managed_agent_desired_running", entry=entry)
    err_console.print(f"[green]Desired state set to running:[/green] @{name}")


@agents_app.command("stop")
def stop_agent(name: str = typer.Argument(..., help="Managed agent name")):
    """Set a managed agent's desired state to stopped."""
    registry = load_gateway_registry()
    entry = find_agent_entry(registry, name)
    if not entry:
        err_console.print(f"[red]Managed agent not found:[/red] {name}")
        raise typer.Exit(1)
    entry["desired_state"] = "stopped"
    save_gateway_registry(registry)
    record_gateway_activity("managed_agent_desired_stopped", entry=entry)
    err_console.print(f"[green]Desired state set to stopped:[/green] @{name}")


@agents_app.command("remove")
def remove_agent(name: str = typer.Argument(..., help="Managed agent name")):
    """Remove a managed agent from local Gateway control."""
    registry = load_gateway_registry()
    entry = remove_agent_entry(registry, name)
    if not entry:
        err_console.print(f"[red]Managed agent not found:[/red] {name}")
        raise typer.Exit(1)
    save_gateway_registry(registry)
    token_file = Path(str(entry.get("token_file") or ""))
    if token_file.exists():
        token_file.unlink()
    record_gateway_activity("managed_agent_removed", entry=entry)
    err_console.print(f"[green]Removed managed agent:[/green] @{name}")
