"""ax auth — identity and token management."""
from pathlib import Path

import httpx
import typer

from ..config import (
    get_client, save_token, resolve_token, resolve_agent_name,
    _global_config_dir, _local_config_dir, _save_config, _load_local_config,
)
from ..output import JSON_OPTION, print_json, print_kv, handle_error, console

app = typer.Typer(name="auth", help="Authentication & identity", no_args_is_help=True)
token_app = typer.Typer(name="token", help="Token management", no_args_is_help=True)
app.add_typer(token_app, name="token")


@app.command()
def whoami(as_json: bool = JSON_OPTION):
    """Show current identity — principal, bound agent, resolved spaces."""
    client = get_client()
    try:
        data = client.whoami()
    except httpx.HTTPStatusError as e:
        handle_error(e)

    bound = data.get("bound_agent")
    if bound:
        data["resolved_space_id"] = bound.get("default_space_id", "none")
    else:
        from ..config import resolve_space_id
        try:
            space_id = resolve_space_id(client, explicit=None)
            data["resolved_space_id"] = space_id
        except SystemExit:
            data["resolved_space_id"] = "unresolved (set AX_SPACE_ID or use --space-id)"

    # Show resolved agent name
    resolved = resolve_agent_name(client=client)
    if resolved:
        data["resolved_agent"] = resolved

    # Show local config path if it exists
    local = _local_config_dir()
    if local and (local / "config.toml").exists():
        data["local_config"] = str(local / "config.toml")

    if as_json:
        print_json(data)
    else:
        print_kv(data)


@app.command("init")
def init(
    token: str = typer.Option(None, "--token", "-t", help="PAT token"),
    base_url: str = typer.Option("http://localhost:8001", "--url", "-u", help="API base URL"),
    agent_name: str = typer.Option(None, "--agent", "-a", help="Default agent name"),
    space_id: str = typer.Option(None, "--space-id", "-s", help="Default space ID"),
):
    """Set up a project-local .ax/config.toml in the current repo.

    Stores everything locally — token, URL, agent, space. No flags needed after init.
    Add .ax/ to .gitignore — credentials stay out of version control.

    Examples:
        ax auth init --token axp_u_... --agent protocol --space-id a632f74e-...
        ax auth init --token axp_u_... --url https://dev.paxai.app --agent canvas
    """
    local = _local_config_dir()
    if not local:
        # No .ax/ or .git found — create .ax/ in current directory
        local = Path.cwd() / ".ax"

    cfg = _load_local_config()

    if token:
        cfg["token"] = token
    if base_url:
        cfg["base_url"] = base_url
    if agent_name:
        cfg["agent_name"] = agent_name
    if space_id:
        cfg["space_id"] = space_id

    if not cfg:
        typer.echo("Error: Provide at least --agent or --space-id.", err=True)
        raise typer.Exit(1)

    _save_config(cfg, local=True)
    config_path = local / "config.toml"
    console.print(f"[green]Saved:[/green] {config_path}")
    for k, v in cfg.items():
        if k == "token":
            v = v[:6] + "..." + v[-4:] if len(v) > 10 else "***"
        console.print(f"  {k} = {v}")

    # Check .gitignore
    root = local.parent
    gitignore = root / ".gitignore"
    if gitignore.exists():
        content = gitignore.read_text()
        if ".ax/" not in content and ".ax" not in content:
            console.print(f"\n[yellow]Reminder:[/yellow] Add .ax/ to {gitignore}")
    else:
        console.print(f"\n[yellow]Reminder:[/yellow] Add .ax/ to .gitignore")


@app.command("exchange")
def exchange(
    token_class: str = typer.Option("user_access", "--class", "-c", help="Token class: user_access, user_admin, agent_access"),
    scope: str = typer.Option("messages tasks context agents spaces search", "--scope", "-s", help="Space-separated scopes"),
    agent_id: str = typer.Option(None, "--agent", "-a", help="Agent ID (required for agent_access)"),
    audience: str = typer.Option("ax-api", "--audience", help="Target audience"),
    as_json: bool = JSON_OPTION,
):
    """Exchange PAT for a short-lived JWT (AUTH-SPEC-001 §9).

    The PAT is read from config. The JWT is printed (masked by default).
    Use --json to get the full exchange response for scripting.
    """
    token = resolve_token()
    if not token:
        console.print("[red]No token configured.[/red] Use `ax auth init` or `ax auth token set`.")
        raise typer.Exit(1)
    if not token.startswith("axp_"):
        console.print("[red]Token is not a PAT (must start with axp_).[/red]")
        raise typer.Exit(1)

    from ..token_cache import TokenExchanger
    from ..config import resolve_base_url

    exchanger = TokenExchanger(resolve_base_url(), token)
    try:
        jwt = exchanger.get_token(
            token_class, agent_id=agent_id, audience=audience, scope=scope,
        )
    except httpx.HTTPStatusError as e:
        handle_error(e)

    if as_json:
        # Decode claims for display without verification
        import base64, json as json_mod
        parts = jwt.split(".")
        if len(parts) == 3:
            payload = parts[1] + "=" * (-len(parts[1]) % 4)
            claims = json_mod.loads(base64.urlsafe_b64decode(payload))
            print_json({
                "access_token": jwt[:20] + "...",
                "token_class": claims.get("token_class"),
                "sub": claims.get("sub"),
                "scope": claims.get("scope"),
                "expires_in": claims.get("exp", 0) - claims.get("iat", 0),
                "agent_id": claims.get("agent_id"),
            })
        else:
            print_json({"access_token": jwt[:20] + "..."})
    else:
        console.print(f"[green]Exchanged:[/green] {token_class}")
        console.print(f"  JWT: {jwt[:20]}...{jwt[-10:]}")
        console.print(f"  Cached until expiry. Use --json for details.")


@token_app.command("set")
def token_set(
    token: str = typer.Argument(..., help="PAT token (axp_u_...)"),
    global_: bool = typer.Option(False, "--global", "-g", help="Save to ~/.ax/ instead of local .ax/"),
):
    """Save token to local .ax/config.toml (default) or ~/.ax/ with --global."""
    save_token(token, local=not global_)
    if global_:
        config_path = _global_config_dir() / "config.toml"
    else:
        local_dir = _local_config_dir() or (Path.cwd() / ".ax")
        config_path = local_dir / "config.toml"
    typer.echo(f"Token saved to {config_path}")


@token_app.command("show")
def token_show():
    """Show saved token (masked)."""
    token = resolve_token()
    if not token:
        typer.echo("No token configured.", err=True)
        raise typer.Exit(1)
    if len(token) > 10:
        masked = token[:6] + "..." + token[-4:]
    else:
        masked = token[:2] + "..." + token[-2:]
    typer.echo(masked)
