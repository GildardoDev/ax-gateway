"""Local Gateway runtime and state management.

The Gateway is a local control-plane daemon that owns bootstrap and agent
credentials, supervises managed runtimes, and keeps lightweight desired vs
effective state in a registry file. The first slice intentionally uses
filesystem state plus a foreground daemon so it can ship quickly without
introducing a second backend.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import queue
import re
import shlex
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from .client import AxClient
from .commands.listen import (
    _is_self_authored,
    _iter_sse,
    _remember_reply_anchor,
    _should_respond,
    _strip_mention,
)
from .config import _global_config_dir

RuntimeLogger = Callable[[str], None]

REPLY_ANCHOR_MAX = 500
SEEN_IDS_MAX = 500
DEFAULT_QUEUE_SIZE = 50
DEFAULT_ACTIVITY_LIMIT = 10
DEFAULT_HANDLER_TIMEOUT_SECONDS = 900
SSE_IDLE_TIMEOUT_SECONDS = 45.0
RUNTIME_STALE_AFTER_SECONDS = 75.0
GATEWAY_EVENT_PREFIX = "AX_GATEWAY_EVENT "
ENV_DENYLIST = {
    "AX_AGENT_ID",
    "AX_AGENT_NAME",
    "AX_BASE_URL",
    "AX_CONFIG_FILE",
    "AX_ENV",
    "AX_SPACE_ID",
    "AX_TOKEN",
    "AX_TOKEN_FILE",
    "AX_USER_BASE_URL",
    "AX_USER_ENV",
    "AX_USER_TOKEN",
}
_ACTIVITY_LOCK = threading.Lock()
_GATEWAY_PROCESS_RE = re.compile(r"(?:^|\s)(?:uv\s+run\s+ax\s+gateway\s+run|.+?/ax\s+gateway\s+run)(?:\s|$)")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso8601(value: object) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _age_seconds(value: object, *, now: datetime | None = None) -> int | None:
    parsed = _parse_iso8601(value)
    if parsed is None:
        return None
    current = now or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = current - parsed.astimezone(timezone.utc)
    return max(0, int(delta.total_seconds()))


def annotate_runtime_health(snapshot: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    enriched = dict(snapshot)
    last_seen_age = _age_seconds(enriched.get("last_seen_at"), now=now)
    last_error_age = _age_seconds(enriched.get("last_listener_error_at"), now=now)
    if last_seen_age is not None:
        enriched["last_seen_age_seconds"] = last_seen_age
    if last_error_age is not None:
        enriched["last_listener_error_age_seconds"] = last_error_age

    state = str(enriched.get("effective_state") or "stopped").lower()
    connected = False
    if state == "running":
        if last_seen_age is None or last_seen_age > RUNTIME_STALE_AFTER_SECONDS:
            state = "stale"
        else:
            connected = True
    enriched["effective_state"] = state
    enriched["connected"] = connected
    return enriched


def gateway_dir() -> Path:
    path = _global_config_dir() / "gateway"
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def gateway_agents_dir() -> Path:
    path = gateway_dir() / "agents"
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def session_path() -> Path:
    return gateway_dir() / "session.json"


def registry_path() -> Path:
    return gateway_dir() / "registry.json"


def pid_path() -> Path:
    return gateway_dir() / "gateway.pid"


def activity_log_path() -> Path:
    return gateway_dir() / "activity.jsonl"


def agent_dir(name: str) -> Path:
    path = gateway_agents_dir() / name
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def agent_token_path(name: str) -> Path:
    return agent_dir(name) / "token"


def _default_registry() -> dict[str, Any]:
    return {
        "version": 1,
        "gateway": {
            "gateway_id": str(uuid.uuid4()),
            "desired_state": "stopped",
            "effective_state": "stopped",
            "session_connected": False,
            "pid": None,
            "last_started_at": None,
            "last_reconcile_at": None,
        },
        "agents": [],
    }


def _write_json(path: Path, payload: dict[str, Any], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.chmod(mode)
    tmp.replace(path)
    path.chmod(mode)


def _read_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return copy.deepcopy(default)
    return json.loads(path.read_text())


def load_gateway_session() -> dict[str, Any]:
    return _read_json(session_path(), default={})


def save_gateway_session(data: dict[str, Any]) -> Path:
    payload = dict(data)
    payload.setdefault("saved_at", _now_iso())
    _write_json(session_path(), payload)
    return session_path()


def load_gateway_registry() -> dict[str, Any]:
    registry = _read_json(registry_path(), default=_default_registry())
    registry.setdefault("version", 1)
    registry.setdefault("gateway", {})
    registry.setdefault("agents", [])
    gateway = registry["gateway"]
    gateway.setdefault("gateway_id", str(uuid.uuid4()))
    gateway.setdefault("desired_state", "stopped")
    gateway.setdefault("effective_state", "stopped")
    gateway.setdefault("session_connected", False)
    gateway.setdefault("pid", None)
    gateway.setdefault("last_started_at", None)
    gateway.setdefault("last_reconcile_at", None)
    return registry


def save_gateway_registry(registry: dict[str, Any]) -> Path:
    _write_json(registry_path(), registry)
    return registry_path()


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def daemon_status() -> dict[str, Any]:
    pid = None
    if pid_path().exists():
        try:
            pid = int(pid_path().read_text().strip())
        except ValueError:
            pid = None
    registry = load_gateway_registry()
    return {
        "pid": pid,
        "running": _pid_alive(pid),
        "registry_path": str(registry_path()),
        "session_path": str(session_path()),
        "registry": registry,
    }


def _scan_gateway_process_pids() -> list[int]:
    """Best-effort fallback for live daemons that predate the pid file."""
    current_pid = os.getpid()
    parent_pid = os.getppid()
    try:
        output = subprocess.check_output(
            ["ps", "-axo", "pid=,command="],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []

    pids: list[int] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        pid_text, _, command = line.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid in {current_pid, parent_pid} or not _pid_alive(pid):
            continue
        command = command.strip()
        if command and _GATEWAY_PROCESS_RE.search(command):
            pids.append(pid)
    return sorted(set(pids))


def active_gateway_pids() -> list[int]:
    """Return all known live Gateway daemon PIDs except the current process."""
    status = daemon_status()
    pids: list[int] = []
    pid = status.get("pid")
    if isinstance(pid, int) and status.get("running") and pid != os.getpid():
        pids.append(pid)
    pids.extend(_scan_gateway_process_pids())
    return sorted(set(pids))


def active_gateway_pid() -> int | None:
    """Return the PID of a live Gateway daemon, if one is already running."""
    pids = active_gateway_pids()
    return pids[0] if pids else None


def write_gateway_pid(pid: int) -> None:
    pid_path().write_text(f"{pid}\n")
    pid_path().chmod(0o600)


def clear_gateway_pid(pid: int | None = None) -> None:
    if not pid_path().exists():
        return
    if pid is not None:
        try:
            existing_pid = int(pid_path().read_text().strip())
        except ValueError:
            existing_pid = None
        if existing_pid not in {None, pid}:
            return
    pid_path().unlink()


def record_gateway_activity(
    event: str,
    *,
    entry: dict[str, Any] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "ts": _now_iso(),
        "event": event,
    }
    registry = load_gateway_registry()
    gateway = registry.get("gateway", {})
    if gateway.get("gateway_id"):
        record["gateway_id"] = gateway["gateway_id"]
    if entry:
        record.update(
            {
                "agent_name": entry.get("name"),
                "agent_id": entry.get("agent_id"),
                "runtime_type": entry.get("runtime_type"),
                "transport": entry.get("transport", "gateway"),
                "credential_source": entry.get("credential_source", "gateway"),
            }
        )
    for key, value in fields.items():
        if value is not None:
            record[key] = value

    path = activity_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _ACTIVITY_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        path.chmod(0o600)
    return record


def load_recent_gateway_activity(
    limit: int = DEFAULT_ACTIVITY_LIMIT,
    *,
    agent_name: str | None = None,
) -> list[dict[str, Any]]:
    path = activity_log_path()
    if not path.exists():
        return []
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []
    if limit <= 0:
        return []
    agent_filter = agent_name.strip().lower() if agent_name else None
    items: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if agent_filter and str(payload.get("agent_name") or "").lower() != agent_filter:
            continue
        items.append(payload)
        if len(items) >= limit:
            break
    items.reverse()
    return items


def find_agent_entry(registry: dict[str, Any], name: str) -> dict[str, Any] | None:
    for entry in registry.get("agents", []):
        if str(entry.get("name", "")).lower() == name.lower():
            return entry
    return None


def upsert_agent_entry(registry: dict[str, Any], agent: dict[str, Any]) -> dict[str, Any]:
    agents = registry.setdefault("agents", [])
    for idx, existing in enumerate(agents):
        if str(existing.get("name", "")).lower() == str(agent.get("name", "")).lower():
            merged = dict(existing)
            merged.update(agent)
            agents[idx] = merged
            return merged
    agents.append(agent)
    return agent


def remove_agent_entry(registry: dict[str, Any], name: str) -> dict[str, Any] | None:
    agents = registry.setdefault("agents", [])
    for idx, entry in enumerate(agents):
        if str(entry.get("name", "")).lower() == name.lower():
            return agents.pop(idx)
    return None


def sanitize_exec_env(prompt: str, entry: dict[str, Any]) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in ENV_DENYLIST}
    env["AX_GATEWAY_AGENT_ID"] = str(entry.get("agent_id") or "")
    env["AX_GATEWAY_AGENT_NAME"] = str(entry.get("name") or "")
    env["AX_GATEWAY_RUNTIME_TYPE"] = str(entry.get("runtime_type") or "")
    env["AX_MENTION_CONTENT"] = prompt
    return env


def _parse_gateway_exec_event(raw_line: str) -> dict[str, Any] | None:
    line = raw_line.strip()
    if not line.startswith(GATEWAY_EVENT_PREFIX):
        return None
    payload = line[len(GATEWAY_EVENT_PREFIX) :].strip()
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _hash_tool_arguments(arguments: dict[str, Any] | None) -> str | None:
    if not arguments:
        return None
    encoded = json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _run_exec_handler(
    command: str,
    prompt: str,
    entry: dict[str, Any],
    *,
    message_id: str | None = None,
    space_id: str | None = None,
    on_event: Callable[[dict[str, Any]], None] | None = None,
) -> str:
    argv = [*shlex.split(command), prompt]
    env = sanitize_exec_env(prompt, entry)
    if message_id:
        env["AX_GATEWAY_MESSAGE_ID"] = message_id
    if space_id:
        env["AX_GATEWAY_SPACE_ID"] = space_id
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=entry.get("workdir") or None,
            env=env,
        )
    except FileNotFoundError:
        return f"(handler not found: {argv[0]})"

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    def _consume_stdout() -> None:
        if process.stdout is None:
            return
        for raw in process.stdout:
            event = _parse_gateway_exec_event(raw)
            if event is not None:
                if on_event is not None:
                    try:
                        on_event(event)
                    except Exception:
                        pass
                continue
            stdout_lines.append(raw)

    def _consume_stderr() -> None:
        if process.stderr is None:
            return
        for raw in process.stderr:
            stderr_lines.append(raw)

    stdout_thread = threading.Thread(target=_consume_stdout, daemon=True, name=f"gw-exec-stdout-{entry.get('name')}")
    stderr_thread = threading.Thread(target=_consume_stderr, daemon=True, name=f"gw-exec-stderr-{entry.get('name')}")
    stdout_thread.start()
    stderr_thread.start()

    timed_out = False
    try:
        process.wait(timeout=DEFAULT_HANDLER_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.kill()
    finally:
        stdout_thread.join(timeout=1.0)
        stderr_thread.join(timeout=1.0)
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()

    if timed_out:
        return f"(handler timed out after {DEFAULT_HANDLER_TIMEOUT_SECONDS}s)"

    output = "".join(stdout_lines).strip()
    stderr = "".join(stderr_lines).strip()
    if process.returncode != 0 and stderr:
        output = f"{output}\n(stderr: {stderr[:400]})".strip()
    return output or "(no output)"


def _echo_handler(prompt: str, _entry: dict[str, Any]) -> str:
    return f"Echo: {prompt}"


def _is_passive_runtime(runtime_type: object) -> bool:
    return str(runtime_type or "").lower() in {"inbox", "passive", "monitor"}


class ManagedAgentRuntime:
    """Listener + worker pair for one managed agent."""

    def __init__(
        self,
        entry: dict[str, Any],
        *,
        client_factory: Callable[..., Any] = AxClient,
        logger: RuntimeLogger | None = None,
    ) -> None:
        self.entry = dict(entry)
        self.client_factory = client_factory
        self.logger = logger or (lambda _msg: None)
        self.stop_event = threading.Event()
        self._listener_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None
        self._queue: queue.Queue = queue.Queue(maxsize=int(entry.get("queue_size") or DEFAULT_QUEUE_SIZE))
        self._reply_anchor_ids: set[str] = set()
        self._seen_ids: set[str] = set()
        self._completed_seen_ids: set[str] = set()
        self._state_lock = threading.Lock()
        self._stream_client = None
        self._send_client = None
        self._stream_response = None
        self._state: dict[str, Any] = {
            "effective_state": "stopped",
            "backlog_depth": 0,
            "dropped_count": 0,
            "processed_count": 0,
            "current_status": None,
            "current_activity": None,
            "current_tool": None,
            "current_tool_call_id": None,
            "last_error": None,
            "last_connected_at": None,
            "last_listener_error_at": None,
            "last_started_at": None,
            "last_seen_at": None,
            "last_work_received_at": None,
            "last_work_completed_at": None,
            "last_received_message_id": None,
            "last_reply_message_id": None,
            "last_reply_preview": None,
            "reconnect_backoff_seconds": 0,
        }

    @property
    def name(self) -> str:
        return str(self.entry.get("name") or "")

    @property
    def agent_id(self) -> str | None:
        value = self.entry.get("agent_id")
        return str(value) if value else None

    @property
    def base_url(self) -> str:
        return str(self.entry.get("base_url") or "")

    @property
    def space_id(self) -> str:
        return str(self.entry.get("space_id") or "")

    @property
    def token_file(self) -> Path:
        return Path(str(self.entry.get("token_file") or "")).expanduser()

    def _log(self, message: str) -> None:
        self.logger(f"{self.name}: {message}")

    def _token(self) -> str:
        return self.token_file.read_text().strip()

    def _new_client(self):
        return self.client_factory(
            base_url=self.base_url,
            token=self._token(),
            agent_name=self.name,
            agent_id=self.agent_id,
        )

    def _update_state(self, **fields: Any) -> None:
        with self._state_lock:
            self._state.update(fields)

    def _bump(self, field: str, amount: int = 1) -> None:
        with self._state_lock:
            self._state[field] = int(self._state.get(field) or 0) + amount

    def _mark_completed_seen(self, message_id: str) -> None:
        if not message_id:
            return
        with self._state_lock:
            self._completed_seen_ids.add(message_id)

    def _consume_completed_seen(self, message_id: str) -> bool:
        if not message_id:
            return False
        with self._state_lock:
            seen = message_id in self._completed_seen_ids
            if seen:
                self._completed_seen_ids.discard(message_id)
            return seen

    def snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            return annotate_runtime_health(dict(self._state))

    def start(self) -> None:
        if self._listener_thread and self._listener_thread.is_alive():
            return
        self.stop_event.clear()
        self._queue = queue.Queue(maxsize=int(self.entry.get("queue_size") or DEFAULT_QUEUE_SIZE))
        self._reply_anchor_ids = set()
        self._seen_ids = set()
        self._completed_seen_ids = set()
        self._update_state(
            effective_state="starting",
            backlog_depth=0,
            current_status=None,
            current_activity=None,
            current_tool=None,
            current_tool_call_id=None,
            last_error=None,
            last_listener_error_at=None,
            last_started_at=_now_iso(),
            reconnect_backoff_seconds=0,
        )
        self._worker_thread = None
        if not _is_passive_runtime(self.entry.get("runtime_type")):
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                daemon=True,
                name=f"gw-worker-{self.name}",
            )
        self._listener_thread = threading.Thread(
            target=self._listener_loop,
            daemon=True,
            name=f"gw-listener-{self.name}",
        )
        if self._worker_thread is not None:
            self._worker_thread.start()
        self._listener_thread.start()
        record_gateway_activity("runtime_started", entry=self.entry)
        self._log("started")

    def stop(self, timeout: float = 5.0) -> None:
        self.stop_event.set()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._stream_response is not None:
            try:
                self._stream_response.close()
            except Exception:
                pass
        for thread in (self._listener_thread, self._worker_thread):
            if thread and thread.is_alive():
                thread.join(timeout=timeout)
        for client in (self._stream_client, self._send_client):
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
        self._stream_client = None
        self._send_client = None
        self._stream_response = None
        self._update_state(
            effective_state="stopped",
            backlog_depth=0,
            current_status=None,
            current_activity=None,
            current_tool=None,
            current_tool_call_id=None,
        )
        record_gateway_activity("runtime_stopped", entry=self.entry)
        self._log("stopped")

    def _publish_processing_status(
        self,
        message_id: str,
        status: str,
        *,
        activity: str | None = None,
        tool_name: str | None = None,
        progress: dict[str, Any] | None = None,
        detail: dict[str, Any] | None = None,
        reason: str | None = None,
        error_message: str | None = None,
        retry_after_seconds: int | None = None,
        parent_message_id: str | None = None,
    ) -> None:
        if not self._send_client:
            return
        try:
            self._send_client.set_agent_processing_status(
                message_id,
                status,
                agent_name=self.name,
                space_id=self.space_id,
                activity=activity,
                tool_name=tool_name,
                progress=progress,
                detail=detail,
                reason=reason,
                error_message=error_message,
                retry_after_seconds=retry_after_seconds,
                parent_message_id=parent_message_id,
            )
        except Exception:
            pass

    @staticmethod
    def _processing_status_metadata(event: dict[str, Any]) -> dict[str, Any]:
        progress = event.get("progress") if isinstance(event.get("progress"), dict) else None
        detail = event.get("detail") if isinstance(event.get("detail"), dict) else None
        if detail is None and isinstance(event.get("initial_data"), dict):
            detail = event.get("initial_data")
        reason = str(event.get("reason") or "").strip() or None
        error_message = str(event.get("error_message") or "").strip() or None
        parent_message_id = str(event.get("parent_message_id") or "").strip() or None

        retry_after_seconds = None
        retry_after_raw = event.get("retry_after_seconds")
        if retry_after_raw is not None:
            try:
                retry_after_seconds = int(retry_after_raw)
            except (TypeError, ValueError):
                retry_after_seconds = None

        return {
            "progress": progress,
            "detail": detail,
            "reason": reason,
            "error_message": error_message,
            "retry_after_seconds": retry_after_seconds,
            "parent_message_id": parent_message_id,
        }

    def _record_tool_call(self, *, message_id: str, event: dict[str, Any]) -> None:
        if not self._send_client:
            return
        tool_name = str(event.get("tool_name") or event.get("tool") or "").strip()
        if not tool_name:
            return
        tool_call_id = str(event.get("tool_call_id") or uuid.uuid4())
        arguments = event.get("arguments") if isinstance(event.get("arguments"), dict) else None
        initial_data = event.get("initial_data") if isinstance(event.get("initial_data"), dict) else None
        duration_raw = event.get("duration_ms")
        try:
            duration_ms = int(duration_raw) if duration_raw is not None else None
        except (TypeError, ValueError):
            duration_ms = None
        try:
            self._send_client.record_tool_call(
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                space_id=self.space_id,
                tool_action=str(event.get("tool_action") or event.get("tool_action_name") or event.get("command") or "") or None,
                resource_uri=str(event.get("resource_uri") or "ui://gateway/tool-call"),
                arguments_hash=_hash_tool_arguments(arguments),
                kind=str(event.get("kind_name") or event.get("result_kind") or "gateway_runtime"),
                arguments=arguments,
                initial_data=initial_data,
                status=str(event.get("status") or "success"),
                duration_ms=duration_ms,
                agent_name=self.name,
                agent_id=self.agent_id,
                message_id=message_id,
                correlation_id=str(event.get("correlation_id") or message_id),
            )
            record_gateway_activity(
                "tool_call_recorded",
                entry=self.entry,
                message_id=message_id,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
            )
        except Exception as exc:
            record_gateway_activity(
                "tool_call_record_failed",
                entry=self.entry,
                message_id=message_id,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                error=str(exc)[:400],
            )

    def _handle_exec_event(self, event: dict[str, Any], *, message_id: str) -> None:
        kind = str(event.get("kind") or event.get("type") or "").strip().lower()
        if not kind:
            return
        if kind == "status":
            status = str(event.get("status") or "processing").strip()
            if status == "completed":
                self._mark_completed_seen(message_id)
            activity = str(event.get("message") or event.get("activity") or "").strip() or None
            tool_name = str(event.get("tool") or event.get("tool_name") or "").strip() or None
            metadata = self._processing_status_metadata(event)
            updates: dict[str, Any] = {}
            updates["current_status"] = status
            if activity is not None:
                updates["current_activity"] = activity[:240]
            if tool_name is not None:
                updates["current_tool"] = tool_name[:120]
            if status == "completed":
                updates["current_status"] = None
                updates.setdefault("current_activity", None)
                updates.setdefault("current_tool", None)
                updates["current_tool_call_id"] = None
            if updates:
                self._update_state(**updates)
            if message_id:
                self._publish_processing_status(
                    message_id,
                    status,
                    activity=activity,
                    tool_name=tool_name,
                    **metadata,
                )
            record_gateway_activity(
                "runtime_status",
                entry=self.entry,
                message_id=message_id,
                status=status,
                activity_message=activity,
                tool_name=tool_name,
            )
            return

        if kind == "tool_start":
            tool_name = str(event.get("tool_name") or event.get("tool") or "tool").strip()
            tool_call_id = str(event.get("tool_call_id") or uuid.uuid4())
            activity = str(event.get("message") or f"Using {tool_name}").strip()
            status = str(event.get("status") or "tool_call").strip()
            metadata = self._processing_status_metadata(event)
            self._update_state(
                current_status=status,
                current_activity=activity[:240],
                current_tool=tool_name[:120] or None,
                current_tool_call_id=tool_call_id,
            )
            if message_id:
                self._publish_processing_status(
                    message_id,
                    status,
                    activity=activity,
                    tool_name=tool_name or None,
                    **metadata,
                )
            record_gateway_activity(
                "tool_started",
                entry=self.entry,
                message_id=message_id,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                tool_action=str(event.get("tool_action") or event.get("command") or "") or None,
            )
            return

        if kind == "tool_result":
            tool_name = str(event.get("tool_name") or event.get("tool") or "tool").strip()
            tool_call_id = str(event.get("tool_call_id") or uuid.uuid4())
            status = str(event.get("status") or "success").strip()
            metadata = self._processing_status_metadata(event)
            self._record_tool_call(message_id=message_id, event=event)
            step_status = "tool_complete" if status.lower() in {"success", "completed", "ok", "tool_complete"} else "error"
            self._update_state(
                current_status=None if step_status == "tool_complete" else step_status,
                current_activity=None,
                current_tool=None,
                current_tool_call_id=None,
            )
            if message_id:
                self._publish_processing_status(
                    message_id,
                    step_status,
                    tool_name=tool_name or None,
                    detail=metadata["detail"],
                    reason=metadata["reason"] or (None if step_status == "tool_complete" else status),
                    error_message=metadata["error_message"],
                    retry_after_seconds=metadata["retry_after_seconds"],
                    parent_message_id=metadata["parent_message_id"],
                )
            record_gateway_activity(
                "tool_finished",
                entry=self.entry,
                message_id=message_id,
                tool_name=tool_name,
                tool_call_id=tool_call_id,
                status=status,
            )
            return

        if kind == "activity":
            activity = str(event.get("message") or event.get("activity") or "").strip()
            if activity:
                self._update_state(current_activity=activity[:240])
            record_gateway_activity(
                "runtime_activity",
                entry=self.entry,
                message_id=message_id,
                activity_message=activity or None,
            )

    def _handle_prompt(self, prompt: str, *, message_id: str) -> str:
        runtime_type = str(self.entry.get("runtime_type") or "echo").lower()
        if runtime_type == "echo":
            return _echo_handler(prompt, self.entry)
        if runtime_type in {"inbox", "passive", "monitor"}:
            return ""
        if runtime_type in {"exec", "command"}:
            command = str(self.entry.get("exec_command") or "").strip()
            if not command:
                raise ValueError("exec runtime requires exec_command")
            return _run_exec_handler(
                command,
                prompt,
                self.entry,
                message_id=message_id or None,
                space_id=self.space_id,
                on_event=lambda event: self._handle_exec_event(event, message_id=message_id),
            )
        raise ValueError(f"Unsupported runtime_type: {runtime_type}")

    def _gateway_message_metadata(self, parent_message_id: str | None = None) -> dict[str, Any]:
        registry = load_gateway_registry()
        gateway = registry.get("gateway", {})
        metadata: dict[str, Any] = {
            "control_plane": "gateway",
            "gateway": {
                "managed": True,
                "gateway_id": gateway.get("gateway_id"),
                "agent_name": self.name,
                "agent_id": self.agent_id,
                "runtime_type": self.entry.get("runtime_type"),
                "transport": self.entry.get("transport", "gateway"),
                "credential_source": self.entry.get("credential_source", "gateway"),
            },
        }
        if parent_message_id:
            metadata["gateway"]["parent_message_id"] = parent_message_id
        return metadata

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                data = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if data is None:
                break

            message_id = str(data.get("id") or "")
            prompt = _strip_mention(str(data.get("content") or ""), self.name)
            self._update_state(backlog_depth=self._queue.qsize())
            if not prompt:
                self._queue.task_done()
                continue

            if message_id:
                runtime_type = str(self.entry.get("runtime_type") or "echo").lower()
                start_status = "processing"
                start_activity = "Preparing response"
                if runtime_type == "echo":
                    start_activity = "Composing echo reply"
                elif runtime_type in {"exec", "command"}:
                    start_activity = "Preparing runtime"
                if runtime_type in {"echo", "exec", "command"}:
                    self._update_state(current_status=start_status, current_activity=start_activity[:240])
                    self._publish_processing_status(message_id, start_status, activity=start_activity)
                    record_gateway_activity(
                        "runtime_status",
                        entry=self.entry,
                        message_id=message_id,
                        status=start_status,
                        activity_message=start_activity,
                    )
            try:
                response_text = self._handle_prompt(prompt, message_id=message_id)
                if response_text and self._send_client:
                    result = self._send_client.send_message(
                        self.space_id,
                        response_text,
                        agent_id=self.agent_id,
                        parent_id=message_id or None,
                        metadata=self._gateway_message_metadata(message_id or None),
                    )
                    message = result.get("message", result) if isinstance(result, dict) else {}
                    _remember_reply_anchor(self._reply_anchor_ids, message.get("id"))
                    reply_id = message.get("id")
                    preview = response_text.strip().replace("\n", " ")
                    if len(preview) > 120:
                        preview = preview[:117] + "..."
                    self._update_state(last_reply_message_id=reply_id, last_reply_preview=preview or None)
                    record_gateway_activity(
                        "reply_sent",
                        entry=self.entry,
                        message_id=message_id or None,
                        reply_message_id=reply_id,
                        reply_preview=preview or None,
                    )
                runtime_type = str(self.entry.get("runtime_type") or "echo").lower()
                bridge_already_closed = runtime_type in {"exec", "command"} and self._consume_completed_seen(message_id)
                if message_id and not bridge_already_closed:
                    self._publish_processing_status(message_id, "completed")
                self._bump("processed_count")
                self._update_state(
                    current_status=None,
                    current_activity=None,
                    current_tool=None,
                    current_tool_call_id=None,
                    last_error=None,
                    last_work_completed_at=_now_iso(),
                    backlog_depth=self._queue.qsize(),
                )
            except Exception as exc:
                self._update_state(
                    current_status="error",
                    current_activity=None,
                    current_tool=None,
                    current_tool_call_id=None,
                    last_error=str(exc)[:400],
                    backlog_depth=self._queue.qsize(),
                )
                if message_id:
                    self._publish_processing_status(
                        message_id,
                        "error",
                        error_message=str(exc)[:400],
                    )
                record_gateway_activity(
                    "runtime_error",
                    entry=self.entry,
                    message_id=message_id or None,
                    error=str(exc)[:400],
                )
                self._log(f"worker error: {exc}")
            finally:
                self._queue.task_done()

    def _listener_loop(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                self._stream_client = self._new_client()
                self._send_client = self._new_client()
                timeout = httpx.Timeout(
                    connect=10.0,
                    read=SSE_IDLE_TIMEOUT_SECONDS,
                    write=10.0,
                    pool=10.0,
                )
                reconnected = backoff > 1.0
                with self._stream_client.connect_sse(space_id=self.space_id, timeout=timeout) as response:
                    self._stream_response = response
                    if response.status_code != 200:
                        raise ConnectionError(f"SSE failed: {response.status_code}")
                    self._update_state(
                        effective_state="running",
                        current_status=None,
                        last_error=None,
                        last_connected_at=_now_iso(),
                        last_listener_error_at=None,
                        last_seen_at=_now_iso(),
                        reconnect_backoff_seconds=0,
                    )
                    record_gateway_activity("listener_connected", entry=self.entry, reconnected=reconnected)
                    backoff = 1.0
                    for event_type, data in _iter_sse(response):
                        if self.stop_event.is_set():
                            break
                        if event_type in {"bootstrap", "heartbeat", "ping", "identity_bootstrap", "connected"}:
                            self._update_state(last_seen_at=_now_iso())
                            continue
                        if event_type not in {"message", "mention"} or not isinstance(data, dict):
                            continue
                        message_id = str(data.get("id") or "")
                        if not message_id or message_id in self._seen_ids:
                            continue
                        if _is_self_authored(data, self.name, self.agent_id):
                            _remember_reply_anchor(self._reply_anchor_ids, message_id)
                            self._seen_ids.add(message_id)
                            continue
                        if not _should_respond(
                            data,
                            self.name,
                            self.agent_id,
                            reply_anchor_ids=self._reply_anchor_ids,
                        ):
                            continue

                        self._seen_ids.add(message_id)
                        if len(self._seen_ids) > SEEN_IDS_MAX:
                            self._seen_ids = set(list(self._seen_ids)[-SEEN_IDS_MAX // 2 :])
                        _remember_reply_anchor(self._reply_anchor_ids, message_id)
                        self._update_state(
                            last_seen_at=_now_iso(),
                            last_work_received_at=_now_iso(),
                            last_received_message_id=message_id,
                        )
                        record_gateway_activity("message_received", entry=self.entry, message_id=message_id)
                        try:
                            self._queue.put_nowait(data)
                            backlog_depth = self._queue.qsize()
                            runtime_type = str(self.entry.get("runtime_type") or "").lower()
                            pickup_status = "queued" if _is_passive_runtime(runtime_type) else "started"
                            accepted_activity = "Queued in Gateway"
                            if not _is_passive_runtime(runtime_type):
                                accepted_activity = "Picked up by Gateway"
                            if backlog_depth > 1:
                                if _is_passive_runtime(runtime_type):
                                    accepted_activity = f"Queued in Gateway ({backlog_depth} pending)"
                                else:
                                    accepted_activity = f"Picked up by Gateway ({backlog_depth} pending)"
                            self._update_state(
                                backlog_depth=backlog_depth,
                                current_status=pickup_status,
                                current_activity=accepted_activity[:240],
                            )
                            self._publish_processing_status(
                                message_id,
                                pickup_status,
                                activity=accepted_activity,
                                detail={
                                    "backlog_depth": backlog_depth,
                                    "pickup_state": "queued" if _is_passive_runtime(runtime_type) else "claimed",
                                },
                            )
                            if _is_passive_runtime(self.entry.get("runtime_type")):
                                record_gateway_activity(
                                    "message_queued",
                                    entry=self.entry,
                                    message_id=message_id,
                                    backlog_depth=backlog_depth,
                                )
                            else:
                                record_gateway_activity(
                                    "message_claimed",
                                    entry=self.entry,
                                    message_id=message_id,
                                    backlog_depth=backlog_depth,
                                )
                        except queue.Full:
                            self._bump("dropped_count")
                            self._update_state(last_error="queue full", backlog_depth=self._queue.qsize())
                            self._publish_processing_status(
                                message_id,
                                "error",
                                reason="queue_full",
                                error_message="Gateway queue full",
                            )
                            record_gateway_activity(
                                "message_dropped",
                                entry=self.entry,
                                message_id=message_id,
                                error="queue full",
                            )
                            self._log("queue full; dropped message")
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                error_text = str(exc)[:400]
                event_name = "listener_error"
                if isinstance(exc, httpx.ReadTimeout):
                    error_text = f"idle timeout after {int(SSE_IDLE_TIMEOUT_SECONDS)}s without SSE heartbeat"
                    event_name = "listener_timeout"
                self._update_state(
                    effective_state="reconnecting",
                    last_error=error_text,
                    last_listener_error_at=_now_iso(),
                    reconnect_backoff_seconds=int(backoff),
                )
                record_gateway_activity(event_name, entry=self.entry, error=error_text, reconnect_in_seconds=int(backoff))
                self._log(f"listener error: {error_text}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                self._stream_response = None
                if self._stream_client is not None:
                    try:
                        self._stream_client.close()
                    except Exception:
                        pass
                    self._stream_client = None
        self._update_state(
            effective_state="stopped",
            backlog_depth=self._queue.qsize(),
            current_status=None,
            current_activity=None,
            current_tool=None,
            current_tool_call_id=None,
        )


class GatewayDaemon:
    """Foreground Gateway supervisor."""

    def __init__(
        self,
        *,
        client_factory: Callable[..., Any] = AxClient,
        logger: RuntimeLogger | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        self.client_factory = client_factory
        self.logger = logger or (lambda _msg: None)
        self.poll_interval = poll_interval
        self._runtimes: dict[str, ManagedAgentRuntime] = {}
        self._stop = threading.Event()

    def _log(self, message: str) -> None:
        self.logger(message)

    def stop(self) -> None:
        self._stop.set()

    def _reconcile_runtime(self, entry: dict[str, Any]) -> None:
        name = str(entry.get("name") or "")
        desired_state = str(entry.get("desired_state") or "stopped").lower()
        runtime = self._runtimes.get(name)
        if desired_state == "running":
            if runtime is None:
                runtime = ManagedAgentRuntime(entry, client_factory=self.client_factory, logger=self.logger)
                self._runtimes[name] = runtime
                runtime.start()
            else:
                runtime.entry.update(entry)
                runtime.start()
        else:
            if runtime is not None:
                runtime.stop()
                self._runtimes.pop(name, None)

    def _reconcile_registry(self, registry: dict[str, Any], session: dict[str, Any]) -> dict[str, Any]:
        agents = registry.setdefault("agents", [])
        agent_names = {str(entry.get("name") or "") for entry in agents}
        for name, runtime in list(self._runtimes.items()):
            if name not in agent_names:
                runtime.stop()
                self._runtimes.pop(name, None)

        for entry in agents:
            entry.setdefault("transport", "gateway")
            entry.setdefault("credential_source", "gateway")
            entry.setdefault("runtime_type", "echo")
            entry.setdefault("desired_state", "stopped")
            self._reconcile_runtime(entry)
            runtime = self._runtimes.get(str(entry.get("name") or ""))
            snapshot = runtime.snapshot() if runtime is not None else annotate_runtime_health({"effective_state": "stopped"})
            entry.update(snapshot)

        gateway = registry.setdefault("gateway", {})
        gateway.update(
            {
                "desired_state": "running",
                "effective_state": "running" if session else "stopped",
                "session_connected": bool(session),
                "pid": os.getpid(),
                "last_started_at": gateway.get("last_started_at") or _now_iso(),
                "last_reconcile_at": _now_iso(),
            }
        )
        return registry

    def run(self, *, once: bool = False) -> None:
        session = load_gateway_session()
        if not session:
            raise RuntimeError("Gateway login required. Run `ax gateway login` first.")

        existing_pids = active_gateway_pids()
        if existing_pids:
            existing_pid = existing_pids[0]
            record_gateway_activity(
                "gateway_start_blocked",
                pid=os.getpid(),
                existing_pid=existing_pid,
                existing_pids=existing_pids,
            )
            raise RuntimeError(f"Gateway already running (pid {existing_pid}).")

        write_gateway_pid(os.getpid())
        registry = load_gateway_registry()
        registry.setdefault("gateway", {})
        registry["gateway"]["last_started_at"] = registry["gateway"].get("last_started_at") or _now_iso()
        record_gateway_activity("gateway_started", pid=os.getpid())
        try:
            while not self._stop.is_set():
                registry = load_gateway_registry()
                registry = self._reconcile_registry(registry, session)
                save_gateway_registry(registry)
                if once:
                    break
                time.sleep(self.poll_interval)
        finally:
            for runtime in list(self._runtimes.values()):
                runtime.stop()
            final_registry = load_gateway_registry()
            final_gateway = final_registry.setdefault("gateway", {})
            final_gateway.update(
                {
                    "desired_state": final_gateway.get("desired_state") or "stopped",
                    "effective_state": "stopped",
                    "session_connected": bool(session),
                    "pid": None,
                    "last_reconcile_at": _now_iso(),
                }
            )
            for entry in final_registry.get("agents", []):
                name = str(entry.get("name") or "")
                entry.update({"effective_state": "stopped", "backlog_depth": 0})
                runtime = self._runtimes.get(name)
                if runtime is not None:
                    entry.update(runtime.snapshot())
            save_gateway_registry(final_registry)
            record_gateway_activity("gateway_stopped")
            clear_gateway_pid(os.getpid())
