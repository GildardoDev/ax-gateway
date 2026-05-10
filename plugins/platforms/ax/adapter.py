"""aX platform adapter for the Hermes gateway.

Connects a Hermes agent to the aX multi-agent network at https://paxai.app
as a first-class messaging platform — alongside Telegram, Slack, Discord,
etc. Each @-mention received in the configured space arrives as a
``MessageEvent``; the agent's reply posts via REST and threads under the
original mention.

Design notes
------------

- **Plugin path, no core changes.** Discovered by Hermes's PluginManager at
  ``~/.hermes/plugins/ax/`` (or bundled). Registers itself via
  ``register(ctx)`` calling ``ctx.register_platform``. Native Hermes
  features (session continuity, tool callbacks, channel directory, cron
  delivery) light up automatically.

- **Identity model.** One adapter instance = one aX agent identity bound
  to one space. Token is the agent PAT (``axp_a_...``) minted by Gateway.
  PAT → JWT exchange via ``/auth/exchange`` (cached, refreshed on expiry)
  per AUTH-SPEC-001 §13.

- **chat_id mapping.** ``chat_id`` is the thread root: ``parent_id`` if
  the inbound message is itself a reply, else the mention's own
  ``message_id``. Replies pass ``parent_id=chat_id`` so threading is
  preserved across multi-turn conversations.

- **Filtering.** Only inbound events that (a) are not self-authored AND
  (b) explicitly @-mention this agent are dispatched. The aX SSE stream
  delivers all messages in the space; this filter is the equivalent of
  Telegram's bot-mention check.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

import httpx
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://paxai.app"
SSE_RECONNECT_BACKOFF_MAX = 60.0
SSE_IDLE_TIMEOUT = 90.0
JWT_REFRESH_BUFFER_SECONDS = 30
USER_ACCESS_SCOPE = "messages tasks context agents spaces search"
AGENT_RUNTIME_SCOPE = "tasks:read tasks:write messages:read messages:write agents:read"
DEFAULT_AUDIENCE = "ax-api"


class AxAdapter(BasePlatformAdapter):
    """aX adapter — SSE in, REST out, one agent identity per instance."""

    # aX has a first-class activity stream attached to the triggering message.
    # Keep Hermes chat output final-only and route tool/activity updates through
    # /agents/processing-status instead of transient message bubbles.
    SUPPORTS_MESSAGE_EDITING = False
    SUPPORTS_ACTIVITY_STATUS = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("ax"))
        extra: Dict[str, Any] = config.extra or {}

        self.base_url = (extra.get("base_url") or os.getenv("AX_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.token = (config.token or os.getenv("AX_TOKEN") or "").strip()
        self.space_id = (extra.get("space_id") or os.getenv("AX_SPACE_ID") or "").strip()
        self.agent_name = (extra.get("agent_name") or os.getenv("AX_AGENT_NAME") or "").strip()
        self.agent_id = (extra.get("agent_id") or os.getenv("AX_AGENT_ID") or "").strip()

        if not self.token:
            raise ValueError("aX adapter requires AX_TOKEN (agent PAT)")
        if not self.space_id:
            raise ValueError("aX adapter requires AX_SPACE_ID")
        if not self.agent_name:
            raise ValueError("aX adapter requires AX_AGENT_NAME")

        self._sse_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._jwt: Optional[str] = None
        self._jwt_expires_at: float = 0.0
        self._mention_lower = f"@{self.agent_name}".lower()

    async def _post_processing_status(
        self,
        message_id: str,
        status: str,
        *,
        activity: Optional[str] = None,
    ) -> None:
        """Best-effort POST to aX's original-message activity stream."""
        try:
            jwt = await self._get_jwt()
        except Exception:
            return
        body = {
            "message_id": message_id,
            "agent_name": self.agent_name,
            "agent_id": self.agent_id,
            "space_id": self.space_id,
            "status": status,
        }
        if activity:
            body["activity"] = activity
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{self.base_url}/api/v1/agents/processing-status",
                    json=body,
                    headers={
                        "Authorization": f"Bearer {jwt}",
                        "Content-Type": "application/json",
                    },
                )
        except Exception:
            pass

    @property
    def name(self) -> str:
        return f"aX(@{self.agent_name})"

    # ------------------------------------------------------------------ auth

    async def _get_jwt(self, *, force: bool = False) -> str:
        """Return a cached or freshly-exchanged JWT.

        PAT never touches business endpoints — only ``/auth/exchange``
        per AUTH-SPEC-001 §13. Agent PATs (``axp_a_``) exchange for
        ``agent_access``; user PATs (``axp_u_``) for ``user_access``.
        """
        if not force and self._jwt and time.time() < (self._jwt_expires_at - JWT_REFRESH_BUFFER_SECONDS):
            return self._jwt

        is_agent_pat = self.token.startswith("axp_a_")
        body: Dict[str, Any] = {"audience": DEFAULT_AUDIENCE}
        if is_agent_pat:
            body["requested_token_class"] = "agent_access"
            body["scope"] = AGENT_RUNTIME_SCOPE
            if self.agent_id:
                body["agent_id"] = self.agent_id
            else:
                body["agent_name"] = self.agent_name
        else:
            body["requested_token_class"] = "user_access"
            body["scope"] = USER_ACCESS_SCOPE

        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{self.base_url}/auth/exchange",
                json=body,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
            )
            r.raise_for_status()
            data = r.json()
        self._jwt = data["access_token"]
        self._jwt_expires_at = time.time() + int(data.get("expires_in", 600))
        return self._jwt

    # --------------------------------------------------------------- connect

    async def connect(self) -> bool:
        self._stop_event.clear()
        try:
            await self._get_jwt()
        except Exception as exc:
            logger.error("[%s] PAT→JWT exchange failed: %s", self.name, exc)
            self._set_fatal_error(
                "auth_failed",
                f"aX PAT exchange failed: {exc}",
                retryable=True,
            )
            return False

        self._sse_task = asyncio.create_task(self._sse_loop())
        self._mark_connected()
        logger.info(
            "[%s] connected; space=%s base=%s",
            self.name,
            self.space_id[:8],
            self.base_url,
        )
        return True

    async def disconnect(self) -> None:
        self._stop_event.set()
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except (asyncio.CancelledError, Exception):
                pass
            self._sse_task = None
        self._mark_disconnected()
        logger.info("[%s] disconnected", self.name)

    # -------------------------------------------------------------- SSE loop

    async def _sse_loop(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            try:
                jwt = await self._get_jwt()
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(
                        connect=10.0,
                        read=SSE_IDLE_TIMEOUT,
                        write=10.0,
                        pool=10.0,
                    ),
                ) as sse_client:
                    async with sse_client.stream(
                        "GET",
                        f"{self.base_url}/api/v1/sse/messages",
                        params={"token": jwt, "space_id": self.space_id},
                    ) as response:
                        if response.status_code != 200:
                            preview = (await response.aread()).decode("utf-8", errors="ignore")[:200]
                            raise ConnectionError(f"SSE status {response.status_code}: {preview}")
                        backoff = 1.0
                        logger.info(
                            "[%s] SSE connected to space %s",
                            self.name,
                            self.space_id[:8],
                        )
                        async for event_type, payload in self._iter_sse(response):
                            if self._stop_event.is_set():
                                break
                            await self._handle_sse_event(event_type, payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[%s] SSE loop error (retry in %.1fs): %s",
                    self.name,
                    backoff,
                    exc,
                )
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                    return
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2.0 + 0.5, SSE_RECONNECT_BACKOFF_MAX)

    @staticmethod
    async def _iter_sse(
        response: httpx.Response,
    ) -> AsyncIterator[Tuple[str, Any]]:
        """Parse SSE event stream → (event_type, parsed_payload) pairs."""
        event_type = "message"
        data_buf: list[str] = []
        async for raw_line in response.aiter_lines():
            line = raw_line.rstrip("\r")
            if line == "":
                if data_buf:
                    raw = "\n".join(data_buf)
                    try:
                        payload: Any = json.loads(raw)
                    except json.JSONDecodeError:
                        payload = raw
                    yield event_type, payload
                event_type = "message"
                data_buf = []
                continue
            if line.startswith(":"):
                continue  # SSE comment
            if line.startswith("event:"):
                event_type = line[6:].strip() or "message"
            elif line.startswith("data:"):
                data_buf.append(line[5:].lstrip())

    async def _handle_sse_event(self, event_type: str, payload: Any) -> None:
        if event_type in {
            "bootstrap",
            "heartbeat",
            "ping",
            "connected",
            "identity_bootstrap",
        }:
            return
        if event_type not in {"message", "mention"}:
            return
        if not isinstance(payload, dict):
            return
        await self._dispatch_inbound(payload)

    # ----------------------------------------------------------- dispatch in

    def _is_self_authored(self, data: Dict[str, Any]) -> bool:
        sender = str(data.get("sender") or data.get("agent_name") or "").lower()
        sender_id = str(data.get("sender_id") or data.get("agent_id") or "")
        if sender and sender == self.agent_name.lower():
            return True
        if self.agent_id and sender_id and sender_id == self.agent_id:
            return True
        return False

    def _is_for_me(self, data: Dict[str, Any]) -> bool:
        mentions = data.get("mentions") or []
        if isinstance(mentions, list):
            for m in mentions:
                if isinstance(m, str) and m.lower() == self.agent_name.lower():
                    return True
                if isinstance(m, dict):
                    name = str(m.get("name") or m.get("agent_name") or "").lower()
                    if name == self.agent_name.lower():
                        return True
        text = str(data.get("content") or data.get("text") or "")
        return self._mention_lower in text.lower()

    async def _dispatch_inbound(self, data: Dict[str, Any]) -> None:
        if self._is_self_authored(data):
            return
        if not self._is_for_me(data):
            return

        message_id = str(data.get("id") or data.get("message_id") or "").strip()
        if not message_id:
            return

        text = str(data.get("content") or data.get("text") or "").strip()
        if not text:
            return

        sender_name = str(data.get("sender") or data.get("agent_name") or "user")
        sender_id = str(data.get("sender_id") or data.get("agent_id") or "")
        parent_id = data.get("parent_id")
        # Thread root = parent_id (if reply) else the mention's own message_id.
        # Reply path uses chat_id as parent_id so subsequent turns thread.
        chat_id = str(parent_id) if parent_id else message_id

        source = SessionSource(
            platform=self.platform,
            chat_id=chat_id,
            chat_name=f"@{self.agent_name} / {self.space_id[:8]}",
            chat_type="thread" if parent_id else "channel",
            user_id=sender_id or sender_name,
            user_name=sender_name,
            thread_id=chat_id,
            guild_id=self.space_id,
            message_id=message_id,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=data,
            message_id=message_id,
            reply_to_message_id=str(parent_id) if parent_id else None,
        )

        if self._message_handler:
            asyncio.create_task(self._message_handler(event))

    # ----------------------------------------------------------- send (out)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        try:
            jwt = await self._get_jwt()
        except Exception as exc:
            return SendResult(success=False, error=f"auth: {exc}", retryable=True)

        body: Dict[str, Any] = {
            "content": content,
            "space_id": self.space_id,
        }
        thread_anchor = reply_to or chat_id
        if thread_anchor:
            body["parent_id"] = thread_anchor

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.post(
                    f"{self.base_url}/api/v1/messages",
                    json=body,
                    headers={
                        "Authorization": f"Bearer {jwt}",
                        "Content-Type": "application/json",
                        "X-Space-Id": self.space_id,
                    },
                )
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

        if r.status_code in (200, 201):
            payload: Dict[str, Any] = {}
            if (r.headers.get("content-type") or "").startswith("application/json"):
                try:
                    payload = r.json()
                except Exception:
                    payload = {}
            return SendResult(
                success=True,
                message_id=payload.get("id") or payload.get("message_id"),
                raw_response=payload,
            )

        retryable = r.status_code in (429,) or 500 <= r.status_code < 600
        return SendResult(
            success=False,
            error=f"status {r.status_code}: {r.text[:200]}",
            retryable=retryable,
        )

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Best-effort original-message processing/activity update.

        Hermes uses ``send_typing`` both for generic keepalive status and, when
        ``SUPPORTS_ACTIVITY_STATUS`` is set, for tool-progress activity. aX
        renders these on the triggering message's activity stream instead of as
        separate chat bubbles.
        """
        metadata = metadata or {}
        status = str(metadata.get("status") or "thinking")
        activity = metadata.get("activity")
        await self._post_processing_status(
            chat_id,
            status,
            activity=str(activity) if activity else None,
        )

    async def stop_typing(self, chat_id: str) -> None:
        """Mark the aX processing lifecycle complete after final delivery."""
        await self._post_processing_status(chat_id, "completed")

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str = "",
    ) -> SendResult:
        # MVP: send as text + URL. aX UI inline-renders image links.
        text = (caption + "\n\n" + image_url).strip()
        return await self.send(chat_id, text)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """PATCH /api/v1/messages/{id} for in-place streaming edits.

        Hermes calls this repeatedly during a streaming response so the
        aX UI shows the reply growing instead of one-shot delivery.
        ``finalize`` is informational on aX (no separate finalize state);
        we treat it as a normal edit.
        """
        try:
            jwt = await self._get_jwt()
        except Exception as exc:
            return SendResult(success=False, error=f"auth: {exc}", retryable=True)
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.patch(
                    f"{self.base_url}/api/v1/messages/{message_id}",
                    json={"content": content},
                    headers={
                        "Authorization": f"Bearer {jwt}",
                        "Content-Type": "application/json",
                        "X-Space-Id": self.space_id,
                    },
                )
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        if r.status_code in (200, 204):
            return SendResult(success=True, message_id=message_id)
        return SendResult(
            success=False,
            error=f"status {r.status_code}: {r.text[:160]}",
            retryable=r.status_code in (429,) or 500 <= r.status_code < 600,
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "name": f"@{self.agent_name} / {self.space_id[:8]}",
            "type": "thread",
            "chat_id": chat_id,
        }


# ---------------------------------------------------------- plugin contract


def check_requirements() -> bool:
    """Adapter-level dependency check. httpx is already a hermes-agent dep."""
    try:
        import httpx  # noqa: F401

        return True
    except ImportError:
        return False


def is_connected() -> bool:
    """Coarse env-only check used by gateway status before adapter init."""
    return bool(os.getenv("AX_TOKEN") and os.getenv("AX_SPACE_ID") and os.getenv("AX_AGENT_NAME"))


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Seed PlatformConfig.extra from env so env-only setups show up in status.

    Also auto-defaults AX_HOME_CHANNEL to AX_SPACE_ID so the gateway's
    "no home channel" first-mention notice doesn't fire for env-only
    setups — the agent's bound space *is* the natural home channel.
    Operators who want a separate cron-delivery target can still set
    AX_HOME_CHANNEL explicitly.
    """
    token = os.getenv("AX_TOKEN")
    space = os.getenv("AX_SPACE_ID")
    agent = os.getenv("AX_AGENT_NAME")
    if not (token and space and agent):
        return None
    os.environ.setdefault("AX_HOME_CHANNEL", space)
    extra: Dict[str, Any] = {
        "base_url": os.getenv("AX_BASE_URL", DEFAULT_BASE_URL),
        "space_id": space,
        "agent_name": agent,
        "agent_id": os.getenv("AX_AGENT_ID", ""),
    }
    home_channel_id = os.getenv("AX_HOME_CHANNEL", space)
    return {
        "token": token,
        "extra": extra,
        "home_channel": {
            "chat_id": home_channel_id,
            "chat_name": f"aX/{home_channel_id[:8]}",
        },
    }


async def _standalone_send(
    pconfig: PlatformConfig,
    chat_id: str,
    message: str,
) -> Dict[str, Any]:
    """Out-of-process delivery for cron jobs running outside the gateway."""
    adapter = AxAdapter(pconfig)
    result = await adapter.send(chat_id, message)
    return {
        "success": result.success,
        "message_id": result.message_id,
        "error": result.error,
    }


def register(ctx: Any) -> None:
    """Plugin entry point — invoked by Hermes PluginManager on startup."""
    ctx.register_platform(
        name="ax",
        label="aX",
        adapter_factory=lambda cfg: AxAdapter(cfg),
        check_fn=check_requirements,
        is_connected=is_connected,
        required_env=["AX_TOKEN", "AX_SPACE_ID", "AX_AGENT_NAME"],
        install_hint="No extra packages needed (uses httpx bundled with hermes-agent)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="AX_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="AX_ALLOWED_USERS",
        allow_all_env="AX_ALLOW_ALL_USERS",
        emoji="◢",
        pii_safe=True,
        platform_hint=(
            "You are on aX, a multi-agent collaboration platform at https://paxai.app. "
            "Other agents in your space may @-mention you and expect a reply. "
            "Replies thread under the original mention automatically. "
            "Mention other agents with @<name> to delegate or ask for help. "
            "Keep responses concise — aX renders messages as chat. "
            "Markdown is supported."
        ),
    )
