#!/usr/bin/env python3
"""Gateway-managed bridge for Amazon Bedrock AgentCore Runtime.

Designed for `ax gateway agents add ... --template bedrock_agentcore`.
Invokes an AgentCore runtime once per inbound mention, streams the response,
and prints the reply on stdout following the Gateway exec-runtime contract.

Auth: boto3 default credential chain (env vars, ~/.aws/credentials, instance
profile, SSO session). Gateway never stores AWS secrets.

Session continuity: one persistent AgentCore runtime session per
(agent_id, space_id) pair. Cross-user contamination in wide group spaces
is documented in docs/gateway-agent-runtimes.md.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid
from typing import Any

EVENT_PREFIX = "AX_GATEWAY_EVENT "

_AGENTCORE_ARN_RE = re.compile(r"^arn:aws:bedrock-agentcore:([a-z0-9-]+):(\d{12}):runtime/(.+)$")
_MODEL_ARN_RE = re.compile(r"^arn:aws:bedrock:")

_HEARTBEAT_INTERVAL_SECS = 0.25
_HEARTBEAT_BYTE_BUDGET = 1024


def emit_event(payload: dict[str, Any]) -> None:
    print(f"{EVENT_PREFIX}{json.dumps(payload, sort_keys=True)}", flush=True)


def _read_prompt() -> str:
    if len(sys.argv) > 1 and sys.argv[-1] != "-":
        return sys.argv[-1]
    env_prompt = os.environ.get("AX_MENTION_CONTENT", "").strip()
    if env_prompt:
        return env_prompt
    return sys.stdin.read().strip()


# ---------------------------------------------------------------------------
# Deep module 1: template-options validator
# ---------------------------------------------------------------------------


def validate_bedrock_options(
    *,
    runtime_arn: str | None,
    region: str | None,
    qualifier: str | None,
    payload_key: str | None,
    aws_profile: str | None,
    template_id: str,
) -> dict[str, str]:
    """Validate and normalize bedrock template options.

    Pure function — no I/O, no boto3. Returns a validated options dict
    or raises ValueError with an actionable message.
    """
    bedrock_flags_set = any([runtime_arn, region, qualifier, payload_key, aws_profile])

    if bedrock_flags_set and template_id != "bedrock_agentcore":
        raise ValueError(f"--bedrock-* flags are only valid with --template bedrock_agentcore, not '{template_id}'.")

    arn = (runtime_arn or os.environ.get("AX_BEDROCK_RUNTIME_ARN", "")).strip()
    if not arn:
        raise ValueError(
            "bedrock_agentcore template requires --bedrock-runtime-arn "
            "(or AX_BEDROCK_RUNTIME_ARN env var). "
            "Expected shape: arn:aws:bedrock-agentcore:<region>:<account>:runtime/<id>"
        )

    if _MODEL_ARN_RE.match(arn):
        raise ValueError(
            f"ARN looks like a Bedrock foundation-model ARN, not an AgentCore runtime ARN: {arn!r}. "
            "Use the ARN from `agentcore list-runtimes`, not from the model catalog."
        )

    m = _AGENTCORE_ARN_RE.match(arn)
    if not m:
        raise ValueError(
            f"Invalid AgentCore runtime ARN: {arn!r}. "
            "Expected: arn:aws:bedrock-agentcore:<region>:<12-digit-account>:runtime/<id>"
        )

    arn_region = m.group(1)
    resolved_region = (region or "").strip() or arn_region or None

    return {
        "bedrock_runtime_arn": arn,
        "bedrock_region": resolved_region or "",
        "bedrock_qualifier": (qualifier or "").strip() or "DEFAULT",
        "bedrock_payload_key": (payload_key or "").strip() or "prompt",
        "aws_profile": (aws_profile or "").strip(),
    }


# ---------------------------------------------------------------------------
# Deep module 2: stream-event classifier
# ---------------------------------------------------------------------------


def classify_stream_event(line: str) -> tuple[str, Any]:
    """Classify one decoded SSE data line (minus the 'data: ' prefix).

    Returns one of:
        ("reply_text",    str)   — plain text, append to reply buffer
        ("error_event",   dict)  — in-band error, abort with status:error
        ("typed_event",   dict)  — tool_start / tool_result / activity / status
        ("heartbeat_chunk", dict) — unknown JSON dict, throttle before emitting
    """
    if not line:
        return ("reply_text", "")

    try:
        data = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return ("reply_text", line)

    if not isinstance(data, dict):
        return ("reply_text", str(data) if data else "")

    # Error shapes
    event_field = str(data.get("event") or "").lower()
    type_field = str(data.get("type") or "").lower()
    if event_field in ("error", "failed", "exception") or type_field == "error" or data.get("error"):
        msg = str(data.get("message") or data.get("error") or data.get("detail") or "unknown error")
        return ("error_event", {"message": msg, "detail": data})

    # Tool use → tool_start
    if data.get("type") == "tool_use" or (data.get("tool_name") and data.get("tool_call_id") and "input" in data):
        return (
            "typed_event",
            {
                "kind": "tool_start",
                "tool_name": str(data.get("tool_name") or data.get("name") or ""),
                "tool_call_id": str(data.get("tool_call_id") or data.get("id") or ""),
            },
        )

    # Tool result → tool_result
    if data.get("type") == "tool_result" or (data.get("tool_call_id") and "result" in data):
        status = "ok" if not data.get("is_error") else "error"
        return (
            "typed_event",
            {
                "kind": "tool_result",
                "tool_name": str(data.get("tool_name") or ""),
                "tool_call_id": str(data.get("tool_call_id") or ""),
                "status": status,
            },
        )

    # Thinking / trace / step / delta → activity
    if data.get("type") in ("thinking", "trace", "step", "delta") or data.get("thinking"):
        summary = str(data.get("thinking") or data.get("trace") or data.get("content") or "agent thinking")
        if len(summary) > 120:
            summary = summary[:117] + "..."
        return ("typed_event", {"kind": "activity", "activity": summary})

    # Assistant text delta (Strands-style)
    if data.get("type") == "content_block_delta" and isinstance(data.get("delta"), dict):
        delta_text = str(data["delta"].get("text") or "")
        if delta_text:
            return ("reply_text", delta_text)

    return ("heartbeat_chunk", data)


# ---------------------------------------------------------------------------
# Deep module 3: boto3 client builder
# ---------------------------------------------------------------------------


def build_bedrock_client(*, aws_profile: str, region: str, runtime_arn: str) -> Any:
    """Build and return a boto3 bedrock-agentcore client.

    Validates that the service is available in the installed boto3 version.
    Raises RuntimeError with an actionable message if not.
    """
    try:
        import boto3
        import botocore.config
    except ImportError as exc:
        raise RuntimeError(f"boto3 is not installed: {exc}. Install it with: pip install 'axctl[bedrock]'") from exc

    session = boto3.Session()
    available = session.get_available_services()
    if "bedrock-agentcore" not in available:
        raise RuntimeError(
            f"boto3 {boto3.__version__} does not include the bedrock-agentcore service client. "
            "Upgrade with: pip install --upgrade 'boto3>=1.38' 'axctl[bedrock]'"
        )

    kwargs: dict[str, Any] = {
        "config": botocore.config.Config(
            read_timeout=900,
            connect_timeout=10,
            retries={"max_attempts": 1, "mode": "standard"},
        ),
    }
    if region:
        kwargs["region_name"] = region

    return session.client("bedrock-agentcore", **kwargs)


# ---------------------------------------------------------------------------
# Session ID
# ---------------------------------------------------------------------------


def _compute_session_id(agent_id: str, space_id: str) -> str:
    """Stable, deterministic session id per (agent_id, space_id) pair.

    AgentCore requires runtimeSessionId >= 33 chars.
    """
    raw = uuid.uuid5(uuid.NAMESPACE_URL, f"ax-gateway:{agent_id}:{space_id}").hex
    return raw.ljust(33, "_")


# ---------------------------------------------------------------------------
# Bridge I/O loop
# ---------------------------------------------------------------------------


def _invoke_event_stream(
    client: Any, *, runtime_arn: str, session_id: str, payload_key: str, qualifier: str, prompt: str
) -> tuple[str, int]:
    """Invoke the AgentCore runtime and consume the streaming response.

    Returns (reply_text, chunk_count).
    """
    payload_body = json.dumps({payload_key: prompt})
    response = client.invoke_agent_runtime(
        agentRuntimeArn=runtime_arn,
        runtimeSessionId=session_id,
        qualifier=qualifier,
        payload=payload_body,
    )

    content_type = str(response.get("contentType") or "").lower()
    chunks = 0
    reply_parts: list[str] = []
    last_heartbeat_at = 0.0
    heartbeat_bytes = 0

    if "event-stream" in content_type or "text/event-stream" in content_type:
        streaming_body = response.get("response") or response.get("body")
        for raw_line in streaming_body.iter_lines():
            line = (
                raw_line.decode("utf-8", errors="replace").strip() if isinstance(raw_line, bytes) else raw_line.strip()
            )
            if not line:
                continue
            if line.startswith("data: "):
                line = line[6:]
            chunks += 1
            tag, value = classify_stream_event(line)

            if tag == "reply_text":
                if value:
                    reply_parts.append(value)
            elif tag == "error_event":
                emit_event({"kind": "status", "status": "error", "error_message": value.get("message", "unknown")})
                return ("", chunks)
            elif tag == "typed_event":
                emit_event(value)
            elif tag == "heartbeat_chunk":
                now = time.monotonic()
                heartbeat_bytes += len(line)
                if now - last_heartbeat_at >= _HEARTBEAT_INTERVAL_SECS or heartbeat_bytes >= _HEARTBEAT_BYTE_BUDGET:
                    emit_event({"kind": "activity", "activity": "AgentCore processing..."})
                    last_heartbeat_at = now
                    heartbeat_bytes = 0

    elif "application/json" in content_type:
        emit_event({"kind": "activity", "activity": "Buffering JSON response from AgentCore"})
        body_bytes = b""
        streaming_body = response.get("response") or response.get("body")
        for chunk in streaming_body.iter_chunks():
            body_bytes += chunk
            chunks += 1
        try:
            parsed = json.loads(body_bytes.decode("utf-8"))
        except Exception:
            parsed = body_bytes.decode("utf-8", errors="replace")
        if isinstance(parsed, dict):
            text = str(
                parsed.get("result") or parsed.get("output") or parsed.get("response") or parsed.get("text") or ""
            )
            reply_parts.append(text or json.dumps(parsed))
        else:
            reply_parts.append(str(parsed))
    else:
        emit_event({"kind": "activity", "activity": f"AgentCore response content-type: {content_type}"})
        streaming_body = response.get("response") or response.get("body")
        body_bytes = streaming_body.read() if hasattr(streaming_body, "read") else b""
        reply_parts.append(body_bytes.decode("utf-8", errors="replace"))

    return ("".join(reply_parts).strip(), chunks)


def main() -> int:
    prompt = _read_prompt()
    if not prompt:
        print("(no mention content received)", file=sys.stderr)
        return 1

    runtime_arn = os.environ.get("AX_BEDROCK_RUNTIME_ARN", "").strip()
    region = os.environ.get("AX_BEDROCK_REGION", "").strip()
    qualifier = os.environ.get("AX_BEDROCK_QUALIFIER", "DEFAULT").strip() or "DEFAULT"
    payload_key = os.environ.get("AX_BEDROCK_PAYLOAD_KEY", "prompt").strip() or "prompt"
    aws_profile = os.environ.get("AWS_PROFILE", "").strip()
    agent_id = (
        os.environ.get("AX_GATEWAY_AGENT_ID", "").strip() or os.environ.get("AX_AGENT_ID", "").strip() or "unknown"
    )
    space_id = (
        os.environ.get("AX_GATEWAY_SPACE_ID", "").strip() or os.environ.get("AX_SPACE_ID", "").strip() or "unknown"
    )

    if not runtime_arn:
        emit_event({"kind": "status", "status": "error", "error_message": "AX_BEDROCK_RUNTIME_ARN is not set"})
        print("bedrock_agentcore bridge: AX_BEDROCK_RUNTIME_ARN not set", file=sys.stderr)
        return 1

    try:
        client = build_bedrock_client(aws_profile=aws_profile, region=region, runtime_arn=runtime_arn)
    except RuntimeError as exc:
        emit_event({"kind": "status", "status": "error", "error_message": str(exc)})
        print(f"bedrock_agentcore bridge: {exc}", file=sys.stderr)
        return 1

    session_id = _compute_session_id(agent_id, space_id)

    emit_event({"kind": "status", "status": "processing", "message": "Invoking AgentCore runtime"})
    emit_event(
        {"kind": "activity", "activity": f"AgentCore runtime accepted invocation (session {session_id[:12]}...)"}
    )

    started = time.monotonic()
    try:
        reply, chunks = _invoke_event_stream(
            client,
            runtime_arn=runtime_arn,
            session_id=session_id,
            payload_key=payload_key,
            qualifier=qualifier,
            prompt=prompt,
        )
    except Exception as exc:
        try:
            import botocore.exceptions

            error_code = "unknown"
            if isinstance(exc, botocore.exceptions.ClientError):
                error_code = exc.response.get("Error", {}).get("Code", "ClientError")
            elif isinstance(exc, botocore.exceptions.BotoCoreError):
                error_code = type(exc).__name__
        except ImportError:
            error_code = type(exc).__name__
        emit_event(
            {"kind": "status", "status": "error", "error_message": str(exc), "detail": {"error_code": error_code}}
        )
        print(f"bedrock_agentcore bridge: {exc}", file=sys.stderr)
        return 1

    duration_ms = int((time.monotonic() - started) * 1000)
    emit_event(
        {
            "kind": "status",
            "status": "completed",
            "message": f"AgentCore completed in {duration_ms}ms",
            "detail": {"chunks": chunks, "session_id": session_id, "duration_ms": duration_ms},
        }
    )

    if reply:
        print(reply)
    else:
        print("(AgentCore returned no text reply — see activity stream for details)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
