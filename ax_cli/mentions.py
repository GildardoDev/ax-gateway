"""Helpers for explicit aX handle mentions in message content."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

MENTION_RE = re.compile(r"(?<![\w/])@([A-Za-z0-9][A-Za-z0-9_-]{0,63})(?![\w/-])")

_FENCED_CODE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`\n]*`")


def _strip_code_spans(content: str) -> str:
    """Remove fenced code blocks and inline backtick spans from message content.

    Agent-authored replies frequently embed code that contains tokens which
    look like @-mentions (Python decorators, npm scopes such as ``@types/foo``,
    Ruby/Markdown attribute references). Those should not generate routing
    targets. We strip code first and then run the mention regex on the
    remainder. An unclosed triple-backtick fence is treated conservatively:
    everything from the opening fence to the end of content is dropped, on the
    theory that it is better to miss a real mention than to send a fake one.
    """
    if not content:
        return ""
    stripped = _FENCED_CODE_RE.sub(" ", content)
    unclosed = stripped.find("```")
    if unclosed != -1:
        stripped = stripped[:unclosed]
    stripped = _INLINE_CODE_RE.sub(" ", stripped)
    return stripped


def extract_explicit_mentions(content: str, *, exclude: Iterable[str] = ()) -> list[str]:
    """Return unique explicit @handles from user-visible message content.

    Mentions inside fenced code blocks or inline backtick spans are ignored,
    so routine code samples in agent replies do not generate spurious routing
    targets. Path-like values such as ``@types/node`` are also ignored — the
    trailing ``/`` lookahead pairs with the existing leading-slash lookbehind
    so neither side of a path emits a mention.
    """
    excluded = {item.lower().lstrip("@").strip() for item in exclude if item}
    seen: set[str] = set()
    mentions: list[str] = []
    for match in MENTION_RE.finditer(_strip_code_spans(content or "")):
        handle = match.group(1).strip()
        key = handle.lower()
        if not handle or key in excluded or key in seen:
            continue
        seen.add(key)
        mentions.append(handle)
    return mentions


def merge_explicit_mentions_metadata(
    metadata: dict[str, Any] | None,
    content: str,
    *,
    exclude: Iterable[str] = (),
) -> dict[str, Any] | None:
    """Merge explicit @mentions from content into message metadata.

    The backend remains the enforcement point for whether each handle can be
    routed. This helper preserves the client-side intent for replies, where the
    server may otherwise only route to the parent thread.
    """
    mentions = extract_explicit_mentions(content, exclude=exclude)
    if not mentions:
        return metadata

    merged = dict(metadata or {})
    existing_raw = merged.get("mentions") if isinstance(merged.get("mentions"), list) else []
    existing: list[Any] = list(existing_raw)
    existing_keys: set[str] = set()
    for item in existing:
        if isinstance(item, dict):
            raw = item.get("agent_name") or item.get("handle") or item.get("name") or ""
        else:
            raw = item
        key = str(raw).lower().lstrip("@").strip()
        if key:
            existing_keys.add(key)
    for mention in mentions:
        if mention.lower() not in existing_keys:
            existing.append(mention)
            existing_keys.add(mention.lower())
    merged["mentions"] = existing
    return merged
