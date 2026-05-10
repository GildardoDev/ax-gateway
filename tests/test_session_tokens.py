from typing import Any

import pytest

from ax_cli import gateway as gateway_core


def _make_entry() -> dict[str, Any]:
    return {
        "name": "test-agent",
        "agent_id": "agent-test-1",
        "space_id": "space-1",
    }


def _make_registry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "agents": [entry],
        "gateway": {"gateway_id": "gw-test-1"},
    }


def test_issue_then_verify_round_trips_session_fields():
    entry = _make_entry()
    registry = _make_registry(entry)

    issued = gateway_core.issue_local_session(registry, entry)
    token = issued["session_token"]

    assert token.startswith("axgw_s_")
    session = gateway_core.verify_local_session_token(registry, token)
    assert session["agent_name"] == "test-agent"
    assert session["agent_id"] == "agent-test-1"
    assert session["session_id"] == issued["session"]["session_id"]


def test_tampered_signature_is_rejected():
    entry = _make_entry()
    registry = _make_registry(entry)
    token = gateway_core.issue_local_session(registry, entry)["session_token"]

    payload, signature = token.removeprefix("axgw_s_").split(".", 1)
    flipped = signature[:-1] + ("A" if signature[-1] != "A" else "B")
    tampered = f"axgw_s_{payload}.{flipped}"

    with pytest.raises(ValueError, match="Invalid Gateway local session token"):
        gateway_core.verify_local_session_token(registry, tampered)


def test_tampered_payload_breaks_signature():
    entry = _make_entry()
    registry = _make_registry(entry)
    token = gateway_core.issue_local_session(registry, entry)["session_token"]

    payload, signature = token.removeprefix("axgw_s_").split(".", 1)
    flipped_payload = ("A" if payload[0] != "A" else "B") + payload[1:]
    tampered = f"axgw_s_{flipped_payload}.{signature}"

    with pytest.raises(ValueError, match="Invalid Gateway local session token"):
        gateway_core.verify_local_session_token(registry, tampered)


def test_expired_token_is_rejected():
    entry = _make_entry()
    registry = _make_registry(entry)
    # Negative TTL => expires_at lands in the past at issue time.
    token = gateway_core.issue_local_session(registry, entry, ttl_seconds=-60)["session_token"]

    with pytest.raises(ValueError, match="expired"):
        gateway_core.verify_local_session_token(registry, token)


def test_revoked_session_is_rejected_even_with_valid_signature():
    entry = _make_entry()
    registry = _make_registry(entry)
    token = gateway_core.issue_local_session(registry, entry)["session_token"]

    # Flip the stored row to a non-active status without touching the token.
    registry["local_sessions"][0]["status"] = "revoked"

    with pytest.raises(ValueError, match="no longer active"):
        gateway_core.verify_local_session_token(registry, token)


def test_missing_registry_row_still_verifies_when_signature_and_ttl_are_valid():
    # Signature + TTL are the trust anchors; the registry row only carries
    # revocation state. A token with no matching row (e.g. registry rebuilt
    # or session_id pruned) is treated as active, not rejected.
    entry = _make_entry()
    registry = _make_registry(entry)
    token = gateway_core.issue_local_session(registry, entry)["session_token"]

    registry["local_sessions"] = []

    session = gateway_core.verify_local_session_token(registry, token)
    assert session["agent_name"] == "test-agent"


@pytest.mark.parametrize(
    "token",
    [
        "",
        None,
        "not-a-token",
        "axgw_s_no-dot-here",
        "wrong_prefix_abc.def",
    ],
)
def test_malformed_tokens_are_rejected(token):
    registry = _make_registry(_make_entry())
    with pytest.raises(ValueError, match="Invalid Gateway local session token"):
        gateway_core.verify_local_session_token(registry, token)


def test_payload_with_valid_signature_but_corrupt_json_is_rejected(monkeypatch):
    # Force a payload that signs cleanly but isn't valid JSON, to exercise
    # the json.loads branch in verify_local_session_token.
    secret = gateway_core.load_local_secret()
    bad_payload = gateway_core._b64url_encode(b"\xff\xfe not-json")
    signature = gateway_core._local_session_signature(bad_payload, secret)
    token = f"axgw_s_{bad_payload}.{signature}"

    registry = _make_registry(_make_entry())
    with pytest.raises(ValueError, match="payload"):
        gateway_core.verify_local_session_token(registry, token)
