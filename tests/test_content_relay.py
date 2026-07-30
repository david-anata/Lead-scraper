from __future__ import annotations

from typing import Any

import pytest

from sales_support_agent.integrations.content_relay import ContentRelayClient


class FakeTransport:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url, *, headers, payload, timeout_seconds):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        return self.responses.pop(0)


def _client(transport: FakeTransport) -> ContentRelayClient:
    return ContentRelayClient(
        base_url="https://relay.example.com",
        api_key="secret-key",
        transport=transport,
    )


def test_relay_requires_https_and_never_executes_staged_writes() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        ContentRelayClient(base_url="http://relay.example.com", api_key="key")
    transport = FakeTransport([])
    result = _client(transport).execute(
        action_key="linkedin_create_company_update",
        destination_identity="anata-company",
        idempotency_key="content-idempotency-key",
        payload={"text": "Useful operator lesson"},
        allow_write=False,
    )
    assert result.status == "staged"
    assert transport.calls == []


def test_relay_blocks_unknown_actions_and_missing_destination() -> None:
    transport = FakeTransport([])
    client = _client(transport)
    unknown = client.execute(
        action_key="clickup_create_task",
        destination_identity="not-allowed",
        idempotency_key="content-idempotency-key",
        payload={},
        allow_write=True,
    )
    missing = client.execute(
        action_key="youtube_upload_video",
        destination_identity="",
        idempotency_key="content-idempotency-key",
        payload={},
        allow_write=True,
    )
    assert unknown.status == "blocked"
    assert missing.status == "blocked"
    assert transport.calls == []


def test_relay_retries_transient_failure_with_stable_idempotency() -> None:
    transport = FakeTransport(
        [
            (503, {}),
            (
                200,
                {
                    "accepted": True,
                    "verified": True,
                    "provider_receipt": "receipt-1",
                    "public_url": "https://www.linkedin.com/feed/update/1",
                },
            ),
        ]
    )
    result = _client(transport).execute(
        action_key="linkedin_create_company_update",
        destination_identity="anata-company",
        idempotency_key="content-idempotency-key",
        payload={"text": "Useful operator lesson"},
        allow_write=True,
    )
    assert result.status == "delivered"
    assert result.verified is True
    assert len(transport.calls) == 2
    assert {
        call["headers"]["Idempotency-Key"] for call in transport.calls
    } == {"content-idempotency-key"}
    assert all("secret-key" not in str(call["payload"]) for call in transport.calls)


def test_relay_does_not_label_acceptance_as_delivery_without_live_evidence() -> None:
    transport = FakeTransport(
        [(202, {"accepted": True, "provider_receipt": "queued-1"})]
    )
    result = _client(transport).execute(
        action_key="youtube_upload_video",
        destination_identity="anata-youtube",
        idempotency_key="content-idempotency-key",
        payload={"media_url": "https://media.example.com/video.mp4"},
        allow_write=True,
    )
    assert result.accepted is True
    assert result.verified is False
    assert result.status == "running"
