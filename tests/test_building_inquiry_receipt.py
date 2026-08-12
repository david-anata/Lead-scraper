from types import SimpleNamespace
from unittest.mock import patch

from sales_support_agent.services.building_inquiry_receipt import (
    RECEIPT_SUBJECT,
    attempt_inquiry_receipt,
    receipt_body,
)


def test_event_inquiry_receipt_uses_approved_plain_copy() -> None:
    assert RECEIPT_SUBJECT == "We received your event inquiry"
    assert receipt_body("Jordan Lee") == (
        "Hi Jordan,\n\n"
        "Thank you for your interest in hosting your event at The Arena. "
        "We received your inquiry and will review the details. "
        "We’ll be in contact soon.\n\n"
        "The Anata Team"
    )
    assert "available" not in receipt_body("Jordan Lee").lower()
    assert "reserved" not in receipt_body("Jordan Lee").lower()


def test_customer_receipt_does_not_copy_internal_staff() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.added = []

        def get(self, _model, _identifier):
            return None

        def add(self, value) -> None:
            self.added.append(value)

    session = FakeSession()
    inquiry = SimpleNamespace(
        id="inquiry-1",
        name="Jordan Lee",
        email="jordan@example.com",
        payload_json={},
        updated_at=None,
    )
    with (
        patch(
            "sales_support_agent.services.building_inquiry_receipt.receipt_delivery_ready",
            return_value=(True, ""),
        ),
        patch(
            "sales_support_agent.services.building_inquiry_receipt.ResendClient.send_message",
            return_value="email-1",
        ) as send_message,
    ):
        result = attempt_inquiry_receipt(
            session,
            settings=SimpleNamespace(),
            inquiry=inquiry,
            actor="test",
        )

    assert result["status"] == "sent"
    assert send_message.call_args.kwargs["cc"] == ()
    assert send_message.call_args.kwargs["idempotency_key"].endswith(":v2")
