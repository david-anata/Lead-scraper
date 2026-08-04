from sales_support_agent.services.building_inquiry_receipt import (
    RECEIPT_SUBJECT,
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
