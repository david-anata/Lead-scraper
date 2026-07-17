from datetime import date

from sales_support_agent.services.cashflow.qbo_bank_sync import (
    _bill_payment_to_event,
    _check_to_event,
)


def test_bill_payment_keeps_vendor_amount_and_date_for_settlement_matching():
    event = _bill_payment_to_event({
        "Id": "bill-payment-123",
        "TxnDate": "2026-07-17",
        "TotalAmt": "1100.00",
        "VendorRef": {"name": "Fulfillment Pay - Von"},
        "PrivateNote": "July fulfillment payment",
    })

    assert event == {
        "id": "qbo-billpayment-bill-payment-123",
        "source": "qbo_bank",
        "source_id": "billpayment-bill-payment-123",
        "event_type": "outflow",
        "category": "fulfillment",
        "subcategory": "",
        "description": "July fulfillment payment",
        "name": "Fulfillment Pay - Von",
        "vendor_or_customer": "Fulfillment Pay - Von",
        "amount_cents": 110_000,
        "due_date": date(2026, 7, 17),
        "status": "posted",
        "confidence": "confirmed",
        "recurring_rule": "",
        "clickup_task_id": "",
        "bank_transaction_type": "BillPayment",
        "bank_reference": "bill-payment-123",
        "notes": "QBO BillPayment | July fulfillment payment",
    }


def test_check_can_use_payee_reference_when_vendor_reference_is_absent():
    event = _check_to_event({
        "Id": "check-456",
        "TxnDate": "2026-07-16",
        "TotalAmt": 174,
        "PayeeRef": {"name": "Rocky Mountain Power"},
    })

    assert event is not None
    assert event["vendor_or_customer"] == "Rocky Mountain Power"
    assert event["amount_cents"] == 17_400
    assert event["bank_transaction_type"] == "Check"


def test_vendor_settlement_without_a_nontrivial_amount_is_ignored():
    assert _bill_payment_to_event({"Id": "zero", "TxnDate": "2026-07-17", "TotalAmt": 0}) is None
