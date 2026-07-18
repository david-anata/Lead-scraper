from datetime import date

from sales_support_agent.services.cashflow.qbo_bank_sync import (
    _bill_payment_to_event,
    _planned_events_for_matching,
    _qbo_entity_queries,
    _qbo_query_all,
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


def test_qbo_actuals_uses_queryable_entities_and_keeps_checks_under_purchase():
    entities = [name for name, _query, _converter in _qbo_entity_queries("2026-01-01")]

    assert entities == ["Purchase", "Deposit", "BillPayment", "Payment"]
    assert "Check" not in entities


def test_vendor_settlement_without_a_nontrivial_amount_is_ignored():
    assert _bill_payment_to_event({"Id": "zero", "TxnDate": "2026-07-17", "TotalAmt": 0}) is None


def test_qbo_actuals_can_match_a_completed_clickup_obligation():
    eligible = _planned_events_for_matching([
        {"id": "completed", "source": "clickup", "status": "completed", "amount_cents": 110_000},
        {"id": "posted", "source": "qbo_bank", "record_kind": "transaction", "status": "posted", "amount_cents": 110_000},
        {"id": "duplicate", "source": "clickup", "status": "completed", "source_status": "probable_duplicate", "amount_cents": 110_000},
    ])

    assert [row["id"] for row in eligible] == ["completed"]


def test_qbo_actuals_query_paginates_beyond_the_provider_page_limit(monkeypatch):
    pages = [
        [{"Id": str(index)} for index in range(1_000)],
        [{"Id": "1000"}],
    ]
    queries = []

    def fake_query(_base, _realm, _token, query):
        queries.append(query)
        return pages.pop(0)

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.qbo_bank_sync._qbo_query", fake_query
    )
    rows = _qbo_query_all(
        "https://qbo.example", "realm", "token",
        "SELECT * FROM Payment WHERE TxnDate >= '2026-01-01' MAXRESULTS 1000",
    )

    assert len(rows) == 1_001
    assert queries == [
        "SELECT * FROM Payment WHERE TxnDate >= '2026-01-01' STARTPOSITION 1 MAXRESULTS 1000",
        "SELECT * FROM Payment WHERE TxnDate >= '2026-01-01' STARTPOSITION 1001 MAXRESULTS 1000",
    ]
