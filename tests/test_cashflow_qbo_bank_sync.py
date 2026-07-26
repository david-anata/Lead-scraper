from datetime import date

import pytest

from sales_support_agent.services.cashflow.qbo_bank_sync import (
    _bill_payment_to_event,
    _booked_account,
    _category_from_account,
    _planned_events_for_matching,
    _purchase_to_event,
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


# --- QuickBooks' own categorisation is the answer, not ours ----------------

def test_the_account_quickbooks_booked_it_to_becomes_the_category():
    """The books already answered this. Guessing again and then asking the
    operator to answer a third time is the bug this closes."""
    event = _purchase_to_event({
        "Id": "77",
        "TxnDate": "2026-07-10",
        "TotalAmt": "412.00",
        "EntityRef": {"name": "Madison Bicycle Shop"},
        "Line": [{
            "Description": "Repair parts",
            "AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Job Expenses:Job Materials"}},
        }],
    })

    assert event["category"] == "job materials", "QuickBooks' account, not our keyword guess"
    assert event["subcategory"] == "Job Expenses:Job Materials", "the books' own wording is kept verbatim"


def test_a_booked_transaction_no_longer_looks_unfiled():
    """"other" is what puts a row in the decision queue. A booked transaction
    must never land there, whatever our keyword table would have said."""
    event = _purchase_to_event({
        "Id": "78",
        "TxnDate": "2026-07-10",
        "TotalAmt": "99.00",
        "EntityRef": {"name": "Some Vendor Nobody Recognises"},
        "Line": [{"AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Meals and Entertainment"}}}],
    })

    assert event["category"] == "meals and entertainment"
    assert event["category"] not in {"", "other", "uncategorized"}


def test_quickbooks_saying_it_does_not_know_still_needs_a_decision():
    """Booking to "Uncategorized Expense" or "Ask My Accountant" is QuickBooks
    admitting it has no answer, so the row must stay in the queue."""
    for account in ("Uncategorized Expense", "Ask My Accountant", "Other Expense", ""):
        assert _category_from_account(account) == "", account

    event = _purchase_to_event({
        "Id": "79", "TxnDate": "2026-07-10", "TotalAmt": "50.00",
        "EntityRef": {"name": "Mystery Vendor"},
        "Line": [{"AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Uncategorized Expense"}}}],
    })
    assert event["category"] == "other", "falls back to our guess, which finds nothing"
    assert event["subcategory"] == "Uncategorized Expense", "what QuickBooks said is still recorded"


@pytest.mark.parametrize("account,expected", [
    ("Payroll Expenses:Wages", "payroll"),
    ("Payroll Taxes", "payroll"),          # payroll wins over tax, deliberately
    ("Rent or Lease", "rent"),
    ("Insurance:Liability Insurance", "insurance"),
    ("Utilities:Gas and Electric", "utilities"),
    ("Dues and Subscriptions", "software"),
    ("Interest Expense", "loan"),
    ("Taxes and Licenses", "tax"),
])
def test_familiar_accounts_map_onto_buckets_the_app_already_reasons_about(account, expected):
    assert _category_from_account(account) == expected


def test_a_word_inside_another_account_does_not_trigger_a_bucket():
    """"Automobile:Gas" is fuel, not a gas utility bill."""
    assert _category_from_account("Automobile:Gas") == "gas"
    assert _category_from_account("Parent Teacher Store") == "parent teacher store"


def test_the_first_line_carrying_an_account_is_the_one_read():
    assert _booked_account([
        {"Description": "no detail here"},
        {"AccountBasedExpenseLineDetail": {"AccountRef": {"name": "Office Supplies"}}},
    ]) == "Office Supplies"
    assert _booked_account([]) == ""
    assert _booked_account(None) == ""


def test_a_bill_payment_without_an_expense_account_keeps_the_old_behaviour():
    """BillPayment lines link to bills rather than accounts, so there is
    usually nothing to read and the vendor keyword guess still applies."""
    event = _bill_payment_to_event({
        "Id": "bp-1", "TxnDate": "2026-07-17", "TotalAmt": "1100.00",
        "VendorRef": {"name": "Fulfillment Pay - Von"},
    })
    assert event["category"] == "fulfillment"
    assert event["subcategory"] == ""
