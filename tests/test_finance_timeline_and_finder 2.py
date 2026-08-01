from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.cash_timeline import build_cash_timeline
from sales_support_agent.services.cashflow.payment_finder import (
    find_overdue_needing_payment,
    find_payment_candidates,
    overdue_summary,
)
from sales_support_agent.services.cashflow.plaid import store_item
from sales_support_agent.services.cashflow.plaid_match import confirm_matches

TODAY = date(2026, 7, 25)


def _setup(*, cash_cents=50000_00):
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    item = store_item(item_id="i", access_token="t", token_secret="s", actor="qa", display_name="B")
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO plaid_accounts (id, plaid_item_id, external_account_id, name,
              official_name, mask, account_type, subtype, cash_role, currency,
              current_balance_cents, available_balance_cents, balance_as_of, active,
              created_at, updated_at)
            VALUES ('a1',:item,'e1','Checking','','1234','depository','checking',
              'spendable','USD',:bal,:bal,:now,TRUE,:now,:now)
        """), {"item": item, "bal": cash_cents, "now": now})
    return engine


def _bill(engine, cid, name, amount, due, status="planned", ctype="general"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            created_at=now, updated_at=now,
        )
        connection.execute(text("UPDATE cash_events SET commitment_type=:c WHERE id=:i"),
                           {"c": ctype, "i": cid})


def _inflow(engine, cid, name, amount, due):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="qbo", source_id=cid,
            record_kind="obligation", event_type="inflow", category="revenue",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status="planned", confidence="confirmed",
            created_at=now, updated_at=now,
        )


def _payment(engine, cid, name, amount, paid_on, source="qbo_bank"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source=source, source_id=cid,
            record_kind="transaction", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=paid_on, status="posted", confidence="confirmed",
            created_at=now, updated_at=now,
        )


# --- Timeline -------------------------------------------------------------

def test_timeline_runs_a_balance_through_each_item_in_date_order():
    engine = _setup(cash_cents=50000_00)
    _bill(engine, "rent", "Rent", 12000_00, TODAY + timedelta(days=2))
    _inflow(engine, "acme", "Acme payment", 8000_00, TODAY + timedelta(days=3))
    _bill(engine, "soft", "Software", 1000_00, TODAY + timedelta(days=5))

    timeline = build_cash_timeline(days=14, as_of=TODAY)
    assert timeline["opening_cents"] == 50000_00
    names = [entry["name"] for entry in timeline["entries"]]
    assert names == ["Rent", "Acme payment", "Software"]
    balances = [entry["running_cents"] for entry in timeline["entries"]]
    assert balances == [38000_00, 46000_00, 45000_00]
    assert timeline["closing_cents"] == 45000_00
    assert timeline["total_in_cents"] == 8000_00
    assert timeline["total_out_cents"] == 13000_00
    assert timeline["net_cents"] == -5000_00


def test_timeline_flags_going_negative_with_the_date():
    engine = _setup(cash_cents=1000_00)
    _bill(engine, "big", "Big bill", 5000_00, TODAY + timedelta(days=1))
    timeline = build_cash_timeline(days=14, as_of=TODAY)
    assert timeline["goes_negative"] is True
    assert timeline["lowest_cents"] == -4000_00
    assert timeline["lowest_on"] == (TODAY + timedelta(days=1)).isoformat()
    assert timeline["entries"][0]["negative"] is True


def test_timeline_excludes_overdue_and_out_of_window_items_but_reports_overdue():
    engine = _setup()
    _bill(engine, "old", "Old rent", 12000_00, TODAY - timedelta(days=40))
    _bill(engine, "far", "Far bill", 500_00, TODAY + timedelta(days=45))
    _bill(engine, "soon", "Soon bill", 300_00, TODAY + timedelta(days=1))

    timeline = build_cash_timeline(days=14, as_of=TODAY)
    assert [entry["name"] for entry in timeline["entries"]] == ["Soon bill"]
    # The overdue one is reported separately, not silently dropped.
    assert timeline["overdue"]["count"] == 1
    assert timeline["overdue"]["amount_cents"] == 12000_00


def test_timeline_puts_outflows_before_inflows_on_the_same_day():
    engine = _setup(cash_cents=10000_00)
    _inflow(engine, "in", "Money in", 5000_00, TODAY + timedelta(days=1))
    _bill(engine, "out", "Money out", 3000_00, TODAY + timedelta(days=1))
    timeline = build_cash_timeline(days=14, as_of=TODAY)
    assert [entry["direction"] for entry in timeline["entries"]] == ["out", "in"]


# --- Payment finder -----------------------------------------------------

def test_finds_a_check_payment_the_automatic_matcher_would_miss():
    """Vendor name unrecognizable and paid 20 days late: the strict matcher
    cannot link this, but the operator-driven finder must surface it."""
    engine = _setup()
    _bill(engine, "rent", "Lehi Rent", 12000_00, TODAY - timedelta(days=30))
    _payment(engine, "chk", "CHECK 1042", 12000_00, TODAY - timedelta(days=10))

    found = find_payment_candidates("rent")
    assert len(found["candidates"]) == 1
    candidate = found["candidates"][0]
    assert candidate["transaction_id"] == "chk"
    assert candidate["exact_amount"] is True
    assert candidate["day_gap"] == 20


def test_finder_ranks_exact_amount_first_and_respects_tolerance():
    engine = _setup()
    _bill(engine, "b", "Bill", 1000_00, TODAY - timedelta(days=5))
    _payment(engine, "close", "SOMETHING", 1010_00, TODAY - timedelta(days=4))
    _payment(engine, "exact", "OTHER", 1000_00, TODAY - timedelta(days=3))
    _payment(engine, "wayoff", "NOPE", 5000_00, TODAY - timedelta(days=3))

    ids = [c["transaction_id"] for c in find_payment_candidates("b")["candidates"]]
    assert ids[0] == "exact"
    assert "wayoff" not in ids  # outside the amount tolerance


def test_finder_ignores_payments_outside_the_day_window():
    engine = _setup()
    _bill(engine, "b", "Bill", 1000_00, TODAY - timedelta(days=200))
    _payment(engine, "far", "PAYMENT", 1000_00, TODAY - timedelta(days=10))
    assert find_payment_candidates("b", day_window=45)["candidates"] == []


def test_finder_skips_payments_already_linked_to_something():
    engine = _setup()
    _bill(engine, "b1", "Bill one", 1000_00, TODAY - timedelta(days=5))
    _bill(engine, "b2", "Bill two", 1000_00, TODAY - timedelta(days=5))
    _payment(engine, "p", "PAYMENT", 1000_00, TODAY - timedelta(days=4))

    assert len(find_payment_candidates("b1")["candidates"]) == 1
    confirm_matches([("p", "b1")], actor="qa")
    # Now spoken for, so it must not be offered to the other bill.
    assert find_payment_candidates("b2")["candidates"] == []


def test_overdue_list_and_summary_exclude_settled_bills():
    engine = _setup()
    _bill(engine, "paid", "Paid bill", 1000_00, TODAY - timedelta(days=10))
    _bill(engine, "unpaid", "Unpaid bill", 2000_00, TODAY - timedelta(days=20))
    _payment(engine, "p", "PAYMENT", 1000_00, TODAY - timedelta(days=9))
    confirm_matches([("p", "paid")], actor="qa")

    overdue = find_overdue_needing_payment(as_of=TODAY)
    assert [item["id"] for item in overdue] == ["unpaid"]
    summary = overdue_summary(as_of=TODAY)
    assert summary["count"] == 1
    assert summary["amount_cents"] == 2000_00
    assert summary["oldest_days"] == 20


def test_finder_rejects_a_transaction_as_the_target():
    engine = _setup()
    _payment(engine, "p", "PAYMENT", 1000_00, TODAY)
    with pytest.raises(ValueError, match="not a bill"):
        find_payment_candidates("p")
