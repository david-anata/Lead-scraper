from datetime import date, datetime, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.plaid_match import (
    auto_match_on_sync,
    confirm_matches,
    latest_run,
    propose_matches,
    undo_run,
)
from sales_support_agent.services.cashflow.settlements import get_open_balance_cents

PAID_ON = date(2026, 7, 20)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _bill(engine, *, cid, name, amount, due, commitment_type="general", status="planned"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type="outflow", category="rent",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            created_at=now, updated_at=now,
        )
        connection.execute(
            text("UPDATE cash_events SET commitment_type=:ct WHERE id=:id"),
            {"ct": commitment_type, "id": cid},
        )


def _payment(engine, *, cid, name, amount, paid_on=PAID_ON, source="plaid"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source=source, source_id=cid,
            record_kind="transaction", event_type="outflow", category="rent",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=paid_on, status="posted", confidence="confirmed",
            created_at=now, updated_at=now,
        )


def test_proposes_match_for_matching_payment():
    engine = _setup()
    _bill(engine, cid="bill-rent", name="Lehi Rent", amount=12000_00, due=date(2026, 7, 19))
    _payment(engine, cid="pay-rent", name="Lehi Rent", amount=12000_00)

    proposals = propose_matches()
    assert len(proposals) == 1
    assert proposals[0]["obligation_id"] == "bill-rent"
    assert proposals[0]["transaction_id"] == "pay-rent"
    assert proposals[0]["confidence"] in {"high", "medium"}
    assert proposals[0]["protected"] is False


def test_confirming_a_match_settles_the_bill_and_undo_restores_it():
    engine = _setup()
    _bill(engine, cid="bill-rent", name="Lehi Rent", amount=12000_00, due=date(2026, 7, 19))
    _payment(engine, cid="pay-rent", name="Lehi Rent", amount=12000_00)
    assert get_open_balance_cents("bill-rent") == 12000_00

    result = confirm_matches([("pay-rent", "bill-rent")], actor="qa@example.com")
    assert result["confirmed"] == 1
    assert result["failed"] == 0
    assert get_open_balance_cents("bill-rent") == 0  # settled

    run = latest_run()
    assert run is not None and run["confirmed_count"] == 1

    undo = undo_run(str(run["id"]), actor="qa@example.com")
    assert undo["reversed"] == 1
    assert get_open_balance_cents("bill-rent") == 12000_00  # reopened
    assert latest_run() is None  # run marked undone


def test_already_matched_payment_is_not_proposed_again():
    engine = _setup()
    _bill(engine, cid="bill-rent", name="Lehi Rent", amount=12000_00, due=date(2026, 7, 19))
    _payment(engine, cid="pay-rent", name="Lehi Rent", amount=12000_00)
    confirm_matches([("pay-rent", "bill-rent")], actor="qa")

    assert propose_matches() == []


def test_protected_commitments_are_not_auto_matched_on_sync():
    engine = _setup()
    _bill(engine, cid="bill-payroll", name="Gusto Payroll", amount=22400_00,
          due=date(2026, 7, 19), commitment_type="payroll")
    _payment(engine, cid="pay-payroll", name="Gusto Payroll", amount=22400_00)

    proposals = propose_matches()
    assert len(proposals) == 1
    assert proposals[0]["protected"] is True

    # Sync-time auto-matching must skip it; it stays open for explicit review.
    result = auto_match_on_sync(actor="plaid-sync")
    assert result["confirmed"] == 0
    assert get_open_balance_cents("bill-payroll") == 22400_00


def test_unrelated_payment_is_not_proposed():
    engine = _setup()
    _bill(engine, cid="bill-rent", name="Lehi Rent", amount=12000_00, due=date(2026, 7, 19))
    _payment(engine, cid="pay-random", name="Totally Different Vendor", amount=37_42)
    assert propose_matches() == []


def test_non_plaid_transactions_are_left_to_their_own_sync():
    engine = _setup()
    _bill(engine, cid="bill-rent", name="Lehi Rent", amount=12000_00, due=date(2026, 7, 19))
    _payment(engine, cid="pay-qbo", name="Lehi Rent", amount=12000_00, source="qbo_bank")
    assert propose_matches() == []
