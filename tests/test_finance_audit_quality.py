"""The audit reported 421 findings against real data and almost none were real.

These tests use the actual descriptor strings from that run.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bill_audit import (
    clear_dismissals,
    dismiss_finding,
    run_bill_audit,
)
from sales_support_agent.services.cashflow.transfers import is_internal_transfer

TODAY = date(2026, 7, 25)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _txn(engine, cid, name, amount, day, *, source="plaid", category="software"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source=source, source_id=cid,
            record_kind="transaction", event_type="outflow", category=category,
            name=name, vendor_or_customer=name, description=name,
            amount_cents=amount, due_date=day, status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )


def _kinds(findings):
    return [item["kind"] for item in findings]


# --- internal transfers ---------------------------------------------------

@pytest.mark.parametrize("descriptor", [
    "Home banking Withdrawal Transfer to S0050",
    "Home banking Deposit Transfer from S0058",
    "To Share 58",
    "To Share 01 Rentreserve",
    "Withdrawal Home A2A Transfer: ****5196",
    "Home banking Withdrawal COMMENT: A2A Transfer: ****5196",
])
def test_real_internal_transfer_descriptors_are_recognised(descriptor):
    assert is_internal_transfer({"name": descriptor}) is True


@pytest.mark.parametrize("descriptor", [
    "Transfer to Wise.com",          # paying a contractor, not an internal move
    "Fora Financial",
    "Rocky Mountain Power",
    "VA - Judie Terry",
])
def test_real_payments_are_not_mistaken_for_transfers(descriptor):
    assert is_internal_transfer({"name": descriptor}) is False


def test_transfers_never_appear_in_the_audit():
    engine = _setup()
    _txn(engine, "t1", "Home banking Withdrawal Transfer to S0050", 25000_00, TODAY - timedelta(days=2))
    _txn(engine, "t2", "Home banking Withdrawal Transfer to S0050", 25000_00, TODAY - timedelta(days=2))
    assert run_bill_audit(as_of=TODAY) == []


# --- duplicates -----------------------------------------------------------

def test_two_same_day_charges_from_a_frequent_merchant_are_not_flagged():
    """Two Amazon orders in a day is shopping, not a double charge."""
    engine = _setup()
    for index in range(14):  # Amazon is a frequent merchant
        _txn(engine, f"a{index}", "Amazon", 20_00 + index, TODAY - timedelta(days=index + 5))
    _txn(engine, "dup1", "Amazon", 24_14, TODAY - timedelta(days=2))
    _txn(engine, "dup2", "Amazon", 24_14, TODAY - timedelta(days=2))

    assert "duplicate" not in _kinds(run_bill_audit(as_of=TODAY))


def test_three_identical_same_day_charges_are_flagged_even_for_a_frequent_merchant():
    engine = _setup()
    for index in range(14):
        _txn(engine, f"g{index}", "GoDaddy", 20_00 + index, TODAY - timedelta(days=index + 5))
    for index in range(3):
        _txn(engine, f"trip{index}", "GoDaddy", 23_19, TODAY - timedelta(days=2))

    findings = [f for f in run_bill_audit(as_of=TODAY) if f["kind"] == "duplicate"]
    assert len(findings) == 1
    assert "3 identical charges" in findings[0]["detail"]


def test_a_large_pair_from_an_infrequent_merchant_is_flagged():
    engine = _setup()
    _txn(engine, "d1", "Eliteworks", 3200_00, TODAY - timedelta(days=3))
    _txn(engine, "d2", "Eliteworks", 3200_00, TODAY - timedelta(days=3))
    findings = [f for f in run_bill_audit(as_of=TODAY) if f["kind"] == "duplicate"]
    assert len(findings) == 1
    assert "not a frequent merchant" in findings[0]["detail"]


def test_a_small_pair_is_not_worth_flagging():
    engine = _setup()
    _txn(engine, "s1", "Dollar Tree", 1_34, TODAY - timedelta(days=3))
    _txn(engine, "s2", "Dollar Tree", 1_34, TODAY - timedelta(days=3))
    assert "duplicate" not in _kinds(run_bill_audit(as_of=TODAY))


def test_the_same_transaction_from_two_bank_sources_is_reported_once_as_a_data_issue():
    """Three feeds import the same payment, which was showing as hundreds of
    fake double charges."""
    engine = _setup()
    for index in range(30):
        day = TODAY - timedelta(days=index + 2)
        _txn(engine, f"p{index}", "Fora Financial", 2056_00, day, source="plaid")
        _txn(engine, f"q{index}", "Fora Financial", 2056_00, day, source="qbo_bank")

    findings = run_bill_audit(as_of=TODAY)
    overlaps = [f for f in findings if f["kind"] == "source_overlap"]
    assert len(overlaps) == 1, "one finding, not one per transaction"
    assert "30 transaction(s)" in overlaps[0]["detail"]
    assert "plaid" in overlaps[0]["detail"] and "qbo_bank" in overlaps[0]["detail"]
    # And they must not also be reported as double charges.
    assert "duplicate" not in _kinds(findings)


# --- price creep ----------------------------------------------------------

def test_price_creep_compares_recent_to_prior_not_min_to_max():
    engine = _setup()
    for index in range(4):  # prior window, around $99
        _txn(engine, f"old{index}", "Helium10", 99_00, TODAY - timedelta(days=100 + index * 5))
    for index in range(4):  # recent window, around $129
        _txn(engine, f"new{index}", "Helium10", 129_00, TODAY - timedelta(days=10 + index * 5))

    findings = [f for f in run_bill_audit(as_of=TODAY) if f["kind"] == "price_creep"]
    assert len(findings) == 1
    assert "+30%" in findings[0]["detail"]


def test_a_merchant_with_varying_amounts_but_no_real_rise_is_not_flagged():
    """Amazon Prime went "$2.14 to $16.11" only because those were different
    purchases, not a price rise."""
    engine = _setup()
    for index, amount in enumerate([2_14, 16_11, 5_00, 16_11]):
        _txn(engine, f"old{index}", "Amazon Prime", amount, TODAY - timedelta(days=100 + index * 5))
    for index, amount in enumerate([16_11, 2_14, 16_11, 5_00]):
        _txn(engine, f"new{index}", "Amazon Prime", amount, TODAY - timedelta(days=10 + index * 5))

    assert "price_creep" not in _kinds(run_bill_audit(as_of=TODAY))


@pytest.mark.parametrize("descriptor", [
    "Withdrawal", "Draft Withdrawal Draft #**0001", "Check # 40",
    "Analysis Fee", "To Share 58",
])
def test_generic_bank_wording_is_never_reported_as_a_price(descriptor):
    engine = _setup()
    for index in range(4):
        _txn(engine, f"old{index}", descriptor, 100_00, TODAY - timedelta(days=100 + index * 5))
    for index in range(4):
        _txn(engine, f"new{index}", descriptor, 900_00, TODAY - timedelta(days=10 + index * 5))
    assert "price_creep" not in _kinds(run_bill_audit(as_of=TODAY))


def test_too_few_charges_to_judge_is_not_a_finding():
    engine = _setup()
    _txn(engine, "a", "Loom", 30_00, TODAY - timedelta(days=100))
    _txn(engine, "b", "Loom", 48_00, TODAY - timedelta(days=10))
    assert "price_creep" not in _kinds(run_bill_audit(as_of=TODAY))


# --- dismissals -----------------------------------------------------------

def test_dismissals_can_be_cleared_for_a_fresh_start():
    engine = _setup()
    _txn(engine, "d1", "Eliteworks", 3200_00, TODAY - timedelta(days=3))
    _txn(engine, "d2", "Eliteworks", 3200_00, TODAY - timedelta(days=3))
    finding = run_bill_audit(as_of=TODAY)[0]
    dismiss_finding(finding["fingerprint"])
    assert run_bill_audit(as_of=TODAY) == []

    assert clear_dismissals() == 1
    assert len(run_bill_audit(as_of=TODAY)) == 1
