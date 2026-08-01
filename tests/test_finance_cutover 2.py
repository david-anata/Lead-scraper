"""Switching the old ClickUp bill list off has to be an evidence-based decision.

Every test here asks the question the operator asks: if I flip the switch now,
what leaves my forecast?
"""

from datetime import date, datetime, timedelta, timezone

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.cutover import (
    assess_cutover_readiness,
    render_cutover_readiness,
)
from sales_support_agent.services.cashflow.obligations import create_recurring_template

TODAY = date(2026, 7, 26)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _clickup_bill(engine, cid, name, amount, due, *, status="planned",
                  rule="", event_type="outflow"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type=event_type, category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="estimated",
            recurring_rule=rule, created_at=now, updated_at=now,
        )


def _bank_payment(engine, cid, name, amount, paid_on):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="qbo_bank", source_id=cid,
            record_kind="transaction", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=paid_on, status="posted", confidence="confirmed",
            created_at=now, updated_at=now,
        )


def _template(name, amount, frequency="monthly", next_due=None):
    return create_recurring_template(
        name=name, vendor_or_customer=name, event_type="outflow",
        category="other", amount_cents=amount, frequency=frequency,
        next_due_date=next_due or (TODAY + timedelta(days=10)),
    )


# --- coverage --------------------------------------------------------------

def test_bill_with_matching_native_schedule_is_covered_and_not_at_risk():
    engine = _setup()
    _clickup_bill(engine, "c1", "Acme Hosting", 120_00, TODAY + timedelta(days=12),
                  rule="monthly")
    _template("Acme Hosting", 120_00)

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["covered"] == 1
    assert result["counts"]["uncovered"] == 0
    assert result["at_risk_cents"] == 0
    assert result["covered"][0]["covered_by"] == "Acme Hosting"


def test_future_bill_with_no_schedule_blocks_the_switch_with_its_money_named():
    engine = _setup()
    _clickup_bill(engine, "c1", "Riverside Rent", 4_200_00, TODAY + timedelta(days=9))

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["ready"] is False
    assert result["counts"]["uncovered"] == 1
    assert result["at_risk_cents"] == 4_200_00
    assert result["uncovered"][0]["name"] == "Riverside Rent"
    assert result["uncovered"][0]["due_date"] == (TODAY + timedelta(days=9)).isoformat()
    assert "$4,200" in result["summary"]


def test_a_repeating_series_counts_once_not_once_per_occurrence():
    engine = _setup()
    for index, month in enumerate((4, 5, 6, 7)):
        _clickup_bill(engine, f"c{index}", "Riverside Rent", 4_200_00,
                      date(2026, month, 5), rule="monthly")

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["uncovered"] == 1
    assert result["at_risk_cents"] == 4_200_00
    assert result["uncovered"][0]["occurrences"] == 4
    # The date shown is the next one that would have arrived, not an old one.
    assert result["uncovered"][0]["due_date"] > TODAY.isoformat()


def test_series_covered_by_a_schedule_leaves_the_blocking_list():
    engine = _setup()
    for index, month in enumerate((5, 6, 7)):
        _clickup_bill(engine, f"c{index}", "Riverside Rent", 4_200_00,
                      date(2026, month, 5), rule="monthly")
    before = assess_cutover_readiness(as_of=TODAY)
    assert before["counts"]["uncovered"] == 1

    _template("Riverside Rent", 4_200_00)
    after = assess_cutover_readiness(as_of=TODAY)

    assert after["counts"]["uncovered"] == 0
    assert after["counts"]["covered"] == 1
    assert after["at_risk_cents"] == 0
    assert after["ready"] is True


def test_a_schedule_on_the_wrong_cadence_does_not_count_as_cover():
    engine = _setup()
    _clickup_bill(engine, "c1", "Acme Hosting", 120_00, TODAY + timedelta(days=12),
                  rule="monthly")
    _template("Acme Hosting", 120_00, frequency="annual")

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["covered"] == 0
    assert result["counts"]["uncovered"] == 1


# --- history and leftovers -------------------------------------------------

def test_past_bill_the_bank_already_paid_is_history_and_does_not_block():
    engine = _setup()
    paid_on = TODAY - timedelta(days=6)
    _clickup_bill(engine, "c1", "Northside Insurance", 850_00, paid_on, status="overdue")
    _bank_payment(engine, "b1", "NORTHSIDE INS", 850_00, paid_on)

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["settled"] == 1
    assert result["counts"]["uncovered"] == 0
    assert result["ready"] is True


def test_undated_bill_does_not_block_the_switch():
    engine = _setup()
    _clickup_bill(engine, "c1", "Someday Software", 300_00, None)

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["undated"] == 1
    assert result["counts"]["uncovered"] == 0
    assert result["at_risk_cents"] == 0
    assert result["ready"] is True


def test_a_cancelled_bill_does_not_block_the_switch():
    engine = _setup()
    _clickup_bill(engine, "c1", "Dropped Service", 90_00, TODAY + timedelta(days=4),
                  status="cancelled")

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["counts"]["uncovered"] == 0
    assert result["ready"] is True


def test_one_uncovered_bill_among_safe_ones_still_blocks():
    engine = _setup()
    _clickup_bill(engine, "c1", "Acme Hosting", 120_00, TODAY + timedelta(days=12))
    _template("Acme Hosting", 120_00)
    _clickup_bill(engine, "c2", "Someday Software", 300_00, None)
    _clickup_bill(engine, "c3", "Riverside Rent", 4_200_00, TODAY + timedelta(days=9))

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["ready"] is False
    assert result["counts"] == {
        "covered": 1, "settled": 0, "undated": 1, "uncovered": 1, "total": 3,
    }
    assert result["at_risk_cents"] == 4_200_00


# --- the verdict a person reads -------------------------------------------

def test_everything_covered_reads_as_safe_to_switch_off():
    engine = _setup()
    _clickup_bill(engine, "c1", "Acme Hosting", 120_00, TODAY + timedelta(days=12))
    _template("Acme Hosting", 120_00)
    _clickup_bill(engine, "c2", "Someday Software", 300_00, None)

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["ready"] is True
    assert "costs you nothing" in result["summary"]
    assert "disappear" not in result["summary"]


def test_empty_list_is_ready_and_says_so_plainly():
    _setup()

    result = assess_cutover_readiness(as_of=TODAY)

    assert result["ready"] is True
    assert result["counts"]["total"] == 0
    assert "nothing live" in result["summary"]


def test_the_flag_state_is_reported_from_settings(monkeypatch):
    _setup()
    import sales_support_agent.services.cashflow.cutover as cutover

    monkeypatch.setattr(cutover, "_flag_is_set", lambda: True)
    result = assess_cutover_readiness(as_of=TODAY)

    assert result["already_switched_off"] is True
    assert "switched off already" in result["summary"]


def test_the_card_names_the_bills_that_would_disappear():
    engine = _setup()
    _clickup_bill(engine, "c1", "Riverside <Rent>", 4_200_00, TODAY + timedelta(days=9))

    fragment = render_cutover_readiness(assess_cutover_readiness(as_of=TODAY))

    assert "Not safe yet" in fragment
    assert "Riverside &lt;Rent&gt;" in fragment  # user data is escaped, never raw
    assert "$4,200" in fragment
    assert "&mdash;" not in fragment


def test_the_card_says_it_is_safe_when_nothing_is_uncovered():
    engine = _setup()
    _clickup_bill(engine, "c1", "Acme Hosting", 120_00, TODAY + timedelta(days=12))
    _template("Acme Hosting", 120_00)

    fragment = render_cutover_readiness(assess_cutover_readiness(as_of=TODAY))

    assert "Safe to switch off" in fragment
    assert "Not safe yet" not in fragment
    assert "1 covered by a schedule here" in fragment
