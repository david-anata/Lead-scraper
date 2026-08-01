"""Rebuilding the native schedule from existing ClickUp data."""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.schedule_import import (
    _series_name,
    apply_import,
    build_import_plan,
    propose_overdue_disposition,
    propose_schedules,
    propose_unscheduled,
)

TODAY = date(2026, 7, 25)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _clickup(engine, cid, name, amount, due, *, rule="", confidence="estimated",
             status="planned", ctype="general", event_type="outflow"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type=event_type, category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence=confidence,
            recurring_rule=rule, created_at=now, updated_at=now,
        )
        connection.execute(text("UPDATE cash_events SET commitment_type=:c WHERE id=:i"),
                           {"c": ctype, "i": cid})


def _bank_payment(engine, cid, name, amount, paid_on):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="plaid", source_id=cid,
            record_kind="transaction", event_type="outflow", category="other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=paid_on, status="posted", confidence="confirmed",
            created_at=now, updated_at=now,
        )


def test_series_name_strips_per_occurrence_noise():
    assert _series_name("Software (Week 4)") == "Software"
    assert _series_name("Fulfillment Pay - Von") == "Fulfillment Pay - Von"


def test_weekly_series_is_proposed_with_cadence_and_weekday():
    engine = _setup()
    # Fulfillment Pay to Von: $1,100 every Monday, marked Manual in ClickUp.
    for index, day in enumerate([date(2026, 6, 29), date(2026, 7, 6), date(2026, 7, 13)]):
        _clickup(engine, f"von{index}", "Fulfillment Pay - Von", 1100_00, day, rule="weekly")

    proposals = propose_schedules(as_of=TODAY)
    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal["amount_cents"] == 1100_00
    assert proposal["frequency"] == "weekly"
    assert proposal["weekday"] == "Monday"
    assert proposal["payment_method"] == "manual"
    assert date.fromisoformat(proposal["next_due"]) >= TODAY


def test_auto_flag_survives_only_when_the_source_was_consistently_auto():
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 9), date(2026, 6, 9), date(2026, 7, 9)]):
        _clickup(engine, f"rmp{index}", "Rocky Mountain Power", 174_00, day,
                 rule="monthly", confidence="confirmed")
    assert propose_schedules(as_of=TODAY)[0]["payment_method"] == "auto"


def test_mixed_signals_fall_back_to_manual():
    """A wrong 'manual' costs a minute; a wrong 'auto' costs a missed payment."""
    engine = _setup()
    _clickup(engine, "m1", "Lehi City Power", 1860_00, date(2026, 5, 17), rule="monthly", confidence="confirmed")
    _clickup(engine, "m2", "Lehi City Power", 1860_00, date(2026, 6, 17), rule="monthly", confidence="estimated")
    assert propose_schedules(as_of=TODAY)[0]["payment_method"] == "manual"


def test_varying_amounts_are_flagged_and_use_the_middle_value():
    engine = _setup()
    for index, (day, amount) in enumerate([
        (date(2026, 5, 8), 180_00), (date(2026, 6, 8), 200_00), (date(2026, 7, 8), 260_00),
    ]):
        _clickup(engine, f"citi{index}", "Citi Card Minimum", amount, day, rule="monthly")
    proposal = propose_schedules(as_of=TODAY)[0]
    assert proposal["amount_varies"] is True
    assert proposal["amount_cents"] == 200_00  # median, not the extreme


def test_payroll_is_marked_as_an_estimate():
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 5), date(2026, 6, 5), date(2026, 7, 5)]):
        _clickup(engine, f"pay{index}", "Payroll 5th", 5000_00, day, rule="monthly", ctype="payroll")
    assert propose_schedules(as_of=TODAY)[0]["is_estimate"] is True


def test_a_one_off_is_not_turned_into_a_schedule():
    engine = _setup()
    _clickup(engine, "once", "Annual audit fee", 900_00, date(2026, 6, 1))
    assert propose_schedules(as_of=TODAY) == []


def test_overdue_is_sorted_by_whether_the_bank_shows_a_payment():
    engine = _setup()
    _clickup(engine, "paid", "Old rent", 12000_00, TODAY - timedelta(days=40))
    _clickup(engine, "unpaid", "Forgotten bill", 750_00, TODAY - timedelta(days=30))
    _bank_payment(engine, "txn", "CHECK 1042", 12000_00, TODAY - timedelta(days=35))

    by_id = {item["id"]: item for item in propose_overdue_disposition(as_of=TODAY)}
    assert by_id["paid"]["recommendation"] == "archive"
    assert by_id["paid"]["looks_paid"] is True
    assert by_id["unpaid"]["recommendation"] == "keep"
    assert by_id["unpaid"]["looks_paid"] is False


def test_items_covered_by_a_proposed_schedule_are_not_also_listed_as_overdue():
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 5), date(2026, 6, 5), date(2026, 7, 5)]):
        _clickup(engine, f"pay{index}", "Payroll 5th", 5000_00, day, rule="monthly")
    ids = {item["id"] for item in propose_overdue_disposition(as_of=TODAY)}
    assert ids == set(), "schedule occurrences must not double up in the overdue list"


def test_undated_items_are_listed_separately():
    engine = _setup()
    _clickup(engine, "nodate", "Maybe a bill", 500_00, None)
    assert [item["id"] for item in propose_unscheduled()] == ["nodate"]
    assert propose_schedules(as_of=TODAY) == []


def test_apply_creates_schedules_and_vendors_and_archives_the_source():
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 9), date(2026, 6, 9), date(2026, 7, 9)]):
        _clickup(engine, f"rmp{index}", "Rocky Mountain Power", 174_00, day,
                 rule="monthly", confidence="confirmed")

    key = propose_schedules(as_of=TODAY)[0]["key"]
    result = apply_import(schedule_keys=[key], archive_ids=[], keep_ids=[], actor="qa", as_of=TODAY)

    assert result["templates_created"] == 1
    assert result["vendors_created"] == 1
    assert result["archived"] == 3, "the ClickUp rows it replaces are filed away"

    from sales_support_agent.services.cashflow.obligations import list_recurring_templates
    from sales_support_agent.services.cashflow.vendors import list_vendors_with_progress
    template = list_recurring_templates()[0]
    assert template["name"] == "Rocky Mountain Power"
    assert template["amount_cents"] == 174_00
    assert template["frequency"] == "monthly"
    vendor = list_vendors_with_progress()[0]
    assert vendor["payment_method"] == "auto"


def test_apply_keeps_what_the_operator_chose_to_keep():
    engine = _setup()
    _clickup(engine, "unpaid", "Forgotten bill", 750_00, TODAY - timedelta(days=30))
    apply_import(schedule_keys=[], archive_ids=["unpaid"], keep_ids=["unpaid"], actor="qa", as_of=TODAY)
    with engine.connect() as connection:
        archived = connection.execute(
            text("SELECT archived_at FROM cash_events WHERE id='unpaid'")
        ).scalar()
    assert archived is None, "an explicit keep must win over an archive tick"


def test_nothing_is_written_by_building_the_plan():
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 9), date(2026, 6, 9), date(2026, 7, 9)]):
        _clickup(engine, f"rmp{index}", "Rocky Mountain Power", 174_00, day, rule="monthly")
    build_import_plan(as_of=TODAY)

    from sales_support_agent.services.cashflow.obligations import list_recurring_templates
    assert list_recurring_templates() == []
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM cash_events WHERE archived_at IS NOT NULL")
        ).scalar() == 0


def test_monthly_schedules_keep_their_day_and_do_not_drift():
    """Stepping 30 days walks a monthly bill backwards, so "Payroll 5th" would
    become the 4th, then the 3rd. It must advance by calendar month."""
    engine = _setup()
    for index, day in enumerate([date(2026, 5, 5), date(2026, 6, 5), date(2026, 7, 5)]):
        _clickup(engine, f"pay{index}", "Payroll 5th", 5000_00, day, rule="monthly")
    for index, day in enumerate([date(2026, 5, 20), date(2026, 6, 20), date(2026, 7, 20)]):
        _clickup(engine, f"res{index}", "Payroll Reserve 20th", 5000_00, day, rule="monthly")

    by_name = {item["name"]: item for item in propose_schedules(as_of=TODAY)}
    assert by_name["Payroll 5th"]["day_of_month"] == 5
    assert date.fromisoformat(by_name["Payroll 5th"]["next_due"]).day == 5
    assert by_name["Payroll Reserve 20th"]["day_of_month"] == 20
    assert date.fromisoformat(by_name["Payroll Reserve 20th"]["next_due"]).day == 20


def test_a_one_off_date_shift_does_not_move_the_schedule_day():
    engine = _setup()
    # Landed on the 9th twice and once on the 11th (a weekend shift).
    for index, day in enumerate([date(2026, 5, 9), date(2026, 6, 11), date(2026, 7, 9)]):
        _clickup(engine, f"rmp{index}", "Rocky Mountain Power", 174_00, day, rule="monthly")
    assert propose_schedules(as_of=TODAY)[0]["day_of_month"] == 9
