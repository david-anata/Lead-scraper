"""A recurring schedule entry is a plan. It must roll forward, never rot."""

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.control import _summary_metrics
from sales_support_agent.services.cashflow.obligations import (
    supersede_stale_template_occurrences,
)

TODAY = date.today()


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _occurrence(engine, cid, *, template, days_ago, amount=5000_00, status="planned"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="manual", source_id=cid,
            record_kind="obligation", event_type="outflow", category="payroll",
            name="Payroll 5th", vendor_or_customer="Payroll",
            amount_cents=amount, due_date=TODAY - timedelta(days=days_ago),
            status=status, confidence="estimated", created_at=now, updated_at=now,
        )
        connection.execute(
            text("UPDATE cash_events SET recurring_template_id=:t WHERE id=:i"),
            {"t": template, "i": cid},
        )


def _archived(engine, cid):
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT archived_at FROM cash_events WHERE id=:i"), {"i": cid}
        ).scalar() is not None


def test_past_occurrence_is_superseded_when_a_later_one_exists():
    engine = _setup()
    _occurrence(engine, "march", template="payroll-5th", days_ago=120)
    _occurrence(engine, "next", template="payroll-5th", days_ago=-10)  # upcoming

    superseded = supersede_stale_template_occurrences()
    assert [item["id"] for item in superseded] == ["march"]
    assert _archived(engine, "march") is True
    assert _archived(engine, "next") is False, "the live occurrence must survive"


def test_the_only_occurrence_is_never_superseded():
    """With no later occurrence there is nothing to roll forward to, so the
    obligation stands and must keep counting."""
    engine = _setup()
    _occurrence(engine, "lonely", template="rent", days_ago=60)

    assert supersede_stale_template_occurrences() == []
    assert _archived(engine, "lonely") is False


def test_recent_occurrence_inside_the_grace_period_is_left_alone():
    engine = _setup()
    _occurrence(engine, "just_missed", template="payroll-5th", days_ago=2)
    _occurrence(engine, "next", template="payroll-5th", days_ago=-13)

    assert supersede_stale_template_occurrences(grace_days=5) == []
    assert _archived(engine, "just_missed") is False


def test_an_occurrence_with_a_payment_against_it_is_never_touched():
    engine = _setup()
    _occurrence(engine, "paid", template="payroll-5th", days_ago=90)
    _occurrence(engine, "next", template="payroll-5th", days_ago=-10)
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="txn", source="plaid", source_id="txn",
            record_kind="transaction", event_type="outflow", category="payroll",
            name="GUSTO", vendor_or_customer="GUSTO", amount_cents=5000_00,
            due_date=TODAY - timedelta(days=89), status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )
    from sales_support_agent.services.cashflow.plaid_match import confirm_matches
    confirm_matches([("txn", "paid")], actor="qa")

    assert supersede_stale_template_occurrences() == []
    assert _archived(engine, "paid") is False


def test_clickup_rows_are_not_touched_by_the_native_roll_forward():
    """Only template-generated occurrences roll forward; ClickUp rows have no
    template id and stay exactly as they are."""
    engine = _setup()
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for cid, days in (("cu_old", 120), ("cu_new", -10)):
            insert_cash_event(
                connection, id=cid, source="clickup", source_id=cid,
                record_kind="obligation", event_type="outflow", category="payroll",
                name="Payroll 5th", vendor_or_customer="Payroll", amount_cents=5000_00,
                due_date=TODAY - timedelta(days=days), status="planned",
                confidence="estimated", created_at=now, updated_at=now,
            )

    assert supersede_stale_template_occurrences() == []
    assert _archived(engine, "cu_old") is False


def test_superseding_removes_the_amount_from_required_out():
    """Uses a 30-day-old occurrence on purpose: anything past 90 days is
    already excluded as historical backlog, so it could not show a change."""
    engine = _setup()
    _occurrence(engine, "last_month", template="payroll-5th", days_ago=30)
    _occurrence(engine, "next", template="payroll-5th", days_ago=-5)

    from sales_support_agent.services.cashflow.obligations import list_obligations

    def required_out():
        rows = [{**row, "open_amount_cents": row.get("amount_cents")}
                for row in list_obligations(limit=100)]
        return _summary_metrics(rows, TODAY, 14)["required_outgoing_cents"]

    before = required_out()
    supersede_stale_template_occurrences()
    after = required_out()
    assert after < before, "a rolled-forward forecast must stop counting as cash needed"
