"""A schedule has to carry its own truth: how it is paid, how sure it is, and
what happens to its future when David stops it."""

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import inspect, text

from sales_support_agent.models.database import (
    _apply_sqlite_compat_migrations,
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.control import _summary_metrics
from sales_support_agent.services.cashflow.obligations import (
    create_recurring_template,
    delete_recurring_template,
    generate_upcoming_from_templates,
    get_recurring_template,
    list_obligations,
    supersede_stale_template_occurrences,
    update_recurring_template,
)

TODAY = date.today()
RENT_CENTS = 450000


def _fresh_database():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    # Every real database picked up match_candidates_json from the catch-up
    # migration, which gave the column a default. A database built from scratch
    # gets it from the table build instead, with no default, and then any insert
    # that leaves it out fails. Line the test database up with the real one.
    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE cash_events DROP COLUMN match_candidates_json"))
        connection.execute(text(
            "ALTER TABLE cash_events ADD COLUMN match_candidates_json JSON NOT NULL DEFAULT '[]'"
        ))
    return engine


def _template_columns(engine):
    return {column["name"] for column in inspect(engine).get_columns("recurring_templates")}


def _rent_template(**overrides):
    fields = {
        "name": "Boulder Ranch rent",
        "vendor_or_customer": "Boulder Ranch",
        "event_type": "outflow",
        "category": "rent",
        "amount_cents": RENT_CENTS,
        "frequency": "monthly",
        "next_due_date": TODAY + timedelta(days=3),
    }
    fields.update(overrides)
    return create_recurring_template(**fields)


def _occurrence(engine, cid, *, template, due, status="planned", amount=RENT_CENTS):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="manual", source_id=cid,
            record_kind="obligation", event_type="outflow", category="rent",
            name="Boulder Ranch rent", vendor_or_customer="Boulder Ranch",
            amount_cents=amount, due_date=due, status=status,
            confidence="estimated", created_at=now, updated_at=now,
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


def _row(engine, cid):
    with engine.connect() as connection:
        return dict(connection.execute(
            text("SELECT * FROM cash_events WHERE id=:i"), {"i": cid}
        ).fetchone()._mapping)


def _occurrence_count(engine, template_id):
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT COUNT(*) FROM cash_events WHERE recurring_template_id=:t"),
            {"t": template_id},
        ).scalar_one()


def _required_out(window_days=30):
    rows = [{**row, "open_amount_cents": row.get("amount_cents")}
            for row in list_obligations(limit=200)]
    return _summary_metrics(rows, TODAY, window_days)["required_outgoing_cents"]


# ---------------------------------------------------------------------------
# The column itself
# ---------------------------------------------------------------------------

def test_a_brand_new_database_gets_the_column_and_can_use_it():
    """The table build and the catch-up migration must not fight each other on a
    database that has never existed before."""
    engine = _fresh_database()
    assert "flexibility" in _template_columns(engine)

    template = _rent_template(flexibility="chunkable")
    assert get_recurring_template(template["id"])["flexibility"] == "chunkable"


def test_a_database_that_predates_the_column_gets_it_added():
    engine = _fresh_database()
    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE recurring_templates DROP COLUMN flexibility"))
    assert "flexibility" not in _template_columns(engine), "old shape not simulated"

    _apply_sqlite_compat_migrations(engine)

    assert "flexibility" in _template_columns(engine)
    template = _rent_template(flexibility="chunkable")
    assert get_recurring_template(template["id"])["flexibility"] == "chunkable"


def test_running_the_migration_twice_changes_nothing():
    engine = _fresh_database()
    _apply_sqlite_compat_migrations(engine)
    _apply_sqlite_compat_migrations(engine)

    template = _rent_template(flexibility="chunkable")
    assert get_recurring_template(template["id"])["flexibility"] == "chunkable"


def test_a_schedule_with_nothing_said_about_it_defaults_to_unknown():
    _fresh_database()
    template = _rent_template()
    assert get_recurring_template(template["id"])["flexibility"] == "unknown"


def test_how_a_bill_is_paid_can_be_changed_after_the_schedule_is_written():
    _fresh_database()
    template = _rent_template()
    updated = update_recurring_template(template["id"], flexibility="chunkable")
    assert updated["flexibility"] == "chunkable"


# ---------------------------------------------------------------------------
# What generation hands on
# ---------------------------------------------------------------------------

def test_a_bill_paid_in_pieces_says_so_on_the_occurrence_it_generates():
    engine = _fresh_database()
    template = _rent_template(flexibility="chunkable")

    created = generate_upcoming_from_templates(horizon_days=30, advance_template=False)

    assert [row["flexibility"] for row in created] == ["chunkable"]
    assert _row(engine, created[0]["id"])["flexibility"] == "chunkable"
    assert _occurrence_count(engine, template["id"]) == 1


def test_a_confirmed_schedule_generates_confirmed_occurrences():
    engine = _fresh_database()
    _rent_template(confidence="confirmed")

    created = generate_upcoming_from_templates(horizon_days=30, advance_template=False)

    assert [row["confidence"] for row in created] == ["confirmed"]
    assert _row(engine, created[0]["id"])["confidence"] == "confirmed"


def test_a_schedule_with_no_confidence_recorded_still_generates_a_guess():
    _fresh_database()
    _rent_template(confidence="")

    created = generate_upcoming_from_templates(horizon_days=30, advance_template=False)

    assert [row["confidence"] for row in created] == ["estimated"]


def test_generating_twice_creates_no_second_copy_of_the_same_bill():
    engine = _fresh_database()
    template = _rent_template(confidence="confirmed", flexibility="chunkable")

    generate_upcoming_from_templates(horizon_days=30, advance_template=False)
    before = _required_out()
    generate_upcoming_from_templates(horizon_days=30, advance_template=False)

    assert _occurrence_count(engine, template["id"]) == 1
    assert _required_out() == before, "a second run must not ask for the rent twice"


# ---------------------------------------------------------------------------
# Deleting a schedule
# ---------------------------------------------------------------------------

def test_deleting_a_schedule_stops_its_future_costing_money():
    engine = _fresh_database()
    template = _rent_template()
    _occurrence(engine, "next_month", template=template["id"], due=TODAY + timedelta(days=20))

    before = _required_out()
    assert delete_recurring_template(template["id"]) is True
    after = _required_out()

    assert _archived(engine, "next_month") is True
    assert after == before - RENT_CENTS, "a stopped bill must stop asking for cash"


def test_deleting_a_schedule_leaves_the_past_alone():
    engine = _fresh_database()
    template = _rent_template()
    _occurrence(engine, "last_month", template=template["id"], due=TODAY - timedelta(days=20))
    _occurrence(engine, "next_month", template=template["id"], due=TODAY + timedelta(days=20))

    delete_recurring_template(template["id"])

    assert _archived(engine, "last_month") is False, "history is not ours to rewrite"
    assert _archived(engine, "next_month") is True


def test_deleting_a_schedule_never_touches_an_occurrence_with_a_payment_on_it():
    engine = _fresh_database()
    template = _rent_template(flexibility="chunkable")
    _occurrence(engine, "part_paid", template=template["id"], due=TODAY + timedelta(days=20))

    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="txn", source="plaid", source_id="txn",
            record_kind="transaction", event_type="outflow", category="rent",
            name="BOULDER RANCH", vendor_or_customer="BOULDER RANCH",
            amount_cents=150000, due_date=TODAY, status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )
    from sales_support_agent.services.cashflow.plaid_match import confirm_matches
    confirm_matches([("txn", "part_paid")], actor="qa")
    # Put the row back in the queue on purpose, so the recorded payment is the
    # only thing standing between it and the archive.
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE cash_events SET status='pending' WHERE id='part_paid'")
        )

    delete_recurring_template(template["id"])

    row = _row(engine, "part_paid")
    assert row["archived_at"] is None
    assert row["workflow_status"] != "cancelled"


def test_deleting_a_schedule_leaves_a_trail_that_can_be_undone():
    engine = _fresh_database()
    template = _rent_template()
    _occurrence(engine, "next_month", template=template["id"], due=TODAY + timedelta(days=20))

    delete_recurring_template(template["id"], actor="david")

    with engine.connect() as connection:
        entry = connection.execute(text("""
            SELECT actor, entity_id, evidence_json FROM finance_action_audit
            WHERE action_type='recurring_template_deleted'
        """)).fetchone()
    assert entry is not None
    assert entry[0] == "david"
    assert entry[1] == template["id"]
    assert "next_month" in str(entry[2]), "the retired rows must be recoverable"


def test_deleting_a_schedule_that_is_not_there_reports_nothing_done():
    _fresh_database()
    assert delete_recurring_template("no-such-schedule") is False


def test_other_schedules_keep_their_future_when_one_is_deleted():
    engine = _fresh_database()
    doomed = _rent_template(name="Old rent")
    kept = _rent_template(name="New rent")
    _occurrence(engine, "doomed_next", template=doomed["id"], due=TODAY + timedelta(days=20))
    _occurrence(engine, "kept_next", template=kept["id"], due=TODAY + timedelta(days=21))

    delete_recurring_template(doomed["id"])

    assert _archived(engine, "doomed_next") is True
    assert _archived(engine, "kept_next") is False


# ---------------------------------------------------------------------------
# Rolling the forecast forward on demand
# ---------------------------------------------------------------------------

def test_the_roll_forward_can_be_run_on_demand_for_a_chosen_day():
    engine = _fresh_database()
    template = _rent_template()
    _occurrence(engine, "march", template=template["id"], due=TODAY - timedelta(days=120))
    _occurrence(engine, "next", template=template["id"], due=TODAY + timedelta(days=10))

    superseded = supersede_stale_template_occurrences(as_of=TODAY, grace_days=5)

    assert [item["id"] for item in superseded] == ["march"]
    assert _archived(engine, "march") is True
    assert supersede_stale_template_occurrences(as_of=TODAY) == [], "a second run has nothing left to do"
