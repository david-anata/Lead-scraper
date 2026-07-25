from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bulk_resolve import (
    apply_bulk_action,
    list_historical_backlog,
    set_follow_up,
    snooze_events,
)
from sales_support_agent.services.cashflow.control import (
    HISTORICAL_BACKLOG_DAYS,
    _summary_metrics,
)
from sales_support_agent.services.cashflow.vendors import (
    create_vendor,
    list_vendors_with_progress,
)

TODAY = date(2026, 7, 25)


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _event(engine, cid, name, amount, due, *, event_type="outflow", ctype="general", status="planned"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type=event_type,
            category="revenue" if event_type == "inflow" else "other",
            name=name, vendor_or_customer=name, amount_cents=amount,
            due_date=due, status=status, confidence="confirmed",
            created_at=now, updated_at=now,
        )
        connection.execute(text("UPDATE cash_events SET commitment_type=:c WHERE id=:i"),
                           {"c": ctype, "i": cid})


# --- Step 2: stale scheduler entries stop counting as required out ---------

def _row(cid, amount, due, event_type="outflow"):
    return {
        "id": cid, "event_type": event_type, "amount_cents": amount,
        "open_amount_cents": amount, "due_date": due.isoformat(),
        "status": "planned", "confidence": "confirmed", "record_kind": "obligation",
    }


def test_old_scheduler_entries_move_out_of_required_out_into_backlog():
    stale_due = TODAY - timedelta(days=HISTORICAL_BACKLOG_DAYS + 30)
    rows = [
        _row("fresh", 1000_00, TODAY + timedelta(days=3)),
        _row("stale", 5000_00, stale_due),
    ]
    metrics = _summary_metrics(rows, TODAY, 14)
    # Only the genuinely upcoming bill is a 14-day requirement.
    assert metrics["required_outgoing_cents"] == 1000_00
    # The old one is still visible, just in its own bucket.
    assert metrics["historical_backlog_cents"] == 5000_00
    assert metrics["historical_backlog_count"] == 1


def test_recently_overdue_still_counts_as_required_out():
    """Only items past the 90-day cutoff are treated as history."""
    rows = [_row("recent", 2000_00, TODAY - timedelta(days=10))]
    metrics = _summary_metrics(rows, TODAY, 14)
    assert metrics["required_outgoing_cents"] == 2000_00
    assert metrics["historical_backlog_cents"] == 0


def test_old_incoming_is_not_swept_into_the_outgoing_backlog():
    rows = [_row("old-in", 9000_00, TODAY - timedelta(days=200), event_type="inflow")]
    metrics = _summary_metrics(rows, TODAY, 14)
    assert metrics["historical_backlog_cents"] == 0


# --- Step 1: start-fresh cleanup -----------------------------------------

def test_historical_backlog_lists_only_old_unsettled_items():
    engine = _setup()
    _event(engine, "old", "Old payroll placeholder", 5000_00, TODAY - timedelta(days=120))
    _event(engine, "recent", "Recent bill", 100_00, TODAY - timedelta(days=10))
    _event(engine, "future", "Upcoming", 100_00, TODAY + timedelta(days=5))

    backlog = list_historical_backlog(older_than_days=90, as_of=TODAY)
    assert backlog["actionable_ids"] == ["old"]
    assert backlog["amount_cents"] == 5000_00
    assert backlog["cutoff_date"] == (TODAY - timedelta(days=90)).isoformat()


def test_historical_backlog_separates_protected_items():
    engine = _setup()
    _event(engine, "payroll", "Payroll 5th", 5000_00, TODAY - timedelta(days=120), ctype="payroll")
    _event(engine, "other", "Old software", 50_00, TODAY - timedelta(days=120))

    backlog = list_historical_backlog(older_than_days=90, as_of=TODAY)
    assert backlog["count"] == 2
    assert backlog["protected_count"] == 1
    assert backlog["actionable_ids"] == ["other"]  # payroll excluded from bulk


def test_archive_historical_clears_the_backlog_and_is_reversible():
    engine = _setup()
    _event(engine, "old", "Old placeholder", 5000_00, TODAY - timedelta(days=120))
    ids = list_historical_backlog(older_than_days=90, as_of=TODAY)["actionable_ids"]

    result = apply_bulk_action(ids, "archive_historical", reason="start fresh", actor="qa")
    assert result["applied"] == 1
    assert list_historical_backlog(older_than_days=90, as_of=TODAY)["actionable_count"] == 0

    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT workflow_status, archived_at FROM cash_events WHERE id='old'"
        )).one()
    assert row._mapping["archived_at"] is not None
    # Not deleted.
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM cash_events WHERE id='old'")).scalar() == 1


def test_inflow_backlog_is_listed_separately():
    engine = _setup()
    _event(engine, "ar", "Old invoice", 9000_00, TODAY - timedelta(days=200), event_type="inflow")
    _event(engine, "ap", "Old bill", 100_00, TODAY - timedelta(days=200))

    assert list_historical_backlog(event_type="inflow", as_of=TODAY)["actionable_ids"] == ["ar"]
    assert list_historical_backlog(event_type="outflow", as_of=TODAY)["actionable_ids"] == ["ap"]


# --- Step 3: receivable options ------------------------------------------

def test_uncollectible_and_invoiced_in_error_are_valid_actions():
    engine = _setup()
    _event(engine, "a", "Bad debt", 5000_00, TODAY - timedelta(days=200), event_type="inflow")
    _event(engine, "b", "Wrong invoice", 100_00, TODAY - timedelta(days=200), event_type="inflow")

    assert apply_bulk_action(["a"], "uncollectible", reason="customer gone")["applied"] == 1
    assert apply_bulk_action(["b"], "invoiced_in_error", reason="billed twice")["applied"] == 1
    with engine.connect() as connection:
        statuses = dict(connection.execute(text(
            "SELECT id, workflow_status FROM cash_events WHERE id IN ('a','b')"
        )).fetchall())
    assert statuses["a"] == "written_off"
    assert statuses["b"] == "cancelled"


def test_snooze_sets_a_return_date_without_resolving_the_item():
    engine = _setup()
    _event(engine, "a", "Maybe later", 5000_00, TODAY - timedelta(days=90), event_type="inflow")
    until = TODAY + timedelta(days=30)

    result = snooze_events(["a"], until=until, actor="qa")
    assert result["snoozed"] == 1
    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT snoozed_until, workflow_status, archived_at FROM cash_events WHERE id='a'"
        )).one()
    assert str(row._mapping["snoozed_until"])[:10] == until.isoformat()
    assert row._mapping["archived_at"] is None  # still open, just hidden


def test_keep_chasing_sets_a_follow_up_date():
    engine = _setup()
    _event(engine, "a", "Chase this", 5000_00, TODAY - timedelta(days=90), event_type="inflow")
    follow = TODAY + timedelta(days=7)

    assert set_follow_up(["a"], follow_up_on=follow, actor="qa")["scheduled"] == 1
    with engine.connect() as connection:
        row = connection.execute(text(
            "SELECT follow_up_on, archived_at FROM cash_events WHERE id='a'"
        )).one()
    assert str(row._mapping["follow_up_on"])[:10] == follow.isoformat()
    assert row._mapping["archived_at"] is None


# --- Step 4: running-balance vendors -------------------------------------

def test_running_account_vendor_reports_a_balance_from_partial_payments():
    engine = _setup()
    now = datetime.now(timezone.utc)
    # Three partial payments into the same vendor.
    for index, amount in enumerate([4000_00, 3000_00, 2000_00]):
        with engine.begin() as connection:
            insert_cash_event(
                connection, id=f"p{index}", source="plaid", source_id=f"p{index}",
                record_kind="transaction", event_type="outflow", category="rent",
                name="BOULDER RANCH", vendor_or_customer="BOULDER RANCH",
                amount_cents=amount, due_date=TODAY - timedelta(days=index * 10),
                status="posted", confidence="confirmed", created_at=now, updated_at=now,
            )

    create_vendor({
        "name": "Boulder Ranch", "terms_type": "recurring",
        "total_committed_cents": 20000_00, "match_terms": "boulder ranch",
        "running_account": True,
    })

    vendor = list_vendors_with_progress()[0]
    assert vendor["running_account"] is True
    # Partial payments accumulate instead of failing to match a single bill.
    assert vendor["paid_cents"] == 9000_00
    assert vendor["matched_count"] == 3
    assert vendor["remaining_cents"] == 11000_00


def test_normal_vendor_is_not_marked_as_a_running_account():
    _setup()
    create_vendor({"name": "One Off", "terms_type": "one_off", "match_terms": "one off"})
    assert list_vendors_with_progress()[0]["running_account"] is False
