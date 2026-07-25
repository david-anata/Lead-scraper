"""Regression tests for holes found by adversarially reviewing the build.

The original tests asserted that database fields changed. They did not assert
that the figures the operator actually reads changed, so five resolution
actions appeared to work while moving nothing. These tests assert the outcome.
"""

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.bulk_resolve import (
    apply_bulk_action,
    preview_bulk_action,
    snooze_events,
)
from sales_support_agent.services.cashflow.control import _summary_metrics
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.vendors import (
    create_vendor,
    list_vendors_with_progress,
)

TODAY = date.today()


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _add(engine, cid, amount, *, event_type="outflow", days_ahead=3, ctype="general"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="clickup", source_id=cid,
            record_kind="obligation", event_type=event_type,
            category="revenue" if event_type == "inflow" else "other",
            name=cid, vendor_or_customer=cid, amount_cents=amount,
            due_date=TODAY + timedelta(days=days_ahead), status="planned",
            confidence="confirmed", created_at=now, updated_at=now,
        )
        connection.execute(text("UPDATE cash_events SET commitment_type=:c WHERE id=:i"),
                           {"c": ctype, "i": cid})


def _figures():
    rows = [{**row, "open_amount_cents": row.get("amount_cents")}
            for row in list_obligations(limit=500)]
    metrics = _summary_metrics(rows, TODAY, 14)
    return metrics["required_outgoing_cents"], metrics["confirmed_incoming_cents"]


def test_write_off_actually_reduces_required_out():
    engine = _setup()
    _add(engine, "bill", 5000_00)
    assert _figures()[0] == 5000_00

    apply_bulk_action(["bill"], "write_off", reason="lost cause", actor="qa")
    assert _figures()[0] == 0, "writing off a bill must reduce required out"


def test_archive_historical_actually_reduces_required_out():
    engine = _setup()
    _add(engine, "old", 4000_00, days_ahead=-120)
    apply_bulk_action(["old"], "archive_historical", reason="start fresh", actor="qa")
    assert _figures()[0] == 0


def test_uncollectible_actually_reduces_expected_incoming():
    engine = _setup()
    _add(engine, "ar", 12000_00, event_type="inflow")
    assert _figures()[1] == 12000_00

    apply_bulk_action(["ar"], "uncollectible", reason="customer gone", actor="qa")
    assert _figures()[1] == 0, "writing off a receivable must reduce expected income"


def test_invoiced_in_error_actually_reduces_expected_incoming():
    engine = _setup()
    _add(engine, "ar", 900_00, event_type="inflow")
    apply_bulk_action(["ar"], "invoiced_in_error", reason="billed twice", actor="qa")
    assert _figures()[1] == 0


def test_snooze_actually_hides_the_item_until_the_date():
    engine = _setup()
    _add(engine, "bill", 7777_00)
    assert _figures()[0] == 7777_00

    snooze_events(["bill"], until=TODAY + timedelta(days=30), actor="qa")
    assert _figures()[0] == 0, "a snoozed item must leave the figures"


def test_snooze_that_has_expired_brings_the_item_back():
    engine = _setup()
    _add(engine, "bill", 500_00)
    snooze_events(["bill"], until=TODAY - timedelta(days=1), actor="qa")
    assert _figures()[0] == 500_00, "an expired snooze must not hide the item forever"


def test_undo_restores_the_item_to_the_figures():
    engine = _setup()
    _add(engine, "bill", 3000_00)
    apply_bulk_action(["bill"], "write_off", reason="oops", actor="qa")
    assert _figures()[0] == 0

    from sales_support_agent.services.cashflow.bulk_resolve import latest_batch, undo_batch
    undo_batch(str(latest_batch()["id"]), actor="qa")
    assert _figures()[0] == 3000_00, "undo must put the money back in the figures"


def test_a_receivable_action_cannot_be_applied_to_a_bill():
    engine = _setup()
    _add(engine, "bill", 100_00, event_type="outflow")
    preview = preview_bulk_action(["bill"], "uncollectible")
    assert preview["eligible_count"] == 0
    assert preview["skipped_count"] == 1


def test_a_payables_action_cannot_be_applied_to_a_receivable():
    engine = _setup()
    _add(engine, "ar", 100_00, event_type="inflow")
    preview = preview_bulk_action(["ar"], "write_off")
    assert preview["eligible_count"] == 0
    assert preview["skipped_count"] == 1


def test_vendor_terms_match_whole_words_only():
    """"von" must not claim VONAGE, and "rent" must not claim PARENT."""
    engine = _setup()
    now = datetime.now(timezone.utc)
    for cid, name, amount in [
        ("t1", "VONAGE BUSINESS PHONE", 99_00),
        ("t2", "VON HILL CONSULTING", 2400_00),
        ("t3", "PARENT TEACHER STORE", 50_00),
        ("t4", "BOULDER RANCH RENT", 12000_00),
    ]:
        with engine.begin() as connection:
            insert_cash_event(
                connection, id=cid, source="plaid", source_id=cid,
                record_kind="transaction", event_type="outflow", category="other",
                name=name, vendor_or_customer=name, description=name,
                amount_cents=amount, due_date=TODAY - timedelta(days=5),
                status="posted", confidence="confirmed", created_at=now, updated_at=now,
            )

    create_vendor({"name": "Von Hill", "terms_type": "recurring", "match_terms": "von"})
    create_vendor({"name": "Rent", "terms_type": "recurring", "match_terms": "rent"})
    by_name = {v["name"]: v for v in list_vendors_with_progress()}

    assert by_name["Von Hill"]["matched_count"] == 1
    assert by_name["Von Hill"]["paid_cents"] == 2400_00
    assert by_name["Rent"]["matched_count"] == 1
    assert by_name["Rent"]["paid_cents"] == 12000_00


# --- Deferral must come back and demand a decision ------------------------

def test_a_deferral_returns_on_its_date_and_counts_itself():
    from sales_support_agent.services.cashflow.bulk_resolve import (
        list_due_followups,
        set_follow_up,
    )

    engine = _setup()
    _add(engine, "ar", 12000_00, event_type="inflow", days_ahead=-120)

    # Pushed into the future: off the plate.
    snooze_events(["ar"], until=TODAY + timedelta(days=10), actor="qa")
    assert list_due_followups()["count"] == 0

    # Date arrives: it comes back and demands a decision.
    set_follow_up(["ar"], follow_up_on=TODAY, actor="qa")
    due = list_due_followups()
    assert due["count"] == 1
    assert due["items"][0]["defer_count"] == 2, "each deferral must be counted"


def test_repeated_deferral_is_flagged_rather_than_allowed_to_rot():
    from sales_support_agent.services.cashflow.bulk_resolve import (
        list_due_followups,
        set_follow_up,
    )

    engine = _setup()
    _add(engine, "ar", 500_00, event_type="inflow", days_ahead=-200)
    for _ in range(3):
        set_follow_up(["ar"], follow_up_on=TODAY, actor="qa")

    due = list_due_followups()
    assert due["items"][0]["nagging"] is True
    assert due["nagging_count"] == 1


def test_resolving_a_deferred_item_stops_it_coming_back():
    from sales_support_agent.services.cashflow.bulk_resolve import list_due_followups

    engine = _setup()
    _add(engine, "ar", 500_00, event_type="inflow", days_ahead=-200)
    snooze_events(["ar"], until=TODAY, actor="qa")
    assert list_due_followups()["count"] == 1

    apply_bulk_action(["ar"], "uncollectible", reason="not collectible", actor="qa")
    assert list_due_followups()["count"] == 0, "a resolved item must not return"
    assert _figures()[1] == 0
