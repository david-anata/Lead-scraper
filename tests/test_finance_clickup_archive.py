"""Taking the ClickUp estimates out of the numbers.

Turning the sync off only stopped new rows arriving. Every row already written
kept driving the calendar, required out and the paydown plan, so a bill somebody
typed in three weeks earlier still counted as money leaving the account. This is
what actually removes them, and it must be reversible because it moves numbers
the operator makes payment decisions on.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.models.database import init_database, insert_cash_event
from sales_support_agent.services.cashflow.cutover import (
    active_clickup_event_ids,
    archive_clickup_ledger,
)

TODAY = date.today()


@pytest.fixture()
def ledger(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=engine, future=True))
    monkeypatch.setattr(database, "engine", engine)
    now = datetime.now(timezone.utc)

    def add(event_id, **kw):
        base = dict(
            id=event_id, source="clickup", source_id=event_id, record_kind="obligation",
            event_type="outflow", category="rent", name=event_id,
            vendor_or_customer=event_id, description=event_id, amount_cents=100_000,
            due_date=TODAY, status="planned", confidence="estimated",
            created_at=now, updated_at=now,
        )
        base.update(kw)
        with engine.begin() as connection:
            insert_cash_event(connection, **base)

    add("clickup-1")
    add("clickup-2")
    add("bank-posted", source="csv", record_kind="transaction", status="posted",
        confidence="confirmed")
    add("clickup-posted", record_kind="transaction", status="posted")
    add("native", source="manual")
    return engine


def test_only_the_clickup_estimates_are_taken_out(ledger):
    ids = active_clickup_event_ids()

    assert set(ids) == {"clickup-1", "clickup-2"}
    assert "bank-posted" not in ids, "bank evidence is money that actually moved"
    assert "native" not in ids, "a native schedule is not an estimate"
    assert "clickup-posted" not in ids, (
        "a posted transaction is evidence, whatever source recorded it"
    )


def test_archiving_reports_what_it_did_and_how_to_undo_it(ledger):
    result = archive_clickup_ledger(actor="qa@example.com")

    assert result["archived"] == 2
    assert result["batch_id"], "an irreversible bulk change to money is not acceptable"
    assert "2" in result["message"]


def test_archived_estimates_stop_counting(ledger):
    archive_clickup_ledger(actor="qa@example.com")

    with ledger.connect() as connection:
        still_active = connection.execute(text("""
            SELECT COUNT(*) FROM cash_events
            WHERE source='clickup' AND archived_at IS NULL
              AND COALESCE(record_kind,'obligation') <> 'transaction'
        """)).scalar()
    assert still_active == 0


def test_nothing_is_deleted(ledger):
    before = _row_count(ledger)

    archive_clickup_ledger(actor="qa@example.com")

    assert _row_count(ledger) == before, "archiving must never remove a record"


def test_running_it_twice_is_harmless(ledger):
    first = archive_clickup_ledger(actor="qa@example.com")
    second = archive_clickup_ledger(actor="qa@example.com")

    assert first["archived"] == 2
    assert second["archived"] == 0
    assert "Nothing left" in second["message"]


def test_the_batch_can_be_undone(ledger):
    from sales_support_agent.services.cashflow.bulk_resolve import undo_batch

    result = archive_clickup_ledger(actor="qa@example.com")
    undo_batch(result["batch_id"], actor="qa@example.com")

    assert set(active_clickup_event_ids()) == {"clickup-1", "clickup-2"}, (
        "undo must put the estimates back exactly as they were"
    )


def _row_count(engine) -> int:
    with engine.connect() as connection:
        return int(connection.execute(text("SELECT COUNT(*) FROM cash_events")).scalar() or 0)
