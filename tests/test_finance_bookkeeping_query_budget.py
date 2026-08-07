"""The filing queue must not query the database once per transaction.

Working out which merchant a row belongs to consults the vendor combinations the
operator has taught. Doing that lookup inside the per-row loop cost two round
trips per transaction: grouping a 1,700 row queue meant roughly 3,500 of them,
which is milliseconds on a local database and tens of seconds on a remote one.

This guard counts statements rather than measuring time, because a timing
threshold passes on a fast laptop while the live page is unusable.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.models.database import init_database, insert_cash_event
from sales_support_agent.services.cashflow.bookkeeping import (
    group_needs_decision,
    merchant_key,
)

ROWS = 60


@pytest.fixture()
def seeded(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=engine, future=True))
    monkeypatch.setattr(database, "engine", engine)
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for index in range(ROWS):
            vendor = f"Merchant {chr(65 + index % 26)}{chr(65 + index // 26)} Trading"
            insert_cash_event(
                connection, id=f"t{index}", source="plaid", source_id=f"t{index}",
                record_kind="transaction", event_type="outflow",
                category="uncategorized", name=vendor, vendor_or_customer=vendor,
                description=vendor, amount_cents=1_000 + index,
                due_date=date(2026, 7, 10), status="posted", confidence="confirmed",
                created_at=now, updated_at=now,
            )
    return engine


def _count_statements(engine, work):
    seen: list[str] = []

    def record(conn, cursor, statement, parameters, context, executemany):
        seen.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        result = work()
    finally:
        event.remove(engine, "before_cursor_execute", record)
    return result, seen


def test_grouping_the_queue_does_not_scale_its_queries_with_the_row_count(seeded):
    grouped, statements = _count_statements(seeded, group_needs_decision)

    assert grouped["transaction_count"] == ROWS, "the fixture must really be grouped"
    assert len(statements) <= 8, (
        f"{len(statements)} statements for {ROWS} transactions. This must stay flat: "
        "one lookup for the whole queue, not one per row."
    )


def test_the_merchant_reader_can_be_given_the_combinations_to_reuse(seeded):
    """The parameter is what lets a caller hoist the lookup out of its loop."""
    from sales_support_agent.services.cashflow.vendor_aliases import alias_map

    aliases = alias_map()
    _key, statements = _count_statements(
        seeded, lambda: merchant_key("Madison Bicycle Shop", aliases=aliases)
    )

    assert statements == [], "a supplied map must be used instead of querying again"


def test_the_merchant_reader_still_works_without_being_given_one(seeded):
    """Callers outside a loop stay simple and must keep resolving correctly."""
    assert merchant_key("Madison Bicycle Shop") == "madison bicycle shop"
