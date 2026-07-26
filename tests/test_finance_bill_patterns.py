"""Bill prediction: what the bank history says is coming, and what it costs."""

from __future__ import annotations

import statistics
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from sales_support_agent.models import database
from sales_support_agent.models.database import Base, _register_models
from sales_support_agent.models.entities import CashEvent, RecurringTemplate
from sales_support_agent.services.cashflow.bill_patterns import (
    bill_pattern_key,
    confirmed_bill_projections,
    list_bill_patterns,
    load_bill_pattern_decisions,
    record_bill_pattern_decision,
)


# The shared detector mines history relative to today, so the fixtures are
# anchored to today too and every expectation is derived, never hard coded.
AS_OF = date.today()
ACTOR = "finance@example.com"


@pytest.fixture()
def finance_engine(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    _register_models()
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(database, "engine", engine)
    return engine


def _month_before(day: date) -> date:
    month, year = day.month - 1, day.year
    if month < 1:
        month, year = 12, year - 1
    return date(year, month, day.day)


def _monthly_dates(count: int, *, day: int) -> list[date]:
    """Past payment dates on the same day of month, oldest first.

    Kept to five or fewer so the whole run stays inside the 180 day lookback.
    """
    assert 1 <= day <= 28, "pick a day every month has"
    latest = _month_before(date(AS_OF.year, AS_OF.month, day))
    dates = [latest]
    for _ in range(count - 1):
        dates.append(_month_before(dates[-1]))
    return sorted(dates)


def _upcoming_month_days(*, day: int, horizon_days: int) -> list[date]:
    """Every future occurrence of that day of month inside the horizon."""
    horizon_end = AS_OF + timedelta(days=horizon_days)
    cursor = date(AS_OF.year, AS_OF.month, day)
    dates: list[date] = []
    while cursor <= horizon_end:
        if cursor > AS_OF:
            dates.append(cursor)
        month, year = cursor.month + 1, cursor.year
        if month > 12:
            month, year = 1, year + 1
        cursor = date(year, month, day)
    return dates


def _post_history(
    engine,
    vendor: str,
    payments: list[tuple[date, int]],
    *,
    category: str = "software",
) -> None:
    """Insert posted bank outflows, the only evidence bill prediction reads."""
    with Session(engine) as session:
        for index, (day, amount_cents) in enumerate(payments):
            session.add(CashEvent(
                id=f"csv-{vendor}-{index}".replace(" ", "-").lower(),
                source="csv",
                source_id=f"{vendor}-{index}",
                record_kind="transaction",
                event_type="outflow",
                category=category,
                name=vendor,
                description=vendor,
                vendor_or_customer=vendor,
                amount_cents=amount_cents,
                due_date=datetime(day.year, day.month, day.day),
                status="posted",
                confidence="confirmed",
            ))
        session.commit()


def _schedule_real_bill(
    engine, vendor: str, due: date, amount_cents: int, *, status: str = "planned"
) -> None:
    with Session(engine) as session:
        session.add(CashEvent(
            id=f"real-{vendor}-{due.isoformat()}".replace(" ", "-").lower(),
            source="manual",
            record_kind="obligation",
            event_type="outflow",
            category="software",
            name=vendor,
            vendor_or_customer=vendor,
            amount_cents=amount_cents,
            due_date=datetime(due.year, due.month, due.day),
            status=status,
            confidence="estimated",
        ))
        session.commit()


def _track_recurring_template(engine, vendor: str, amount_cents: int) -> None:
    with Session(engine) as session:
        session.add(RecurringTemplate(
            id=f"tmpl-{vendor}".replace(" ", "-").lower(),
            name=vendor,
            vendor_or_customer=vendor,
            event_type="outflow",
            category="utilities",
            amount_cents=amount_cents,
            frequency="monthly",
            is_active=True,
        ))
        session.commit()


def _pattern_for(vendor: str, **kwargs) -> dict:
    listing = list_bill_patterns(as_of=AS_OF, **kwargs)
    matches = [row for row in listing["patterns"] if row["vendor"] == vendor]
    assert matches, f"expected a predicted bill for {vendor}: {listing}"
    return matches[0]


# ---------------------------------------------------------------------------
# The projected amount must never understate a bill
# ---------------------------------------------------------------------------

def test_projected_bill_amount_sits_above_the_median(finance_engine):
    amounts = [100_00, 100_00, 100_00, 400_00]
    _post_history(
        finance_engine, "Vantage Hosting", list(zip(_monthly_dates(4, day=8), amounts))
    )

    pattern = _pattern_for("Vantage Hosting")

    assert pattern["amount_cents"] > statistics.median(amounts)
    assert pattern["amount_cents"] <= max(amounts)


def test_a_confirmed_spiky_bill_reserves_more_cash_than_the_median(finance_engine):
    amounts = [100_00, 100_00, 100_00, 400_00]
    _post_history(
        finance_engine, "Vantage Hosting", list(zip(_monthly_dates(4, day=8), amounts))
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    projections = confirmed_bill_projections(as_of=AS_OF, horizon_days=40)

    assert projections
    assert all(row["amount_cents"] > statistics.median(amounts) for row in projections)


# ---------------------------------------------------------------------------
# What the page shows
# ---------------------------------------------------------------------------

def test_pattern_carries_the_keys_the_page_needs(finance_engine):
    _post_history(
        finance_engine,
        "Canyon View Management",
        [(day, 250_00) for day in _monthly_dates(5, day=3)],
        category="rent",
    )

    pattern = _pattern_for("Canyon View Management")

    assert set(pattern) == {
        "pattern_key",
        "vendor",
        "amount_cents",
        "frequency",
        "next_due",
        "confidence_bps",
        "confidence_label",
        "occurrences",
        "evidence",
        "why",
        "category",
        "already_tracked",
        "decision",
    }
    assert pattern["pattern_key"] == bill_pattern_key("Canyon View Management")
    assert len(pattern["pattern_key"]) == 16
    assert pattern["frequency"] == "monthly"
    assert pattern["category"] == "rent"
    assert pattern["occurrences"] == 5
    assert pattern["amount_cents"] == 250_00
    assert pattern["decision"] == ""
    assert pattern["already_tracked"] is False
    assert 0 <= pattern["confidence_bps"] <= 10_000
    assert pattern["confidence_label"] in {"Very likely", "Likely", "Possible"}
    assert pattern["why"] == "paid 5 times, always on the 3rd"
    assert pattern["next_due"] > AS_OF
    assert pattern["next_due"].day == 3


def test_evidence_shows_the_six_most_recent_payments(finance_engine):
    _post_history(
        finance_engine,
        "Vantage Hosting",
        [(AS_OF - timedelta(days=14 * step), 60_00) for step in range(1, 9)],
    )

    pattern = _pattern_for("Vantage Hosting")

    assert pattern["occurrences"] == 8
    assert len(pattern["evidence"]) == 6
    assert pattern["evidence"][0]["due_date"] == AS_OF - timedelta(days=14)
    assert [row["due_date"] for row in pattern["evidence"]] == sorted(
        (row["due_date"] for row in pattern["evidence"]), reverse=True
    )
    assert pattern["frequency"] == "biweekly"
    assert "paid 8 times" in pattern["why"]


def test_irregular_gaps_are_dropped_and_yearly_bills_are_kept(finance_engine):
    _post_history(
        finance_engine,
        "Riverbend Landscaping",
        [(AS_OF - timedelta(days=days), 60_00) for days in (460, 330, 200)],
    )
    _post_history(
        finance_engine,
        "Statewide Filing Fee",
        [(AS_OF - timedelta(days=days), 800_00) for days in (800, 435, 70)],
        category="fees",
    )

    listing = list_bill_patterns(as_of=AS_OF, lookback_days=900)
    vendors = {row["vendor"]: row for row in listing["patterns"] + listing["tracked"]}

    assert "Riverbend Landscaping" not in vendors
    assert vendors["Statewide Filing Fee"]["frequency"] == "annual"
    assert all(
        row["frequency"] in {"weekly", "biweekly", "monthly", "quarterly", "annual"}
        for row in listing["patterns"] + listing["tracked"]
    )


def test_biggest_monthly_cost_is_offered_first(finance_engine):
    _post_history(
        finance_engine, "Small Tools", [(day, 40_00) for day in _monthly_dates(4, day=6)]
    )
    _post_history(
        finance_engine,
        "Harborline Insurance",
        [(day, 900_00) for day in _monthly_dates(4, day=12)],
        category="insurance",
    )

    listing = list_bill_patterns(as_of=AS_OF)

    assert [row["vendor"] for row in listing["patterns"]] == [
        "Harborline Insurance",
        "Small Tools",
    ]
    assert listing["counts"]["patterns"] == 2
    assert listing["counts"]["unreviewed"] == 2
    assert listing["counts"]["monthly_cost_cents"] == 940_00


def test_a_bill_on_the_schedule_is_reported_as_already_tracked(finance_engine):
    _post_history(
        finance_engine,
        "Northgate Telecom",
        [(day, 310_00) for day in _monthly_dates(5, day=15)],
    )
    _track_recurring_template(finance_engine, "Northgate Telecom", 310_00)

    listing = list_bill_patterns(as_of=AS_OF)

    assert [row["vendor"] for row in listing["tracked"]] == ["Northgate Telecom"]
    assert listing["patterns"] == []
    assert listing["counts"]["tracked"] == 1
    assert listing["counts"]["patterns"] == 0


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("decision", ["", "TRACK", "review", "yes", "track_expected"])
def test_only_the_three_answers_are_accepted(finance_engine, decision):
    with pytest.raises(ValueError):
        record_bill_pattern_decision(bill_pattern_key("Anything"), decision, actor=ACTOR)

    with finance_engine.connect() as connection:
        count = connection.execute(
            text("SELECT COUNT(*) FROM finance_action_audit")
        ).scalar_one()
    assert count == 0


def test_latest_answer_wins_and_the_trail_is_kept(finance_engine):
    key = bill_pattern_key("Vantage Hosting")
    record_bill_pattern_decision(key, "track", actor=ACTOR)
    record_bill_pattern_decision(key, "not_a_bill", actor=ACTOR, evidence={"note": "one off"})

    assert load_bill_pattern_decisions() == {key: "not_a_bill"}
    with finance_engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT action_type, entity_type FROM finance_action_audit
        """)).fetchall()
    assert len(rows) == 2
    assert {row.action_type for row in rows} == {"bill_pattern_decision_recorded"}
    assert {row.entity_type for row in rows} == {"bill_pattern"}


def test_repeating_the_same_answer_does_not_add_a_second_audit_row(finance_engine):
    key = bill_pattern_key("Vantage Hosting")
    first = record_bill_pattern_decision(key, "track", actor=ACTOR, evidence={"seen": 4})
    retry = record_bill_pattern_decision(key, "track", actor=ACTOR, evidence={"seen": 4})

    assert first["created"] is True
    assert retry["created"] is False
    with finance_engine.connect() as connection:
        count = connection.execute(
            text("SELECT COUNT(*) FROM finance_action_audit")
        ).scalar_one()
    assert count == 1


def test_a_dismissed_pattern_leaves_the_queue_and_stops_counting(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 120_00) for day in _monthly_dates(5, day=9)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "not_a_bill", actor=ACTOR)

    listing = list_bill_patterns(as_of=AS_OF)

    assert listing["patterns"] == []
    assert listing["counts"]["dismissed"] == 1
    assert listing["counts"]["monthly_cost_cents"] == 0
    assert confirmed_bill_projections(as_of=AS_OF, horizon_days=90) == []


def test_a_snooze_hides_the_pattern_then_brings_it_back(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 75_00) for day in _monthly_dates(5, day=11)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(
        pattern["pattern_key"],
        "snooze",
        actor=ACTOR,
        evidence={"snoozed_on": AS_OF.isoformat()},
    )

    while_snoozed = list_bill_patterns(as_of=AS_OF + timedelta(days=3))
    after_expiry = list_bill_patterns(as_of=AS_OF + timedelta(days=8))

    assert while_snoozed["patterns"] == []
    assert while_snoozed["counts"]["snoozed"] == 1
    assert [row["vendor"] for row in after_expiry["patterns"]] == ["Vantage Hosting"]
    assert after_expiry["counts"]["snoozed"] == 0
    assert after_expiry["counts"]["unreviewed"] == 1


def test_an_unreviewed_pattern_contributes_nothing_to_cash(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 200_00) for day in _monthly_dates(5, day=7)]
    )

    assert list_bill_patterns(as_of=AS_OF)["patterns"]
    assert confirmed_bill_projections(as_of=AS_OF, horizon_days=90) == []


# ---------------------------------------------------------------------------
# Projections
# ---------------------------------------------------------------------------

def test_confirmed_bill_projects_every_occurrence_in_the_horizon(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 180_00) for day in _monthly_dates(5, day=5)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    projections = confirmed_bill_projections(as_of=AS_OF, horizon_days=95)

    expected_dates = _upcoming_month_days(day=5, horizon_days=95)
    assert len(expected_dates) >= 3
    assert [row["due_date"] for row in projections] == expected_dates
    row = projections[0]
    assert row["id"] == f"bill-trend-{pattern['pattern_key']}-{expected_dates[0].isoformat()}"
    assert row["source"] == "bill_trend"
    assert row["record_kind"] == "obligation"
    assert row["event_type"] == "outflow"
    assert row["category"] == pattern["category"]
    assert row["name"] == "Vantage Hosting"
    assert row["vendor_or_customer"] == "Vantage Hosting"
    assert row["amount_cents"] == 180_00
    assert row["open_amount_cents"] == row["amount_cents"]
    assert row["status"] == "planned"
    assert row["confidence"] == "medium"
    assert row["probability_bps"] == pattern["confidence_bps"]
    assert row["read_only"] is True
    assert row["trend_inferred"] is True
    assert row["bill_trend"] is True


def test_projected_dates_are_always_in_the_future(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 90_00) for day in _monthly_dates(5, day=26)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    projections = confirmed_bill_projections(as_of=AS_OF, horizon_days=60)

    assert projections
    assert all(row["due_date"] > AS_OF for row in projections)
    assert all(row["due_date"] <= AS_OF + timedelta(days=60) for row in projections)


def test_a_real_bill_on_the_schedule_suppresses_the_guess(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 180_00) for day in _monthly_dates(5, day=5)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)
    before = confirmed_bill_projections(as_of=AS_OF, horizon_days=95)
    expected_dates = _upcoming_month_days(day=5, horizon_days=95)
    # The operator has now typed the first one in by hand, a day off the guess.
    _schedule_real_bill(
        finance_engine, "Vantage Hosting", expected_dates[0] + timedelta(days=1), 180_00
    )

    after = confirmed_bill_projections(as_of=AS_OF, horizon_days=95)

    assert [row["due_date"] for row in before] == expected_dates
    assert [row["due_date"] for row in after] == expected_dates[1:]
    assert sum(row["amount_cents"] for row in after) == sum(
        row["amount_cents"] for row in before
    ) - 180_00


def test_a_settled_obligation_does_not_suppress_the_guess(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 180_00) for day in _monthly_dates(5, day=5)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)
    expected_dates = _upcoming_month_days(day=5, horizon_days=40)
    _schedule_real_bill(
        finance_engine, "Vantage Hosting", expected_dates[0], 180_00, status="paid"
    )

    projections = confirmed_bill_projections(as_of=AS_OF, horizon_days=40)

    assert [row["due_date"] for row in projections] == expected_dates


def test_predicting_bills_never_writes_a_forecast_row_to_history(finance_engine):
    _post_history(
        finance_engine, "Vantage Hosting", [(day, 150_00) for day in _monthly_dates(5, day=5)]
    )
    pattern = _pattern_for("Vantage Hosting")
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    projections = confirmed_bill_projections(as_of=AS_OF, horizon_days=95)

    assert projections
    with finance_engine.connect() as connection:
        planned = connection.execute(text("""
            SELECT COUNT(*) FROM cash_events WHERE source <> 'csv'
        """)).scalar_one()
        total = connection.execute(text("SELECT COUNT(*) FROM cash_events")).scalar_one()
    assert planned == 0
    assert total == 5


def test_an_already_tracked_bill_is_never_projected_twice(finance_engine):
    _post_history(
        finance_engine,
        "Northgate Telecom",
        [(day, 310_00) for day in _monthly_dates(5, day=15)],
    )
    _track_recurring_template(finance_engine, "Northgate Telecom", 310_00)
    record_bill_pattern_decision(
        bill_pattern_key("Northgate Telecom"), "track", actor=ACTOR
    )

    assert confirmed_bill_projections(as_of=AS_OF, horizon_days=95) == []
