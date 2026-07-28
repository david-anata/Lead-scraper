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
    bill_merchant_key,
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
        "monthly_cost_cents",
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
        "merchant_key",
        "paid_in_pieces",
    }
    # The key follows the merchant the descriptor is reduced to, not the raw
    # descriptor, so the same vendor keeps one key however the bank words it.
    assert pattern["merchant_key"] == bill_merchant_key("Canyon View Management")
    assert pattern["pattern_key"] == bill_pattern_key(pattern["merchant_key"])
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


# ---------------------------------------------------------------------------
# The shapes David's real bank history actually has
#
# Everything below was found by loading the live page. Clean synthetic data of
# one steady payment per period passed every earlier test while the real page
# offered internal transfers as bills, split one rent into two, and read a rent
# paid in pieces as a weekly bill projecting four times its cost.
# ---------------------------------------------------------------------------

RENT_A = "Withdrawal ACH B TYPE: WEB PMTS CO: Boulder Ranch L."
RENT_B = "Ach Withdrawal Company: Boulder Ranch Entry: Web"


def _post_mixed(engine, rows, *, category="other"):
    """Post payments whose descriptor varies between them, as the bank really does."""
    with Session(engine) as session:
        for index, (descriptor, cents, when) in enumerate(rows):
            session.add(CashEvent(
                id=f"csv-mixed-{index}",
                source="csv",
                source_id=f"mixed-{index}",
                record_kind="transaction",
                event_type="outflow",
                category=category,
                name=descriptor,
                description=descriptor,
                vendor_or_customer=descriptor,
                amount_cents=cents,
                due_date=datetime(when.year, when.month, when.day),
                status="posted",
                confidence="confirmed",
            ))
        session.commit()


def test_moving_your_own_money_is_never_offered_as_a_bill(finance_engine):
    _post_mixed(finance_engine, [
        *[("Home banking Withdrawal Transfer to S0050", 25_000_00,
           AS_OF - timedelta(days=6 * (i + 1))) for i in range(9)],
        *[("To Share 58", 14_499_51, AS_OF - timedelta(days=9 * (i + 1))) for i in range(6)],
    ])

    listing = list_bill_patterns(as_of=AS_OF, lookback_days=400)

    assert listing["patterns"] == [], (
        "a transfer between the operator's own accounts is not a bill, and "
        "tracking one inflates the cash the forecast says is needed"
    )


def test_a_descriptor_that_names_nobody_is_not_offered(finance_engine):
    _post_mixed(finance_engine, [
        ("Draft Withdrawal Draft #**0012 Tracer: ****5078", 1_087_00,
         AS_OF - timedelta(days=6 * (i + 1))) for i in range(9)
    ])

    assert list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"] == []


def test_a_merchant_charged_almost_daily_is_not_a_bill_on_a_cycle(finance_engine):
    """Sixty charges from one merchant was being called a weekly bill and
    multiplied up. It is a pile of charges, not something arriving on a cycle."""
    _post_mixed(finance_engine, [
        ("Store Leads", 14_00 + i, AS_OF - timedelta(days=i + 1)) for i in range(60)
    ])

    assert list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"] == []


def test_one_rent_worded_two_ways_stays_one_bill(finance_engine):
    """The bank writes the same payment differently between runs. Grouping on the
    raw descriptor counted the rent twice and halved each half's cadence."""
    _post_mixed(finance_engine, [
        (RENT_A, 10_000_00, date(2026, 6, 3)),
        (RENT_B, 6_042_00, date(2026, 6, 12)),
        (RENT_A, 5_000_00, date(2026, 6, 21)),
        (RENT_A, 12_000_00, date(2026, 5, 2)),
        (RENT_B, 9_042_00, date(2026, 5, 15)),
        (RENT_A, 8_000_00, date(2026, 4, 4)),
        (RENT_B, 7_000_00, date(2026, 4, 18)),
        (RENT_A, 6_042_00, date(2026, 4, 26)),
    ], category="rent")

    found = list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"]

    assert len(found) == 1, [row["vendor"] for row in found]
    assert found[0]["vendor"] == "Boulder Ranch", "the raw descriptor is not a name"


def test_a_rent_paid_in_pieces_is_one_monthly_bill_at_its_real_cost(finance_engine):
    """This is the case David warned about. Read as a weekly bill of one
    instalment it projected roughly four times the rent."""
    _post_mixed(finance_engine, [
        (RENT_A, 10_000_00, date(2026, 6, 3)),
        (RENT_B, 6_042_00, date(2026, 6, 12)),
        (RENT_A, 5_000_00, date(2026, 6, 21)),
        (RENT_A, 12_000_00, date(2026, 5, 2)),
        (RENT_B, 9_042_00, date(2026, 5, 15)),
        (RENT_A, 8_000_00, date(2026, 4, 4)),
        (RENT_B, 7_000_00, date(2026, 4, 18)),
        (RENT_A, 6_042_00, date(2026, 4, 26)),
    ], category="rent")

    bill = list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"][0]

    assert bill["frequency"] == "monthly", f"a monthly rent read as {bill['frequency']}"
    assert bill["paid_in_pieces"] is True
    assert abs(bill["amount_cents"] - 21_042_00) <= 100_00, (
        f"the rent is 21,042 a month; projected {bill['amount_cents'] / 100:,.2f}"
    )
    assert "pieces" in bill["why"], "the operator has to be told why it was added up"


def test_a_steady_weekly_charge_is_left_weekly_and_not_lumped_monthly(finance_engine):
    """Adding up the month is only right for uneven pieces of one bill. A real
    weekly repayment of the same figure forecasts better as four hits."""
    _post_mixed(finance_engine, [
        (
            "Withdrawal ACH A TYPE: Stripe Cap CO: Anata Entry Class Code: CCD" if i % 2
            else "Ach Withdrawal Company: Anata Entry: Stripe Cap David Narayan",
            843_00,
            AS_OF - timedelta(days=7 * (i + 1)),
        ) for i in range(12)
    ])

    found = list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"]

    assert len(found) == 1, [row["vendor"] for row in found]
    assert found[0]["frequency"] == "weekly"
    assert found[0]["paid_in_pieces"] is False
    assert found[0]["amount_cents"] == 843_00


def test_a_wildly_uneven_series_is_not_called_likely(finance_engine):
    """Arriving often was enough to earn "Likely" while the amount swung between
    75 and 30,000, which is the opposite of what the word should mean."""
    _post_mixed(finance_engine, [
        ("Scattergun Supplies", cents, AS_OF - timedelta(days=30 * (i + 1)))
        for i, cents in enumerate([75_00, 30_000_00, 5_000_00, 100_00, 12_000_00, 250_00])
    ])

    found = list_bill_patterns(as_of=AS_OF, lookback_days=400)["patterns"]

    assert found, "it is still a repeating payment, it just should not look certain"
    assert found[0]["confidence_label"] == "Possible", found[0]["confidence_label"]


def test_a_bank_prefix_does_not_split_one_vendor_in_two(finance_engine):
    """The live page showed "Payment Canyon View Fede Name Davi" and "Canyon View
    Fede Canyon View F" as two separate $300 bills. A word that names no payee
    must not take one of the two slots the grouping key has."""
    assert bill_merchant_key("Payment Canyon View Fede Name Davi") == bill_merchant_key(
        "Canyon View Fede Canyon View F"
    )
    assert bill_merchant_key("Purch Ups Billing Center Ga") == bill_merchant_key(
        "UPS*BILLING CENTER 800-811-1648 GA"
    )


def test_genuinely_different_vendors_are_still_kept_apart(finance_engine):
    """Merging harder must not start joining unrelated payees, which would hide a
    real bill inside another one's figure."""
    for left, right in [
        ("Select Hea Instamed", "Dentalsel Select Benefits"),
        ("Amazon Prime Amzn Com Bill Wa", "Wal Mart Grassland Dr American"),
        ("Google Workspace Anatai Ca", "Slack Slack Com Ca"),
    ]:
        assert bill_merchant_key(left) != bill_merchant_key(right), (left, right)


def test_the_month_in_progress_does_not_drag_a_bill_below_its_real_cost(finance_engine):
    """David's Boulder Ranch figures exactly. The app read $38,735 while he said
    it is now $40,000 a month. Half of July's rent was being counted as though
    July were finished, and it was the smallest number in the set."""
    # Uneven pieces, as the real payments are, summing to the totals he sees:
    # Feb 39,636 | Mar 30,065 | Apr 36,000 | May 36,033 | Jun 40,084 | Jul 15,075
    pieces = [
        (date(2026, 2, 10), 30_000_00), (date(2026, 2, 26), 9_636_00),
        (date(2026, 3, 6), 25_000_00), (date(2026, 3, 26), 5_065_00),
        (date(2026, 4, 7), 28_000_00), (date(2026, 4, 21), 8_000_00),
        (date(2026, 5, 8), 30_000_00), (date(2026, 5, 21), 6_033_00),
        (date(2026, 6, 9), 32_000_00), (date(2026, 6, 30), 8_084_00),
        # July is only half gone, so this is not a month's cost.
        (date(2026, 7, 14), 15_075_00),
    ]
    _post_mixed(finance_engine, [
        (RENT_A if index % 2 else RENT_B, cents, when)
        for index, (when, cents) in enumerate(pieces)
    ], category="rent")

    bill = list_bill_patterns(as_of=date(2026, 7, 26), lookback_days=400)["patterns"][0]

    assert bill["frequency"] == "monthly"
    assert bill["amount_cents"] >= 39_000_00, (
        f"projected {bill['amount_cents'] / 100:,.2f} for a bill now costing about 40,000"
    )
    assert bill["occurrences"] == 5, "the unfinished month is not one of the observations"
    assert all(
        row["due_date"].month != 7 for row in bill["evidence"]
    ), "the month in progress must not appear as evidence of a month's cost"


def test_the_month_in_progress_is_kept_when_dropping_it_leaves_too_little(finance_engine):
    """Three months of history is already the minimum. Discarding one to be tidy
    would silently stop predicting the bill at all."""
    _post_mixed(finance_engine, [
        (RENT_A, 10_000_00, date(2026, 5, 4)), (RENT_B, 6_000_00, date(2026, 5, 20)),
        (RENT_A, 12_000_00, date(2026, 6, 3)), (RENT_B, 4_000_00, date(2026, 6, 19)),
        (RENT_A, 9_000_00, date(2026, 7, 2)), (RENT_B, 5_000_00, date(2026, 7, 14)),
    ], category="rent")

    found = list_bill_patterns(as_of=date(2026, 7, 26), lookback_days=400)["patterns"]

    assert found, "dropping the current month must not drop the bill"
    assert found[0]["occurrences"] == 3


def test_a_tracked_bill_past_the_horizon_is_still_accounted_for(finance_engine):
    """Boulder Ranch came out 31 days away, past the forecast window, so it
    produced nothing and the Today page went silent about a bill David had just
    tracked. Tracking something and finding no trace of it is the same failure as
    the button appearing to do nothing."""
    _post_mixed(finance_engine, [
        ("Faraway Leasing", 5_000_00, date(2026, 4, 26)),
        ("Faraway Leasing", 5_000_00, date(2026, 5, 26)),
        ("Faraway Leasing", 5_000_00, date(2026, 6, 26)),
    ])
    listing = list_bill_patterns(as_of=date(2026, 7, 1), lookback_days=400)
    pattern = listing["patterns"][0]
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    # A 14 day window, while the bill is due on the 26th, four weeks out.
    projections = confirmed_bill_projections(as_of=date(2026, 7, 1), horizon_days=14)

    assert projections, "the next occurrence must survive a short horizon"
    assert len(projections) == 1, "only the next one, not an unbounded run of them"
    assert projections[0]["due_date"] > date(2026, 7, 15), "and it is genuinely outside the window"


def test_a_short_horizon_does_not_invent_extra_occurrences(finance_engine):
    """Reaching past the horizon is for the next occurrence only."""
    _post_mixed(finance_engine, [
        ("Weekly Yard Service", 100_00, date(2026, 6, 1)),
        ("Weekly Yard Service", 100_00, date(2026, 6, 8)),
        ("Weekly Yard Service", 100_00, date(2026, 6, 15)),
        ("Weekly Yard Service", 100_00, date(2026, 6, 22)),
    ])
    listing = list_bill_patterns(as_of=date(2026, 6, 23), lookback_days=400)
    pattern = listing["patterns"][0]
    record_bill_pattern_decision(pattern["pattern_key"], "track", actor=ACTOR)

    projections = confirmed_bill_projections(as_of=date(2026, 6, 23), horizon_days=21)

    assert projections
    for row in projections[:-1]:
        assert row["due_date"] <= date(2026, 6, 23) + timedelta(days=21), (
            "everything except the reach-ahead must sit inside the window"
        )
