"""The next-week headline and the paydown block on the Calendar page.

The headline restates a figure the weekly table already shows. Two numbers
describing the same week is exactly the shape that has produced wrong answers in
this section before, so the reconciliation test below matters more than the
formatting ones.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from sales_support_agent.services.cashflow.cash_calendar import (
    _date_label,
    _next_week_headline,
    _paydown_block,
    build_cash_calendar,
)
from sales_support_agent.services.cashflow.overview import _money

TODAY = date.today()
NEXT_MONDAY = TODAY + timedelta(days=(7 - TODAY.weekday()))


def test_calendar_date_labels_do_not_depend_on_linux_only_format_flags():
    assert _date_label(date(2026, 8, 3)) == "Monday 3"
    assert _date_label(date(2026, 8, 3), weekday="%a", month="%b") == "Mon 3 Aug"

    source = Path(__file__).resolve().parents[1].joinpath(
        "sales_support_agent/services/cashflow/cash_calendar.py"
    ).read_text(encoding="utf-8")
    assert "%-d" not in source


def _week(**overrides) -> dict:
    week = {
        "label": "Next week",
        "date_label": "Mon 3 to Sun 9",
        "start": NEXT_MONDAY.isoformat(),
        "end": (NEXT_MONDAY + timedelta(days=6)).isoformat(),
        "unpaid_cents": 18_340_00, "unpaid_count": 9,
        "possible_cents": 3_120_00, "possible_count": 4,
    }
    week.update(overrides)
    return week


def _calendar(week: dict, *, heaviest: int = 8_900_00) -> dict:
    return {
        "weeks": [week],
        "days": [
            {"date": (NEXT_MONDAY + timedelta(days=index)).isoformat(),
             "planned_cents": heaviest if index == 1 else (1_000_00 if heaviest else 0)}
            for index in range(7)
        ],
    }


def test_the_headline_keeps_certain_and_possible_apart():
    """Adding them would hide which is which at the moment money is decided."""
    headline = _next_week_headline(_calendar(_week()))

    assert "$18,340" in headline
    assert "$3,120" in headline
    assert "$21,460" not in headline, "the two must not be added together"


def test_the_headline_reports_the_heaviest_day():
    """Knowing $18,340 leaves next week matters less than knowing $8,900 of it
    goes on one day."""
    headline = _next_week_headline(_calendar(_week()))

    assert "Heaviest day" in headline
    assert "$8,900" in headline


def test_the_headline_matches_the_weekly_table_it_sits_above():
    """If these two ever disagree, one of them is lying to the operator."""
    week = _week(unpaid_cents=7_231_45, possible_cents=1_004_99)

    headline = _next_week_headline(_calendar(week))

    # Compared through the same formatter the table uses, so the two cannot
    # drift apart on rounding. Hard-coding the strings would let them.
    assert _money(week["unpaid_cents"]) in headline
    assert _money(week["possible_cents"]) in headline


def test_no_next_week_in_range_renders_nothing_rather_than_an_empty_box():
    assert _next_week_headline({"weeks": [], "days": []}) == ""
    assert _next_week_headline({"weeks": [_week(label="This week")], "days": []}) == ""


def test_a_week_with_nothing_in_it_still_reads_correctly():
    headline = _next_week_headline(
        _calendar(_week(unpaid_cents=0, unpaid_count=0,
                        possible_cents=0, possible_count=0), heaviest=0)
    )

    assert "$0" in headline
    assert "Heaviest day" not in headline, "no heaviest day when nothing is due"


# --- the paydown block ----------------------------------------------------

def _plan(**overrides) -> dict:
    plan = {
        "status": "ok", "vendor": "Boulder Ranch",
        "monthly_cents": 39_636_00, "paid_this_month_cents": 12_000_00,
        "remaining_cents": 27_636_00,
        "instalments": [
            {"date": TODAY, "amount_cents": 9_400_00, "why": "leaves your cushion untouched"},
            {"date": TODAY + timedelta(days=9), "amount_cents": 10_200_00,
             "why": "once the bills before it have cleared"},
            {"date": TODAY + timedelta(days=20), "amount_cents": 8_036_00,
             "why": "next point there is room"},
        ],
        "planned_total_cents": 27_636_00, "shortfall_cents": 0,
        "reserved_cents": 18_300_00, "unconfirmed_reserved_cents": 3_100_00,
        "floor_cents": 10_000_00, "savings_would_unlock_cents": 0,
    }
    plan.update(overrides)
    return plan


def test_every_instalment_shows_its_date_amount_and_reason():
    block = _paydown_block(_plan())

    assert "Today" in block
    assert "$9,400" in block
    assert "leaves your cushion untouched" in block
    assert "$27,636" in block


def test_the_block_says_what_it_reserved_and_how_much_is_a_guess():
    """Without this the operator cannot tell whether to trust the plan."""
    block = _paydown_block(_plan())

    assert "$18,300" in block
    assert "$3,100" in block
    assert "not confirmed" in block


def test_no_spare_cash_proposes_nothing_and_points_at_collections():
    block = _paydown_block(_plan(instalments=[], planned_total_cents=0))

    assert "No rent payment is recommended yet" in block
    assert "Nothing spare this month" in block
    assert "$15,200 planned" in block
    assert "$3,100 possible" in block
    assert "$18,300 total" in block
    assert "/admin/finances/collections" in block
    assert "Today" not in block, "it must not still show a payment it cannot fund"


def test_the_savings_line_only_appears_when_savings_could_cover_it():
    without = _paydown_block(_plan(instalments=[], savings_would_unlock_cents=0))
    with_savings = _paydown_block(_plan(instalments=[], savings_would_unlock_cents=6_000_00))

    assert "Last-resort option only" not in without
    assert "Last-resort option only" in with_savings
    assert "including TAX" in with_savings
    assert "$6,000" in with_savings


def test_a_bill_already_settled_this_month_says_so():
    block = _paydown_block(_plan(remaining_cents=0))

    assert "Nothing left to pay this month" in block
    assert "Today" not in block


def test_it_never_disappears_silently():
    """A section that vanishes is indistinguishable from one that was never
    built. Two features in this app were rediscovered and nearly rebuilt for
    exactly that reason."""
    no_vendor = _paydown_block({"status": "no_vendor"})
    failed = _paydown_block(None)

    assert "No repeating bill to plan around yet" in no_vendor
    assert "Could not work this out" in failed
    assert "Nothing was" in failed, "and it must say the calendar is unaffected"


def test_a_missing_month_end_feed_pauses_the_rent_recommendation():
    block = _paydown_block({
        "status": "paused",
        "message": "Rent recommendation paused because not all upcoming expenses were included.",
        "reason": "Recurring expense history is unavailable.",
    })

    assert "Rent recommendation paused" in block
    assert "not all upcoming expenses" in block
    assert "Recurring expense history is unavailable" in block
    assert "Nothing spare" not in block


def test_operator_copy_carries_no_em_dashes_or_leaked_placeholders():
    for block in (_paydown_block(_plan()), _next_week_headline(_calendar(_week()))):
        assert "—" not in block
        assert "None" not in block
        assert "{" not in block, "an unrendered placeholder reached the page"


def test_the_page_builder_still_produces_a_next_week_the_headline_can_find():
    """Guards the label the headline matches on. Renaming it in the builder
    would silently empty the headline while every other test stayed green."""
    calendar = build_cash_calendar([], as_of=TODAY)

    assert any(
        str(week.get("label")) == "Next week" for week in calendar["weeks"]
    ), "the builder must still label a week 'Next week'"
