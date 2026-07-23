from datetime import date
from decimal import Decimal

from sales_support_agent.services.hr.payroll import (
    hourly_gross,
    period_overtime,
    periods_for_year,
    semimonthly_period,
    weekly_overtime,
)


def test_semimonthly_periods_and_weekend_adjustments():
    first = semimonthly_period(date(2026, 8, 10))
    assert (first.start_date, first.end_date, first.pay_date) == (
        date(2026, 8, 1), date(2026, 8, 15), date(2026, 8, 20)
    )
    second = semimonthly_period(date(2026, 8, 22))
    assert (second.start_date, second.end_date, second.pay_date) == (
        date(2026, 8, 16), date(2026, 8, 31), date(2026, 9, 4)
    )
    assert len(periods_for_year(2026)) == 24


def test_sunday_saturday_overtime_does_not_include_holiday_or_pto():
    hours = {
        date(2026, 8, 2): Decimal("9"),
        date(2026, 8, 3): Decimal("9"),
        date(2026, 8, 4): Decimal("9"),
        date(2026, 8, 5): Decimal("9"),
        date(2026, 8, 6): Decimal("9"),
    }
    regular, overtime = weekly_overtime(hours)
    assert regular == Decimal("40")
    assert overtime == Decimal("5")
    gross = hourly_gross(
        rate_cents=2000, regular_hours=regular, overtime_hours=overtime,
        holiday_hours=Decimal("8"), pto_hours=Decimal("4"),
    )
    assert gross == {
        "regular_cents": 80000,
        "overtime_cents": 15000,
        "holiday_cents": 16000,
        "pto_cents": 8000,
        "gross_cents": 119000,
    }


def test_period_overtime_includes_prior_days_from_same_workweek():
    # August 1, 2026 is Saturday. Forty hours from the preceding Sunday–Friday
    # make Saturday's in-period work overtime even though those earlier hours
    # belong to the prior semimonthly pay period.
    hours = {
        date(2026, 7, 27): Decimal("8"),
        date(2026, 7, 28): Decimal("8"),
        date(2026, 7, 29): Decimal("8"),
        date(2026, 7, 30): Decimal("8"),
        date(2026, 7, 31): Decimal("8"),
        date(2026, 8, 1): Decimal("8"),
    }
    regular, overtime = period_overtime(
        hours, date(2026, 8, 1), date(2026, 8, 15)
    )
    assert regular == Decimal("0")
    assert overtime == Decimal("8")
