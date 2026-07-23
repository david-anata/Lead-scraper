"""Deterministic, provider-independent payroll preparation helpers.

This module prepares evidence and gross-pay inputs. It never moves money and it
never represents an unreviewed tax estimate as an approved payroll.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP


MONEY = Decimal("0.01")


@dataclass(frozen=True)
class SemimonthlyPeriod:
    start_date: date
    end_date: date
    pay_date: date


def adjusted_business_date(day: date) -> date:
    """Apply Anata's approved Saturday-Friday / Sunday-Monday rule."""
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def semimonthly_period(containing: date) -> SemimonthlyPeriod:
    """Return the Anata period containing a calendar date."""
    if containing.day <= 15:
        start = containing.replace(day=1)
        end = containing.replace(day=15)
        pay = containing.replace(day=20)
    else:
        last = calendar.monthrange(containing.year, containing.month)[1]
        start = containing.replace(day=16)
        end = containing.replace(day=last)
        if containing.month == 12:
            pay = date(containing.year + 1, 1, 5)
        else:
            pay = date(containing.year, containing.month + 1, 5)
    return SemimonthlyPeriod(start, end, adjusted_business_date(pay))


def periods_for_year(year: int) -> list[SemimonthlyPeriod]:
    periods: list[SemimonthlyPeriod] = []
    for month in range(1, 13):
        periods.append(semimonthly_period(date(year, month, 1)))
        periods.append(semimonthly_period(date(year, month, 16)))
    return periods


def weekly_overtime(hours_by_date: dict[date, Decimal]) -> tuple[Decimal, Decimal]:
    """Split exact worked hours using Sunday-Saturday workweeks."""
    week_totals: dict[date, Decimal] = {}
    for worked_date, hours in hours_by_date.items():
        sunday = worked_date - timedelta(days=(worked_date.weekday() + 1) % 7)
        week_totals[sunday] = week_totals.get(sunday, Decimal("0")) + Decimal(hours)
    overtime = sum((max(Decimal("0"), total - Decimal("40")) for total in week_totals.values()),
                   Decimal("0"))
    total = sum(week_totals.values(), Decimal("0"))
    return total - overtime, overtime


def period_overtime(
    hours_by_date: dict[date, Decimal], period_start: date, period_end: date
) -> tuple[Decimal, Decimal]:
    """Allocate worked hours inside a period using complete Sunday–Saturday weeks.

    Hours before a semimonthly boundary can make the first in-period hours
    overtime. Later days cannot retroactively change earlier hours, so callers
    need the full workweek start through the period end.
    """
    regular_in_period = Decimal("0")
    overtime_in_period = Decimal("0")
    week_running: dict[date, Decimal] = {}
    for worked_date in sorted(hours_by_date):
        hours = max(Decimal("0"), Decimal(hours_by_date[worked_date]))
        sunday = worked_date - timedelta(days=(worked_date.weekday() + 1) % 7)
        prior = week_running.get(sunday, Decimal("0"))
        regular = min(hours, max(Decimal("0"), Decimal("40") - prior))
        overtime = hours - regular
        week_running[sunday] = prior + hours
        if period_start <= worked_date <= period_end:
            regular_in_period += regular
            overtime_in_period += overtime
    return regular_in_period, overtime_in_period


def hourly_gross(*, rate_cents: int, regular_hours: Decimal,
                 overtime_hours: Decimal = Decimal("0"),
                 holiday_hours: Decimal = Decimal("0"),
                 pto_hours: Decimal = Decimal("0")) -> dict[str, int]:
    """Calculate gross components; holiday/PTO never enter overtime hours."""
    rate = Decimal(rate_cents) / Decimal(100)
    regular = (rate * regular_hours).quantize(MONEY, rounding=ROUND_HALF_UP)
    overtime = (rate * Decimal("1.5") * overtime_hours).quantize(MONEY, rounding=ROUND_HALF_UP)
    holiday = (rate * holiday_hours).quantize(MONEY, rounding=ROUND_HALF_UP)
    pto = (rate * pto_hours).quantize(MONEY, rounding=ROUND_HALF_UP)
    values = {
        "regular_cents": int(regular * 100),
        "overtime_cents": int(overtime * 100),
        "holiday_cents": int(holiday * 100),
        "pto_cents": int(pto * 100),
    }
    values["gross_cents"] = sum(values.values())
    return values


def payroll_readiness(*, employees: list[dict], open_time_entries: list[dict],
                      pending_corrections: list[dict], pending_inputs: list[dict],
                      tax_engine_configured: bool, eftps_ready: bool,
                      utah_tax_ready: bool) -> dict:
    """Build explicit blockers. A red item prevents preparation/approval."""
    blockers: list[dict] = []
    for employee in employees:
        employment = employee.get("employment") or {}
        missing = []
        if not employment.get("hire_date"):
            missing.append("hire date")
        if not employment.get("classification"):
            missing.append("classification")
        if employment.get("pay_basis") == "hourly" and not employee.get("hourly_rate_cents"):
            missing.append("hourly rate")
        if employment.get("pay_basis") == "fixed_semimonthly" and not employment.get(
            "fixed_pay_per_period_cents"
        ):
            missing.append("fixed check amount")
        if missing:
            blockers.append({
                "kind": "employee_setup", "severity": "blocker",
                "employee_email": employee.get("email"),
                "message": "Missing " + ", ".join(missing),
            })
    for entry in open_time_entries:
        blockers.append({
            "kind": "open_time", "severity": "blocker",
            "employee_email": entry.get("employee_email"),
            "message": f"Open time entry on {entry.get('date')}",
        })
    for correction in pending_corrections:
        blockers.append({
            "kind": "time_correction", "severity": "blocker",
            "employee_email": correction.get("employee_email"),
            "message": "Time correction still needs another person's review",
        })
    for item in pending_inputs:
        blockers.append({
            "kind": "payroll_input", "severity": "blocker",
            "employee_email": item.get("employee_email"),
            "message": f"{item.get('input_type', 'Payroll input')} still needs review",
        })
    for ready, label in (
        (tax_engine_configured, "Qualified payroll tax calculation review"),
        (eftps_ready, "EFTPS payment access"),
        (utah_tax_ready, "Utah tax payment access"),
    ):
        if not ready:
            blockers.append({"kind": "tax_setup", "severity": "blocker", "message": f"{label} not ready"})
    return {"ready": not blockers, "blockers": blockers}
