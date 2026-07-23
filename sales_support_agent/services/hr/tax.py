"""Effective-dated 2026 payroll tax calculations for Anata's Utah employees.

The functions return both amounts and a calculation trace so a reviewer can
reproduce every result. They do not file or pay taxes.
"""

from __future__ import annotations

from dataclasses import dataclass
import calendar
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP


MONEY = Decimal("0.01")
EFFECTIVE_2026 = date(2026, 1, 1)
SOCIAL_SECURITY_RATE = Decimal("0.062")
SOCIAL_SECURITY_WAGE_BASE = Decimal("184500")
MEDICARE_RATE = Decimal("0.0145")
ADDITIONAL_MEDICARE_RATE = Decimal("0.009")
ADDITIONAL_MEDICARE_THRESHOLD = Decimal("200000")
FUTA_STANDARD_RATE = Decimal("0.006")
FUTA_WAGE_BASE = Decimal("7000")
UTAH_UI_WAGE_BASE = Decimal("50700")
FEDERAL_HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
    date(2026, 4, 16), date(2026, 5, 25), date(2026, 6, 19),
    date(2026, 7, 3), date(2026, 9, 7), date(2026, 10, 12),
    date(2026, 11, 11), date(2026, 11, 26), date(2026, 12, 25),
}


@dataclass(frozen=True)
class TaxBracket:
    lower: Decimal
    upper: Decimal | None
    base_tax: Decimal
    rate: Decimal


_FEDERAL_STANDARD = {
    "married_filing_jointly": (
        (0, 19300, 0, 0), (19300, 44100, 0, .10), (44100, 120100, 2480, .12),
        (120100, 230700, 11600, .22), (230700, 422850, 35932, .24),
        (422850, 531750, 82048, .32), (531750, 788000, 116896, .35),
        (788000, None, 206583.50, .37),
    ),
    "single": (
        (0, 7500, 0, 0), (7500, 19900, 0, .10), (19900, 57900, 1240, .12),
        (57900, 113200, 5800, .22), (113200, 209275, 17966, .24),
        (209275, 263725, 41024, .32), (263725, 648100, 58448, .35),
        (648100, None, 192979.25, .37),
    ),
    "head_of_household": (
        (0, 15550, 0, 0), (15550, 33250, 0, .10), (33250, 83000, 1770, .12),
        (83000, 121250, 7740, .22), (121250, 217300, 16155, .24),
        (217300, 271750, 39207, .32), (271750, 656150, 56631, .35),
        (656150, None, 191171, .37),
    ),
}

_FEDERAL_CHECKBOX = {
    "married_filing_jointly": (
        (0, 16100, 0, 0), (16100, 28500, 0, .10), (28500, 66500, 1240, .12),
        (66500, 121800, 5800, .22), (121800, 217875, 17966, .24),
        (217875, 272325, 41024, .32), (272325, 400450, 58448, .35),
        (400450, None, 103291.75, .37),
    ),
    "single": (
        (0, 8050, 0, 0), (8050, 14250, 0, .10), (14250, 33250, 620, .12),
        (33250, 60900, 2900, .22), (60900, 108938, 8983, .24),
        (108938, 136163, 20512, .32), (136163, 328350, 29224, .35),
        (328350, None, 96489.63, .37),
    ),
    "head_of_household": (
        (0, 12075, 0, 0), (12075, 20925, 0, .10), (20925, 45800, 885, .12),
        (45800, 64925, 3870, .22), (64925, 112950, 8077.50, .24),
        (112950, 140175, 19603.50, .32), (140175, 332375, 28315.50, .35),
        (332375, None, 95585.50, .37),
    ),
}


def _money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _cents(value: Decimal) -> int:
    return int(_money(value) * 100)


def _status_key(filing_status: str) -> str:
    normalized = (filing_status or "").strip().lower().replace(" ", "_")
    if normalized in {"married", "married_joint", "married_jointly", "married_filing_jointly"}:
        return "married_filing_jointly"
    if normalized in {"head", "head_household", "head_of_household"}:
        return "head_of_household"
    return "single"


def federal_income_tax_2026(
    taxable_wages_cents: int, *, filing_status: str, two_jobs: bool = False,
    dependents_credit_cents: int = 0, other_income_cents: int = 0,
    deductions_cents: int = 0, extra_withholding_cents: int = 0,
    periods_per_year: int = 24,
) -> dict:
    """IRS Pub. 15-T Worksheet 1A for a 2020-or-later Form W-4."""
    wages = Decimal(taxable_wages_cents) / 100
    other_income = Decimal(other_income_cents) / 100
    deductions = Decimal(deductions_cents) / 100
    annual_wages = wages * periods_per_year
    filing_key = _status_key(filing_status)
    standard_adjustment = Decimal("0") if two_jobs else (
        Decimal("12900") if filing_key == "married_filing_jointly" else Decimal("8600")
    )
    adjusted_annual = max(
        Decimal("0"), annual_wages + other_income - deductions - standard_adjustment
    )
    source = _FEDERAL_CHECKBOX if two_jobs else _FEDERAL_STANDARD
    raw_rows = source[filing_key]
    selected = next(
        row for row in raw_rows
        if adjusted_annual >= Decimal(str(row[0]))
        and (row[1] is None or adjusted_annual < Decimal(str(row[1])))
    )
    lower, _, base, rate = selected
    annual_tax = Decimal(str(base)) + (
        adjusted_annual - Decimal(str(lower))
    ) * Decimal(str(rate))
    annual_tax = max(Decimal("0"), annual_tax - Decimal(dependents_credit_cents) / 100)
    period_tax = annual_tax / periods_per_year + Decimal(extra_withholding_cents) / 100
    return {
        "withholding_cents": _cents(max(Decimal("0"), period_tax)),
        "trace": {
            "method": "IRS Pub 15-T 2026 Worksheet 1A automated percentage",
            "effective_date": EFFECTIVE_2026.isoformat(),
            "filing_status": filing_key,
            "two_jobs": two_jobs,
            "annual_wages": str(_money(annual_wages)),
            "standard_adjustment": str(standard_adjustment),
            "adjusted_annual_wages": str(_money(adjusted_annual)),
            "bracket_floor": str(lower),
            "annual_tax_before_credits": str(_money(
                Decimal(str(base)) + (adjusted_annual - Decimal(str(lower))) * Decimal(str(rate))
            )),
            "dependents_credit": str(_money(Decimal(dependents_credit_cents) / 100)),
            "extra_per_period": str(_money(Decimal(extra_withholding_cents) / 100)),
        },
    }


def utah_income_tax_2026(taxable_wages_cents: int, *, filing_status: str) -> dict:
    """Utah Pub. 14 revision 04/26, Schedule 3 (semimonthly)."""
    wages = Decimal(taxable_wages_cents) / 100
    married = _status_key(filing_status) == "married_filing_jointly"
    base_allowance = Decimal("40") if married else Decimal("20")
    wage_offset = Decimal("779") if married else Decimal("390")
    # Pub. 14's schedules and worked examples calculate in whole dollars.
    line_2 = (wages * Decimal("0.0445")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    line_4 = max(Decimal("0"), wages - wage_offset)
    line_5 = (line_4 * Decimal("0.013")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    line_6 = max(Decimal("0"), base_allowance - line_5)
    result = max(Decimal("0"), line_2 - line_6)
    return {
        "withholding_cents": _cents(result),
        "trace": {
            "method": "Utah Pub 14 rev. 04/26 Schedule 3",
            "taxable_wages": str(_money(wages)),
            "line_2": str(line_2), "line_3": str(base_allowance),
            "line_4": str(_money(line_4)), "line_5": str(line_5),
            "line_6": str(_money(line_6)),
        },
    }


def fica_2026(taxable_wages_cents: int, *, ytd_before_cents: int = 0) -> dict:
    wages = Decimal(taxable_wages_cents) / 100
    ytd = Decimal(ytd_before_cents) / 100
    ss_taxable = min(wages, max(Decimal("0"), SOCIAL_SECURITY_WAGE_BASE - ytd))
    medicare = wages * MEDICARE_RATE
    additional_taxable = max(
        Decimal("0"), ytd + wages - ADDITIONAL_MEDICARE_THRESHOLD
    ) - max(Decimal("0"), ytd - ADDITIONAL_MEDICARE_THRESHOLD)
    additional = additional_taxable * ADDITIONAL_MEDICARE_RATE
    return {
        "social_security_employee_cents": _cents(ss_taxable * SOCIAL_SECURITY_RATE),
        "social_security_employer_cents": _cents(ss_taxable * SOCIAL_SECURITY_RATE),
        "medicare_employee_cents": _cents(medicare + additional),
        "medicare_employer_cents": _cents(medicare),
        "additional_medicare_employee_cents": _cents(additional),
    }


def employer_unemployment_2026(
    taxable_wages_cents: int, *, futa_ytd_before_cents: int = 0,
    utah_ui_ytd_before_cents: int = 0, utah_ui_rate: Decimal,
) -> dict:
    wages = Decimal(taxable_wages_cents) / 100
    futa_ytd = Decimal(futa_ytd_before_cents) / 100
    ui_ytd = Decimal(utah_ui_ytd_before_cents) / 100
    futa_taxable = min(wages, max(Decimal("0"), FUTA_WAGE_BASE - futa_ytd))
    ui_taxable = min(wages, max(Decimal("0"), UTAH_UI_WAGE_BASE - ui_ytd))
    return {
        "futa_cents": _cents(futa_taxable * FUTA_STANDARD_RATE),
        "utah_ui_cents": _cents(ui_taxable * Decimal(utah_ui_rate)),
        "futa_taxable_wages_cents": _cents(futa_taxable),
        "utah_ui_taxable_wages_cents": _cents(ui_taxable),
    }


def _next_business_day(day: date, holidays: set[date] | None = None) -> date:
    holidays = holidays or set()
    while day.weekday() >= 5 or day in holidays:
        day += timedelta(days=1)
    return day


def federal_deposit_due_date(pay_date: date, schedule: str) -> date:
    """Pub. 15 deposit date including 2026 federal/D.C. holidays."""
    if schedule == "monthly":
        if pay_date.month == 12:
            due = date(pay_date.year + 1, 1, 15)
        else:
            due = date(pay_date.year, pay_date.month + 1, 15)
        return _next_business_day(due, FEDERAL_HOLIDAYS_2026)
    # Wed–Fri wages form a period that closes Friday; Sat–Tue closes Tuesday.
    # Pub. 15 gives at least three business days after that close. Count those
    # days explicitly so a holiday inside the window extends the deadline.
    period_close = pay_date + timedelta(
        days=((4 if pay_date.weekday() in {2, 3, 4} else 1) - pay_date.weekday()) % 7
    )
    due = period_close
    business_days = 0
    while business_days < 3:
        due += timedelta(days=1)
        if due.weekday() < 5 and due not in FEDERAL_HOLIDAYS_2026:
            business_days += 1
    return due


def quarter_due_date(day: date) -> date:
    quarter_end_month = ((day.month - 1) // 3 + 1) * 3
    if quarter_end_month == 12:
        year, month = day.year + 1, 1
    else:
        year, month = day.year, quarter_end_month + 1
    last = calendar.monthrange(year, month)[1]
    return _next_business_day(date(year, month, last))


def month_due_date(day: date) -> date:
    if day.month == 12:
        year, month = day.year + 1, 1
    else:
        year, month = day.year, day.month + 1
    return _next_business_day(date(year, month, calendar.monthrange(year, month)[1]))
