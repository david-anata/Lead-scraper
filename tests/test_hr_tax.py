from decimal import Decimal

from sales_support_agent.services.hr.tax import (
    employer_unemployment_2026,
    federal_deposit_due_date,
    federal_income_tax_2026,
    fica_2026,
    utah_income_tax_2026,
)
from datetime import date


def test_utah_semimonthly_married_matches_official_example():
    result = utah_income_tax_2026(120000, filing_status="married_filing_jointly")
    assert result["withholding_cents"] == 1800


def test_federal_worksheet_is_reproducible_and_applies_w4_adjustments():
    baseline = federal_income_tax_2026(100000, filing_status="single")
    adjusted = federal_income_tax_2026(
        100000, filing_status="single", dependents_credit_cents=240000,
        extra_withholding_cents=2500,
    )
    assert baseline["withholding_cents"] == 3292
    assert adjusted["withholding_cents"] == 2500
    assert adjusted["trace"]["method"].startswith("IRS Pub 15-T 2026")


def test_fica_caps_social_security_and_additional_medicare_is_employee_only():
    result = fica_2026(1000000, ytd_before_cents=18000000)
    assert result["social_security_employee_cents"] == 27900
    high = fica_2026(1500000, ytd_before_cents=19500000)
    assert high["additional_medicare_employee_cents"] == 9000
    assert high["medicare_employee_cents"] > high["medicare_employer_cents"]


def test_employer_unemployment_uses_ytd_wage_bases():
    result = employer_unemployment_2026(
        1000000, futa_ytd_before_cents=650000,
        utah_ui_ytd_before_cents=5000000, utah_ui_rate=Decimal("0.001"),
    )
    assert result["futa_taxable_wages_cents"] == 50000
    assert result["utah_ui_taxable_wages_cents"] == 70000


def test_federal_semiweekly_due_dates_follow_payday_rule_and_holidays():
    assert federal_deposit_due_date(date(2026, 8, 20), "semiweekly") == date(2026, 8, 26)
    assert federal_deposit_due_date(date(2026, 9, 4), "semiweekly") == date(2026, 9, 10)
