from __future__ import annotations

import unittest

from sales_support_agent.services.lead_build_revenue import (
    build_revenue_fields,
    extract_money_amount,
    format_money_compact,
    format_money_exact,
    parse_monthly_sales,
)

try:
    from main import (
        build_csv_rows,
        time,
    )

    MAIN_IMPORT_AVAILABLE = True
except Exception:
    MAIN_IMPORT_AVAILABLE = False

try:
    from unittest import mock

    UNITTEST_MOCK_AVAILABLE = True
except ModuleNotFoundError:
    UNITTEST_MOCK_AVAILABLE = False

APP_DEPS_AVAILABLE = True


class LeadBuildRevenueParsingTests(unittest.TestCase):
    def test_extract_money_amount_accepts_currency_text(self) -> None:
        self.assertEqual(extract_money_amount("$76,023"), 76023.0)

    def test_parse_monthly_sales_accepts_currency_text(self) -> None:
        self.assertEqual(parse_monthly_sales({"estimated_monthly_revenue": "$76,023"}), 76023.0)

    def test_parse_monthly_sales_uses_annual_range_fallback(self) -> None:
        self.assertEqual(parse_monthly_sales({"estimated_revenue_range": "$900K - $1.2M"}), 100000.0)

    def test_money_formatters_emit_exact_and_compact_values(self) -> None:
        self.assertEqual(format_money_exact(76023), "$76,023")
        self.assertEqual(format_money_compact(76023), "$75K")

    def test_build_revenue_fields_returns_both_exact_and_estimated(self) -> None:
        self.assertEqual(
            build_revenue_fields({"estimated_monthly_revenue": "$76,023"}),
            {"revenue": "$76,023", "estimated_revenue": "$75K"},
        )


@unittest.skipUnless(MAIN_IMPORT_AVAILABLE and UNITTEST_MOCK_AVAILABLE, "main import dependencies are required")
class LeadBuildRevenueRowTests(unittest.TestCase):
    def test_build_csv_rows_writes_exact_and_estimated_revenue(self) -> None:
        domains = [
            {
                "name": "example.com",
                "title": "Example",
                "platform": "Shopify",
                "estimated_monthly_revenue": "$76,023",
                "avg_price_usd": 2500,
            }
        ]
        settings = type("Settings", (), {"apollo_api_key": "", "instantly_campaign_id": "cmp_123"})()

        with mock.patch("main.search_apollo_contacts", return_value=(
            [
                {
                    "name": "Jane Doe",
                    "email": "jane@example.com",
                    "title": "Founder",
                    "linkedin_url": "https://linkedin.com/in/janedoe",
                }
            ],
            {},
        )), mock.patch.object(time, "sleep", return_value=None):
            result = build_csv_rows(domains, "2026-03-30", settings)

        self.assertEqual(len(result.instantly_rows), 1)
        row = result.instantly_rows[0]
        self.assertEqual(row["revenue"], "$76,023")
        self.assertEqual(row["estimated_revenue"], "$75K")


if __name__ == "__main__":
    unittest.main()
