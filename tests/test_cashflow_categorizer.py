"""Tests for the cashflow categorizer."""

from __future__ import annotations

import unittest

from sales_support_agent.services.cashflow.categorizer import categorize


class TestCategorizer(unittest.TestCase):
    # --- Known merchant rules (source of truth: our CSV data) ---

    def test_fora_financial_is_debt(self) -> None:
        self.assertEqual(categorize("FORAFINANCIAL PAYMT"), "debt")

    def test_boulder_ranch_is_rent(self) -> None:
        self.assertEqual(categorize("BOULDER RANCH LLC"), "rent")

    def test_intuit_payroll_is_payroll(self) -> None:
        self.assertEqual(categorize("INTUIT PAYROLL"), "payroll")

    def test_intuit_tax_is_tax(self) -> None:
        self.assertEqual(categorize("INTUIT TAX PAYMENTS"), "tax")

    def test_intuit_deposit_is_revenue(self) -> None:
        self.assertEqual(categorize("INTUIT DEPOSIT SQ"), "revenue")

    def test_stripe_capital_is_debt(self) -> None:
        self.assertEqual(categorize("STRIPE CAP REPAYMENT"), "debt")

    def test_openai_is_software(self) -> None:
        self.assertEqual(categorize("OPENAI API"), "software")

    def test_anthropic_is_software(self) -> None:
        self.assertEqual(categorize("ANTHROPIC AI"), "software")

    def test_zapier_is_software(self) -> None:
        self.assertEqual(categorize("ZAPIER INC"), "software")

    def test_clickup_is_software(self) -> None:
        self.assertEqual(categorize("CLICKUP SUBSCRIPTION"), "software")

    def test_lehi_city_is_utilities(self) -> None:
        self.assertEqual(categorize("LEHI CITY UTILITY"), "utilities")

    def test_questargas_is_utilities(self) -> None:
        self.assertEqual(categorize("QUESTARGAS PAYMENT"), "utilities")

    def test_canyon_view_is_owner_draw(self) -> None:
        self.assertEqual(categorize("CANYON VIEW PAYMENT"), "owner_draw")

    def test_david_narayan_is_owner_draw(self) -> None:
        self.assertEqual(categorize("DAVID NARAYAN TRANSFER"), "owner_draw")

    def test_wise_is_transfer(self) -> None:
        self.assertEqual(categorize("WISE TRANSFER"), "transfer")

    def test_paypal_is_revenue(self) -> None:
        self.assertEqual(categorize("PAYPAL PAYMENT"), "revenue")

    def test_sweetwater_is_equipment(self) -> None:
        self.assertEqual(categorize("SWEETWATER PURCHASE"), "equipment")

    def test_costco_is_supplies(self) -> None:
        self.assertEqual(categorize("COSTCO WHOLESALE"), "supplies")

    def test_walmart_is_supplies(self) -> None:
        self.assertEqual(categorize("WALMART PURCHASE"), "supplies")

    # --- Case insensitivity ---

    def test_case_insensitive(self) -> None:
        self.assertEqual(categorize("openai api"), "software")
        self.assertEqual(categorize("Intuit Payroll"), "payroll")

    # --- Bank category fallback ---

    def test_bank_category_fallback_when_no_rule_matches(self) -> None:
        # Should not crash when bank_category provided and no pattern matches
        result = categorize("UNKNOWN VENDOR XYZ", bank_category="Restaurants & Dining")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_our_rules_override_bank_category(self) -> None:
        # Loom is a software tool — bank may label as "Restaurants & Dining"
        result = categorize("LOOM VIDEO", bank_category="Restaurants & Dining")
        self.assertEqual(result, "software")

    # --- Default fallback ---

    def test_unknown_falls_back_to_other(self) -> None:
        result = categorize("COMPLETELY UNKNOWN MERCHANT 12345")
        self.assertEqual(result, "other")

    # --- Return type ---

    def test_always_returns_string(self) -> None:
        self.assertIsInstance(categorize(""), str)
        self.assertIsInstance(categorize("   "), str)


if __name__ == "__main__":
    unittest.main()
