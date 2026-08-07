from __future__ import annotations

import unittest

from sales_support_agent.services.building_lead_pricing import (
    LeadPricingError,
    compute_totals,
    default_pricing,
    merge_values_from_pricing,
    parse_pricing_form,
)


class LeadPricingTests(unittest.TestCase):
    """Pricing belongs to one lead. Changing it must never reach another."""

    def test_a_lead_starts_from_the_approved_plan(self) -> None:
        seeded = default_pricing({
            "id": "arena-rate-v1", "name": "Arena standard", "currency": "USD",
            "unit_amount_cents": 17_500, "minimum_units": 6, "deposit_percent_bps": 5_000,
        })
        self.assertEqual(seeded["hourly_rate_cents"], 17_500)
        self.assertEqual(seeded["hours"], 6)
        self.assertEqual(seeded["deposit_percent_bps"], 5_000)

    def test_it_still_opens_filled_in_with_no_plan(self) -> None:
        seeded = default_pricing(None)
        self.assertEqual(seeded["hourly_rate_cents"], 17_500)
        self.assertEqual(seeded["hours"], 6)

    def test_totals_follow_the_owner_approved_shape(self) -> None:
        totals = compute_totals({
            "hourly_rate_cents": 17_500, "hours": 6,
            "cleaning_fee_cents": 25_000,
            "addons": [{"name": "A/V technician", "amount_cents": 15_000}],
            "discount_cents": 0, "deposit_percent_bps": 5_000,
        })
        self.assertEqual(totals["venue_cents"], 105_000)
        self.assertEqual(totals["subtotal_cents"], 145_000)
        self.assertEqual(totals["total_cents"], 145_000)
        self.assertEqual(totals["deposit_cents"], 72_500)

    def test_a_discount_reduces_the_total_and_the_deposit(self) -> None:
        totals = compute_totals({
            "hourly_rate_cents": 17_500, "hours": 6, "cleaning_fee_cents": 25_000,
            "addons": [], "discount_cents": 30_000, "deposit_percent_bps": 5_000,
        })
        self.assertEqual(totals["total_cents"], 100_000)
        self.assertEqual(totals["deposit_cents"], 50_000)

    def test_a_discount_cannot_exceed_the_subtotal(self) -> None:
        totals = compute_totals({
            "hourly_rate_cents": 10_000, "hours": 1, "cleaning_fee_cents": 0,
            "addons": [], "discount_cents": 999_999, "deposit_percent_bps": 5_000,
        })
        self.assertEqual(totals["total_cents"], 0)
        self.assertEqual(totals["deposit_cents"], 0)

    def test_a_discount_requires_a_recorded_reason(self) -> None:
        form = {"hourly_rate": "175", "hours": "6", "cleaning_fee": "250",
                "discount": "100", "discount_reason": "", "deposit_percent": "50"}
        with self.assertRaises(LeadPricingError) as caught:
            parse_pricing_form(form, existing=default_pricing(None), actor="d@a.com")
        self.assertIn("reason", str(caught.exception))

    def test_form_values_are_read_as_dollars(self) -> None:
        form = {"hourly_rate": "$1,250.50", "hours": "8", "cleaning_fee": "250",
                "discount": "0", "discount_reason": "", "deposit_percent": "25",
                "addon_name_0": "Security", "addon_amount_0": "400",
                "addon_name_1": "", "addon_amount_1": ""}
        priced = parse_pricing_form(form, existing=default_pricing(None), actor="d@a.com")
        self.assertEqual(priced["hourly_rate_cents"], 125_050)
        self.assertEqual(priced["deposit_percent_bps"], 2_500)
        self.assertEqual(priced["addons"], [{"name": "Security", "amount_cents": 40_000}])
        self.assertEqual(priced["updated_by"], "d@a.com")

    def test_nonsense_is_refused_rather_than_silently_zeroed(self) -> None:
        base = default_pricing(None)
        for field, value in (("hourly_rate", "abc"), ("hours", "many"), ("deposit_percent", "150")):
            form = {"hourly_rate": "175", "hours": "6", "cleaning_fee": "250",
                    "discount": "0", "discount_reason": "", "deposit_percent": "50"}
            form[field] = value
            with self.assertRaises(LeadPricingError, msg=field):
                parse_pricing_form(form, existing=base, actor="d@a.com")

    def test_contract_fields_come_straight_from_the_lead(self) -> None:
        values = merge_values_from_pricing({
            "hourly_rate_cents": 17_500, "hours": 6, "cleaning_fee_cents": 25_000,
            "addons": [{"name": "A/V technician", "amount_cents": 15_000}],
            "discount_cents": 20_000, "discount_reason": "Repeat customer",
            "deposit_percent_bps": 5_000, "currency": "USD",
        })
        self.assertEqual(values["subtotal_before_discount"], 145_000)
        self.assertEqual(values["quote_total"], 125_000)
        self.assertEqual(values["discount_reason"], "Repeat customer")
        self.assertEqual(values["addons"], ["A/V technician"])


if __name__ == "__main__":
    unittest.main()
