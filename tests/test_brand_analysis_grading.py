"""Grading overhaul: standard bands + penalise-unknowns.

Pins the standard 90/80/70/60 scale and the rule that an unmeasured dimension
scores zero (assessed=False) rather than a neutral C — so incomplete data
trends toward F until more is supplied.
"""

from __future__ import annotations

import unittest

from sales_support_agent.services.brand_analysis import scoring
from sales_support_agent.services.brand_analysis.schema import (
    NOT_ASSESSED,
    PeriodFinancials,
    letter_from_score,
)


class StandardBandTests(unittest.TestCase):
    def test_standard_academic_scale(self) -> None:
        self.assertEqual(letter_from_score(95), "A")
        self.assertEqual(letter_from_score(90), "A")
        self.assertEqual(letter_from_score(85), "B")
        self.assertEqual(letter_from_score(72), "C")
        self.assertEqual(letter_from_score(63), "D")
        self.assertEqual(letter_from_score(59), "F")
        # The case the user flagged: 46 is an F, not a D.
        self.assertEqual(letter_from_score(46), "F")


class PenalizeUnknownsTests(unittest.TestCase):
    def test_sparse_submission_is_penalised_toward_f(self) -> None:
        sparse = PeriodFinancials(net_revenue_cents=100_000_00, cogs_cents=40_000_00)
        sc = scoring.score(sparse, None, category="dtc")["scorecard"]
        unassessed = [d for d in sc.dimensions if not d.assessed]
        self.assertTrue(unassessed, "most dimensions should be unassessed on sparse data")
        for d in unassessed:
            self.assertEqual(d.letter, NOT_ASSESSED)
            self.assertEqual(d.points, 0.0)  # zero, not a neutral C (2.0)
        self.assertEqual(sc.letter, "F")

    def test_complete_healthy_brand_grades_well(self) -> None:
        cur = PeriodFinancials(
            net_revenue_cents=100_000_00, cogs_cents=35_000_00, reported_gross_profit_cents=65_000_00,
            marketing_total_cents=25_000_00, net_earnings_cents=12_000_00, gross_sales_cents=105_000_00,
            discounts_cents=3_000_00, returns_cents=2_000_00, owned_channel_revenue_cents=28_000_00,
            total_assets_cents=80_000_00, total_equity_cents=50_000_00,
            marketing_by_channel={"meta": 10_000_00, "google": 8_000_00, "tiktok": 4_000_00, "email_sms": 3_000_00},
            new_customer_revenue_cents=60_000_00, returning_customer_revenue_cents=40_000_00)
        prior = PeriodFinancials(net_revenue_cents=80_000_00, cogs_cents=29_000_00,
                                 reported_gross_profit_cents=51_000_00, marketing_total_cents=22_000_00,
                                 net_earnings_cents=8_000_00)
        sc = scoring.score(cur, prior, category="dtc", email_list_size=75_000)["scorecard"]
        self.assertTrue(all(d.assessed for d in sc.dimensions))
        self.assertIn(sc.letter, ("A", "B"))

    def test_unassessed_dim_serializes_with_flag(self) -> None:
        sparse = PeriodFinancials(net_revenue_cents=100_000_00)
        sc = scoring.score(sparse, None, category="dtc")["scorecard"]
        from sales_support_agent.services.brand_analysis.schema import Scorecard
        back = Scorecard.from_dict(sc.to_dict())
        self.assertEqual([d.assessed for d in back.dimensions], [d.assessed for d in sc.dimensions])


if __name__ == "__main__":
    unittest.main()
