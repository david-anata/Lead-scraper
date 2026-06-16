"""Indicative valuation ranges — multiples, grade shift, thin-data widening,
and the negative-earnings guard."""

from __future__ import annotations

import unittest

from sales_support_agent.services.brand_analysis import valuation as V
from sales_support_agent.services.brand_analysis.schema import CATEGORY_DTC, Metrics


class ValuationTests(unittest.TestCase):
    def _metrics(self, **kw) -> Metrics:
        m = Metrics()
        for k, v in kw.items():
            setattr(m, k, v)
        return m

    def test_revenue_and_earnings_blend(self) -> None:
        m = self._metrics(net_revenue_cents=100_000_00, operating_result_ex_other_cents=15_000_00)
        out = V.estimate(m, category=CATEGORY_DTC, grade="B", data_completeness_pct=90)
        self.assertEqual(out.primary_basis, "blended")
        self.assertTrue(out.is_meaningful())
        self.assertIsNotNone(out.rev_ev_low_cents)
        self.assertIsNotNone(out.earn_ev_low_cents)
        self.assertLess(out.ev_low_cents, out.ev_high_cents)
        self.assertEqual(out.earnings_basis_label, "Operating result ex-other-income")

    def test_negative_earnings_falls_back_to_revenue(self) -> None:
        # Luxmery-shaped: profit only from non-recurring income → op result negative.
        m = self._metrics(net_revenue_cents=656_075_00, operating_result_ex_other_cents=-22_077_00)
        out = V.estimate(m, category=CATEGORY_DTC, grade="D", data_completeness_pct=55)
        self.assertEqual(out.primary_basis, "revenue")
        self.assertIsNone(out.earn_ev_low_cents)
        self.assertTrue(any("negative" in c.lower() for c in out.caveats))
        self.assertTrue(out.is_meaningful())

    def test_grade_shifts_the_band(self) -> None:
        m = self._metrics(net_revenue_cents=100_000_00)
        a = V.estimate(m, grade="A", data_completeness_pct=90)
        f = V.estimate(m, grade="F", data_completeness_pct=90)
        self.assertGreater(a.rev_multiple_high, f.rev_multiple_high)
        self.assertGreater(a.ev_high_cents, f.ev_high_cents)

    def test_thin_data_widens_and_caveats(self) -> None:
        m = self._metrics(net_revenue_cents=100_000_00)
        thin = V.estimate(m, grade="C", data_completeness_pct=40)
        full = V.estimate(m, grade="C", data_completeness_pct=90)
        # Thin band is wider at the top (widened +25%).
        self.assertGreater(thin.rev_multiple_high, full.rev_multiple_high)
        self.assertEqual(thin.confidence, "Low")
        self.assertTrue(any("completeness" in c.lower() for c in thin.caveats))

    def test_no_basis_is_not_meaningful(self) -> None:
        out = V.estimate(Metrics(), grade="C", data_completeness_pct=10)
        self.assertFalse(out.is_meaningful())
        self.assertEqual(out.headline(), "Insufficient data for an indicative range")

    def test_always_carries_indicative_caveat(self) -> None:
        out = V.estimate(self._metrics(net_revenue_cents=50_000_00), grade="B", data_completeness_pct=90)
        self.assertTrue(any("indicative only" in c.lower() for c in out.caveats))

    def test_round_cents_avoids_false_precision(self) -> None:
        self.assertEqual(V._round_cents(1_284_113), 1_300_000)  # ~2 sig figs
        self.assertEqual(V._round_cents(0), 0)

    def test_serialization_round_trip(self) -> None:
        out = V.estimate(self._metrics(net_revenue_cents=80_000_00, net_earnings_cents=5_000_00),
                         grade="C", data_completeness_pct=70)
        back = V.ValuationRange.from_dict(out.to_dict())
        self.assertEqual(back.ev_low_cents, out.ev_low_cents)
        self.assertEqual(back.caveats, out.caveats)


if __name__ == "__main__":
    unittest.main()
