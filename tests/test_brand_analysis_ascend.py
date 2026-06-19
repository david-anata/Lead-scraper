"""Ascend acquisition framing tests — SDE, hard disqualifiers, _grade_brand fix,
competitive grader.

These tests exercise the new engine changes without LLM or database dependencies.
"""

from __future__ import annotations

import unittest

from sales_support_agent.services.brand_analysis.competitive import build_competitive_grade
from sales_support_agent.services.brand_analysis.schema import (
    CATEGORY_DTC,
    CompetitiveSignals,
    Metrics,
    PeriodFinancials,
    benchmarks_for,
)
from sales_support_agent.services.brand_analysis.scoring import (
    _grade_brand,
    build_red_flags,
    derive_metrics,
)
from sales_support_agent.services.brand_analysis.valuation import estimate

_BM = benchmarks_for(CATEGORY_DTC)


SEV_CRITICAL = "Critical"
SEV_HIGH = "High"


class SDECalculationTests(unittest.TestCase):
    def test_sde_sums_addbacks(self) -> None:
        p = PeriodFinancials(
            net_revenue_cents=200_000_00,
            cogs_cents=60_000_00,
            net_earnings_cents=30_000_00,
            owner_compensation_cents=12_000_00,
            depreciation_cents=2_000_00,
            addback_items_cents=1_000_00,
        )
        m = derive_metrics(p)
        self.assertEqual(m.sde_cents, 45_000_00)

    def test_sde_is_none_when_net_earnings_missing(self) -> None:
        p = PeriodFinancials(
            net_revenue_cents=100_000_00,
            owner_compensation_cents=10_000_00,
        )
        m = derive_metrics(p)
        self.assertIsNone(m.sde_cents)

    def test_sde_preferred_in_valuation(self) -> None:
        p = PeriodFinancials(
            net_revenue_cents=200_000_00,
            net_earnings_cents=20_000_00,
            owner_compensation_cents=15_000_00,
        )
        m = derive_metrics(p)
        v = estimate(m, category=CATEGORY_DTC, grade="B", data_completeness_pct=80)
        self.assertIn("SDE", v.earnings_basis_label)

    def test_valuation_earn_multiple_ascend_range(self) -> None:
        m = Metrics()
        m.net_revenue_cents = 150_000_000  # $1.5M
        m.sde_cents = 60_000_000           # $600K SDE
        v = estimate(m, category=CATEGORY_DTC, grade="A", data_completeness_pct=90)
        # Ascend's earn multiples are 2–3.5× base (grade "A" boosts by 1.2× to ≈4.2 max)
        # Previously was 3–5× base → 6× at top. Assert we're meaningfully below the old ceiling.
        self.assertLessEqual(v.earn_multiple_high, 4.5, "Earn multiple should be capped by Ascend's 2–3.5× range")
        self.assertGreaterEqual(v.earn_multiple_low, 1.9)
        # Also verify it's well below the old 6× ceiling
        self.assertLess(v.earn_multiple_high, 5.0, "Old DTC earn range (3–5×) should no longer apply")


class HardDisqualifierTests(unittest.TestCase):
    def _flags(self, cur: PeriodFinancials, social_signals=None):
        m = derive_metrics(cur)
        return build_red_flags(m, cur, None, _BM, social_signals=social_signals)

    @staticmethod
    def _sev(f):
        return getattr(f, "severity", None) if not isinstance(f, dict) else f.get("severity", "")

    @staticmethod
    def _title(f):
        return getattr(f, "title", "") if not isinstance(f, dict) else f.get("title", "")

    def test_below_1m_revenue_is_critical(self) -> None:
        p = PeriodFinancials(net_revenue_cents=500_000_00)
        flags = self._flags(p)
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_CRITICAL, sevs, "Sub-$1M revenue should be Critical")
        matched = [f for f in flags if self._sev(f) == SEV_CRITICAL and "revenue" in self._title(f).lower()]
        self.assertTrue(matched)

    def test_above_1m_revenue_no_critical_for_revenue(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000)  # $1.5M
        flags = self._flags(p)
        crit_titles = [self._title(f) for f in flags if self._sev(f) == SEV_CRITICAL]
        self.assertFalse(any("revenue" in t.lower() for t in crit_titles))

    def test_low_review_rating_is_critical(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000)
        flags = self._flags(p, social_signals={"review_rating": 4.1})
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_CRITICAL, sevs)

    def test_acceptable_review_rating_no_critical(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000)
        flags = self._flags(p, social_signals={"review_rating": 4.5})
        sevs = [self._sev(f) for f in flags]
        self.assertNotIn(SEV_CRITICAL, sevs)

    def test_high_tacos_is_high_severity(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000, tacos_bps=1800)  # 18%
        flags = self._flags(p)
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_HIGH, sevs)

    def test_low_sku_count_is_high_severity(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000, sku_count=3)
        flags = self._flags(p)
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_HIGH, sevs)

    def test_no_trademark_is_high_severity(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000, has_trademark=False)
        flags = self._flags(p)
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_HIGH, sevs)

    def test_no_brand_registry_is_high_severity(self) -> None:
        p = PeriodFinancials(net_revenue_cents=150_000_000, has_brand_registry=False)
        flags = self._flags(p)
        sevs = [self._sev(f) for f in flags]
        self.assertIn(SEV_HIGH, sevs)

    def test_all_criteria_pass_no_disqualifiers(self) -> None:
        p = PeriodFinancials(
            net_revenue_cents=150_000_000,
            tacos_bps=1200,
            sku_count=8,
            has_trademark=True,
            has_brand_registry=True,
        )
        flags = self._flags(p, social_signals={"review_rating": 4.6})
        crit_flags = [f for f in flags if self._sev(f) == SEV_CRITICAL]
        self.assertEqual(len(crit_flags), 0)


class GradeBrandFixTests(unittest.TestCase):
    """_grade_brand must not penalize owned_pct_bps=0 — that's an Ascend opportunity,
    not a weakness. Grading should use gross margin + returning customer rate."""

    def test_zero_owned_pct_not_penalized(self) -> None:
        cur = Metrics(product_gm_bps=5500, owned_pct_bps=0)
        p = PeriodFinancials()
        letter, _ = _grade_brand(cur, p)
        self.assertIn(letter, ("A", "B"), "Strong margin + zero owned pct should grade A or B, not penalized")

    def test_high_gm_grades_well(self) -> None:
        cur = Metrics(product_gm_bps=6000)
        p = PeriodFinancials()
        letter, reason = _grade_brand(cur, p)
        self.assertIn(letter, ("A", "B"))
        self.assertIn("margin", reason.lower())

    def test_low_gm_grades_lower(self) -> None:
        cur = Metrics(product_gm_bps=2500)
        p = PeriodFinancials()
        letter, _ = _grade_brand(cur, p)
        self.assertIn(letter, ("C", "D", "F"))

    def test_high_repeat_rate_boosts_grade(self) -> None:
        cur = Metrics(product_gm_bps=5000)
        p = PeriodFinancials(
            returning_customer_revenue_cents=40_000_00,
            new_customer_revenue_cents=60_000_00,
        )
        letter, reason = _grade_brand(cur, p)
        self.assertIn(letter, ("A", "B"))
        self.assertIn("returning", reason.lower())

    def test_no_signals_grades_neutral(self) -> None:
        cur = Metrics()
        p = PeriodFinancials()
        letter, reason = _grade_brand(cur, p)
        self.assertEqual(letter, "C")
        self.assertIn("not supplied", reason.lower())


class CompetitiveGraderTests(unittest.TestCase):
    def test_dominant_review_moat_grades_a(self) -> None:
        s = CompetitiveSignals(brand_review_count=2000, top_competitor_review_count=800)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["review_moat"]["letter"], "A")

    def test_weak_review_moat_grades_d(self) -> None:
        s = CompetitiveSignals(brand_review_count=100, top_competitor_review_count=1000)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["review_moat"]["letter"], "D")

    def test_high_rating_grades_a(self) -> None:
        s = CompetitiveSignals(brand_review_rating=4.7)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["rating_quality"]["letter"], "A")

    def test_below_threshold_rating_grades_d_or_f(self) -> None:
        s = CompetitiveSignals(brand_review_rating=3.8)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertIn(dims["rating_quality"]["letter"], ("D", "F"))

    def test_premium_price_positioning_grades_a(self) -> None:
        s = CompetitiveSignals(brand_price_cents=3500, category_median_price_cents=2500)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["price_positioning"]["letter"], "A")

    def test_deep_discount_grades_d(self) -> None:
        s = CompetitiveSignals(brand_price_cents=1500, category_median_price_cents=2500)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["price_positioning"]["letter"], "D")

    def test_top_bsr_grades_a(self) -> None:
        s = CompetitiveSignals(brand_bsr=50)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["bsr_rank"]["letter"], "A")

    def test_high_bsr_grades_f(self) -> None:
        s = CompetitiveSignals(brand_bsr=50_000)
        out = build_competitive_grade(s)
        dims = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dims["bsr_rank"]["letter"], "F")

    def test_no_signals_low_confidence(self) -> None:
        s = CompetitiveSignals()
        out = build_competitive_grade(s)
        self.assertEqual(out["confidence"], "Low")
        self.assertEqual(out["assessed_weight_pct"], 0)

    def test_full_signals_high_confidence(self) -> None:
        s = CompetitiveSignals(
            brand_review_count=500,
            top_competitor_review_count=300,
            brand_review_rating=4.5,
            brand_price_cents=2500,
            category_median_price_cents=2000,
            brand_bsr=200,
        )
        out = build_competitive_grade(s)
        self.assertEqual(out["confidence"], "High")
        self.assertEqual(out["assessed_weight_pct"], 100)

    def test_result_structure(self) -> None:
        s = CompetitiveSignals(brand_review_rating=4.5)
        out = build_competitive_grade(s)
        for key in ("letter", "score_100", "confidence", "assessed_weight_pct", "dimensions"):
            self.assertIn(key, out)
        self.assertIsInstance(out["dimensions"], list)
        self.assertEqual(len(out["dimensions"]), 4)


if __name__ == "__main__":
    unittest.main()
