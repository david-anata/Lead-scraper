"""Brand & Social track — separate A–F, penalise-unknowns, social discovery."""

from __future__ import annotations

import unittest

from sales_support_agent.services.brand_analysis import social as S
from sales_support_agent.services.brand_analysis.schema import (
    BRAND_SOCIAL_DIMENSIONS, DIMENSIONS, Metrics, NOT_ASSESSED, PeriodFinancials,
)


class TwoTrackSplitTests(unittest.TestCase):
    def test_financial_track_excludes_brand_and_sums_to_one(self) -> None:
        keys = [k for k, _, _ in DIMENSIONS]
        self.assertNotIn("brand", keys)
        self.assertEqual(len(keys), 8)
        self.assertAlmostEqual(sum(w for _, _, w in DIMENSIONS), 1.0, places=3)

    def test_brand_social_dims_sum_to_one(self) -> None:
        self.assertAlmostEqual(sum(w for _, _, w in BRAND_SOCIAL_DIMENSIONS), 1.0, places=3)


class BrandSocialScoreTests(unittest.TestCase):
    def test_strong_signals_grade_well_with_high_confidence(self) -> None:
        # All 4 dimensions assessed; multi-channel already = "B" (less incremental
        # build upside than clean slate), but all data supplied → High confidence.
        m = Metrics(owned_pct_bps=2800, product_gm_bps=6000)
        out = S.build_brand_social(m, PeriodFinancials(), email_list_size=120_000,
            social_handles={"instagram": "x", "tiktok": "y", "facebook": "z", "youtube": "w"},
            social_signals={"review_rating": 4.6, "review_count": 800, "posting_recency_days": 3})
        self.assertIn(out["letter"], ("A", "B"))
        self.assertEqual(out["confidence"], "High")
        self.assertTrue(all(d["assessed"] for d in out["dimensions"]))

    def test_no_social_no_email_is_ascend_opportunity(self) -> None:
        # For Ascend: no social + no email = maximum build runway (A on those dims).
        # 2/4 dims assessed (dtc_opportunity + social_oppty) → Medium confidence.
        out = S.build_brand_social(Metrics(), PeriodFinancials(),
                                   email_list_size=0, social_handles={}, social_signals={})
        self.assertEqual(out["confidence"], "Medium")
        dim_map = {d["key"]: d for d in out["dimensions"]}
        self.assertEqual(dim_map["dtc_opportunity"]["letter"], "A")
        self.assertEqual(dim_map["social_oppty"]["letter"], "A")
        self.assertEqual(dim_map["product_signal"]["letter"], NOT_ASSESSED)

    def test_dtc_opportunity_bands(self) -> None:
        # Ascend-framed: 0 = clean slate = A; large list = B (less incremental upside).
        def g(n):
            return S._grade_dtc_opportunity(n)[0]
        self.assertEqual(g(0), "A")           # maximum build opportunity
        self.assertEqual(g(500), "B")          # minimal = clean-slate-ish
        self.assertEqual(g(5_000), "C")        # small list — Ascend builds from here
        self.assertEqual(g(20_000), "B")       # growing
        self.assertEqual(g(60_000), "A")       # strong DTC asset
        self.assertEqual(g(120_000), "B")      # large existing → less incremental

    def test_caveat_states_separate_from_financial(self) -> None:
        out = S.build_brand_social(Metrics(), PeriodFinancials(), email_list_size=10_000)
        self.assertTrue(any("does NOT affect" in c for c in out["caveats"]))


class SocialDiscoveryTests(unittest.TestCase):
    def test_parse_social_patterns(self) -> None:
        html = ('<a href="https://instagram.com/luxmery">IG</a>'
                '<a href="https://www.tiktok.com/@luxmery">TT</a>'
                '<a href="https://facebook.com/sharer/sharer.php?u=x">share</a>')
        found = {}
        for platform, pat in S._SOCIAL_PATTERNS.items():
            for m in pat.findall(html):
                link = m.rstrip("/\"'")
                if any(s in link.lower() for s in S._SKIP):
                    continue
                found.setdefault(platform, link)
                break
        self.assertIn("instagram", found)
        self.assertIn("tiktok", found)
        self.assertNotIn("facebook", found)  # sharer link skipped

    def test_discover_socials_empty_for_blank(self) -> None:
        self.assertEqual(S.discover_socials(""), {})

    def test_router_parse_social_urls(self) -> None:
        # Regression: the router's _parse_social_urls uses re.split — exercise
        # the router glue so a missing `import re` (which broke prod) is caught.
        try:
            from sales_support_agent.api.brand_analysis_router import _parse_social_urls
        except ModuleNotFoundError as exc:
            if exc.name in {"fastapi", "sqlalchemy"}:
                self.skipTest("fastapi/sqlalchemy not installed")
            raise
        out = _parse_social_urls("instagram.com/luxmery  https://tiktok.com/@luxmery")
        self.assertEqual(out.get("instagram"), "https://instagram.com/luxmery")
        self.assertEqual(out.get("tiktok"), "https://tiktok.com/@luxmery")
        self.assertEqual(_parse_social_urls(""), {})


if __name__ == "__main__":
    unittest.main()
