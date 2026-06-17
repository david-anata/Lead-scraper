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
        self.assertEqual(len(keys), 7)
        self.assertAlmostEqual(sum(w for _, _, w in DIMENSIONS), 1.0, places=3)

    def test_brand_social_dims_sum_to_one(self) -> None:
        self.assertAlmostEqual(sum(w for _, _, w in BRAND_SOCIAL_DIMENSIONS), 1.0, places=3)


class BrandSocialScoreTests(unittest.TestCase):
    def test_strong_signals_grade_high(self) -> None:
        m = Metrics(owned_pct_bps=2800, product_gm_bps=6000)
        out = S.build_brand_social(m, PeriodFinancials(), email_list_size=120_000,
            social_handles={"instagram": "x", "tiktok": "y", "facebook": "z", "youtube": "w"},
            social_signals={"review_rating": 4.6, "review_count": 800, "posting_recency_days": 3})
        self.assertEqual(out["letter"], "A")
        self.assertEqual(out["confidence"], "High")
        self.assertTrue(all(d["assessed"] for d in out["dimensions"]))

    def test_no_signals_penalised_low_confidence(self) -> None:
        out = S.build_brand_social(Metrics(), PeriodFinancials(),
                                   email_list_size=0, social_handles={}, social_signals={})
        self.assertEqual(out["confidence"], "Low")
        self.assertTrue(all(d["letter"] == NOT_ASSESSED for d in out["dimensions"]))
        self.assertEqual(out["score_100"], 0)

    def test_owned_audience_bands(self) -> None:
        def g(n):
            return S._grade_owned_audience(n)[0]
        self.assertEqual(g(120_000), "A")
        self.assertEqual(g(60_000), "B")
        self.assertEqual(g(20_000), "C")
        self.assertEqual(g(5_000), "D")
        self.assertEqual(g(500), "F")
        self.assertEqual(g(0), NOT_ASSESSED)

    def test_caveat_states_separate_from_financial(self) -> None:
        out = S.build_brand_social(Metrics(), PeriodFinancials(), email_list_size=10_000)
        self.assertTrue(any("does NOT affect the financial" in c for c in out["caveats"]))


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


if __name__ == "__main__":
    unittest.main()
