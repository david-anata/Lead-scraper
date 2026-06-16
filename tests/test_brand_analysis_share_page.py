"""Standalone branded share page — structure, valid embedded chart JSON, and
graceful rendering when data is sparse."""

from __future__ import annotations

import json
import re
import unittest

from sales_support_agent.services.brand_analysis.schema import (
    BenchmarkRow,
    BrandReport,
    DimensionGrade,
    Metrics,
    RedFlag,
    Scorecard,
)
from sales_support_agent.services.brand_analysis.share_page import render_share_page


def _full_report() -> BrandReport:
    cur = Metrics(net_revenue_cents=64_407_500, cogs_cents=40_000_000,
                  reported_gross_profit_cents=24_407_500, marketing_total_cents=5_000_000,
                  marketing_pct_bps=776, blended_mer=12.88, net_earnings_cents=900_000,
                  net_margin_bps=140, product_gm_bps=3790, contribution_margin_bps=3790)
    sc = Scorecard(
        dimensions=[
            DimensionGrade("revenue", "Revenue trajectory & growth", 0.25, "C", 2.0, "No prior year"),
            DimensionGrade("marketing", "Marketing efficiency (MER)", 0.15, "A", 4.0, "MER 12.9x"),
        ],
        score_100=58, letter="D",
    )
    return BrandReport(
        brand="Luxmery", category="dtc", prepared_date="2026-06-16",
        period_current_label="FY2024", has_yoy=False,
        current=cur, prior=Metrics(),
        media_mix={"meta": 4_000_000, "google": 1_000_000},
        monthly_revenue=[["Jan", 5_000_000], ["Feb", 5_200_000], ["Mar", 5_400_000],
                         ["Apr", 5_300_000], ["May", 5_600_000], ["Jun", 5_900_000]],
        scorecard=sc,
        red_flags=[RedFlag("Critical", "Negative operating result", "Profit is non-recurring")],
        benchmarks=[BenchmarkRow("Net margin", "8–15%", "1.4%", False)],
        info_ribbon=[{"label": "Grade", "value": "D · 58/100", "tone": "warn"}],
        investment_thesis=["Efficient acquisition — 12.9x MER"],
        key_risks=["Profit is entirely non-recurring income"],
        data_completeness_pct=62, confidence="Medium",
        data_gaps=["Prior-year P&L"],
        valuation={"primary_basis": "blended", "ev_low_cents": 19_000_000, "ev_high_cents": 90_000_000,
                   "rev_multiple_low": 0.7, "rev_multiple_high": 1.8, "rev_ev_low_cents": 45_000_000,
                   "rev_ev_high_cents": 116_000_000, "earnings_basis_label": "Reported net earnings",
                   "confidence": "Medium", "caveats": ["Indicative only — not a formal valuation."]},
        account_mappings={"net_revenue_cents": {"sources": ["Sales - Shopify", "Sales - Amazon"], "confidence": "high"}},
        unmapped_accounts=["Suspense account"], classifier_model="claude-haiku-4-5-20251001",
        brand_tagline="Comfort, redefined", logo_data_uri="data:image/png;base64,iVBORw0KGgo=",
        executive_summary="Luxmery is a DTC brand with efficient acquisition but thin margins.",
        stands_out=["MER is strong"], recommendation="Proceed with Caution",
    )


class Sh007PageTests(unittest.TestCase):
    def test_full_report_renders_all_sections(self) -> None:
        html = render_share_page(_full_report())
        self.assertIn("<!doctype html>", html)
        self.assertIn("Luxmery", html)
        self.assertIn("Comfort, redefined", html)
        for needle in ("Executive Summary", "Investment thesis", "Indicative valuation",
                       "Financial overview", "Category benchmarks", "Red flags",
                       "Weighted scorecard", "Data completeness", "Data provenance",
                       "chart.js@4.4.0", "yoyChart", "radarChart", "mediaChart", "monthlyChart"):
            self.assertIn(needle, html, f"missing: {needle}")

    def test_embedded_chart_data_is_valid_json(self) -> None:
        html = render_share_page(_full_report())
        m = re.search(r"window\.__BA = (\{.*?\});", html)
        self.assertIsNotNone(m)
        data = json.loads(m.group(1))
        self.assertEqual(data["yoy"]["labels"][0], "Net revenue")
        self.assertEqual(len(data["media"]["values"]), 2)
        self.assertEqual(len(data["monthly"]["values"]), 6)

    def test_logo_embedded_when_present(self) -> None:
        html = render_share_page(_full_report())
        self.assertIn('class="cover-logo"', html)
        self.assertIn("data:image/png;base64", html)

    def test_sparse_report_does_not_crash(self) -> None:
        sparse = BrandReport(brand="Unknown", prepared_date="2026-06-16",
                             scorecard=Scorecard(score_100=0, letter="F"))
        html = render_share_page(sparse)
        self.assertIn("Unknown", html)
        self.assertIn("Insufficient established financials", html)
        # No media/monthly canvas elements when there's no data (the JS guards
        # still reference the ids, so check for the canvas element specifically).
        self.assertNotIn('id="mediaChart"', html)
        self.assertNotIn('id="monthlyChart"', html)

    def test_noindex_for_public_share(self) -> None:
        html = render_share_page(_full_report())
        self.assertIn('name="robots" content="noindex, nofollow"', html)
        self.assertIn("Confidential", html)


if __name__ == "__main__":
    unittest.main()
