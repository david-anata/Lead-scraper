"""Tests for brand focus + the growth-plan workbook deliverable."""

from __future__ import annotations

import io
import unittest

import openpyxl

from sales_support_agent.services.advertising.brand import detect_brand_candidates, filter_by_brand
from sales_support_agent.services.advertising.deliverable import build_growth_plan
from sales_support_agent.services.advertising.schema import AdRow, Goals, SalesRow


def _ad(level, *, campaign="", asin="", text="", spend=0, sales=0, orders=0, clicks=0, ad_type="SP"):
    raw = {"Advertised product ID": asin} if asin else {}
    return AdRow(ad_type=ad_type, entity_level=level, campaign_name=campaign, ad_group_name="",
                entity_text=text or asin, impressions=clicks * 20, clicks=clicks,
                spend_cents=spend, sales_cents=sales, orders=orders, units=orders, raw=raw)


class BrandFilterTest(unittest.TestCase):
    def setUp(self):
        # Zantrex campaign named by ASIN (no brand word), plus a Serovital one.
        self.ads = [
            _ad("product_ad", campaign="P_..._B07NXN4F7X_ATM", asin="B07NXN4F7X", spend=20000, sales=31000, orders=10),
            _ad("search_term", campaign="P_..._B07NXN4F7X_ATM", text="skinny stix", spend=1500, sales=6000, orders=4, clicks=15),
            _ad("product_ad", campaign="Quartile Serovital - NB", asin="B0FN5SCC1T", spend=9000, sales=20000, orders=5),
        ]
        self.sales = [
            SalesRow(asin="B07NXN4F7X", title="Zantrex SkinnyStix Energy Powder", sessions=60000, units=3117,
                     ordered_product_sales_cents=11144718, conversion_bps=519),
            SalesRow(asin="B0FN5SCC1T", title="Serovital SmileCare Toothpaste", sessions=900, units=20,
                     ordered_product_sales_cents=200000),
        ]

    def test_asin_aware_filter_keeps_asin_named_campaigns(self):
        ads, sales = filter_by_brand(self.ads, self.sales, "Zantrex")
        # The product_ad (ASIN match) AND the search term in that campaign are kept,
        # even though neither name contains "Zantrex".
        self.assertEqual(len(ads), 2)
        self.assertEqual(len(sales), 1)
        self.assertEqual(sum(r.spend_cents for r in ads if r.entity_level == "product_ad"), 20000)
        # Serovital is excluded.
        self.assertTrue(all("Serovital" not in r.campaign_name for r in ads))

    def test_blank_brand_returns_all(self):
        ads, sales = filter_by_brand(self.ads, self.sales, "")
        self.assertEqual(len(ads), 3)
        self.assertEqual(len(sales), 2)

    def test_detect_candidates_skips_codes_and_asins(self):
        ads = [
            _ad("campaign", campaign="Quartile Zantrex - Non Branded - Skinnystix"),
            _ad("campaign", campaign="Quartile Zantrex - Branded"),
            _ad("campaign", campaign="P_APRQ8M711018_B07NXN4F7X_ATM"),
        ]
        cands = detect_brand_candidates(ads)
        self.assertIn("Zantrex", cands)
        self.assertNotIn("APRQ8M711018", cands)
        self.assertNotIn("B07NXN4F7X", cands)


class GrowthPlanTest(unittest.TestCase):
    def setUp(self):
        self.ads = [
            _ad("product_ad", campaign="P_B07NXN4F7X", asin="B07NXN4F7X", spend=2000000, sales=3100000, orders=100),
            _ad("search_term", campaign="P_B07NXN4F7X", text="cheap junk", spend=5000, sales=0, orders=0, clicks=25),
            _ad("search_term", campaign="P_B07NXN4F7X", text="skinny stix", spend=1500, sales=6000, orders=4, clicks=15),
        ]
        self.sales = [
            SalesRow(asin="B07NXN4F7X", title="Zantrex SkinnyStix", sessions=60000, units=3117,
                     ordered_product_sales_cents=11144718, conversion_bps=519, buy_box_pct_bps=9513),
        ]
        self.goals = Goals(revenue_target_cents=45000000, acos_target_bps=3000, tacos_target_bps=1800)
        from sales_support_agent.services.advertising.engine import build_recommendations
        self.recs = build_recommendations(self.ads, self.sales, [], [], self.goals)

    def test_workbook_has_all_tabs(self):
        data = build_growth_plan(brand="Zantrex", summary={"total_sales_cents": 11144718, "ad_spend_cents": 2006500,
                                 "tacos_bps": 1800, "acos_bps": 6400, "total_units": 3117, "external_spend_cents": 0,
                                 "blended_tacos_bps": 1800},
                                 recommendations=self.recs, ad_rows=self.ads, sales_rows=self.sales,
                                 goals=self.goals, narrative="Test read.")
        wb = openpyxl.load_workbook(io.BytesIO(data))
        self.assertEqual(wb.sheetnames,
                         ["Exec Brief", "Burn List", "ASIN Scorecard", "Campaign Actions",
                          "Negatives to Add", "Revenue Bridge", "Data Requests"])

    def test_asin_scorecard_joins_org_and_ad(self):
        data = build_growth_plan(brand="Zantrex", summary={"total_sales_cents": 11144718, "ad_spend_cents": 2006500,
                                 "tacos_bps": 1800}, recommendations=self.recs, ad_rows=self.ads,
                                 sales_rows=self.sales, goals=self.goals)
        wb = openpyxl.load_workbook(io.BytesIO(data))
        ws = wb["ASIN Scorecard"]
        rows = list(ws.iter_rows(values_only=True))
        header = next(r for r in rows if r and r[0] == "ASIN")
        data_row = rows[rows.index(header) + 1]
        self.assertIn("ASIN", header)
        self.assertEqual(data_row[0], "B07NXN4F7X")
        # org sales (dollars) and ad spend both present on one row
        self.assertAlmostEqual(data_row[2], 111447.18, places=2)
        self.assertAlmostEqual(data_row[7], 20000.0, places=2)  # ad spend $20,000

    def test_data_requests_flags_cogs_when_absent(self):
        data = build_growth_plan(brand="Zantrex", summary={"total_sales_cents": 1}, recommendations=self.recs,
                                 ad_rows=self.ads, sales_rows=self.sales, goals=self.goals, has_cogs=False)
        wb = openpyxl.load_workbook(io.BytesIO(data))
        text = " ".join(str(v) for row in wb["Data Requests"].iter_rows(values_only=True) for v in row if v)
        self.assertIn("COGS", text)


if __name__ == "__main__":
    unittest.main()
