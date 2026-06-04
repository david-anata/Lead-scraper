"""Tests for COGS recognition/break-even and the template-populated apply-sheet."""

from __future__ import annotations

import io
import unittest

import openpyxl

from sales_support_agent.services.advertising import intake as I
from sales_support_agent.services.advertising import normalizers as N
from sales_support_agent.services.advertising.bulk_sheets import build_apply_sheet
from sales_support_agent.services.advertising.deliverable import _asin_scorecard
from sales_support_agent.services.advertising.schema import AdRow, Recommendation, SalesRow


_COGS_CSV = b"ASIN,COGS,FBA Fee,Referral Fee\nB07NXN4F7X,12.50,3.00,2.00\nB0CC6QGY12,9.00,,\n"


class CogsNormalizeTest(unittest.TestCase):
    def test_sniff_cogs(self):
        self.assertEqual(I.sniff_kind("cogs.csv", _COGS_CSV), I.KIND_COGS)

    def test_business_report_not_cogs(self):
        biz = b"(Child) ASIN,Sessions - Total,Units Ordered,Ordered Product Sales\nB1,900,22,$320\n"
        self.assertEqual(I.sniff_kind("biz.csv", biz), I.KIND_BUSINESS)

    def test_ads_report_not_cogs(self):
        ads = b"Campaign name,Search term,Impressions,Clicks,Total cost,Purchases,Sales,Units sold\nC,x,10,2,5,0,0,0\n"
        self.assertEqual(I.sniff_kind("st.csv", ads), I.KIND_ADS_REPORT)

    def test_normalize_sums_fees(self):
        out = N.normalize_cogs_csv(_COGS_CSV)
        self.assertEqual(out["asin"]["B07NXN4F7X"], 1750)  # 12.50 + 3 + 2
        self.assertEqual(out["asin"]["B0CC6QGY12"], 900)


class BreakEvenTest(unittest.TestCase):
    def test_break_even_and_profit_verdict(self):
        ads = [AdRow("SP", "product_ad", campaign_name="C", entity_text="B1",
                     spend_cents=2000000, sales_cents=3100000, orders=100, raw={"Advertised product ID": "B1"})]
        # price = 3,000,000c / 100 units = $300/unit; COGS $90 -> break-even ACoS 70%.
        sales = [SalesRow(asin="B1", title="Widget", units=100, sessions=5000,
                          ordered_product_sales_cents=3000000, conversion_bps=2000)]
        card = _asin_scorecard(ads, sales, 3000, {"asin": {"B1": 9000}, "sku": {}})[0]
        self.assertEqual(card["cogs_cents"], 9000)
        self.assertEqual(card["breakeven_acos_bps"], 7000)  # (300-90)/300
        # ad ACoS = 2,000,000/3,100,000 = 64.5% < 70% break-even -> profitable
        self.assertIn("Profitable", card["verdict"])

    def test_unprofitable_when_acos_over_breakeven(self):
        ads = [AdRow("SP", "product_ad", campaign_name="C", entity_text="B1",
                     spend_cents=2800000, sales_cents=3000000, orders=100, raw={"Advertised product ID": "B1"})]
        sales = [SalesRow(asin="B1", title="Widget", units=100,
                          ordered_product_sales_cents=3000000, conversion_bps=2000)]
        card = _asin_scorecard(ads, sales, 3000, {"asin": {"B1": 9000}, "sku": {}})[0]
        # ACoS 93% > 70% break-even
        self.assertIn("Unprofitable", card["verdict"])


def _rec(action, **bulk):
    bulk["action"] = action
    bulk.setdefault("ad_type", "SP")
    return Recommendation(category="x", title=action, is_bulk_actionable=True, bulk_row=bulk)


class ApplySheetTest(unittest.TestCase):
    def test_populates_template_with_ids(self):
        recs = [
            _rec("create_negative", campaign_id="111", ad_group_id="222",
                 keyword_text="cheap junk", match_type="negative exact", campaign_name="C", ad_group_name="AG"),
            _rec("create_keyword", campaign_id="333", ad_group_id="444",
                 keyword_text="widget green", match_type="exact", new_bid_cents=120, campaign_name="C", ad_group_name="AG"),
        ]
        res = build_apply_sheet(recs)
        self.assertEqual(res.applied, 2)
        wb = openpyxl.load_workbook(io.BytesIO(res.xlsx_bytes))
        ws = wb["Sponsored Products Campaigns"]
        hdr = [c.value for c in ws[1]]
        gi = lambda r, n: r[hdr.index(n)]
        data = [r for r in ws.iter_rows(min_row=2) if gi(r, "Entity").value]
        neg = next(r for r in data if gi(r, "Keyword Text").value == "cheap junk")
        self.assertEqual(gi(neg, "Entity").value, "Negative Keyword")
        self.assertEqual(gi(neg, "Operation").value, "Create")
        self.assertEqual(str(gi(neg, "Campaign ID").value), "111")
        self.assertEqual(gi(neg, "Match Type").value, "negativeExact")
        kw = next(r for r in data if gi(r, "Keyword Text").value == "widget green")
        self.assertEqual(gi(kw, "Entity").value, "Keyword")
        self.assertEqual(gi(kw, "Bid").value, 1.20)

    def test_skips_recs_without_ids(self):
        recs = [_rec("create_negative", keyword_text="x", match_type="negative exact")]
        res = build_apply_sheet(recs)
        self.assertEqual(res.applied, 0)
        self.assertEqual(res.skipped, 1)


if __name__ == "__main__":
    unittest.main()
