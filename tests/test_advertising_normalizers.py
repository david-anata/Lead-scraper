"""Tests for the Amazon CSV/XLSX normalizers (tolerant header mapping)."""

from __future__ import annotations

import io
import unittest

import openpyxl

from sales_support_agent.services.advertising import normalizers as N


def _bulk_xlsx() -> bytes:
    header = ["Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Keyword ID",
              "Campaign Name (Informational only)", "Ad Group Name (Informational only)",
              "Keyword Text", "Match Type", "Bid", "Impressions", "Clicks", "Spend", "Sales", "Orders", "Units"]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sponsored Products Campaigns"
    ws.append(header)

    def row(**k):
        ws.append([k.get(h, "") for h in header])

    row(Entity="Campaign", **{"Campaign ID": "C1", "Campaign Name (Informational only)": "Brand"})
    row(Entity="Ad Group", **{"Campaign ID": "C1", "Ad Group ID": "A1"})
    row(Entity="Keyword", **{"Campaign ID": "C1", "Ad Group ID": "A1", "Keyword ID": "K1",
                             "Campaign Name (Informational only)": "Brand",
                             "Ad Group Name (Informational only)": "AG",
                             "Keyword Text": "widget blue", "Match Type": "exact", "Bid": 1.20,
                             "Impressions": 1000, "Clicks": 40, "Spend": 40.00, "Sales": 20.00,
                             "Orders": 2, "Units": 2})
    # A structural row with no metrics should be skipped.
    row(Entity="Keyword", **{"Campaign ID": "C1", "Ad Group ID": "A1", "Keyword Text": "dead kw",
                             "Match Type": "exact", "Bid": 0.50})
    # A second sheet that isn't an ad-type sheet should be ignored.
    ws2 = wb.create_sheet("Portfolios")
    ws2.append(["Portfolio ID", "Portfolio Name"])
    ws2.append(["P1", "Main"])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class BulkXlsxTest(unittest.TestCase):
    def test_parses_keyword_rows_with_metrics(self):
        rows = N.normalize_bulk_xlsx(_bulk_xlsx())
        self.assertEqual(len(rows), 1)  # only the keyword with performance
        r = rows[0]
        self.assertEqual(r.ad_type, "SP")
        self.assertEqual(r.entity_level, "keyword")
        self.assertEqual(r.entity_text, "widget blue")
        self.assertEqual(r.bid_cents, 120)
        self.assertEqual(r.spend_cents, 4000)
        self.assertEqual(r.sales_cents, 2000)
        self.assertEqual(r.orders, 2)

    def test_bad_bytes_returns_empty(self):
        self.assertEqual(N.normalize_bulk_xlsx(b"not a workbook"), [])


class SearchTermTest(unittest.TestCase):
    CSV = (
        b"Campaign Name,Ad Group Name,Customer Search Term,Match Type,Impressions,Clicks,Spend,"
        b"7 Day Total Sales,7 Day Total Orders (#),7 Day Total Units (#)\n"
        b"Brand,AG,cheap junk,exact,500,25,$50.00,0,0,0\n"
        b"Brand,AG,widget green,exact,300,15,$15.00,$60.00,4,4\n"
    )

    def test_parses_terms(self):
        rows = N.normalize_search_term_csv(self.CSV)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r.entity_level == "search_term" for r in rows))
        green = next(r for r in rows if r.entity_text == "widget green")
        self.assertEqual(green.sales_cents, 6000)
        self.assertEqual(green.orders, 4)


class BusinessReportTest(unittest.TestCase):
    CSV = (
        b"(Child) ASIN,Title,SKU,Sessions - Total,Page Views - Total,Units Ordered,"
        b"Unit Session Percentage,Featured Offer (Buy Box) Percentage,Ordered Product Sales\n"
        b"B001,Blue Widget,SKU1,\"1,200\",1500,48,4.00%,95.5%,\"$1,920.00\"\n"
    )

    def test_parses_sales(self):
        rows = N.normalize_business_report_csv(self.CSV)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.asin, "B001")
        self.assertEqual(r.sessions, 1200)
        self.assertEqual(r.units, 48)
        self.assertEqual(r.ordered_product_sales_cents, 192000)
        self.assertEqual(r.conversion_bps, 400)
        self.assertEqual(r.buy_box_pct_bps, 9550)


class SqpTest(unittest.TestCase):
    CSV = (
        b"Search Query,Search Query Volume,Impressions: Total Count,Impressions: ASIN Share %,"
        b"Clicks: Total Count,Clicks: ASIN Share %,Purchases: Total Count,Purchases: ASIN Share %\n"
        b"blue widget,10000,50000,12.5%,3000,18.0%,400,22.0%\n"
    )

    def test_parses_share(self):
        rows = N.normalize_sqp_csv(self.CSV)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.search_query, "blue widget")
        self.assertEqual(r.search_query_volume, 10000)
        self.assertEqual(r.impression_share_bps, 1250)
        self.assertEqual(r.purchase_share_bps, 2200)


class DspTest(unittest.TestCase):
    CSV = b"Campaign Name,Impressions,Clicks,Total Cost,Total Sales,Total Orders\nDSP Brand,90000,120,$300.00,$1500.00,10\n"

    def test_parses_dsp(self):
        rows = N.normalize_dsp_csv(self.CSV)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].ad_type, "DSP")
        self.assertEqual(rows[0].spend_cents, 30000)
        self.assertEqual(rows[0].sales_cents, 150000)


class ExternalCostsTest(unittest.TestCase):
    CSV = b"Channel,Amount,Note\nFacebook,$1000.00,prospecting\nInfluencer,$500.00,Q3 deal\nTikTok,250,\n"

    def test_parses_and_maps_channels(self):
        rows = N.normalize_external_costs_csv(self.CSV)
        self.assertEqual(len(rows), 3)
        by_channel = {r.channel: r for r in rows}
        self.assertEqual(by_channel["meta"].amount_cents, 100000)  # Facebook -> meta
        self.assertEqual(by_channel["influencer"].cost_type, "commission")
        self.assertEqual(by_channel["tiktok"].amount_cents, 25000)


class PreambleTest(unittest.TestCase):
    def test_skips_preamble_rows(self):
        csv = (
            b"Detail Page Sales and Traffic\nDownloaded 2026-06-04\n"
            b"(Child) ASIN,Sessions - Total,Units Ordered,Ordered Product Sales\n"
            b"B009,300,10,$100.00\n"
        )
        rows = N.normalize_business_report_csv(csv)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].asin, "B009")


if __name__ == "__main__":
    unittest.main()
