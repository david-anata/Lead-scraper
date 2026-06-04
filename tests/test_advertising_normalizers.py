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


class NewConsoleReportTest(unittest.TestCase):
    """The real Amazon reporting-console format: Total cost / Purchases / Sales /
    Units sold, with the entity column varying by report type."""

    SEARCH_TERM = (
        b"Budget currency,Date range,Campaign name,Ad group name,Search term,"
        b"Impressions,Clicks,CTR,Total cost,Purchases,Sales,Units sold\n"
        b"USD,\"May 07, 2026 - May 28, 2026\",Quartile Zantrex,AG_P,protein packets for water,"
        b"130,20,15%,12.10,3,90.00,3\n"
    )
    ADVERTISED = (
        b"Budget currency,Campaign name,Ad group name,Advertised product SKU,"
        b"Impressions,Clicks,Total cost,Purchases,Sales,Units sold\n"
        b"USD,Quartile Serovital,AG_P,SV_SmileCare_FBA,600,12,30.00,4,200.00,4\n"
    )
    LEGACY_ADGROUP = (
        b"State,Ad group name,Status,Default bid (USD),Keywords,Products,Impressions,"
        b"Clicks,CTR,Total cost (USD),CPC (USD),Purchases,Sales (USD),ACOS,ROAS\n"
        b"ENABLED,AG_P_B0CC,ENABLED,2,1,2,166,18,0.10,15.06,0.84,10,294.90,0.05,19.58\n"
    )

    def test_search_term_new_console(self):
        rows = N.normalize_ads_report_csv(self.SEARCH_TERM)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.entity_level, "search_term")
        self.assertEqual(r.entity_text, "protein packets for water")
        self.assertEqual(r.spend_cents, 1210)     # "Total cost" 12.10
        self.assertEqual(r.sales_cents, 9000)     # "Sales" 90.00
        self.assertEqual(r.orders, 3)             # "Purchases"
        self.assertEqual(r.units, 3)              # "Units sold"

    def test_advertised_product_level(self):
        rows = N.normalize_ads_report_csv(self.ADVERTISED)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].entity_level, "product_ad")
        self.assertEqual(rows[0].entity_text, "SV_SmileCare_FBA")
        self.assertEqual(rows[0].spend_cents, 3000)
        self.assertEqual(rows[0].orders, 4)

    def test_legacy_adgroup_with_usd_columns(self):
        rows = N.normalize_ads_report_csv(self.LEGACY_ADGROUP)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.entity_level, "ad_group")
        self.assertEqual(r.spend_cents, 1506)     # "Total cost (USD)"
        self.assertEqual(r.sales_cents, 29490)    # "Sales (USD)"
        self.assertEqual(r.bid_cents, 200)        # "Default bid (USD)" 2

    def test_search_term_backcompat_alias(self):
        # Old "Customer Search Term" + "Spend" + "7 Day Total" still parses.
        old = (
            b"Campaign Name,Customer Search Term,Impressions,Clicks,Spend,7 Day Total Sales,7 Day Total Orders (#)\n"
            b"C,cheap junk,500,25,50.00,0,0\n"
        )
        rows = N.normalize_ads_report_csv(old)
        self.assertEqual(rows[0].entity_level, "search_term")
        self.assertEqual(rows[0].spend_cents, 5000)


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
