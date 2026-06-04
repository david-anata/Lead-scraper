"""Tests for the auto-detecting mass-upload intake (sniff + route)."""

from __future__ import annotations

import io
import unittest

import openpyxl

from sales_support_agent.services.advertising import intake as I


def _bulk_xlsx() -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sponsored Products Campaigns"
    ws.append(["Product", "Entity", "Operation", "Keyword Text", "Bid", "Impressions", "Clicks", "Spend"])
    ws.append(["Sponsored Products", "Keyword", "", "widget", 1.0, 100, 5, 5.0])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


_SEARCH_TERM = b"Campaign Name,Customer Search Term,Impressions,Clicks,Spend,7 Day Total Orders (#)\nC,blue,100,5,5,0\n"
_BUSINESS = b"(Child) ASIN,Sessions - Total,Units Ordered,Ordered Product Sales\nB001,900,22,$320\n"
_SQP = b"Search Query,Search Query Volume,Impressions: Total Count,Purchases: ASIN Share %\nblue,1000,5000,20%\n"
_DSP = b"Campaign Name,Impressions,Clicks,Total Cost,Total Sales\nBrand,9000,10,$300,$1500\n"
_EXTERNAL = b"Channel,Amount,Note\nMeta,$1000,prospecting\n"


class SniffTest(unittest.TestCase):
    def test_bulk_xlsx(self):
        self.assertEqual(I.sniff_kind("MyBulk.xlsx", _bulk_xlsx()), I.KIND_BULK)

    def test_search_term(self):
        self.assertEqual(I.sniff_kind("st.csv", _SEARCH_TERM), I.KIND_SEARCH_TERM)

    def test_business_report(self):
        self.assertEqual(I.sniff_kind("BusinessReport.csv", _BUSINESS), I.KIND_BUSINESS)

    def test_sqp(self):
        self.assertEqual(I.sniff_kind("sqp.csv", _SQP), I.KIND_SQP)

    def test_dsp(self):
        self.assertEqual(I.sniff_kind("dsp.csv", _DSP), I.KIND_DSP)

    def test_external(self):
        self.assertEqual(I.sniff_kind("spend.csv", _EXTERNAL), I.KIND_EXTERNAL)

    def test_unknown(self):
        self.assertEqual(I.sniff_kind("notes.csv", b"foo,bar\n1,2\n"), I.KIND_UNKNOWN)

    def test_empty(self):
        self.assertEqual(I.sniff_kind("x.csv", b""), I.KIND_UNKNOWN)


class RouteTest(unittest.TestCase):
    def test_routes_mixed_batch(self):
        inputs, report = I.route_files([
            ("bulk.xlsx", _bulk_xlsx()),
            ("st.csv", _SEARCH_TERM),
            ("biz.csv", _BUSINESS),
            ("random.csv", b"a,b\n1,2\n"),
        ])
        self.assertIsNotNone(inputs.bulk_xlsx)
        self.assertIsNotNone(inputs.search_term_csv)
        self.assertIsNotNone(inputs.business_report_csv)
        self.assertIn(I.KIND_BULK, report.detected)
        self.assertEqual(report.ignored, ["random.csv"])
        self.assertEqual(report.missing_core(), [])  # all 3 core present

    def test_missing_core_reported(self):
        _, report = I.route_files([("st.csv", _SEARCH_TERM)])
        missing = report.missing_core()
        self.assertIn(I.KIND_BULK, missing)
        self.assertIn(I.KIND_BUSINESS, missing)

    def test_duplicate_search_terms_merged(self):
        extra = b"Campaign Name,Customer Search Term,Impressions,Clicks,Spend,7 Day Total Orders (#)\nC,red,50,2,2,0\n"
        inputs, _ = I.route_files([("a.csv", _SEARCH_TERM), ("b.csv", extra)])
        from sales_support_agent.services.advertising.normalizers import normalize_search_term_csv
        rows = normalize_search_term_csv(inputs.search_term_csv)
        terms = {r.entity_text for r in rows}
        self.assertEqual(terms, {"blue", "red"})  # both files' rows present

    def test_second_bulk_file_ignored(self):
        inputs, report = I.route_files([("a.xlsx", _bulk_xlsx()), ("b.xlsx", _bulk_xlsx())])
        self.assertIsNotNone(inputs.bulk_xlsx)
        self.assertIn("b.xlsx", report.ignored)

    def test_summary_human_readable(self):
        _, report = I.route_files([("bulk.xlsx", _bulk_xlsx()), ("st.csv", _SEARCH_TERM)])
        s = report.summary()
        self.assertIn("Detected", s)
        self.assertIn("Missing", s)  # business report still missing


if __name__ == "__main__":
    unittest.main()
