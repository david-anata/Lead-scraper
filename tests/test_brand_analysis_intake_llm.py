"""LLM-assisted intake classifier — gap-fill on trial-balance / GL dumps.

The model call itself is stubbed (no network); these tests pin the trigger,
the dollar→cents + sign normalisation, the gap-fill merge (deterministic wins
where present), and the end-to-end parse_dump wiring that fixes the real
"revenue shows missing while COGS is found" bug.
"""

from __future__ import annotations

import unittest

from sales_support_agent.services.brand_analysis import intake as intake_mod
from sales_support_agent.services.brand_analysis import intake_llm as L
from sales_support_agent.services.brand_analysis.schema import PeriodFinancials


class ClassifierLogicTests(unittest.TestCase):
    def test_should_classify_triggers_on_missing_material_field(self) -> None:
        p = PeriodFinancials()
        p.cogs_cents = 59183500  # COGS found but revenue/marketing/opex absent
        self.assertTrue(L.should_classify(p))
        p.net_revenue_cents = 65607500
        p.marketing_total_cents = 5000000
        p.opex_cents = 2000000
        self.assertFalse(L.should_classify(p))

    def test_clean_period_normalises_dollars_and_sign(self) -> None:
        cleaned = L._clean_period({
            "net_revenue_cents": -656075,   # credit shown negative -> positive cents
            "net_earnings_cents": -10951,   # loss keeps its sign
            "cogs_cents": 591835,
            "not_a_bucket": 99,
        })
        self.assertEqual(cleaned["net_revenue_cents"], 65607500)
        self.assertEqual(cleaned["net_earnings_cents"], -1095100)
        self.assertEqual(cleaned["cogs_cents"], 59183500)
        self.assertNotIn("not_a_bucket", cleaned)

    def test_merge_fills_gaps_without_overwriting_deterministic(self) -> None:
        period = PeriodFinancials()
        period.cogs_cents = 59183500  # exact deterministic value
        result = L.ClassificationResult(
            current={"net_revenue_cents": 65607500, "cogs_cents": 99999999},
            provenance={"net_revenue_cents": ["Sales - Shopify", "Sales - Amazon"]},
            confidence={"net_revenue_cents": "high"},
        )
        mapped: dict = {}
        L.merge_into(period, result, mapped)
        self.assertEqual(period.net_revenue_cents, 65607500)   # gap filled
        self.assertEqual(period.cogs_cents, 59183500)          # NOT overwritten
        self.assertEqual(mapped["net_revenue_cents"]["sources"], ["Sales - Shopify", "Sales - Amazon"])

    def test_merge_rolls_channels_into_total(self) -> None:
        period = PeriodFinancials()
        result = L.ClassificationResult(
            current={},
            marketing_by_channel={"meta": 4000000, "google": 1000000},
        )
        L.merge_into(period, result, {})
        self.assertEqual(period.marketing_by_channel, {"meta": 4000000, "google": 1000000})
        self.assertEqual(period.marketing_total_cents, 5000000)

    def test_serialise_rows_emits_label_value_pairs(self) -> None:
        t = intake_mod._Table(
            source="tb.xlsx::Sheet1",
            header=["Account", "Debit", "Credit"],
            rows=[["Account", "Debit", "Credit"],
                  ["Sales - Shopify", "", "420,000"],
                  ["Cost of Goods Sold", "591,835", ""]],
        )
        body = L._serialise_rows([t])
        self.assertIn("Sales - Shopify :: 420,000", body)
        self.assertIn("Cost of Goods Sold :: 591,835", body)


class ParseDumpWiringTests(unittest.TestCase):
    """parse_dump should call the classifier when material fields are missing
    and merge its result — turning a trial balance into populated buckets."""

    def _trial_balance_csv(self) -> bytes:
        # A trial balance: granular GL accounts, NO single "Net Revenue" row.
        return (
            "Account,Amount\n"
            "Sales - Shopify,420000\n"
            "Sales - Amazon,236075\n"
            "Refunds,-12000\n"
            "Cost of Goods Sold,591835\n"
            "Marketing - Meta,40000\n"
            "Marketing - Google,10000\n"
            "Office & Admin,22000\n"
            "Net Loss,-10951\n"
        ).encode()

    def test_classifier_fills_revenue_gap(self) -> None:
        captured = {}

        def fake_classify(tables, **kwargs):
            captured["called"] = True
            return L.ClassificationResult(
                current={
                    "net_revenue_cents": 64407500,   # 656,075 - 12,000 refunds, dollars*100
                    "cogs_cents": 59183500,
                    "marketing_total_cents": 5000000,
                    "opex_cents": 2200000,
                    "net_earnings_cents": -1095100,
                },
                marketing_by_channel={"meta": 4000000, "google": 1000000},
                provenance={"net_revenue_cents": ["Sales - Shopify", "Sales - Amazon", "Refunds"]},
                confidence={"net_revenue_cents": "high"},
                unmapped=[],
                model="claude-haiku-4-5-20251001",
            )

        orig = L.classify
        L.classify = fake_classify
        try:
            res = intake_mod.parse_dump([("doggyvers-trial-balance.csv", self._trial_balance_csv())])
        finally:
            L.classify = orig

        self.assertTrue(captured.get("called"), "classifier should run when revenue is missing")
        # The bug we fixed: revenue is now populated, not None.
        self.assertEqual(res.current.net_revenue_cents, 64407500)
        self.assertEqual(res.current.marketing_total_cents, 5000000)
        self.assertEqual(res.current.marketing_by_channel, {"meta": 4000000, "google": 1000000})
        self.assertIn("net_revenue_cents", res.account_mappings)
        self.assertEqual(res.classifier_model, "claude-haiku-4-5-20251001")

    def test_clean_pnl_skips_classifier(self) -> None:
        # A clean P&L the deterministic matcher fully handles — no LLM needed.
        clean = (
            "Line,FY2024\n"
            "Net Revenue,1000000\n"
            "COGS,400000\n"
            "Total Marketing,250000\n"
            "Operating Expenses,200000\n"
            "Net Income,90000\n"
        ).encode()
        calls = {"n": 0}

        def fake_classify(tables, **kwargs):
            calls["n"] += 1
            return None

        orig = L.classify
        L.classify = fake_classify
        try:
            res = intake_mod.parse_dump([("clean-pnl.csv", clean)])
        finally:
            L.classify = orig
        self.assertEqual(calls["n"], 0, "clean P&L should not invoke the classifier")
        self.assertEqual(res.current.net_revenue_cents, 100000000)

    def test_use_llm_false_disables_classifier(self) -> None:
        calls = {"n": 0}

        def fake_classify(tables, **kwargs):
            calls["n"] += 1
            return None

        orig = L.classify
        L.classify = fake_classify
        try:
            res = intake_mod.parse_dump(
                [("tb.csv", self._trial_balance_csv())], use_llm=False)
        finally:
            L.classify = orig
        self.assertEqual(calls["n"], 0)
        # Without the classifier, revenue stays missing (the old behaviour).
        self.assertIsNone(res.current.net_revenue_cents)


class CrossFilePeriodTests(unittest.TestCase):
    """Prior-year data living in a SEPARATE file (no year-labelled columns)
    must be recognised for YoY — the real Doggyvers case."""

    def test_year_of_file_from_filename_and_title(self) -> None:
        t = intake_mod._Table(source="x", header=["Profit and Loss"],
                              rows=[["January-December, 2024"], ["Net Revenue", "500000"]])
        self.assertEqual(intake_mod._year_of_file("Financials 2024 V3.xlsx", [t]), 2024)
        self.assertEqual(intake_mod._year_of_file("no-year.xlsx", [t]), 2024)  # from title row
        self.assertIsNone(intake_mod._year_of_file(
            "plain.xlsx", [intake_mod._Table(source="y", rows=[["Net Revenue", "1"]])]))

    def test_separate_year_files_split_current_and_prior(self) -> None:
        cur = b"Line,Amount\nNet Revenue,1000000\nCOGS,400000\nNet Income,90000\n"
        prr = b"Line,Amount\nNet Revenue,800000\nCOGS,330000\nNet Income,60000\n"
        res = intake_mod.parse_dump(
            [("Brand Financials 2025.csv", cur), ("Brand Financials 2024.csv", prr)],
            use_llm=False)
        self.assertTrue(res.has_yoy)
        self.assertEqual(res.current.period_label, "FY 2025")
        self.assertEqual(res.prior.period_label, "FY 2024")
        self.assertEqual(res.current.net_revenue_cents, 100_000_000)
        self.assertEqual(res.prior.net_revenue_cents, 80_000_000)  # routed to prior, not current

    def test_classifier_group_serialisation_marks_years(self) -> None:
        t25 = intake_mod._Table(source="pl25", rows=[["400011 Sales", "2280930"]])
        t24 = intake_mod._Table(source="pl24", rows=[["Stripe - Sales", "4623600"]])
        body = L._serialise_groups([("pl-2025.xlsx", 2025, [t25]), ("fin-2024.xlsx", 2024, [t24])])
        self.assertIn("fiscal year 2025", body)
        self.assertIn("fiscal year 2024", body)
        self.assertIn("400011 Sales :: 2280930", body)


class IncomeTotalFallbackTests(unittest.TestCase):
    """Deterministic revenue from a QBO/Xero P&L: numbered income accounts with
    no 'Net Revenue' row, but a 'Total for Income' line (100%-of-income base)."""

    def _pnl_table(self):
        # Mirrors the real Doggyvers P&L: nested totals + a %-of-income column.
        return intake_mod._Table(source="pnl.xlsx::Sheet1", header=["", "Total", "% of Income"], rows=[
            ["Income", "", ""],
            ["400011 Sales", "2280930.47", "203.5"],
            ["Total for Income", "1169300.83", "104.3"],   # inner subtotal
            ["Uncategorised Income", "546.08", "0.04"],
            ["Total for Income", "1120483.26", "100"],       # outer total = 100% base
            ["Total for Cost of Sales", "591835.18", "52.8"],
            ["Total for Other Income(Loss)", "7825.97", "0.7"],
        ])

    def test_income_total_picks_the_100pct_base(self) -> None:
        self.assertEqual(intake_mod._income_total(self._pnl_table()), 112_048_326)

    def test_excludes_other_income_and_cost(self) -> None:
        # Only an "other income" total present -> not treated as revenue.
        t = intake_mod._Table(source="pnl.xlsx::Sheet1", rows=[
            ["Total for Other Income(Loss)", "7825.97"],
            ["Total for Cost of Sales", "591835.18"]])
        self.assertIsNone(intake_mod._income_total(t))

    def test_parse_dump_fills_revenue_from_income_total(self) -> None:
        csv = ("Account,Total,% of Income\n"
               "400011 Sales,2280930,203\n"
               "Total for Income,1120483,100\n"
               "Total for Cost of Sales,591835,52\n").encode()
        res = intake_mod.parse_dump([("Brand Profit and Loss 2025.csv", csv)], use_llm=False)
        self.assertEqual(res.current.net_revenue_cents, 112_048_300)


class MarketingNoiseGuardTests(unittest.TestCase):
    """A tiny mis-matched 'marketing' GL line shouldn't imply an absurd MER and
    a falsely-good grade — treat it as not-supplied (N/A)."""

    def test_implausible_marketing_is_cleared(self) -> None:
        csv = ("Account,Total,% of Income\n"
               "Total for Income,1000000,100\n"
               "Total for Cost of Sales,400000,40\n"
               "Marketing - misc,161,0\n").encode()
        res = intake_mod.parse_dump([("Brand Profit and Loss 2025.csv", csv)], use_llm=False)
        self.assertEqual(res.current.net_revenue_cents, 100_000_000)
        self.assertIsNone(res.current.marketing_total_cents)  # $161 vs $1M -> noise
        self.assertEqual(res.current.marketing_by_channel, {})
        self.assertTrue(any("implausibly small" in n for n in res.notes))

    def test_plausible_marketing_is_kept(self) -> None:
        csv = ("Account,Total,% of Income\n"
               "Total for Income,1000000,100\n"
               "Total Marketing,250000,25\n").encode()
        res = intake_mod.parse_dump([("Brand Profit and Loss 2025.csv", csv)], use_llm=False)
        self.assertEqual(res.current.marketing_total_cents, 25_000_000)  # 25% of rev -> kept


class DocTriageTests(unittest.TestCase):
    """Transaction-level General Ledger sheets are kept OUT of scoring; summary
    statements (P&L, Trial Balance, Balance Sheet) are kept."""

    def test_table_doc_type_classification(self) -> None:
        def tbl(src, rows=1):
            return intake_mod._Table(source=src, rows=[["x", "1"]] * rows)
        self.assertEqual(intake_mod._table_doc_type("General Ledger 2025.xlsx", tbl("GL::Sheet1")), "general_ledger")
        self.assertEqual(intake_mod._table_doc_type("x.xlsx", tbl("x.xlsx::Trial Balance")), "trial_balance")
        self.assertEqual(intake_mod._table_doc_type("x.xlsx", tbl("x.xlsx::BS")), "balance_sheet")
        self.assertEqual(intake_mod._table_doc_type("Profit and Loss.xlsx", tbl("x::Sheet1")), "pnl")
        # Safety net: a huge unnamed table is treated as a transaction dump.
        self.assertEqual(intake_mod._table_doc_type("mystery.xlsx", tbl("mystery::Sheet1", rows=2500)), "general_ledger")

    def test_general_ledger_excluded_from_scoring(self) -> None:
        pnl = b"Line,Amount\nNet Revenue,1000000\nCOGS,400000\nNet Income,90000\n"
        # A 'general ledger' file with a revenue-looking line that must NOT be scored.
        gl = "Date,Account,Amount\n" + "\n".join(
            f"2025-01-{i:02d},Some Txn,{i}" for i in range(1, 13)) + "\n"
        res = intake_mod.parse_dump(
            [("Brand P&L 2025.csv", pnl), ("Brand General Ledger 2025.csv", gl.encode())],
            use_llm=False)
        self.assertTrue(any("Excluded" in n and "Ledger" in n for n in res.notes))
        self.assertEqual(res.current.net_revenue_cents, 100_000_000)  # from the P&L, GL ignored


if __name__ == "__main__":
    unittest.main()
