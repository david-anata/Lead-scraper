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


if __name__ == "__main__":
    unittest.main()
