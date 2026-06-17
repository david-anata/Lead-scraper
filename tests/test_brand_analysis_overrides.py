"""Analyst overrides (correct a mis-parsed number) + version history snapshots."""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/ba_overrides_test.db")

from sales_support_agent.services.brand_analysis.report import build_report, _apply_overrides
from sales_support_agent.services.brand_analysis.schema import PeriodFinancials

try:
    from sales_support_agent.services.brand_analysis import storage
    from sales_support_agent.services.brand_analysis.schema import BrandReport, Scorecard
    from sales_support_agent.models.database import create_session_factory, init_database
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy"}:
        raise
    DEPS = False


class OverrideTests(unittest.TestCase):
    def test_apply_overrides_sets_cents_from_dollars(self) -> None:
        p = PeriodFinancials()
        mapped: dict = {}
        _apply_overrides(p, {"net_revenue_cents": "656,075", "cogs_cents": "$400000"}, mapped)
        self.assertEqual(p.net_revenue_cents, 65_607_500)
        self.assertEqual(p.cogs_cents, 40_000_000)
        self.assertEqual(mapped["net_revenue_cents"]["confidence"], "override")

    def test_override_fills_misparsed_revenue(self) -> None:
        # P&L with no recognizable revenue row → parsed None → override rescues it.
        pnl = b"Line,FY2025\nCOGS,400000\nNet Income,90000\n"
        r = build_report([("p.csv", pnl)], brand="X", use_llm=False,
                         overrides={"net_revenue_cents": "656075"})
        self.assertEqual(r.current.net_revenue_cents, 65_607_500)
        self.assertEqual(r.overrides, {"net_revenue_cents": "656075"})

    def test_override_ignores_unknown_field_and_junk(self) -> None:
        p = PeriodFinancials()
        _apply_overrides(p, {"not_a_field": "5", "cogs_cents": "abc"}, {})
        self.assertIsNone(p.cogs_cents)


@unittest.skipUnless(DEPS, "sqlalchemy required")
class VersionHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db = os.path.join(tempfile.gettempdir(), "ba_overrides_isolated.db")
        if os.path.exists(db):
            os.remove(db)
        init_database(create_session_factory("sqlite:///" + db))

    def _report(self, letter, score):
        return BrandReport(brand="Ev", scorecard=Scorecard(letter=letter, score_100=score))

    def test_rerun_snapshots_prior_grade(self) -> None:
        rid = storage.save_report(self._report("D", 55))
        self.assertEqual(storage.list_versions(rid), [])  # nothing before first rerun
        storage.update_report(rid, self._report("C", 72))  # rerun 1
        storage.update_report(rid, self._report("B", 83))  # rerun 2
        versions = storage.list_versions(rid)
        # Two snapshots captured (the D and the C, before each overwrite).
        self.assertEqual([v["grade"] for v in versions], ["D", "C"])
        self.assertEqual([v["score_100"] for v in versions], [55, 72])


if __name__ == "__main__":
    unittest.main()
