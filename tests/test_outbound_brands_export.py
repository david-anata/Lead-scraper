"""The one brand download.

Every export before 2026-08-17 went out through a mode that pulled fresh from
StoreLeads, so all 218 brands reached Clay with empty amz_* columns and the send
gate blocked every one. That mode is gone; these lock the replacement.
"""

from __future__ import annotations

import unittest

try:
    from sales_support_agent.api import outbound_router as router_mod

    AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - env without sqlalchemy
    AVAILABLE = False


def _lead(domain: str, *, checked: str = "2026-08-17T07:00:00Z", skipped: str = "",
          situation: str = "rivals_on_name", score: int = 10) -> dict:
    return {
        "domain": domain,
        "brand": domain.split(".")[0],
        "amazon_checked_at": checked,
        "amazon_skipped_reason": skipped,
        "amz_situation": situation,
        "score": score,
        "reason": "A few competitors come up on Amazon.",
    }


@unittest.skipUnless(AVAILABLE, "sqlalchemy not installed")
class BrandsExportTests(unittest.TestCase):
    def _export(self, held: list[dict], *, max_new: int = 200):
        """Run the real handler with the store and CSV writer stubbed."""
        captured: dict[str, object] = {}

        def fake_leads_to_csv(leads):
            captured["domains"] = [l["domain"] for l in leads]
            return "csv"

        import outbound_pipeline as _op
        from sales_support_agent.models import database as _db
        from sales_support_agent.services import outbound_memory

        real_csv = _op.leads_to_csv
        real_load = outbound_memory.load_leads
        real_engine = _db.get_engine
        _op.leads_to_csv = fake_leads_to_csv
        outbound_memory.load_leads = lambda engine, limit=None: list(held)
        _db.get_engine = lambda: object()  # any non-None engine
        try:
            response = router_mod.outbound_brands_csv(request=None, max_new=max_new)
        finally:
            _op.leads_to_csv = real_csv
            outbound_memory.load_leads = real_load
            _db.get_engine = real_engine
        return captured.get("domains", []), response

    def test_only_amazon_checked_brands_with_a_finding_are_exported(self) -> None:
        domains, response = self._export([
            _lead("ready.com"),
            _lead("never-checked.com", checked=""),
            _lead("skipped.com", skipped="no marketplace configured"),
            _lead("nothing-found.com", situation=""),
        ])

        self.assertEqual(domains, ["ready.com"])
        self.assertEqual(response.media_type, "text/csv")
        self.assertIn("anata_brands_ready_for_clay.csv", response.headers["content-disposition"])

    def test_export_is_ordered_by_score_so_the_best_brands_lead(self) -> None:
        domains, _ = self._export(
            [_lead("low.com", score=2), _lead("high.com", score=99), _lead("mid.com", score=40)]
        )
        self.assertEqual(domains, ["high.com", "mid.com", "low.com"])

    def test_an_empty_store_returns_an_empty_file_rather_than_falling_back(self) -> None:
        """The old code answered an empty store with a live StoreLeads pull.

        That is the exact path that produced 218 unusable brands, so an empty
        store must now produce an empty file.
        """
        domains, response = self._export([])
        self.assertEqual(domains, [])
        self.assertEqual(response.media_type, "text/csv")

    def test_the_raw_storeleads_download_mode_is_gone(self) -> None:
        """A recipe or scanned argument must not resurrect the old behaviour."""
        import inspect

        params = set(inspect.signature(router_mod.outbound_brands_csv).parameters)
        self.assertNotIn("scanned", params)
        self.assertNotIn("recipe", params)


if __name__ == "__main__":
    unittest.main()
