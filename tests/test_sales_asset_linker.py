"""Unit tests for the asset auto-linker: name normalisation and deal matching."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_asset_linker_test.db",
)

from sales_support_agent.models.database import (  # noqa: E402
    create_session_factory,
    init_database,
    session_scope,
)
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotCompany,
    HubSpotDeal,
    SalesDealAsset,
)
from sales_support_agent.services.sales.asset_linker import (  # noqa: E402
    _normalize,
    try_link_rate_sheet,
)

_DB_URL = os.environ["SALES_AGENT_DB_URL"]


def _make_factory():
    factory = create_session_factory(_DB_URL)
    init_database(factory)
    return factory


class TestNormalize(unittest.TestCase):
    def test_lowercase(self):
        self.assertEqual(_normalize("Acme"), "acme")

    def test_strips_inc(self):
        self.assertEqual(_normalize("Acme Inc."), "acme")

    def test_strips_llc(self):
        self.assertEqual(_normalize("Globex LLC"), "globex")

    def test_strips_punctuation(self):
        self.assertEqual(_normalize("Acme — Fulfillment"), "acme fulfillment")

    def test_collapses_spaces(self):
        self.assertEqual(_normalize("  Acme   Corp  "), "acme")


class TestTryLinkRateSheet(unittest.TestCase):
    factory = None

    @classmethod
    def setUpClass(cls):
        cls.factory = _make_factory()
        with session_scope(cls.factory) as s:
            for row in s.query(SalesDealAsset).all():
                s.delete(row)
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            for row in s.query(HubSpotCompany).all():
                s.delete(row)
            s.flush()
            s.add(HubSpotCompany(hubspot_company_id="co1", name="Acme Inc", domain="acme.com"))
            s.add(HubSpotDeal(
                hubspot_deal_id="deal1", deal_name="Acme Inc — Fulfillment",
                deal_stage="appointmentscheduled", amount_cents=100_000,
                close_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
                hubspot_company_id="co1", is_closed=False,
            ))
            s.add(HubSpotDeal(
                hubspot_deal_id="deal2", deal_name="Globex Services",
                deal_stage="appointmentscheduled", amount_cents=50_000,
                close_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
                is_closed=False,
            ))
            # Closed deal — must never match.
            s.add(HubSpotDeal(
                hubspot_deal_id="deal3", deal_name="Acme Inc — Won",
                deal_stage="closedwon", amount_cents=200_000,
                close_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                is_closed=True, is_won=True,
            ))

    def _link(self, brand, run_id=42, url="/rate-sheets/acme/42/tok"):
        with session_scope(self.factory) as s:
            result = try_link_rate_sheet(s, brand_name=brand, run_id=run_id, url=url)
            s.commit()
        return result

    def test_exact_brand_name_matches(self):
        self.assertEqual(self._link("Acme Inc"), "deal1")

    def test_partial_brand_contained_in_deal_name(self):
        self.assertEqual(self._link("Acme"), "deal1")

    def test_different_company_does_not_match_wrong_deal(self):
        result = self._link("Globex")
        self.assertEqual(result, "deal2")

    def test_closed_deal_not_matched(self):
        # Acme Inc — Won is closed; should match deal1 (open), not deal3.
        result = self._link("Acme Inc", run_id=99, url="/rate-sheets/acme/99/tok")
        self.assertNotEqual(result, "deal3")

    def test_no_match_returns_none(self):
        result = self._link("UnknownCorp XYZ", run_id=10)
        self.assertIsNone(result)

    def test_empty_brand_returns_none(self):
        result = self._link("", run_id=11)
        self.assertIsNone(result)

    def test_asset_row_created_in_db(self):
        self._link("Acme Inc", run_id=55, url="/rate-sheets/acme/55/tok")
        with session_scope(self.factory) as s:
            asset = (
                s.query(SalesDealAsset)
                .filter_by(hubspot_deal_id="deal1", asset_type="rate_sheet", run_id="55")
                .first()
            )
        self.assertIsNotNone(asset)
        self.assertEqual(asset.url, "/rate-sheets/acme/55/tok")

    def test_duplicate_link_updates_url(self):
        self._link("Acme Inc", run_id=77, url="/rate-sheets/acme/77/v1")
        self._link("Acme Inc", run_id=77, url="/rate-sheets/acme/77/v2")
        with session_scope(self.factory) as s:
            asset = (
                s.query(SalesDealAsset)
                .filter_by(hubspot_deal_id="deal1", asset_type="rate_sheet", run_id="77")
                .first()
            )
        self.assertEqual(asset.url, "/rate-sheets/acme/77/v2")

    def test_short_brand_not_a_word_in_deal_does_not_false_match(self):
        # 'ace' normalizes to 'ace'; 'Space Corp' normalizes to 'space' (corp stripped).
        # Word-set matching: {'ace'} is not a subset of {'space'} → no match.
        # This is a regression test for the bare-substring false-positive.
        with session_scope(self.factory) as s:
            s.add(HubSpotDeal(
                hubspot_deal_id="deal9", deal_name="Space Corp",
                deal_stage="appointmentscheduled", amount_cents=0,
                is_closed=False,
            ))
            s.commit()
        try:
            result = self._link("Ace", run_id=88)
            self.assertIsNone(result)
        finally:
            with session_scope(self.factory) as s:
                d = s.get(HubSpotDeal, "deal9")
                if d:
                    s.delete(d)
                s.commit()


if __name__ == "__main__":
    unittest.main()
