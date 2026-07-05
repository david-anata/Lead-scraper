"""Tests for sales/fulfillment_linker.py."""

from __future__ import annotations

import os
import unittest

from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import AutomationRun, HubSpotDeal, SalesDealAsset
from sales_support_agent.services.sales.fulfillment_linker import sync_fulfillment_links


def _make_deal(session, did, name, company_id=""):
    d = HubSpotDeal(
        hubspot_deal_id=did,
        deal_name=name,
        amount_cents=10000,
        deal_stage="qualifiedtobuy",
        deal_stage_label="Qualified",
        pipeline="default",
        owner_email="rep@ex.com",
        is_closed=False,
        is_won=False,
        hubspot_company_id=company_id,
    )
    session.add(d)


def _make_run(session, run_id, prospect, view_path, hubspot_deal_id=""):
    summary = {"prospect": prospect, "view_path": view_path}
    if hubspot_deal_id:
        summary["hubspot_deal_id"] = hubspot_deal_id
    run = AutomationRun(
        id=run_id,
        run_type="fulfillment_rate_sheet",
        status="completed",
        summary_json=summary,
    )
    session.add(run)


class TestSyncFulfillmentLinks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for m in (SalesDealAsset, AutomationRun, HubSpotDeal):
                for row in s.query(m).all():
                    s.delete(row)
            s.flush()

    def test_explicit_deal_id_links_directly(self):
        with session_scope(self.sf) as s:
            _make_deal(s, "d1", "Acme Deal")
            _make_run(s, 101, "Acme", "/rate-sheets/acme/101/tok", hubspot_deal_id="d1")
        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertEqual(counts["linked"], 1)
        with session_scope(self.sf) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="d1", asset_type="rate_sheet").first()
        self.assertIsNotNone(asset)
        self.assertEqual(asset.url, "/rate-sheets/acme/101/tok")

    def test_name_match_links_by_brand(self):
        with session_scope(self.sf) as s:
            _make_deal(s, "d2", "BrandCo Deal")
            _make_run(s, 102, "BrandCo", "/rate-sheets/brandco/102/tok")
        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertGreaterEqual(counts["linked"], 1)
        with session_scope(self.sf) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="d2").first()
        self.assertIsNotNone(asset)

    def test_run_without_view_path_skipped(self):
        with session_scope(self.sf) as s:
            _make_deal(s, "d3", "SkipCo Deal")
            run = AutomationRun(
                id=103, run_type="fulfillment_rate_sheet", status="running",
                summary_json={"prospect": "SkipCo"},
            )
            s.add(run)
        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertEqual(counts["skipped"], 1)
        with session_scope(self.sf) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="d3").first()
        self.assertIsNone(asset)

    def test_no_matching_deal_skipped(self):
        with session_scope(self.sf) as s:
            _make_run(s, 104, "UnknownBrand", "/rate-sheets/unk/104/tok")
        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertEqual(counts["skipped"], 1)

    def test_duplicate_run_refreshes_url(self):
        with session_scope(self.sf) as s:
            _make_deal(s, "d4", "RefreshCo Deal")
            _make_run(s, 105, "RefreshCo", "/rate-sheets/refresh/105/old_tok", hubspot_deal_id="d4")
        with session_scope(self.sf) as s:
            sync_fulfillment_links(s)

        # Update the view_path in the run to simulate a republish.
        with session_scope(self.sf) as s:
            run = s.get(AutomationRun, 105)
            run.summary_json = {"prospect": "RefreshCo", "view_path": "/rate-sheets/refresh/105/new_tok", "hubspot_deal_id": "d4"}

        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertEqual(counts["refreshed"], 1)
        with session_scope(self.sf) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="d4").first()
        self.assertEqual(asset.url, "/rate-sheets/refresh/105/new_tok")

    def test_closed_deal_not_linked(self):
        with session_scope(self.sf) as s:
            d = HubSpotDeal(
                hubspot_deal_id="d5", deal_name="Closed Deal",
                amount_cents=10000, deal_stage="closedwon",
                deal_stage_label="Closed Won", pipeline="default",
                owner_email="rep@ex.com", is_closed=True, is_won=True,
            )
            s.add(d)
            _make_run(s, 106, "Closed", "/rate-sheets/closed/106/tok", hubspot_deal_id="d5")
        with session_scope(self.sf) as s:
            counts = sync_fulfillment_links(s)
        self.assertEqual(counts["linked"], 0)
        with session_scope(self.sf) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="d5").first()
        self.assertIsNone(asset)


if __name__ == "__main__":
    unittest.main()
