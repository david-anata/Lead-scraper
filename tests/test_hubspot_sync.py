"""Unit tests for the HubSpot → mirror sync service. Uses a fake client (no
network) against a temp SQLite DB, verifying deal/company/contact/line-item
upserts, cents/date normalization, and deal↔contact link maintenance."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/hubspot_sync_test.db"
)

from sales_support_agent.models.database import (  # noqa: E402
    create_session_factory,
    init_database,
    session_scope,
)
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
)
from sales_support_agent.services.hubspot_sync.service import sync_hubspot_sales  # noqa: E402


def _deal(did, name, amount, stage, closedate, owner="42"):
    return {
        "id": did,
        "properties": {
            "dealname": name,
            "amount": amount,
            "dealstage": stage,
            "pipeline": "default",
            "closedate": closedate,
            "hubspot_owner_id": owner,
            "createdate": "2026-01-01T00:00:00Z",
            "hs_lastmodifieddate": "2026-02-01T00:00:00Z",
            "hs_is_closed": "false",
            "hs_is_closed_won": "false",
        },
    }


class FakeHubSpotClient:
    """Minimal stand-in matching the read + write surface sync_hubspot_sales uses."""

    is_configured = True

    def __init__(self):
        self.updates: list[tuple[str, dict]] = []  # recorded write calls
        self.deals = [
            _deal("1", "Acme Q3", "12000.50", "presentationscheduled", "2026-07-01"),
            _deal("2", "Globex", "5000", "qualifiedtobuy", "2026-06-15"),
        ]
        self.assoc = {
            ("1", "companies"): ["c100"],
            ("1", "contacts"): ["p1", "p2"],
            ("1", "line_items"): ["l1"],
            ("2", "companies"): [],
            ("2", "contacts"): [],
            ("2", "line_items"): [],
        }

    def list_owners(self):
        return [{"id": "42", "email": "rep@anatainc.com"}]

    def deal_stage_labels(self):
        return {
            "presentationscheduled": "Presentation Scheduled",
            "qualifiedtobuy": "Qualified To Buy",
        }

    def iter_deals(self, *, max_records=None):
        yield from self.deals

    def list_associations(self, from_type, from_id, to_type):
        return list(self.assoc.get((from_id, to_type), []))

    def get_line_items(self, ids):
        return [
            {"id": "l1", "properties": {"name": "Pick & Pack", "quantity": "2",
                                        "price": "100.00", "amount": "200.00"}}
        ]

    def update_deal(self, deal_id: str, properties: dict) -> None:
        self.updates.append((deal_id, properties))

    def batch_read(self, object_type, ids, *, properties):
        if object_type == "companies":
            return [{"id": "c100", "properties": {"name": "Acme Inc", "domain": "acme.com"}}]
        if object_type == "contacts":
            return [
                {"id": "p1", "properties": {"firstname": "Sam", "lastname": "Lee",
                                            "email": "sam@acme.com", "associatedcompanyid": "c100"}},
                {"id": "p2", "properties": {"firstname": "Pat", "lastname": "Doe",
                                            "email": "pat@acme.com"}},
            ]
        return []


class HubSpotSyncTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        # Clean slate per test.
        with session_scope(self.sf) as s:
            for model in (HubSpotDeal, HubSpotCompany, HubSpotContact,
                          HubSpotLineItem, HubSpotDealContact):
                for row in s.query(model).all():
                    s.delete(row)

    def _settings(self):
        return SimpleNamespace(hubspot_sales_pipeline_id="")

    def test_sync_populates_all_mirrors(self):
        client = FakeHubSpotClient()
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, self._settings())
        self.assertEqual(result.deals, 2)
        self.assertEqual(result.companies, 1)
        self.assertEqual(result.contacts, 2)
        self.assertEqual(result.line_items, 1)
        self.assertEqual(result.errors, [])

        with session_scope(self.sf) as s:
            d1 = s.get(HubSpotDeal, "1")
            self.assertIsNotNone(d1)
            # Dollars → cents.
            self.assertEqual(d1.amount_cents, 1_200_050)
            self.assertEqual(d1.owner_email, "rep@anatainc.com")
            # Raw stage id resolved to a human label from the pipeline map.
            self.assertEqual(d1.deal_stage, "presentationscheduled")
            self.assertEqual(d1.deal_stage_label, "Presentation Scheduled")
            self.assertEqual(d1.hubspot_company_id, "c100")
            self.assertIsNotNone(d1.close_date)
            self.assertFalse(d1.is_closed)

            li = s.query(HubSpotLineItem).filter_by(hubspot_deal_id="1").all()
            self.assertEqual(len(li), 1)
            self.assertEqual(li[0].unit_price_cents, 10_000)
            self.assertEqual(li[0].amount_cents, 20_000)
            self.assertEqual(li[0].quantity, 2)

            links = s.query(HubSpotDealContact).filter_by(hubspot_deal_id="1").all()
            self.assertEqual({l.hubspot_contact_id for l in links}, {"p1", "p2"})

            company = s.get(HubSpotCompany, "c100")
            self.assertEqual(company.name, "Acme Inc")

    def test_sync_is_idempotent_and_prunes_contact_links(self):
        client = FakeHubSpotClient()
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())
        # Drop a contact association; re-sync should prune the stale link.
        client.assoc[("1", "contacts")] = ["p1"]
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            self.assertEqual(s.query(HubSpotDeal).count(), 2)  # no dupes
            links = s.query(HubSpotDealContact).filter_by(hubspot_deal_id="1").all()
            self.assertEqual({l.hubspot_contact_id for l in links}, {"p1"})

    def test_unconfigured_client_reports_error(self):
        class Unconfigured(FakeHubSpotClient):
            is_configured = False

        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, Unconfigured(), self._settings())
        self.assertFalse(result.as_dict()["ok"])
        self.assertTrue(result.errors)


class TestContactLinkPreservation(unittest.TestCase):
    """Regression: transient contacts fetch error must not wipe existing links."""

    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for model in (HubSpotDeal, HubSpotCompany, HubSpotContact,
                          HubSpotLineItem, HubSpotDealContact):
                for row in s.query(model).all():
                    s.delete(row)

    def _settings(self):
        return SimpleNamespace(hubspot_sales_pipeline_id="")

    def test_contact_fetch_error_does_not_wipe_links(self):
        client = FakeHubSpotClient()
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())
        with session_scope(self.sf) as s:
            links_before = {lnk.hubspot_contact_id for lnk in
                            s.query(HubSpotDealContact).filter_by(hubspot_deal_id="1").all()}
        self.assertEqual(links_before, {"p1", "p2"})

        original_assoc = client.list_associations

        def _flaky(from_type, from_id, to_type):
            if from_id == "1" and to_type == "contacts":
                raise RuntimeError("transient network error")
            return original_assoc(from_type, from_id, to_type)

        client.list_associations = _flaky
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            links_after = {lnk.hubspot_contact_id for lnk in
                           s.query(HubSpotDealContact).filter_by(hubspot_deal_id="1").all()}
        self.assertEqual(links_after, {"p1", "p2"})
        self.assertTrue(any("contacts" in e for e in result.errors))


class TestAutoSyncAmount(unittest.TestCase):
    """High-confidence amount sync: deal at $0 + line items → auto-update HubSpot."""

    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for model in (HubSpotDeal, HubSpotCompany, HubSpotContact,
                          HubSpotLineItem, HubSpotDealContact):
                for row in s.query(model).all():
                    s.delete(row)
                s.flush()

    def _client_with_zero_amount(self):
        """Fake client with one open deal at $0 and a line item totalling $500."""
        client = FakeHubSpotClient()
        client.deals = [_deal("z1", "ZeroAmountCo", "0", "appointmentscheduled", "2026-09-01")]
        client.assoc = {
            ("z1", "companies"): [],
            ("z1", "contacts"): [],
            ("z1", "line_items"): ["lz1"],
        }
        client.get_line_items = lambda ids: [
            {"id": "lz1", "properties": {"name": "Service", "quantity": "1",
                                         "price": "500.00", "amount": "500.00"}}
        ]
        return client

    def test_zero_amount_deal_gets_updated(self):
        client = self._client_with_zero_amount()
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        # Client should have received one update_deal call.
        self.assertEqual(len(client.updates), 1)
        deal_id, props = client.updates[0]
        self.assertEqual(deal_id, "z1")
        self.assertIn("amount", props)
        self.assertAlmostEqual(float(props["amount"]), 500.0)

        # Local mirror should reflect the updated amount.
        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "z1")
            self.assertEqual(d.amount_cents, 50_000)

        self.assertEqual(result.auto_amount_synced, 1)

    def test_nonzero_amount_deal_not_updated(self):
        client = FakeHubSpotClient()
        # Default deals have non-zero amounts.
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))
        self.assertEqual(client.updates, [])

    def test_closed_deal_not_updated(self):
        """Closed deal at $0 must never be auto-updated."""
        client = FakeHubSpotClient()
        client.deals = [_deal("c1", "ClosedCo", "0", "closedwon", "2025-01-01")]
        client.deals[0]["properties"]["hs_is_closed"] = "true"
        client.deals[0]["properties"]["hs_is_closed_won"] = "true"
        client.assoc = {
            ("c1", "companies"): [],
            ("c1", "contacts"): [],
            ("c1", "line_items"): ["lc1"],
        }
        client.get_line_items = lambda ids: [
            {"id": "lc1", "properties": {"name": "S", "quantity": "1",
                                         "price": "300.00", "amount": "300.00"}}
        ]
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))
        self.assertEqual(client.updates, [])


if __name__ == "__main__":
    unittest.main()
