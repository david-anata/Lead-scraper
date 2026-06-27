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
        # Default deals have non-zero amounts — no amount update should be issued.
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))
        amt_updates = [(did, p) for did, p in client.updates if "amount" in p]
        self.assertEqual(amt_updates, [])

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


class TestAutoFixCloseDate(unittest.TestCase):
    """Auto-fix close dates: overdue → pushed +30d, missing → set to +30d."""

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

    def _client(self, deals):
        client = FakeHubSpotClient()
        client.deals = deals
        client.assoc = {(d["id"], k): [] for d in deals for k in ("companies", "contacts", "line_items")}
        return client

    def test_overdue_close_date_gets_pushed(self):
        """Open deal with close_date 10 days in the past → pushed to today+30d."""
        client = self._client([
            _deal("od1", "OldCo", "5000", "qualifiedtobuy", "2026-01-01T00:00:00Z")
        ])
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        # Should have auto-fixed the close date (one update_deal call with 'closedate').
        cd_updates = [(did, p) for did, p in client.updates if "closedate" in p]
        self.assertEqual(len(cd_updates), 1)
        self.assertEqual(cd_updates[0][0], "od1")
        self.assertEqual(result.auto_close_dates_fixed, 1)

        # Mirror should reflect the new date (roughly today+30d).
        with session_scope(self.sf) as s:
            deal = s.get(HubSpotDeal, "od1")
            self.assertIsNotNone(deal.close_date)
            days_from_now = (deal.close_date.replace(tzinfo=None) -
                             __import__("datetime").datetime.utcnow()).days
            self.assertGreater(days_from_now, 25)

    def test_missing_close_date_gets_set(self):
        """Open deal with no close_date → set to today+30d."""
        client = self._client([
            _deal("nc1", "NoCo", "3000", "appointmentscheduled", None)
        ])
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        cd_updates = [(did, p) for did, p in client.updates if "closedate" in p]
        self.assertEqual(len(cd_updates), 1)
        self.assertEqual(cd_updates[0][0], "nc1")
        self.assertEqual(result.auto_close_dates_fixed, 1)

    def test_future_close_date_not_touched(self):
        """Open deal with a future close_date → no update."""
        client = self._client([
            _deal("fc1", "FutureCo", "5000", "qualifiedtobuy", "2026-12-31T00:00:00Z")
        ])
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        cd_updates = [(did, p) for did, p in client.updates if "closedate" in p]
        self.assertEqual(cd_updates, [])
        self.assertEqual(result.auto_close_dates_fixed, 0)

    def test_closed_deal_close_date_not_touched(self):
        """Closed-won deal with past close_date → never modified."""
        d = _deal("cw1", "WonCo", "10000", "closedwon", "2025-06-01T00:00:00Z")
        d["properties"]["hs_is_closed"] = "true"
        d["properties"]["hs_is_closed_won"] = "true"
        client = self._client([d])
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        self.assertEqual(client.updates, [])
        self.assertEqual(result.auto_close_dates_fixed, 0)

    def test_grace_period_skips_barely_overdue_active_deal(self):
        """≤3 days overdue + touched within 3 days → skip (rep is actively working it)."""
        import datetime as _dt
        # Close date was yesterday
        yesterday = (_dt.datetime.utcnow() - _dt.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        # Last meaningful touch was also yesterday
        d = _deal("gp1", "GraceCo", "7000", "qualifiedtobuy", yesterday)
        client = self._client([d])

        # Pre-seed the mirror with a recent last_meaningful_touch_at
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))
            deal = session.get(HubSpotDeal, "gp1")
            deal.last_meaningful_touch_at = _dt.datetime.utcnow() - _dt.timedelta(days=1)

        client.updates.clear()

        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        cd_updates = [(did, p) for did, p in client.updates if "closedate" in p]
        self.assertEqual(cd_updates, [], "Grace period should prevent auto-push on active deals")
        self.assertEqual(result.auto_close_dates_fixed, 0)

    def test_auto_close_date_reported_in_result_dict(self):
        """auto_close_dates_fixed is exposed in as_dict() for the sync banner."""
        client = self._client([
            _deal("ad1", "DictCo", "2000", "appointmentscheduled", "2026-01-01T00:00:00Z")
        ])
        with session_scope(self.sf) as session:
            result = sync_hubspot_sales(session, client, SimpleNamespace(hubspot_sales_pipeline_id=""))

        d = result.as_dict()
        self.assertIn("auto_close_dates_fixed", d)
        self.assertEqual(d["auto_close_dates_fixed"], 1)


class TestEmailSignalEnhancement(unittest.TestCase):
    """Native HubSpot email signals populate last_outbound_at / last_inbound_at /
    last_meaningful_touch_at, and do not overwrite more-recent Gmail-set values."""

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

    def _settings(self):
        return SimpleNamespace(hubspot_sales_pipeline_id="")

    @staticmethod
    def _deal_with_email_signals(did, name, send_date=None, replied_date=None,
                                  activity_date=None):
        props = {
            "dealname": name,
            "amount": "1000",
            "dealstage": "appointmentscheduled",
            "pipeline": "default",
            "closedate": "2026-09-01",
            "hubspot_owner_id": "42",
            "createdate": "2026-01-01T00:00:00Z",
            "hs_lastmodifieddate": "2026-02-01T00:00:00Z",
            "hs_is_closed": "false",
            "hs_is_closed_won": "false",
        }
        if send_date is not None:
            props["hs_email_last_send_date"] = send_date
        if replied_date is not None:
            props["hs_email_last_replied"] = replied_date
        if activity_date is not None:
            props["hs_last_sales_activity_date"] = activity_date
        return {"id": did, "properties": props}

    def _minimal_client(self, deal):
        """Client returning a single deal, no associations."""
        client = FakeHubSpotClient()
        client.deals = [deal]
        did = deal["id"]
        client.assoc = {
            (did, "companies"): [],
            (did, "contacts"): [],
            (did, "line_items"): [],
        }
        return client

    def test_native_outbound_populates_last_outbound_at(self):
        deal = self._deal_with_email_signals(
            "e1", "OutboundCo", send_date="2026-05-10T10:00:00Z"
        )
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e1")
            self.assertIsNotNone(d.last_outbound_at)
            self.assertEqual(d.last_outbound_at.strftime("%Y-%m-%d"), "2026-05-10")

    def test_native_inbound_populates_last_inbound_at(self):
        deal = self._deal_with_email_signals(
            "e2", "InboundCo", replied_date="2026-05-15T08:00:00Z"
        )
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e2")
            self.assertIsNotNone(d.last_inbound_at)
            self.assertEqual(d.last_inbound_at.strftime("%Y-%m-%d"), "2026-05-15")

    def test_native_activity_populates_last_meaningful_touch(self):
        deal = self._deal_with_email_signals(
            "e3", "ActivityCo", activity_date="2026-05-20T12:00:00Z"
        )
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e3")
            self.assertIsNotNone(d.last_meaningful_touch_at)
            self.assertEqual(d.last_meaningful_touch_at.strftime("%Y-%m-%d"), "2026-05-20")

    def test_existing_outbound_not_overwritten_by_older_native(self):
        """Mirror already has a more-recent last_outbound_at → native should NOT replace it."""
        from datetime import timezone as _tz

        deal = self._deal_with_email_signals(
            "e4", "OldSignalCo", send_date="2026-04-01T10:00:00Z"
        )
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        # Manually set a more-recent outbound on the mirror.
        from datetime import datetime as _dt
        newer_ts = _dt(2026, 6, 1, 0, 0, 0, tzinfo=_tz.utc)
        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e4")
            d.last_outbound_at = newer_ts

        # Re-sync with the same (older) native signal — should not overwrite.
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e4")
            self.assertEqual(d.last_outbound_at.strftime("%Y-%m-%d"), "2026-06-01")

    def test_gmail_touch_takes_priority_if_more_recent(self):
        """If mirror already has a more-recent touch_at (from Gmail), native does not overwrite."""
        from datetime import datetime as _dt, timezone as _tz

        deal = self._deal_with_email_signals(
            "e5", "GmailPriorityCo",
            send_date="2026-03-01T10:00:00Z",
            activity_date="2026-03-05T10:00:00Z",
        )
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        # Simulate Gmail signal job writing a more recent touch.
        gmail_touch = _dt(2026, 6, 15, 0, 0, 0, tzinfo=_tz.utc)
        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e5")
            d.last_meaningful_touch_at = gmail_touch

        # Re-sync — native signals (March) should not overwrite the June Gmail touch.
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e5")
            self.assertEqual(d.last_meaningful_touch_at.strftime("%Y-%m-%d"), "2026-06-15")

    def test_no_native_signals_leaves_fields_none(self):
        """Deal with no email properties → signal fields stay None."""
        deal = self._deal_with_email_signals("e6", "NoSignalCo")
        client = self._minimal_client(deal)
        with session_scope(self.sf) as session:
            sync_hubspot_sales(session, client, self._settings())

        with session_scope(self.sf) as s:
            d = s.get(HubSpotDeal, "e6")
            self.assertIsNone(d.last_outbound_at)
            self.assertIsNone(d.last_inbound_at)
            self.assertIsNone(d.last_meaningful_touch_at)


if __name__ == "__main__":
    unittest.main()
