from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_deck_context_resolver_test.db",
)

from sales_support_agent.api.router import router  # noqa: E402
from sales_support_agent.api.router import _resolve_and_attach_sales_deck  # noqa: E402
from sales_support_agent.models.database import create_session_factory, init_database, session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    AutomationRun,
    Company,
    Contact,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    LeadRecord,
    MailboxSignal,
    SalesDealAsset,
)
from sales_support_agent.services.sales.sales_deck_context_resolver import (  # noqa: E402
    SalesDeckContextInput,
    resolve_sales_deck_context,
)


def _factory():
    factory = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
    init_database(factory)
    return factory


class SalesDeckContextResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = _factory()
        with session_scope(self.factory) as s:
            for model in (
                SalesDealAsset,
                HubSpotDealContact,
                MailboxSignal,
                HubSpotContact,
                HubSpotDeal,
                HubSpotCompany,
                LeadRecord,
                Contact,
                Company,
            ):
                for row in s.query(model).all():
                    s.delete(row)
            s.flush()
            s.add(HubSpotCompany(hubspot_company_id="co1", name="Made For Her Organic", domain="madeforher.com"))
            s.add(HubSpotDeal(
                hubspot_deal_id="deal1",
                deal_name="Made For Her Organic - Marketing",
                hubspot_company_id="co1",
                deal_stage="appointmentscheduled",
                is_closed=False,
            ))
            s.add(HubSpotContact(hubspot_contact_id="c1", hubspot_company_id="co1", email="buyer@madeforher.com"))
            s.add(HubSpotDealContact(hubspot_deal_id="deal1", hubspot_contact_id="c1"))
            s.add(HubSpotDeal(
                hubspot_deal_id="closed",
                deal_name="Closed Organic Deal",
                deal_stage="closedwon",
                is_closed=True,
            ))

    def test_matches_by_exact_contact_email(self) -> None:
        with session_scope(self.factory) as s:
            result = resolve_sales_deck_context(s, SalesDeckContextInput(contact_email="buyer@madeforher.com"))
        self.assertEqual(result.action, "attach_existing")
        self.assertEqual(result.selected.hubspot_deal_id, "deal1")

    def test_matches_by_company_domain_from_mailbox_signal(self) -> None:
        with session_scope(self.factory) as s:
            s.add(MailboxSignal(
                provider="gmail",
                matched_deal_id="deal1",
                sender_email="founder@madeforher.com",
                sender_domain="madeforher.com",
                subject="Deck",
                received_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            ))
        with session_scope(self.factory) as s:
            result = resolve_sales_deck_context(s, SalesDeckContextInput(company_domain="madeforher.com"))
        self.assertEqual(result.action, "attach_existing")
        self.assertEqual(result.matched_source, "hubspot_mirror")

    def test_uses_current_lead_when_no_hubspot_deal_exists(self) -> None:
        with session_scope(self.factory) as s:
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            company = Company(domain="gabrieldaniel.com", company_name="Gabriel & Daniel Organic", website="https://gabrieldaniel.com")
            s.add(company)
            s.flush()
            contact = Contact(company_id=company.id, email="ops@gabrieldaniel.com", full_name="Ops Lead")
            s.add(contact)
            s.flush()
            s.add(LeadRecord(lead_key="gd", company_id=company.id, contact_id=contact.id, status="accepted"))
        with session_scope(self.factory) as s:
            result = resolve_sales_deck_context(
                s,
                SalesDeckContextInput(company_domain="gabrieldaniel.com", brand_name="Gabriel Daniel"),
            )
        self.assertEqual(result.action, "create_then_attach")
        self.assertEqual(result.selected.source, "lead_record")

    def test_closed_deal_is_not_auto_attached(self) -> None:
        with session_scope(self.factory) as s:
            result = resolve_sales_deck_context(s, SalesDeckContextInput(hubspot_deal_id="closed"))
        self.assertNotEqual(result.action, "attach_existing")
        self.assertIn("closed", " ".join(result.audit_lines))


class SalesDeckPreviewRouteTests(unittest.TestCase):
    def test_preview_png_route_is_token_gated(self) -> None:
        factory = _factory()
        with session_scope(factory) as s:
            run = AutomationRun(
                run_type="deck_generation",
                status="completed",
                summary_json={
                    "export_token": "tok",
                    "design_title": "Made For Her Organic x anata strategy deck",
                    "share_preview": {
                        "title": "Made For Her Organic x anata strategy deck",
                        "brand": "Made For Her Organic",
                        "description": "Strategy deck",
                    },
                },
            )
            s.add(run)
            s.flush()
            run_id = run.id
        app = FastAPI()
        app.state.settings = SimpleNamespace()
        app.state.session_factory = factory
        app.include_router(router)
        client = TestClient(app)
        ok = client.get(f"/decks/made-for-her/{run_id}/tok/preview.png")
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(ok.headers["content-type"], "image/png")
        self.assertTrue(ok.content.startswith(b"\x89PNG"))
        bad = client.get(f"/decks/made-for-her/{run_id}/wrong/preview.png")
        self.assertEqual(bad.status_code, 404)


class SalesDeckAttachmentTests(unittest.TestCase):
    def test_generated_deck_attaches_to_matched_open_deal(self) -> None:
        factory = _factory()
        with session_scope(factory) as s:
            for model in (SalesDealAsset, HubSpotDealContact, HubSpotContact, HubSpotDeal, HubSpotCompany, AutomationRun):
                for row in s.query(model).all():
                    s.delete(row)
            s.flush()
            s.add(HubSpotCompany(hubspot_company_id="co1", name="Made For Her Organic", domain="madeforher.com"))
            s.add(HubSpotDeal(
                hubspot_deal_id="deal1",
                deal_name="Made For Her Organic - Marketing",
                hubspot_company_id="co1",
                deal_stage="appointmentscheduled",
                is_closed=False,
            ))
            s.add(HubSpotContact(hubspot_contact_id="c1", hubspot_company_id="co1", email="buyer@madeforher.com"))
            s.add(HubSpotDealContact(hubspot_deal_id="deal1", hubspot_contact_id="c1"))
            run = AutomationRun(
                run_type="deck_generation",
                status="completed",
                summary_json={
                    "export_token": "tok",
                    "design_title": "Made For Her Organic x anata strategy deck",
                    "view_url": "/decks/made/1/tok",
                    "share_preview": {"brand": "Made For Her Organic"},
                },
            )
            s.add(run)
            s.flush()
            run_id = run.id
        settings = SimpleNamespace(
            gmail_access_token="",
            gmail_client_id="",
            gmail_client_secret="",
            gmail_refresh_token="",
            gmail_user_id="me",
            gmail_poll_query="",
            gmail_poll_max_messages=3,
            gmail_source_domains=(),
        )
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=settings, session_factory=factory)))
        result = SimpleNamespace(
            run_id=run_id,
            view_url="/decks/made/1/tok",
            design_title="Made For Her Organic x anata strategy deck",
        )
        details = _resolve_and_attach_sales_deck(
            request,
            result=result,
            form_payload={"contact_email": "buyer@madeforher.com"},
        )
        self.assertEqual(details["attachment_status"], "attached")
        self.assertEqual(details["attached_deal_id"], "deal1")
        with session_scope(factory) as s:
            asset = s.query(SalesDealAsset).filter_by(hubspot_deal_id="deal1", asset_type="deck").one()
            self.assertEqual(asset.url, "/decks/made/1/tok")
            run = s.get(AutomationRun, run_id)
            self.assertEqual((run.summary_json or {}).get("attachment_status"), "attached")


if __name__ == "__main__":
    unittest.main()
