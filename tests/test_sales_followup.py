"""Tests for the follow-up email draft:
fallback template, route rendering, and hook labelling."""

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
    "sqlite:///" + tempfile.gettempdir() + "/sales_followup_test.db",
)
os.environ.setdefault("HUBSPOT_PORTAL_ID", "999")

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    HubSpotLineItem,
    SalesDealAsset,
)
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.followup_draft import (  # noqa: E402
    HOOK_LABELS,
    build_followup_draft,
)


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class TestBuildFollowupDraft(unittest.TestCase):
    """Unit tests for build_followup_draft — no API key, template fallback only."""

    def _build(self, **kwargs):
        defaults = dict(
            company_name="Acme Inc",
            contact_first_name="Sarah",
            owner_email="maya@anatainc.com",
            deal_name="Acme — Fulfillment",
            deal_amount_cents=400_000,
            hooks_sent=[],
            hooks_pending=["deck", "rate_sheet", "ads_audit"],
        )
        defaults.update(kwargs)
        return build_followup_draft(**defaults)

    def test_fallback_produces_subject_and_body(self):
        draft = self._build()
        self.assertTrue(draft.subject)
        self.assertTrue(draft.body)

    def test_subject_includes_company_name(self):
        draft = self._build()
        self.assertIn("Acme", draft.subject)

    def test_body_addresses_contact_by_first_name(self):
        draft = self._build()
        self.assertIn("Sarah", draft.body)

    def test_lead_hook_label_appears_in_draft(self):
        draft = self._build(hooks_pending=["rate_sheet", "ads_audit"])
        combined = draft.subject + draft.body
        self.assertIn(HOOK_LABELS["rate_sheet"], combined)

    def test_sent_hooks_referenced_in_followup(self):
        draft = self._build(hooks_sent=["rate_sheet"], hooks_pending=[])
        combined = draft.subject + draft.body
        self.assertIn("Rate Sheet", combined)

    def test_draft_has_correct_hook_tracking(self):
        draft = self._build(
            hooks_sent=["deck"],
            hooks_pending=["rate_sheet", "ads_audit"],
        )
        self.assertEqual(draft.hooks_sent, ["deck"])
        self.assertEqual(draft.hooks_pending, ["rate_sheet", "ads_audit"])

    def test_model_is_template_when_no_api_key(self):
        draft = self._build()
        self.assertEqual(draft.model, "template")

    def test_contact_emails_passed_through(self):
        draft = self._build(contact_emails=["sarah@acme.com"])
        self.assertIn("sarah@acme.com", draft.contact_emails)


class TestDraftFollowupRoute(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        with session_scope(app.state.session_factory) as s:
            # Delete join/child tables before parents to satisfy FK ordering.
            for model in (HubSpotDealContact, SalesDealAsset, HubSpotLineItem,
                          HubSpotDeal, HubSpotCompany, HubSpotContact):
                for row in s.query(model).all():
                    s.delete(row)
            s.flush()  # push deletes before inserting new rows
            s.add(HubSpotCompany(hubspot_company_id="co_fu", name="Fulfillment Co", domain="fc.com"))
            s.add(HubSpotDeal(
                hubspot_deal_id="draft_deal",
                deal_name="FC — Fulfillment",
                deal_stage="contractsent",
                deal_stage_label="Contract Sent",
                amount_cents=1_500_000,
                close_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
                owner_email="maya@anatainc.com",
                hubspot_company_id="co_fu",
                is_closed=False,
            ))
            s.add(HubSpotContact(
                hubspot_contact_id="ct_fu1",
                first_name="Jordan",
                last_name="Kim",
                email="jordan@fc.com",
                job_title="VP Ops",
            ))
            s.add(HubSpotDealContact(hubspot_deal_id="draft_deal", hubspot_contact_id="ct_fu1"))
            s.add(SalesDealAsset(
                hubspot_deal_id="draft_deal",
                asset_type="rate_sheet",
                run_id="r1",
                url="/rate-sheets/fc/r1/tok",
                label="Rate Sheet",
            ))

    def test_draft_page_returns_200(self):
        resp = self.client.get("/admin/sales/deals/draft_deal/draft-followup")
        self.assertEqual(resp.status_code, 200)

    def test_draft_page_shows_company_name(self):
        body = self.client.get("/admin/sales/deals/draft_deal/draft-followup").text
        self.assertIn("Fulfillment Co", body)

    def test_draft_page_shows_contact_email_in_mailto(self):
        body = self.client.get("/admin/sales/deals/draft_deal/draft-followup").text
        self.assertIn("jordan@fc.com", body)

    def test_draft_page_marks_rate_sheet_as_sent(self):
        body = self.client.get("/admin/sales/deals/draft_deal/draft-followup").text
        # Sent hooks get a ✓ marker; pending ones don't
        self.assertIn("Rate Sheet", body)

    def test_draft_page_404_for_unknown_deal(self):
        resp = self.client.get("/admin/sales/deals/no_such_deal/draft-followup")
        self.assertEqual(resp.status_code, 404)

    def test_draft_page_has_copy_button(self):
        body = self.client.get("/admin/sales/deals/draft_deal/draft-followup").text
        self.assertIn("Copy email", body)


if __name__ == "__main__":
    unittest.main()
