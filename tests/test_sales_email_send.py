"""Tests for Phase 5: send_followup_email + the send-followup route."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_email_send_test.db",
)
os.environ.setdefault("HUBSPOT_PORTAL_ID", "999")

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.integrations.gmail import GmailClient  # noqa: E402
from sales_support_agent.integrations.hubspot import HubSpotClient  # noqa: E402
from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import HubSpotDeal, HubSpotContact, HubSpotDealContact  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.email_send import SendResult, send_followup_email  # noqa: E402


def _make_gmail(*, configured: bool = True) -> MagicMock:
    client = MagicMock(spec=GmailClient)
    client.is_configured.return_value = configured
    client.get_profile.return_value = {"emailAddress": "rep@anata.com"}
    client.send_message.return_value = {"id": "gmail_msg_999"}
    return client


def _make_hubspot(*, configured: bool = True) -> MagicMock:
    client = MagicMock(spec=HubSpotClient)
    type(client).is_configured = PropertyMock(return_value=configured)
    client.log_email_engagement.return_value = {"id": "hs_eng_123"}
    return client


class TestSendFollowupEmail(unittest.TestCase):
    """Unit tests for send_followup_email — no HTTP, no DB."""

    def _call(self, gmail=None, hubspot=None, **kwargs):
        defaults = dict(
            deal_id="d1",
            contact_ids=["c1"],
            to_emails=["buyer@acme.com"],
            subject="Hello",
            body_text="Hi there,\n\nFollowing up.\n\nBest,\nRep",
        )
        defaults.update(kwargs)
        return send_followup_email(
            gmail_client=gmail or _make_gmail(),
            hubspot_client=hubspot or _make_hubspot(),
            **defaults,
        )

    def test_success_returns_ok_with_ids(self):
        result = self._call()
        self.assertTrue(result.ok)
        self.assertEqual(result.gmail_message_id, "gmail_msg_999")
        self.assertEqual(result.hubspot_engagement_id, "hs_eng_123")
        self.assertEqual(result.from_email, "rep@anata.com")

    def test_gmail_send_is_called_with_correct_args(self):
        gmail = _make_gmail()
        self._call(gmail=gmail)
        gmail.send_message.assert_called_once_with(
            to=("buyer@acme.com",),
            subject="Hello",
            text="Hi there,\n\nFollowing up.\n\nBest,\nRep",
        )

    def test_hubspot_log_is_called_with_deal_and_contacts(self):
        hubspot = _make_hubspot()
        self._call(hubspot=hubspot)
        hubspot.log_email_engagement.assert_called_once()
        call_kwargs = hubspot.log_email_engagement.call_args.kwargs
        self.assertEqual(call_kwargs["deal_id"], "d1")
        self.assertIn("c1", call_kwargs["contact_ids"])
        self.assertEqual(call_kwargs["subject"], "Hello")

    def test_gmail_failure_returns_not_ok(self):
        gmail = _make_gmail()
        gmail.send_message.side_effect = RuntimeError("Gmail 401 Unauthorized")
        result = self._call(gmail=gmail)
        self.assertFalse(result.ok)
        self.assertIn("401", result.error)

    def test_hubspot_log_failure_does_not_fail_result(self):
        hubspot = _make_hubspot()
        hubspot.log_email_engagement.side_effect = RuntimeError("HubSpot 429")
        result = self._call(hubspot=hubspot)
        self.assertTrue(result.ok)  # send still succeeded
        self.assertEqual(result.hubspot_engagement_id, "")

    def test_no_recipients_returns_error(self):
        result = self._call(to_emails=[])
        self.assertFalse(result.ok)
        self.assertIn("recipients", result.error.lower())

    def test_empty_subject_returns_error(self):
        result = self._call(subject="")
        self.assertFalse(result.ok)
        self.assertIn("subject", result.error.lower())

    def test_empty_body_returns_error(self):
        result = self._call(body_text="")
        self.assertFalse(result.ok)
        self.assertIn("body", result.error.lower())

    def test_whitespace_only_to_emails_are_filtered(self):
        gmail = _make_gmail()
        result = self._call(gmail=gmail, to_emails=["  ", "buyer@acme.com", ""])
        self.assertTrue(result.ok)
        call_kwargs = gmail.send_message.call_args.kwargs
        self.assertEqual(call_kwargs["to"], ("buyer@acme.com",))

    def test_hubspot_unconfigured_skips_log(self):
        hubspot = _make_hubspot(configured=False)
        result = self._call(hubspot=hubspot)
        self.assertTrue(result.ok)
        hubspot.log_email_engagement.assert_not_called()

    def test_sent_at_is_populated(self):
        result = self._call()
        self.assertIsNotNone(result.sent_at)
        self.assertIsInstance(result.sent_at, datetime)


class TestSendFollowupRoute(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        s = app.state.agent_settings
        cookie_name, token = s.admin_cookie_name, create_user_session_token(
            s, email="david@anatainc.com", name="David", role="member"
        )
        cls.client.cookies.set(cookie_name, token)
        cls._seed()

    @classmethod
    def _seed(cls):
        with session_scope(app.state.session_factory) as s:
            for r in s.query(HubSpotDealContact).all():
                s.delete(r)
            for r in s.query(HubSpotDeal).all():
                s.delete(r)
            for r in s.query(HubSpotContact).all():
                s.delete(r)
            s.add(HubSpotDeal(
                hubspot_deal_id="send_d1",
                deal_name="Send Test Deal",
                deal_stage="appointmentscheduled",
                amount_cents=100_000,
                is_closed=False,
            ))
            s.add(HubSpotContact(
                hubspot_contact_id="send_c1",
                first_name="Buyer",
                last_name="Person",
                email="buyer@acme.com",
            ))
            s.flush()
            s.add(HubSpotDealContact(
                hubspot_deal_id="send_d1",
                hubspot_contact_id="send_c1",
            ))

    def test_preview_without_gmail_configured_redirects(self):
        with patch.object(GmailClient, "is_configured", return_value=False):
            resp = self.client.post(
                "/admin/sales/deals/send_d1/send-followup",
                data={"subject": "Hi", "body": "Hello.", "to_emails": "buyer@acme.com"},
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        self.assertIn("draft-followup", resp.headers.get("location", ""))

    def test_preview_with_gmail_configured_renders_confirmation(self):
        with patch.object(GmailClient, "is_configured", return_value=True), \
             patch.object(GmailClient, "get_profile", return_value={"emailAddress": "rep@co.com"}):
            resp = self.client.post(
                "/admin/sales/deals/send_d1/send-followup",
                data={"subject": "Test subject", "body": "Test body.", "to_emails": "buyer@acme.com"},
                follow_redirects=False,
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Confirm", resp.text)
        self.assertIn("Test subject", resp.text)
        self.assertIn("buyer@acme.com", resp.text)

    def test_confirmed_send_redirects_with_sent_flag(self):
        mock_result = SendResult(ok=True, gmail_message_id="msg1", from_email="rep@co.com")
        with patch.object(GmailClient, "is_configured", return_value=True), \
             patch(
                 "sales_support_agent.api.sales_router.send_followup_email",
                 return_value=mock_result,
             ):
            resp = self.client.post(
                "/admin/sales/deals/send_d1/send-followup",
                data={
                    "subject": "Follow up",
                    "body": "Hi Sarah,\n\nLet me know.\n\nBest",
                    "to_emails": "buyer@acme.com",
                    "confirmed": "1",
                },
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        self.assertIn("sent=1", resp.headers.get("location", ""))

    def test_sent_flag_shows_flash_on_deal_detail(self):
        resp = self.client.get("/admin/sales/deals/send_d1?sent=1")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("sent", resp.text.lower())

    def test_confirmed_send_failure_redirects_with_error(self):
        mock_result = SendResult(ok=False, error="Gmail 403 Forbidden")
        with patch.object(GmailClient, "is_configured", return_value=True), \
             patch(
                 "sales_support_agent.api.sales_router.send_followup_email",
                 return_value=mock_result,
             ):
            resp = self.client.post(
                "/admin/sales/deals/send_d1/send-followup",
                data={
                    "subject": "Hi",
                    "body": "Body.",
                    "to_emails": "buyer@acme.com",
                    "confirmed": "1",
                },
                follow_redirects=False,
            )
        self.assertIn(resp.status_code, (302, 303))
        self.assertIn("error", resp.headers.get("location", ""))


if __name__ == "__main__":
    unittest.main()
