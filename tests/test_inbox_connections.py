import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sales_support_agent.config import GmailMailboxAccount
from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import MailboxSignal
from sales_support_agent.services.inbox_connections import build_inbox_connection_summary
from sales_support_agent.services.settings_page import render_settings_page


class InboxConnectionsTests(unittest.TestCase):
    def setUp(self):
        self.session_factory = create_session_factory("sqlite:///:memory:")
        init_database(self.session_factory)
        self.as_of = datetime(2026, 6, 29, 18, 0, tzinfo=timezone.utc)
        self.settings = SimpleNamespace(
            gmail_mailbox_accounts=(
                GmailMailboxAccount(
                    account_key="david",
                    label="David Narayan",
                    client_id="client-id",
                    client_secret="client-secret",
                    refresh_token="refresh-token",
                    source_domains=("fulfil.com",),
                ),
                GmailMailboxAccount(
                    account_key="alex",
                    label="Alex Smith",
                    client_id="client-id",
                    client_secret="client-secret",
                    refresh_token="refresh-token",
                    source_domains=("fulfil.com",),
                ),
                GmailMailboxAccount(
                    account_key="ops",
                    label="Ops Inbox",
                    client_id="",
                    client_secret="",
                    refresh_token="",
                    source_domains=("anatainc.com",),
                ),
                GmailMailboxAccount(
                    account_key="sam",
                    label="Sam Young",
                    client_id="client-id",
                    client_secret="client-secret",
                    refresh_token="refresh-token",
                    source_domains=("fulfil.com",),
                ),
            )
        )
        with session_scope(self.session_factory) as session:
            session.add(
                MailboxSignal(
                    provider="gmail",
                    sender_email="lead-one@example.com",
                    subject="Proposal review",
                    matched_deal_id="deal-1",
                    received_at=self.as_of - timedelta(hours=3),
                    raw_payload={
                        "gmail_account_key": "david",
                        "gmail_account_label": "David Narayan",
                    },
                )
            )
            session.add(
                MailboxSignal(
                    provider="gmail",
                    sender_email="lead-three@example.com",
                    subject="Updated proposal review",
                    matched_deal_id="deal-3",
                    received_at=self.as_of - timedelta(hours=1),
                    raw_payload={
                        "gmail_account_key": "david",
                        "gmail_account_label": "David Narayan",
                    },
                )
            )
            session.add(
                MailboxSignal(
                    provider="gmail",
                    sender_email="lead-two@example.com",
                    subject="Old check-in",
                    matched_deal_id="deal-2",
                    received_at=self.as_of - timedelta(days=10),
                    raw_payload={
                        "gmail_account_key": "alex",
                        "gmail_account_label": "Alex Smith",
                    },
                )
            )

    def test_build_inbox_connection_summary_classifies_accounts(self):
        with session_scope(self.session_factory) as session:
            summary = build_inbox_connection_summary(session, self.settings, as_of=self.as_of, stale_days=7)

        self.assertEqual(summary["total_configured"], 4)
        self.assertEqual(summary["connected_count"], 1)
        self.assertEqual(summary["attention_count"], 1)
        self.assertEqual(summary["invalid_count"], 1)
        self.assertEqual(summary["configured_not_seen_count"], 1)

        rows = {row["account_key"]: row for row in summary["accounts"]}
        self.assertEqual(rows["david"]["status"], "connected")
        self.assertEqual(rows["david"]["message_count"], 2)
        self.assertEqual(rows["david"]["matched_deal_count"], 2)
        self.assertEqual(rows["david"]["last_sender_email"], "lead-three@example.com")
        self.assertEqual(rows["david"]["last_subject"], "Updated proposal review")
        self.assertEqual(rows["alex"]["status"], "attention")
        self.assertEqual(rows["ops"]["status"], "invalid")
        self.assertEqual(rows["sam"]["status"], "configured_not_seen")

    def test_render_settings_page_includes_connected_inboxes_card(self):
        with session_scope(self.session_factory) as session:
            summary = build_inbox_connection_summary(session, self.settings, as_of=self.as_of, stale_days=7)

        html = render_settings_page(
            {"name": "Admin User", "email": "admin@anatainc.com", "permissions": ("access.manage",)},
            team_counts={"total_users": 1, "active_users": 1, "pending_invites": 0, "pending_requests": 0},
            agent_settings=SimpleNamespace(
                amazon_sp_api_marketplace_id="ATVPDKIKX0DER",
                amazon_sp_api_region="us-east-1",
                amazon_sp_api_base_url="https://sellingpartnerapi-na.amazon.com",
                amazon_sp_api_lwa_client_id="client",
                amazon_sp_api_refresh_token="refresh",
                amazon_sp_api_aws_access_key_id="aws-key",
                slack_bot_token="slack-token",
                slack_channel_id="channel-1",
                stale_lead_slack_digest_enabled=True,
                stale_lead_slack_digest_max_items=20,
                google_oauth_client_id="google-client",
                google_oauth_client_secret="google-secret",
                google_oauth_allowed_domain="anatainc.com",
                admin_password="fallback-password",
                admin_session_secret="session-secret",
            ),
            inbox_summary=summary,
        )

        self.assertIn("Connected Inboxes", html)
        self.assertIn("Auth &amp; Access", html)
        self.assertIn("Google sign-in", html)
        self.assertIn("anatainc.com", html)
        self.assertIn("Fallback password", html)
        self.assertIn("David Narayan", html)
        self.assertIn("Needs Attention", html)
        self.assertIn("/admin/settings/inboxes", html)

    def test_render_settings_page_shows_inbox_warning_when_summary_falls_back(self):
        html = render_settings_page(
            {"name": "Admin User", "email": "admin@anatainc.com", "permissions": ("access.manage",)},
            team_counts={"total_users": 1, "active_users": 1, "pending_invites": 0, "pending_requests": 0},
            agent_settings=SimpleNamespace(
                amazon_sp_api_marketplace_id="ATVPDKIKX0DER",
                amazon_sp_api_region="us-east-1",
                amazon_sp_api_base_url="https://sellingpartnerapi-na.amazon.com",
                amazon_sp_api_lwa_client_id="client",
                amazon_sp_api_refresh_token="refresh",
                amazon_sp_api_aws_access_key_id="aws-key",
                slack_bot_token="slack-token",
                slack_channel_id="channel-1",
                stale_lead_slack_digest_enabled=True,
                stale_lead_slack_digest_max_items=20,
            ),
            inbox_summary={"warning": "Inbox summary unavailable."},
        )

        self.assertIn("Inbox summary unavailable.", html)


if __name__ == "__main__":
    unittest.main()
