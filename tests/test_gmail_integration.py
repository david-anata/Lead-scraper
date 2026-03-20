from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from sales_support_agent.config import ACTIVE_FOLLOW_UP_STATUSES, DEFAULT_STATUS_POLICIES, INACTIVE_STATUSES, ManagedFieldSettings, Settings, build_normalized_status_policies, normalize_status_key
    from sales_support_agent.integrations.gmail import GmailClient, GmailIntegrationError
    from sales_support_agent.jobs.daily_digest import DailyDigestJob
    from sales_support_agent.jobs.mailbox_sync import GmailMailboxSyncJob
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import LeadMirror, MailboxSignal
    from sales_support_agent.services.matching import LeadMatchingService

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _Response:
    def __init__(self, *, ok: bool, status_code: int, text: str, json_payload: dict | None = None, content: bytes = b"{}") -> None:
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self._json_payload = json_payload or {}
        self.content = content

    def json(self) -> dict:
        return dict(self._json_payload)


def _build_settings(database_path: Path) -> Settings:
    return Settings(
        app_name="sales-support-agent",
        admin_username="admin",
        admin_password="password",
        admin_session_secret="secret",
        admin_cookie_name="cookie",
        admin_session_ttl_hours=24,
        clickup_api_token="token",
        clickup_base_url="https://api.clickup.com/api/v2",
        clickup_list_id="list-123",
        clickup_request_timeout_seconds=30,
        clickup_discovery_sample_size=10,
        stale_lead_scan_max_tasks=5,
        stale_lead_scan_sync_max_tasks=7,
        stale_lead_slack_digest_enabled=False,
        stale_lead_slack_digest_mention_channel=False,
        stale_lead_slack_digest_max_items=20,
        stale_lead_immediate_alert_urgencies=(),
        daily_digest_enabled=True,
        daily_digest_email_to=("david@anatainc.com",),
        daily_digest_email_cc=(),
        daily_digest_subject_prefix="[SDR Support]",
        daily_digest_max_items=20,
        slack_bot_token="slack-token",
        slack_channel_id="channel-123",
        slack_assignee_map={},
        slack_immediate_event_types=("inbound_reply_received", "meeting_notes_missing"),
        gmail_api_base_url="https://gmail.googleapis.com/gmail/v1",
        gmail_oauth_token_url="https://oauth2.googleapis.com/token",
        gmail_access_token="",
        gmail_client_id="client-id",
        gmail_client_secret="client-secret",
        gmail_refresh_token="refresh-token",
        gmail_user_id="me",
        gmail_poll_query="newer_than:2d",
        gmail_poll_max_messages=10,
        gmail_source_domains=("fulfil.com",),
        sales_agent_db_url=f"sqlite:///{database_path}",
        internal_api_key="internal-key",
        discovery_snapshot_path=Path("runtime/clickup_schema_snapshot.json"),
        use_due_date_for_follow_up=False,
        openai_api_key="",
        openai_model="gpt-4o-mini",
        instantly_webhook_secret="",
        instantly_webhook_secret_header="X-Instantly-Webhook-Secret",
        instantly_webhook_allowed_event_types=("reply_received",),
        google_sheets_api_base_url="",
        google_sheets_spreadsheet_id="",
        google_sheets_sales_range="",
        google_service_account_json="",
        canva_api_base_url="",
        canva_authorize_url="",
        canva_token_url="",
        canva_client_id="",
        canva_client_secret="",
        canva_redirect_uri="",
        canva_brand_template_id="",
        canva_scopes=(),
        canva_token_secret="",
        deck_canva_poll_interval_seconds=5,
        deck_canva_poll_attempts=1,
        deck_competitor_required_columns=(),
        deck_competitor_allowed_columns=(),
        deck_required_template_fields=(),
        active_statuses=tuple(normalize_status_key(status) for status in ACTIVE_FOLLOW_UP_STATUSES),
        inactive_statuses=tuple(normalize_status_key(status) for status in INACTIVE_STATUSES),
        managed_fields=ManagedFieldSettings(),
        status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
    )


class _FakeClickUpClient:
    def get_task_comments(self, task_id: str) -> list[dict[str, object]]:
        return []


class _FakeSlackClient:
    def post_message(self, **_: object) -> dict[str, object]:
        return {"ok": True}


class _FailingGmailClient:
    def is_configured(self) -> bool:
        return True

    def missing_configuration(self) -> tuple[str, ...]:
        return ()

    def debug_preflight(self) -> dict[str, object]:
        raise GmailIntegrationError(
            stage="token_refresh",
            code="invalid_client",
            http_status=401,
            message='Gmail token refresh failed (401): {"error":"invalid_client"}',
            hint="OAuth client mismatch.",
            provider_payload={"error": "invalid_client"},
        )


class _DigestFailingGmailClient:
    def is_configured(self) -> bool:
        return True

    def missing_configuration(self) -> tuple[str, ...]:
        return ()

    def send_message(self, **_: object) -> dict[str, object]:
        raise GmailIntegrationError(
            stage="send_message",
            code="insufficient_scope",
            http_status=403,
            message='Gmail API request failed: {"error":"insufficient_scope"}',
            hint="Re-authorize with gmail.modify.",
            provider_payload={"error": "insufficient_scope"},
        )


class GmailClientTests(unittest.TestCase):
    def test_missing_configuration_reports_required_env_vars(self) -> None:
        settings = SimpleNamespace(
            gmail_access_token="",
            gmail_client_id="",
            gmail_client_secret="",
            gmail_refresh_token="",
        )
        client = GmailClient(settings)
        self.assertEqual(
            client.missing_configuration(),
            ("GMAIL_CLIENT_ID", "GMAIL_CLIENT_SECRET", "GMAIL_REFRESH_TOKEN"),
        )

    def test_token_refresh_surfaces_invalid_client_details(self) -> None:
        settings = SimpleNamespace(
            gmail_access_token="",
            gmail_client_id="client-id",
            gmail_client_secret="client-secret",
            gmail_refresh_token="refresh-token",
            gmail_oauth_token_url="https://oauth2.googleapis.com/token",
        )
        client = GmailClient(settings)

        with patch("sales_support_agent.integrations.gmail.requests.post") as mock_post:
            mock_post.return_value = _Response(
                ok=False,
                status_code=401,
                text='{"error":"invalid_client","error_description":"Unauthorized"}',
                json_payload={"error": "invalid_client", "error_description": "Unauthorized"},
            )

            with self.assertRaises(GmailIntegrationError) as context:
                client._get_access_token()

        self.assertEqual(context.exception.stage, "token_refresh")
        self.assertEqual(context.exception.code, "invalid_client")
        self.assertIn("OAuth client ID and client secret", context.exception.hint)

    def test_create_draft_posts_to_gmail_drafts_endpoint(self) -> None:
        settings = SimpleNamespace(
            gmail_access_token="test-access-token",
            gmail_client_id="client-id",
            gmail_client_secret="client-secret",
            gmail_refresh_token="refresh-token",
            gmail_api_base_url="https://gmail.googleapis.com/gmail/v1",
            gmail_user_id="me",
        )
        client = GmailClient(settings)

        with patch("sales_support_agent.integrations.gmail.requests.request") as mock_request:
            mock_request.return_value = _Response(
                ok=True,
                status_code=200,
                text='{"id":"draft-123","message":{"id":"message-123"}}',
                json_payload={"id": "draft-123", "message": {"id": "message-123"}},
            )

            result = client.create_draft(
                to=("pat@example.com",),
                subject="Hello",
                text="Body copy",
            )

        self.assertEqual(result["id"], "draft-123")
        self.assertIn("/users/me/drafts", mock_request.call_args.args[1])
        self.assertIn("message", mock_request.call_args.kwargs["json"])


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for gmail integration job tests")
class GmailJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.tempdir.name) / "sales-support-agent.sqlite3"
        self.settings = _build_settings(self.database_path)
        self.session_factory = create_session_factory(self.settings.sales_agent_db_url)
        init_database(self.session_factory)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_mailbox_sync_returns_failed_summary_for_auth_error(self) -> None:
        session = self.session_factory()
        try:
            result = GmailMailboxSyncJob(
                self.settings,
                _FakeClickUpClient(),
                _FakeSlackClient(),
                _FailingGmailClient(),
                session,
            ).run(dry_run=True, query="newer_than:2d", max_messages=5)
        finally:
            session.close()

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["stage"], "token_refresh")
        self.assertEqual(result["error_code"], "invalid_client")
        self.assertEqual(result["query"], "newer_than:2d")
        self.assertEqual(result["max_messages"], 5)

    def test_daily_digest_returns_failed_summary_for_send_error(self) -> None:
        session = self.session_factory()
        try:
            session.add(
                MailboxSignal(
                    provider="gmail",
                    external_message_id="msg-1",
                    external_thread_id="thread-1",
                    dedupe_key="gmail_message:msg-1",
                    matched_task_id="",
                    sender_name="Pat Buyer",
                    sender_email="pat@example.com",
                    sender_domain="example.com",
                    subject="Need pricing",
                    snippet="Can you send pricing?",
                    body_text="Can you send pricing?",
                    classification="pricing_or_offer_request",
                    urgency="needs_immediate_review",
                    owner_id="owner-1",
                    owner_name="Valeria Morales",
                    task_name="ACME Corp",
                    task_url="https://app.clickup.com/t/acme",
                    task_status="WORKING QUALIFIED",
                    action_summary="Send pricing today.",
                    suggested_reply_draft="Thanks. I can send pricing today.",
                    received_at=datetime(2026, 3, 17, 13, 0, tzinfo=timezone.utc),
                    processed_at=datetime(2026, 3, 17, 13, 5, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()

            result = DailyDigestJob(
                self.settings,
                _FakeClickUpClient(),
                _DigestFailingGmailClient(),
                session,
            ).run(as_of_date=datetime(2026, 3, 17, tzinfo=timezone.utc).date(), include_stale=False, include_mailbox=True)
        finally:
            session.close()

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["stage"], "send_message")
        self.assertEqual(result["error_code"], "insufficient_scope")

    def test_mailbox_match_ignores_body_email_mentions_from_non_source_domains(self) -> None:
        session = self.session_factory()
        try:
            session.add(
                LeadMirror(
                    clickup_task_id="task-123",
                    list_id=self.settings.clickup_list_id,
                    task_name="David Narayan | Fulfillment eBook Form | Test",
                    task_url="https://app.clickup.com/t/task-123",
                    status="new lead",
                    email="david@anatainc.com",
                    created_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    last_sync_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    raw_task_payload={},
                )
            )
            session.commit()

            matcher = LeadMatchingService(self.settings, _FakeClickUpClient(), session)
            result = matcher.find_mailbox_match(
                sender_email="alerts@google.com",
                sender_domain="google.com",
                candidate_emails=("alerts@google.com", "david@anatainc.com"),
                sync_on_miss=False,
            )
        finally:
            session.close()

        self.assertIsNone(result)

    def test_mailbox_match_allows_body_email_fallback_for_source_domains(self) -> None:
        session = self.session_factory()
        try:
            session.add(
                LeadMirror(
                    clickup_task_id="task-456",
                    list_id=self.settings.clickup_list_id,
                    task_name="Inbound Lead",
                    task_url="https://app.clickup.com/t/task-456",
                    status="new lead",
                    email="buyer@example.com",
                    created_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    last_sync_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
                    raw_task_payload={},
                )
            )
            session.commit()

            matcher = LeadMatchingService(self.settings, _FakeClickUpClient(), session)
            result = matcher.find_mailbox_match(
                sender_email="notify@fulfil.com",
                sender_domain="fulfil.com",
                candidate_emails=("notify@fulfil.com", "buyer@example.com"),
                sync_on_miss=False,
            )
        finally:
            session.close()

        self.assertIsNotNone(result)
        self.assertEqual(result.clickup_task_id, "task-456")


if __name__ == "__main__":
    unittest.main()
