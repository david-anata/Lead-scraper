from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sales_support_agent.config import (
    ACTIVE_FOLLOW_UP_STATUSES,
    DEFAULT_STATUS_POLICIES,
    INACTIVE_STATUSES,
    ManagedFieldSettings,
    Settings,
    build_normalized_status_policies,
    normalize_status_key,
)

try:
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import CommunicationEvent
    from sales_support_agent.models.schemas import CommunicationEventRequest
    from sales_support_agent.services.communications import CommunicationService

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeClickUpClient:
    def __init__(self) -> None:
        self.created_comments: list[tuple[str, str]] = []

    def get_task(self, task_id: str) -> dict[str, object]:
        return {
            "id": task_id,
            "name": "Example Lead",
            "url": f"https://app.clickup.com/t/{task_id}",
            "status": {"status": "CONTACTED WARM"},
            "assignees": [{"id": "owner-1", "username": "Valeria Morales"}],
            "custom_fields": [],
        }

    def create_task_comment(self, task_id: str, comment_text: str) -> dict[str, object]:
        self.created_comments.append((task_id, comment_text))
        return {"id": f"comment-{len(self.created_comments)}"}

    def get_accessible_custom_fields(self, list_id: str) -> list[dict[str, object]]:
        return []

    def set_custom_field_value(self, task_id: str, field_id: str, value: object) -> dict[str, object]:
        return {"task_id": task_id, "field_id": field_id, "value": value}

    def update_task(self, task_id: str, payload: dict[str, object]) -> dict[str, object]:
        return {"task_id": task_id, "payload": payload}


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def post_message(self, **payload: object) -> dict[str, object]:
        self.messages.append(dict(payload))
        return {"ok": True}


def _build_settings(database_path: Path) -> Settings:
    return Settings(
        app_name="sales-support-agent",
        clickup_api_token="token",
        clickup_base_url="https://api.clickup.com/api/v2",
        clickup_list_id="list-123",
        clickup_request_timeout_seconds=30,
        clickup_discovery_sample_size=10,
        stale_lead_scan_max_tasks=5,
        stale_lead_scan_sync_max_tasks=7,
        stale_lead_slack_digest_enabled=True,
        stale_lead_slack_digest_mention_channel=True,
        stale_lead_slack_digest_max_items=20,
        stale_lead_immediate_alert_urgencies=("overdue",),
        daily_digest_enabled=True,
        daily_digest_email_to=("team@example.com",),
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
        gmail_client_id="",
        gmail_client_secret="",
        gmail_refresh_token="",
        gmail_user_id="me",
        gmail_poll_query="newer_than:2d",
        gmail_poll_max_messages=25,
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
        active_statuses=tuple(normalize_status_key(status) for status in ACTIVE_FOLLOW_UP_STATUSES),
        inactive_statuses=tuple(normalize_status_key(status) for status in INACTIVE_STATUSES),
        managed_fields=ManagedFieldSettings(),
        status_policies=build_normalized_status_policies(DEFAULT_STATUS_POLICIES),
    )


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for communication tests")
class CommunicationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.tempdir.name) / "sales-support-agent.sqlite3"
        self.settings = _build_settings(self.database_path)
        self.session_factory = create_session_factory(self.settings.sales_agent_db_url)
        init_database(self.session_factory)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_duplicate_payload_signature_skips_comment_and_slack(self) -> None:
        session = self.session_factory()
        clickup_client = _FakeClickUpClient()
        slack_client = _FakeSlackClient()
        payload_one = CommunicationEventRequest(
            task_id="task-123",
            event_type="inbound_reply_received",
            external_event_key="evt-1",
            occurred_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
            summary="Prospect replied and asked for pricing.",
            recommended_next_action="Send pricing and confirm timeline.",
            suggested_reply_draft="Thanks for the reply. I can send pricing today.",
            source="gmail",
            metadata={"classification": "pricing_or_offer_request"},
        )
        payload_two = payload_one.model_copy(update={"external_event_key": "evt-2"})

        try:
            with patch("sales_support_agent.services.communications.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_task.return_value = SimpleNamespace(status="CONTACTED WARM")

                service = CommunicationService(self.settings, clickup_client, slack_client, session)
                first = service.process_event(payload_one)
                second = service.process_event(payload_two)

            self.assertTrue(first["comment_posted"])
            self.assertFalse(second["comment_posted"])
            self.assertEqual(second["slack_notification"]["reason"], "duplicate_event_notification")
            self.assertEqual(len(clickup_client.created_comments), 1)
            self.assertEqual(len(slack_client.messages), 1)
            self.assertEqual(session.query(CommunicationEvent).count(), 2)
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
