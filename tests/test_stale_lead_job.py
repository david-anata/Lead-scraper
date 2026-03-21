from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from sales_support_agent.config import ACTIVE_FOLLOW_UP_STATUSES, DEFAULT_STATUS_POLICIES, INACTIVE_STATUSES, ManagedFieldSettings, Settings, build_normalized_status_policies, normalize_status_key

try:
    from sales_support_agent.jobs.stale_leads import StaleLeadJob
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import LeadMirror
    from sales_support_agent.services.reminders import ReminderService
    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeClickUpClient:
    def __init__(self, comments_by_task: dict[str, list[dict[str, object]]] | None = None) -> None:
        self.comments_by_task = comments_by_task or {}
        self.created_comments: list[tuple[str, str]] = []

    def get_task_comments(self, task_id: str) -> list[dict[str, object]]:
        return list(self.comments_by_task.get(task_id, []))

    def create_task_comment(self, task_id: str, comment_text: str) -> dict[str, object]:
        self.created_comments.append((task_id, comment_text))
        return {"id": "comment-1"}


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def post_message(self, **_: object) -> dict[str, object]:
        self.messages.append(dict(_))
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


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for stale-lead job tests")
class StaleLeadJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.tempdir.name) / "sales-support-agent.sqlite3"
        self.settings = _build_settings(self.database_path)
        self.session_factory = create_session_factory(self.settings.sales_agent_db_url)
        init_database(self.session_factory)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _insert_lead(self) -> None:
        session = self.session_factory()
        try:
            session.add(
                LeadMirror(
                    clickup_task_id="task-123",
                    list_id=self.settings.clickup_list_id,
                    task_name="Example Lead",
                    task_url="https://app.clickup.com/t/task-123",
                    status="new lead",
                    created_at=datetime(2026, 3, 10, 9, 0, 0),
                    updated_at=datetime(2026, 3, 10, 9, 0, 0),
                    last_sync_at=datetime(2026, 3, 10, 9, 0, 0),
                    raw_task_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

    def test_run_falls_back_to_existing_mirror_if_sync_refresh_fails(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.side_effect = RuntimeError("ClickUp unavailable")

                result = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    _FakeSlackClient(),
                    session,
                ).run(dry_run=True, as_of_date=date(2026, 3, 13))

            sync_service_cls.return_value.sync_list.assert_called_once_with(include_closed=True, max_tasks=7)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["inspected"], 1)
            self.assertEqual(result["synced_tasks"], 0)
            self.assertTrue(result["sync_failed"])
        finally:
            session.close()

    def test_run_uses_manual_max_tasks_for_sync_limit(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.return_value = {"synced_tasks": 2}

                result = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    _FakeSlackClient(),
                    session,
                ).run(dry_run=True, as_of_date=date(2026, 3, 13), max_tasks=2)

            sync_service_cls.return_value.sync_list.assert_called_once_with(include_closed=True, max_tasks=2)
            self.assertEqual(result["status"], "ok")
            self.assertFalse(result["sync_failed"])
            self.assertEqual(result["synced_tasks"], 2)
        finally:
            session.close()

    def test_run_posts_single_digest_for_routine_due_items(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        slack_client = _FakeSlackClient()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.return_value = {"synced_tasks": 1}

                result = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    slack_client,
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 13))

            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["digest_posted"])
            self.assertEqual(result["immediate_alerted"], 0)
            self.assertEqual(result["alerted"], 1)
            self.assertEqual(len(slack_client.messages), 1)
            self.assertIn("<!channel>", str(slack_client.messages[0].get("text", "")))
        finally:
            session.close()

    def test_run_skips_duplicate_immediate_alert_for_same_signature(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        slack_client = _FakeSlackClient()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.return_value = {"synced_tasks": 1}

                first = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    slack_client,
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 17))

                second = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    slack_client,
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 17))

            self.assertEqual(first["immediate_alerted"], 1)
            self.assertEqual(second["immediate_alerted"], 0)
            self.assertEqual(len(slack_client.messages), 2)
        finally:
            session.close()

    def test_run_skips_duplicate_agent_comment_when_recent_signature_matches(self) -> None:
        self._insert_lead()
        seed_session = self.session_factory()
        try:
            lead = seed_session.query(LeadMirror).filter(LeadMirror.clickup_task_id == "task-123").one()
            reminder_service = ReminderService(self.settings, seed_session)
            evaluation = reminder_service.evaluate_lead(lead, as_of_date=date(2026, 3, 13), comments=[])
            assert evaluation is not None
            existing_comment = reminder_service.build_agent_comment(evaluation)
        finally:
            seed_session.close()

        existing_comments = {
            "task-123": [
                {
                    "comment_text": existing_comment,
                    "date": str(int(datetime(2026, 3, 12, 9, 0, 0).timestamp() * 1000)),
                }
            ]
        }
        clickup_client = _FakeClickUpClient(comments_by_task=existing_comments)
        session = self.session_factory()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.return_value = {"synced_tasks": 1}

                result = StaleLeadJob(
                    self.settings,
                    clickup_client,
                    _FakeSlackClient(),
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 13))

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["commented"], 0)
            self.assertEqual(result["comment_skipped_duplicate"], 1)
            self.assertEqual(clickup_client.created_comments, [])
        finally:
            session.close()

    def test_agent_comment_uses_neutral_clickup_voice(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        try:
            lead = session.query(LeadMirror).filter(LeadMirror.clickup_task_id == "task-123").one()
            reminder_service = ReminderService(self.settings, session)
            evaluation = reminder_service.evaluate_lead(lead, as_of_date=date(2026, 3, 13), comments=[])
            assert evaluation is not None

            comment_text = reminder_service.build_agent_comment(evaluation)

            self.assertIn("[Sales Support Agent] Follow-up state updated.", comment_text)
            self.assertIn("Recommended next step:", comment_text)
            self.assertNotIn("Suggested reply:", comment_text)
        finally:
            session.close()

    def test_run_skips_duplicate_digest_for_same_date(self) -> None:
        self._insert_lead()
        session = self.session_factory()
        slack_client = _FakeSlackClient()
        try:
            with patch("sales_support_agent.jobs.stale_leads.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_list.return_value = {"synced_tasks": 1}

                first = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    slack_client,
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 13))

                second = StaleLeadJob(
                    self.settings,
                    _FakeClickUpClient(),
                    slack_client,
                    session,
                ).run(dry_run=False, as_of_date=date(2026, 3, 13))

            self.assertTrue(first["digest_posted"])
            self.assertFalse(second["digest_posted"])
            self.assertEqual(len(slack_client.messages), 1)
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
