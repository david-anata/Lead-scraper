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
    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeClickUpClient:
    def get_task_comments(self, task_id: str) -> list[dict[str, object]]:
        return []

    def create_task_comment(self, task_id: str, comment_text: str) -> dict[str, object]:
        return {"id": "comment-1"}


class _FakeSlackClient:
    def post_message(self, **_: object) -> dict[str, object]:
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
        slack_bot_token="slack-token",
        slack_channel_id="channel-123",
        slack_assignee_map={},
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


if __name__ == "__main__":
    unittest.main()
