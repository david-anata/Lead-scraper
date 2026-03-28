from __future__ import annotations

import unittest
from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from sales_support_agent.models.database import create_session_factory, init_database, session_scope
    from sales_support_agent.models.entities import LeadMirror
    from sales_support_agent.services.sync import ClickUpSyncService

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for sync tests")
class ClickUpSyncServiceTests(unittest.TestCase):
    def _settings(self) -> SimpleNamespace:
        return SimpleNamespace(
            clickup_list_id="900600702146",
            active_statuses=(
                "new lead",
                "contacted cold",
                "contacted warm",
                "working qualified",
                "working needs offer",
                "working offered",
                "working negotiating",
            ),
            inactive_statuses=(
                "won - onboarding",
                "won - active",
                "lost",
                "lost - not qualified",
                "won - canceled",
            ),
        )

    def _field_map(self) -> SimpleNamespace:
        return SimpleNamespace(
            next_follow_up_date="",
            communication_summary="",
            last_meeting_outcome="",
            recommended_next_action="",
            last_meaningful_touch="",
            last_outbound="",
            last_inbound="",
        )

    def _base_task(self, **overrides) -> dict:
        task = {
            "id": "task-123",
            "name": "Example lead",
            "url": "https://app.clickup.com/t/task-123",
            "status": {"status": "FOLLOW UP"},
            "assignees": [],
            "custom_fields": [],
            "date_created": None,
            "date_updated": None,
            "due_date": None,
        }
        task.update(overrides)
        return task

    def test_sync_updates_existing_mirror_to_current_list_id(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        task = self._base_task()
        field_map = self._field_map()

        with session_scope(session_factory) as session:
            session.add(
                LeadMirror(
                    clickup_task_id="task-123",
                    list_id="old-list-id",
                    task_name="Old name",
                    status="NEW LEAD",
                    raw_task_payload={},
                )
            )

        with session_scope(session_factory) as session:
            service = ClickUpSyncService(self._settings(), clickup_client=object(), session=session)
            service._upsert_task(task, field_map)

        with session_scope(session_factory) as session:
            mirrored = session.get(LeadMirror, "task-123")
            self.assertIsNotNone(mirrored)
            assert mirrored is not None
            self.assertEqual(mirrored.list_id, "900600702146")
            self.assertEqual(mirrored.status_key, "follow up")
            self.assertTrue(mirrored.is_active)
            self.assertFalse(mirrored.is_closed)

    def test_sync_marks_closed_tasks_inactive(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        task = self._base_task(id="task-closed", status={"status": "WON - ACTIVE"})

        with session_scope(session_factory) as session:
            service = ClickUpSyncService(self._settings(), clickup_client=object(), session=session)
            service._upsert_task(task, self._field_map())

        with session_scope(session_factory) as session:
            mirrored = session.get(LeadMirror, "task-closed")
            self.assertIsNotNone(mirrored)
            assert mirrored is not None
            self.assertEqual(mirrored.status_key, "won active")
            self.assertTrue(mirrored.is_closed)
            self.assertFalse(mirrored.is_active)

    def test_sync_keeps_unknown_open_statuses_active(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        task = self._base_task(id="task-open", status={"status": "FOLLOW UP"})

        with session_scope(session_factory) as session:
            service = ClickUpSyncService(self._settings(), clickup_client=object(), session=session)
            service._upsert_task(task, self._field_map())

        with session_scope(session_factory) as session:
            mirrored = session.get(LeadMirror, "task-open")
            self.assertIsNotNone(mirrored)
            assert mirrored is not None
            self.assertEqual(mirrored.status_key, "follow up")
            self.assertFalse(mirrored.is_closed)
            self.assertTrue(mirrored.is_active)

    def test_sync_handles_partial_payloads_with_deterministic_defaults(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

        task = self._base_task(
            id="task-partial",
            name=None,
            url=None,
            status=None,
            assignees=None,
            custom_fields=None,
            date_created="not-a-date",
            date_updated="not-a-date",
            due_date="not-a-date",
        )

        with session_scope(session_factory) as session:
            service = ClickUpSyncService(self._settings(), clickup_client=object(), session=session)
            service._upsert_task(task, self._field_map())

        with session_scope(session_factory) as session:
            mirrored = session.get(LeadMirror, "task-partial")
            self.assertIsNotNone(mirrored)
            assert mirrored is not None
            self.assertEqual(mirrored.task_name, "")
            self.assertEqual(mirrored.task_url, "")
            self.assertEqual(mirrored.status, "")
            self.assertEqual(mirrored.status_key, "")
            self.assertFalse(mirrored.is_closed)
            self.assertFalse(mirrored.is_active)
            self.assertEqual(mirrored.assignee_id, "")
            self.assertEqual(mirrored.assignee_name, "")
            self.assertIsNone(mirrored.created_at)
            self.assertIsNone(mirrored.updated_at)
            self.assertIsNone(mirrored.task_updated_at)
            self.assertIsNone(mirrored.due_date)


if __name__ == "__main__":
    unittest.main()
