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
        return SimpleNamespace(clickup_list_id="900600702146")

    def test_sync_updates_existing_mirror_to_current_list_id(self) -> None:
        session_factory = create_session_factory("sqlite:///:memory:")
        init_database(session_factory)

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
        field_map = SimpleNamespace(
            next_follow_up_date="",
            communication_summary="",
            last_meeting_outcome="",
            recommended_next_action="",
            last_meaningful_touch="",
            last_outbound="",
            last_inbound="",
        )

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


if __name__ == "__main__":
    unittest.main()
