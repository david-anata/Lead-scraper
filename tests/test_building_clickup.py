from __future__ import annotations

import tempfile
import unittest
from types import SimpleNamespace

from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import BuildingInquiry
from sales_support_agent.services.building_clickup import (
    backfill_building_inquiries_to_clickup,
    project_building_inquiry_to_clickup,
)


class _FakeClickUp:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(clickup_api_token="token")
        self.tasks: list[dict] = []
        self.comments: list[tuple[str, str]] = []

    def get_tasks(self, _list_id, *, include_closed=True, page=0):
        return self.tasks if page == 0 else []

    def create_task(self, _list_id, payload):
        task = {"id": f"task-{len(self.tasks) + 1}", "url": "https://clickup.test/t/1", **payload}
        self.tasks.append(task)
        return task

    def create_task_comment(self, task_id, comment_text):
        self.comments.append((task_id, comment_text))
        return {"id": f"comment-{len(self.comments)}"}


class BuildingClickUpProjectionTests(unittest.TestCase):
    def setUp(self) -> None:
        path = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
        self.factory = create_session_factory(f"sqlite:///{path}")
        init_database(self.factory)
        self.client = _FakeClickUp()

    def _inquiry(self, inquiry_id: str, *, source="anata-building", reference=""):
        return BuildingInquiry(
            id=inquiry_id,
            idempotency_key=f"key-{inquiry_id}",
            kind="event",
            source=source,
            source_reference=reference,
            name="Jordan Lead",
            email="jordan@example.com",
            phone="801-555-0100",
            assigned_owner="david@example.com",
            consent_to_contact=True,
            payload_json={"referrer": "https://chatgpt.com/c/answer", "eventType": "Workshop"},
        )

    def test_reuses_one_task_for_repeated_form_fills(self) -> None:
        with self.factory() as session:
            first = self._inquiry("first")
            session.add(first)
            session.flush()
            self.assertTrue(project_building_inquiry_to_clickup(
                session=session, inquiry=first, client=self.client,
                list_id="building-list", actor="test",
            ))
            second = self._inquiry("second", source="eventective", reference="evt-123")
            session.add(second)
            session.flush()
            self.assertTrue(project_building_inquiry_to_clickup(
                session=session, inquiry=second, client=self.client,
                list_id="building-list", actor="test",
            ))
            session.commit()
            self.assertEqual(len(self.client.tasks), 1)
            self.assertEqual(len(self.client.comments), 1)
            self.assertEqual(first.payload_json["_clickup"]["task_id"], second.payload_json["_clickup"]["task_id"])
            self.assertIn("AI · ChatGPT", self.client.tasks[0]["description"])

    def test_backfill_is_idempotent(self) -> None:
        with self.factory() as session:
            session.add(self._inquiry("historical"))
            session.commit()
        settings = SimpleNamespace(clickup_api_token="token", clickup_list_id="building-list")
        from unittest import mock
        with mock.patch(
            "sales_support_agent.services.building_clickup.ClickUpClient",
            return_value=self.client,
        ):
            first = backfill_building_inquiries_to_clickup(self.factory, settings=settings)
            second = backfill_building_inquiries_to_clickup(self.factory, settings=settings)
        self.assertEqual(first["projected"], 1)
        self.assertEqual(second["scanned"], 0)
        self.assertEqual(len(self.client.tasks), 1)


if __name__ == "__main__":
    unittest.main()
