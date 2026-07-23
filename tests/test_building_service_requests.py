from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_service_requests_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingServiceRequest,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingServiceRequestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            "building_service_requests_isolated.db",
        )
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-service-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-service-key"}
        space = cls.client.put(
            "/api/internal/building/spaces/service-office",
            headers=cls.headers,
            json={
                "id": "service-office",
                "slug": "service-office",
                "name": "Service Office",
                "space_type": "private_office",
                "capacity": 4,
                "status": "occupied",
                "is_public": False,
            },
        )
        if space.status_code != 200:
            raise AssertionError(space.text)

    def _create(self, **overrides):
        payload = {
            "id": "service-request-one",
            "category": "maintenance",
            "priority": "normal",
            "title": "Door closer needs adjustment",
            "description": "The suite door does not close consistently.",
            "space_id": "service-office",
            "source": "operator",
            "reported_by": "community@example.com",
            **overrides,
        }
        return self.client.post(
            "/api/internal/building/service-requests",
            headers=self.headers,
            json=payload,
        )

    def _transition(self, status: str, **overrides):
        return self.client.post(
            "/api/internal/building/service-requests/service-request-one/transition",
            headers=self.headers,
            json={
                "target_status": status,
                "reason": "Reviewed by building operations",
                "actor": "operator@example.com",
                **overrides,
            },
        )

    def test_00_priority_ownership_and_due_time_are_enforced(self) -> None:
        high = self._create(id="high-missing-owner", priority="high")
        self.assertEqual(high.status_code, 422)
        urgent = self._create(
            id="urgent-missing-due",
            priority="urgent",
            assigned_owner="operator@example.com",
        )
        self.assertEqual(urgent.status_code, 422)
        unsupported = self._create(
            id="unsupported-category",
            category="magic",
        )
        self.assertEqual(unsupported.status_code, 422)

    def test_01_service_request_lifecycle_is_audited(self) -> None:
        created = self._create()
        self.assertEqual(created.status_code, 201, created.text)
        skipped = self._transition("in_progress", assigned_owner="operator@example.com")
        self.assertEqual(skipped.status_code, 409)
        missing_owner = self._transition("triaged")
        self.assertEqual(missing_owner.status_code, 422)
        triaged = self._transition(
            "triaged",
            assigned_owner="operator@example.com",
            due_at=(datetime.now(timezone.utc) + timedelta(hours=4)).isoformat(),
        )
        self.assertEqual(triaged.status_code, 200, triaged.text)
        in_progress = self._transition("in_progress")
        self.assertEqual(in_progress.status_code, 200, in_progress.text)
        no_resolution = self._transition("completed")
        self.assertEqual(no_resolution.status_code, 422)
        completed = self._transition(
            "completed",
            resolution="Adjusted and tested the door closer.",
        )
        self.assertEqual(completed.status_code, 200, completed.text)
        self.assertEqual(completed.json()["service_request"]["status"], "completed")
        with self.factory() as session:
            row = session.get(BuildingServiceRequest, "service-request-one")
            self.assertIsNotNone(row.completed_at)
            self.assertEqual(row.completed_by, "operator@example.com")
            audits = session.query(BuildingAuditEvent).filter(
                BuildingAuditEvent.entity_type == "service_request",
                BuildingAuditEvent.entity_id == row.id,
            ).count()
            self.assertEqual(audits, 4)

    def test_02_completed_work_can_be_reopened_with_history(self) -> None:
        reopened = self._transition(
            "in_progress",
            reason="The issue returned after completion",
        )
        self.assertEqual(reopened.status_code, 200, reopened.text)
        body = reopened.json()["service_request"]
        self.assertEqual(body["status"], "in_progress")
        self.assertIsNone(body["completed_at"])

    def test_03_list_reports_overdue_open_work(self) -> None:
        with self.factory.begin() as session:
            row = session.get(BuildingServiceRequest, "service-request-one")
            row.due_at = datetime.now(timezone.utc) - timedelta(hours=1)
        listing = self.client.get(
            "/api/internal/building/service-requests",
            headers=self.headers,
        )
        self.assertEqual(listing.status_code, 200, listing.text)
        self.assertTrue(listing.json()["service_requests"][0]["overdue"])


if __name__ == "__main__":
    unittest.main()
