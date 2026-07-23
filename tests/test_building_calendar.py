from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_calendar_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingCalendarProjection,
        BuildingReservation,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


class FakeCalendarClient:
    configured = True

    def __init__(self) -> None:
        self.upserts: list[tuple[str, dict, str]] = []
        self.deletes: list[str] = []

    def upsert_event(
        self,
        *,
        reservation_id: str,
        payload: dict,
        provider_event_id: str = "",
    ) -> str:
        self.upserts.append((reservation_id, payload, provider_event_id))
        return provider_event_id or "google-event-123"

    def delete_event(self, provider_event_id: str) -> None:
        self.deletes.append(provider_event_id)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingCalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_calendar_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-calendar-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-calendar-key"}
        cls.start = datetime.now(timezone.utc) + timedelta(days=21)
        space = cls.client.put(
            "/api/internal/building/spaces/calendar-arena",
            headers=cls.headers,
            json={
                "id": "calendar-arena",
                "slug": "calendar-arena",
                "name": "Calendar Arena",
                "space_type": "event",
                "location": "Main floor",
                "capacity": 100,
                "status": "available",
                "is_public": False,
            },
        )
        if space.status_code != 200:
            raise AssertionError(space.text)

    def _transition(self, status: str, **extra):
        return self.client.post(
            "/api/internal/building/bookings/calendar-event/transition",
            headers=self.headers,
            json={
                "target_status": status,
                "actor": "calendar-operator@example.com",
                **extra,
            },
        )

    def test_00_unapproved_inquiry_is_not_projected(self) -> None:
        created = self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "id": "calendar-event",
                "kind": "event",
                "space_id": "calendar-arena",
                "starts_at": self.start.isoformat(),
                "ends_at": (self.start + timedelta(hours=3)).isoformat(),
                "attendance": 50,
                "assigned_owner": "Events",
                "actor": "calendar-operator@example.com",
            },
        )
        self.assertEqual(created.status_code, 201, created.text)
        listing = self.client.get(
            "/api/internal/building/calendar/projections",
            headers=self.headers,
        )
        self.assertEqual(listing.status_code, 200, listing.text)
        self.assertEqual(listing.json()["projections"], [])

    def test_01_hold_queues_preview_without_external_write(self) -> None:
        self.assertEqual(
            self._transition("requirements_review").status_code,
            200,
        )
        hold = self._transition(
            "soft_hold",
            hold_expires_at=(
                datetime.now(timezone.utc) + timedelta(days=2)
            ).isoformat(),
        )
        self.assertEqual(hold.status_code, 200, hold.text)
        preview = self.client.post(
            "/api/internal/building/calendar/sync",
            headers=self.headers,
            json={
                "execute": False,
                "actor": "calendar-operator@example.com",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        body = preview.json()
        self.assertFalse(body["execute"])
        self.assertEqual(body["pending_count"], 1)
        self.assertEqual(body["projections"][0]["desired_action"], "upsert")

    def test_02_unconfigured_execute_fails_without_losing_queue(self) -> None:
        class Unconfigured:
            configured = False

        with patch(
            "sales_support_agent.api.building_calendar_router.BuildingGoogleCalendarClient",
            return_value=Unconfigured(),
        ):
            response = self.client.post(
                "/api/internal/building/calendar/sync",
                headers=self.headers,
                json={
                    "execute": True,
                    "actor": "calendar-operator@example.com",
                },
            )
        self.assertEqual(response.status_code, 503, response.text)
        with self.factory() as session:
            row = session.query(BuildingCalendarProjection).one()
            self.assertEqual(row.status, "pending")
            self.assertEqual(row.attempt_count, 0)

    def test_03_sync_is_traceable_and_cancellation_queues_delete(self) -> None:
        fake = FakeCalendarClient()
        with patch(
            "sales_support_agent.api.building_calendar_router.BuildingGoogleCalendarClient",
            return_value=fake,
        ):
            synced = self.client.post(
                "/api/internal/building/calendar/sync",
                headers=self.headers,
                json={
                    "execute": True,
                    "actor": "calendar-operator@example.com",
                },
            )
        self.assertEqual(synced.status_code, 200, synced.text)
        self.assertEqual(synced.json()["synced_count"], 1)
        self.assertEqual(len(fake.upserts), 1)
        with self.factory() as session:
            projection = session.query(BuildingCalendarProjection).one()
            reservation = session.get(BuildingReservation, "calendar-event")
            self.assertEqual(projection.status, "synced")
            self.assertEqual(reservation.calendar_event_id, "google-event-123")

        cancelled = self._transition("cancelled", reason="Customer cancelled")
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        with self.factory() as session:
            projection = session.query(BuildingCalendarProjection).one()
            self.assertEqual(projection.status, "pending")
            self.assertEqual(projection.desired_action, "delete")

        with patch(
            "sales_support_agent.api.building_calendar_router.BuildingGoogleCalendarClient",
            return_value=fake,
        ):
            deleted = self.client.post(
                "/api/internal/building/calendar/sync",
                headers=self.headers,
                json={
                    "execute": True,
                    "actor": "calendar-operator@example.com",
                },
            )
        self.assertEqual(deleted.status_code, 200, deleted.text)
        self.assertEqual(fake.deletes, ["google-event-123"])
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "calendar-event")
            self.assertEqual(reservation.calendar_event_id, "")


if __name__ == "__main__":
    unittest.main()
