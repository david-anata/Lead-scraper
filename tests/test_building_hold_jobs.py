from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_hold_jobs_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingAvailabilityBlock,
        BuildingCalendarProjection,
        BuildingReservation,
        BuildingSpace,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingHoldJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_hold_jobs_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="building-job-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "building-job-key"}
        now = datetime.now(timezone.utc)
        with factory() as session:
            session.add_all([
                BuildingSpace(
                    id="hold-job-space",
                    slug="hold-job-space",
                    name="Hold Job Space",
                    space_type="event",
                    status="available",
                ),
                BuildingReservation(
                    id="expired-hold",
                    kind="event",
                    status="soft_hold",
                    space_id="hold-job-space",
                    starts_at=now + timedelta(days=5),
                    ends_at=now + timedelta(days=5, hours=4),
                    hold_expires_at=now - timedelta(hours=1),
                ),
                BuildingReservation(
                    id="current-hold",
                    kind="event",
                    status="soft_hold",
                    space_id="hold-job-space",
                    starts_at=now + timedelta(days=10),
                    ends_at=now + timedelta(days=10, hours=4),
                    hold_expires_at=now + timedelta(hours=2),
                ),
            ])
            session.flush()
            session.add_all([
                BuildingAvailabilityBlock(
                    id="expired-block",
                    space_id="hold-job-space",
                    state="soft_hold",
                    starts_at=now + timedelta(days=5),
                    ends_at=now + timedelta(days=5, hours=4),
                    expires_at=now - timedelta(hours=1),
                    source_reference="reservation:expired-hold",
                ),
                BuildingAvailabilityBlock(
                    id="current-block",
                    space_id="hold-job-space",
                    state="soft_hold",
                    starts_at=now + timedelta(days=10),
                    ends_at=now + timedelta(days=10, hours=4),
                    expires_at=now + timedelta(hours=2),
                    source_reference="reservation:current-hold",
                ),
                BuildingCalendarProjection(
                    id="expired-projection",
                    reservation_id="expired-hold",
                    desired_action="upsert",
                    status="synced",
                    provider_event_id="google-event-1",
                ),
            ])
            session.commit()

    def test_expiration_job_previews_then_releases_only_due_holds(self) -> None:
        unauthorized = self.client.post(
            "/api/jobs/building-holds/run",
            json={"dry_run": False},
        )
        self.assertEqual(unauthorized.status_code, 401)
        preview = self.client.post(
            "/api/jobs/building-holds/run",
            headers=self.headers,
            json={"dry_run": True},
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["details"]["expired_count"], 1)
        with self.factory() as session:
            self.assertEqual(
                session.get(BuildingReservation, "expired-hold").status,
                "soft_hold",
            )

        executed = self.client.post(
            "/api/jobs/building-holds/run",
            headers=self.headers,
            json={"dry_run": False},
        )
        self.assertEqual(executed.status_code, 200, executed.text)
        self.assertEqual(executed.json()["details"]["expired_count"], 1)
        with self.factory() as session:
            expired = session.get(BuildingReservation, "expired-hold")
            current = session.get(BuildingReservation, "current-hold")
            self.assertEqual(expired.status, "expired")
            self.assertIsNone(expired.hold_expires_at)
            self.assertEqual(current.status, "soft_hold")
            self.assertIsNotNone(
                session.get(BuildingAvailabilityBlock, "current-block")
            )
            self.assertIsNone(
                session.get(BuildingAvailabilityBlock, "expired-block")
            )
            projection = session.get(
                BuildingCalendarProjection,
                "expired-projection",
            )
            self.assertEqual(projection.desired_action, "delete")
            self.assertEqual(projection.status, "pending")
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="reservation",
                entity_id="expired-hold",
                action="hold_expired_automatically",
            ).one()
            self.assertTrue(audit.after_json["availability_released"])
