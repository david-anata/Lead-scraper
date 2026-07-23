from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_checklists_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingOperationalChecklist,
        BuildingOperationalChecklistItem,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingChecklistTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_checklists_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-checklist-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-checklist-key"}
        cls.start = datetime.now(timezone.utc) + timedelta(days=28)
        for space_id, space_type in (
            ("checklist-event-space", "event"),
            ("checklist-office", "private_office"),
        ):
            response = cls.client.put(
                f"/api/internal/building/spaces/{space_id}",
                headers=cls.headers,
                json={
                    "id": space_id,
                    "slug": space_id,
                    "name": space_id.replace("-", " ").title(),
                    "space_type": space_type,
                    "capacity": 80 if space_type == "event" else 8,
                    "status": "available",
                    "is_public": False,
                },
            )
            if response.status_code != 200:
                raise AssertionError(response.text)

    def _create(self, reservation_id: str, kind: str, space_id: str):
        return self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "id": reservation_id,
                "kind": kind,
                "space_id": space_id,
                "starts_at": self.start.isoformat(),
                "ends_at": (self.start + timedelta(hours=4)).isoformat(),
                "attendance": 4,
                "deposit_required": False,
                "assigned_owner": "operations@example.com",
                "actor": "operations@example.com",
            },
        )

    def _transition(self, reservation_id: str, status: str):
        return self.client.post(
            f"/api/internal/building/bookings/{reservation_id}/transition",
            headers=self.headers,
            json={
                "target_status": status,
                "actor": "operations@example.com",
            },
        )

    def _sign(self, reservation_id: str):
        return self.client.post(
            f"/api/internal/building/bookings/{reservation_id}/agreements",
            headers=self.headers,
            json={
                "status": "signed",
                "version": 1,
                "provider": "test-signature",
                "provider_reference": f"signed-{reservation_id}",
                "actor": "operations@example.com",
            },
        )

    def test_00_event_confirmation_creates_one_readiness_checklist(self) -> None:
        created = self._create(
            "checklist-event",
            "event",
            "checklist-event-space",
        )
        self.assertEqual(created.status_code, 201, created.text)
        for status in ("requirements_review", "quote_sent", "contract_pending"):
            response = self._transition("checklist-event", status)
            self.assertEqual(response.status_code, 200, response.text)
        signed = self._sign("checklist-event")
        self.assertEqual(signed.status_code, 201, signed.text)
        confirmed = self._transition("checklist-event", "confirmed")
        self.assertEqual(confirmed.status_code, 200, confirmed.text)

        listing = self.client.get(
            "/api/internal/building/checklists",
            headers=self.headers,
            params={"reservation_id": "checklist-event"},
        )
        self.assertEqual(listing.status_code, 200, listing.text)
        rows = listing.json()["checklists"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["checklist_type"], "event_readiness")
        self.assertEqual(len(rows[0]["items"]), 6)

        pre_event = self._transition("checklist-event", "pre_event")
        self.assertEqual(pre_event.status_code, 200, pre_event.text)
        with self.factory() as session:
            self.assertEqual(session.query(BuildingOperationalChecklist).count(), 1)

    def test_01_waiver_requires_reason_and_completion_is_derived(self) -> None:
        listing = self.client.get(
            "/api/internal/building/checklists",
            headers=self.headers,
            params={"reservation_id": "checklist-event"},
        ).json()["checklists"][0]
        first = listing["items"][0]
        missing_reason = self.client.post(
            f"/api/internal/building/checklists/items/{first['id']}/status",
            headers=self.headers,
            json={
                "status": "waived",
                "actor": "operations@example.com",
            },
        )
        self.assertEqual(missing_reason.status_code, 422)

        for index, item in enumerate(listing["items"]):
            status = "waived" if index == 0 else "completed"
            response = self.client.post(
                f"/api/internal/building/checklists/items/{item['id']}/status",
                headers=self.headers,
                json={
                    "status": status,
                    "reason": "Not applicable for this approved event"
                    if status == "waived"
                    else "",
                    "actor": "operations@example.com",
                },
            )
            self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["checklist_status"], "completed")

        checklist_id = listing["id"]
        added = self.client.post(
            f"/api/internal/building/checklists/{checklist_id}/items",
            headers=self.headers,
            json={
                "label": "Record final customer handoff",
                "is_required": True,
                "actor": "operations@example.com",
            },
        )
        self.assertEqual(added.status_code, 201, added.text)
        with self.factory() as session:
            checklist = session.get(BuildingOperationalChecklist, checklist_id)
            self.assertEqual(checklist.status, "open")

    def test_02_workspace_lifecycle_creates_move_in_and_move_out(self) -> None:
        created = self._create(
            "checklist-workspace",
            "workspace",
            "checklist-office",
        )
        self.assertEqual(created.status_code, 201, created.text)
        for status in ("qualified", "proposal_sent", "contract_pending"):
            response = self._transition("checklist-workspace", status)
            self.assertEqual(response.status_code, 200, response.text)
        signed = self._sign("checklist-workspace")
        self.assertEqual(signed.status_code, 201, signed.text)
        confirmed = self._transition("checklist-workspace", "confirmed")
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        occupied = self._transition("checklist-workspace", "occupied")
        self.assertEqual(occupied.status_code, 200, occupied.text)
        move_out = self._transition("checklist-workspace", "move_out")
        self.assertEqual(move_out.status_code, 200, move_out.text)

        listing = self.client.get(
            "/api/internal/building/checklists",
            headers=self.headers,
            params={"reservation_id": "checklist-workspace"},
        )
        self.assertEqual(listing.status_code, 200, listing.text)
        types = {
            row["checklist_type"] for row in listing.json()["checklists"]
        }
        self.assertEqual(types, {"move_in", "move_out"})
        with self.factory() as session:
            self.assertGreater(
                session.query(BuildingOperationalChecklistItem).count(),
                5,
            )


if __name__ == "__main__":
    unittest.main()
