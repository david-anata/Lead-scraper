from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_tour_handoff_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-tour-handoff-session-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingAvailabilityBlock,
        BuildingContact,
        BuildingEventLifecycleCommand,
        BuildingInquiry,
        BuildingOffering,
        BuildingReservation,
        BuildingSpace,
        BuildingTour,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingTourHandoffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_tour_handoff_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="tour-handoff-internal-key",
            building_campaign_token_secret="tour-handoff-csrf-secret",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.internal_headers = {"X-Internal-Api-Key": "tour-handoff-internal-key"}
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        with factory() as session:
            session.add_all([
                BuildingSpace(
                    id="tour-space",
                    slug="tour-space",
                    name="Tour Workspace",
                    space_type="coworking",
                    status="available",
                    is_public=True,
                ),
                BuildingSpace(
                    id="other-space",
                    slug="other-space",
                    name="Other Workspace",
                    space_type="private_office",
                    status="available",
                    is_public=True,
                ),
                BuildingOffering(
                    id="tour-offering",
                    slug="tour-offering",
                    name="Tour Workspace Offering",
                    offering_type="coworking",
                    space_id="tour-space",
                    is_published=True,
                ),
                BuildingOffering(
                    id="other-offering",
                    slug="other-offering",
                    name="Other Offering",
                    offering_type="private_office",
                    space_id="other-space",
                    is_published=True,
                ),
            ])
            session.commit()

    def _inquiry(
        self,
        suffix: str,
        *,
        kind: str = "tour",
        contact_status: str = "active",
    ) -> str:
        inquiry_id = f"tour-inquiry-{suffix}"
        email = f"{suffix}@example.com"
        with self.factory() as session:
            session.add(BuildingContact(
                id=f"tour-contact-{suffix}",
                email=email,
                full_name=f"Tour Contact {suffix}",
                status=contact_status,
                source="test",
            ))
            session.add(BuildingInquiry(
                id=inquiry_id,
                idempotency_key=f"intake-{suffix}",
                kind=kind,
                source="test",
                name=f"Tour Inquiry {suffix}",
                email=email,
                phone="801-555-0100",
                preferred_date=date.today() + timedelta(days=14),
                consent_to_contact=True,
                assigned_owner="host@example.com",
                payload_json={},
            ))
            session.commit()
        return inquiry_id

    def _payload(self, inquiry_id: str, **overrides) -> dict:
        payload = {
            "inquiry_id": inquiry_id,
            "offering_id": "tour-offering",
            "space_id": "tour-space",
            "scheduled_at": (
                datetime.now(timezone.utc) + timedelta(days=7)
            ).replace(microsecond=0).isoformat(),
            "duration_minutes": 45,
            "host": f"{inquiry_id}@host.example",
            "meeting_location": "Anata Building lobby",
            "notes": "Customer requested a workspace walkthrough.",
            "actor": "operator@example.com",
        }
        payload.update(overrides)
        return payload

    def _handoff(self, payload: dict, key: str):
        return self.client.post(
            "/api/internal/building/bookings/tour-inquiry-handoffs",
            headers={**self.internal_headers, "Idempotency-Key": key},
            json=payload,
        )

    def _csrf(self) -> str:
        page = self.client.get("/admin/building")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match, page.text)
        return match.group(1)

    def test_01_atomic_handoff_links_every_record_without_inventory_hold(self) -> None:
        inquiry_id = self._inquiry("happy")
        response = self._handoff(self._payload(inquiry_id), "tour-handoff-happy")
        self.assertEqual(response.status_code, 201, response.text)
        body = response.json()
        self.assertFalse(body["replayed"])
        self.assertFalse(body["inventory_hold_created"])
        self.assertEqual(body["reservation"]["status"], "tour_scheduled")
        self.assertEqual(body["tour"]["status"], "scheduled")
        self.assertTrue(body["tour"]["scheduled_at"].endswith("+00:00"))

        with self.factory() as session:
            reservation = session.get(
                BuildingReservation, body["reservation"]["id"]
            )
            tour = session.get(BuildingTour, body["tour"]["id"])
            inquiry = session.get(BuildingInquiry, inquiry_id)
            self.assertEqual(reservation.inquiry_id, inquiry.id)
            self.assertEqual(reservation.contact_id, f"tour-contact-happy")
            self.assertEqual(reservation.offering_id, "tour-offering")
            self.assertEqual(reservation.space_id, "tour-space")
            self.assertEqual(tour.reservation_id, reservation.id)
            self.assertEqual(
                inquiry.payload_json["_tour_handoff"]["tour_id"], tour.id
            )
            self.assertEqual(session.query(BuildingAvailabilityBlock).count(), 0)
            actions = {
                row.action
                for row in session.query(BuildingAuditEvent)
                .filter(
                    BuildingAuditEvent.entity_id.in_(
                        [inquiry.id, reservation.id, tour.id]
                    )
                )
            }
            self.assertTrue({
                "created_from_tour_inquiry",
                "tour_scheduled_from_inquiry",
                "status_changed",
                "tour_handoff_completed",
            }.issubset(actions))

    def test_02_exact_replay_returns_original_and_changed_retry_is_rejected(self) -> None:
        inquiry_id = self._inquiry("replay")
        payload = self._payload(inquiry_id)
        first = self._handoff(payload, "tour-handoff-replay")
        second = self._handoff(payload, "tour-handoff-replay")
        self.assertEqual(first.status_code, 201, first.text)
        self.assertEqual(second.status_code, 201, second.text)
        self.assertTrue(second.json()["replayed"])
        self.assertEqual(
            first.json()["reservation"]["id"], second.json()["reservation"]["id"]
        )
        self.assertEqual(first.json()["tour"]["id"], second.json()["tour"]["id"])
        changed = self._handoff(
            {**payload, "duration_minutes": 60}, "tour-handoff-replay"
        )
        self.assertEqual(changed.status_code, 409, changed.text)
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation)
                .filter_by(inquiry_id=inquiry_id)
                .count(),
                1,
            )
            reservation = (
                session.query(BuildingReservation)
                .filter_by(inquiry_id=inquiry_id)
                .one()
            )
            self.assertEqual(
                session.query(BuildingTour)
                .filter_by(reservation_id=reservation.id)
                .count(),
                1,
            )
            self.assertEqual(
                session.query(BuildingEventLifecycleCommand)
                .filter_by(idempotency_key="tour-handoff-replay")
                .count(),
                1,
            )

    def test_03_invalid_subject_contact_selection_and_fields_fail_closed(self) -> None:
        inactive = self._inquiry("inactive", contact_status="inactive")
        event = self._inquiry("event-kind", kind="event")
        cases = [
            (
                self._payload(inactive),
                "tour-handoff-inactive",
                409,
            ),
            (
                self._payload(event),
                "tour-handoff-event-kind",
                422,
            ),
            (
                self._payload(
                    self._inquiry("mismatch"),
                    offering_id="other-offering",
                    space_id="tour-space",
                ),
                "tour-handoff-mismatch",
                422,
            ),
            (
                self._payload(
                    self._inquiry("past"),
                    scheduled_at=(
                        datetime.now(timezone.utc) - timedelta(hours=1)
                    ).isoformat(),
                ),
                "tour-handoff-past",
                422,
            ),
            (
                self._payload(self._inquiry("short"), duration_minutes=5),
                "tour-handoff-short",
                422,
            ),
            (
                self._payload(self._inquiry("hostless"), host=""),
                "tour-handoff-hostless",
                422,
            ),
        ]
        for payload, key, expected in cases:
            with self.subTest(key=key):
                response = self._handoff(payload, key)
                self.assertEqual(response.status_code, expected, response.text)
        with self.factory() as session:
            ids = [payload["inquiry_id"] for payload, _, _ in cases]
            self.assertEqual(
                session.query(BuildingReservation)
                .filter(BuildingReservation.inquiry_id.in_(ids))
                .count(),
                0,
            )

    def test_04_inventory_and_host_conflicts_fail_without_partial_state(self) -> None:
        inventory_inquiry = self._inquiry("inventory-conflict")
        inventory_payload = self._payload(inventory_inquiry)
        start = datetime.fromisoformat(inventory_payload["scheduled_at"])
        with self.factory() as session:
            session.add(BuildingAvailabilityBlock(
                id="tour-conflict-block",
                space_id="tour-space",
                state="booked",
                starts_at=start - timedelta(minutes=15),
                ends_at=start + timedelta(hours=1),
                source="test",
                source_reference="test-conflict",
                created_by="test",
            ))
            session.commit()
        blocked = self._handoff(
            inventory_payload, "tour-handoff-inventory-conflict"
        )
        self.assertEqual(blocked.status_code, 409, blocked.text)
        with self.factory() as session:
            session.query(BuildingAvailabilityBlock).filter_by(
                id="tour-conflict-block"
            ).delete()
            session.commit()

        host_one = self._inquiry("host-one")
        host_payload = self._payload(host_one, host="shared-host@example.com")
        self.assertEqual(
            self._handoff(host_payload, "tour-handoff-host-one").status_code,
            201,
        )
        host_two = self._inquiry("host-two")
        host_conflict = self._handoff(
            self._payload(
                host_two,
                scheduled_at=(
                    datetime.fromisoformat(host_payload["scheduled_at"])
                    + timedelta(minutes=15)
                ).isoformat(),
                host="shared-host@example.com",
            ),
            "tour-handoff-host-two",
        )
        self.assertEqual(host_conflict.status_code, 409, host_conflict.text)
        with self.factory() as session:
            self.assertIsNone(
                session.query(BuildingReservation)
                .filter_by(inquiry_id=host_two)
                .one_or_none()
            )

    def test_05_unexpected_failure_rolls_back_every_handoff_write(self) -> None:
        inquiry_id = self._inquiry("rollback")
        with mock.patch(
            "sales_support_agent.api.building_booking_router._tour_payload",
            side_effect=RuntimeError("forced response failure"),
        ):
            with self.assertRaises(RuntimeError):
                self._handoff(
                    self._payload(inquiry_id), "tour-handoff-rollback"
                )
        with self.factory() as session:
            self.assertIsNone(
                session.query(BuildingReservation)
                .filter_by(inquiry_id=inquiry_id)
                .one_or_none()
            )
            self.assertEqual(
                session.query(BuildingEventLifecycleCommand)
                .filter_by(idempotency_key="tour-handoff-rollback")
                .count(),
                0,
            )
            self.assertNotIn(
                "_tour_handoff",
                session.get(BuildingInquiry, inquiry_id).payload_json,
            )

    def test_06_admin_action_is_scoped_rbac_and_csrf_protected(self) -> None:
        inquiry_id = self._inquiry("admin")
        page = self.client.get("/admin/building")
        self.assertIn(
            f"/admin/building/inquiries/{inquiry_id}/schedule-tour", page.text
        )
        self.assertIn("Tour starts (Mountain time)", page.text)
        self.assertIn("Creates no hold", page.text)
        form_data = {
            "_csrf_token": self._csrf(),
            "offering_id": "tour-offering",
            "space_id": "tour-space",
            "scheduled_at": (
                datetime.now() + timedelta(days=8)
            ).strftime("%Y-%m-%dT%H:%M"),
            "duration_minutes": "30",
            "host": "admin-host@example.com",
            "meeting_location": "Anata Building lobby",
            "notes": "",
            "idempotency_key": f"tour-inquiry:{inquiry_id}:v1",
        }
        limited = {
            "email": "limited@example.com",
            "permissions": {"building.manage"},
            "is_superadmin": False,
            "session_issued_at": "",
        }
        with mock.patch(
            "sales_support_agent.services.auth_deps.get_current_user",
            return_value=limited,
        ):
            forbidden = self.client.post(
                f"/admin/building/inquiries/{inquiry_id}/schedule-tour",
                headers=self.browser_headers,
                data=form_data,
                follow_redirects=False,
            )
        self.assertEqual(forbidden.status_code, 403, forbidden.text)

        invalid_csrf = self.client.post(
            f"/admin/building/inquiries/{inquiry_id}/schedule-tour",
            headers=self.browser_headers,
            data={**form_data, "_csrf_token": "invalid"},
            follow_redirects=False,
        )
        self.assertEqual(invalid_csrf.status_code, 403, invalid_csrf.text)
        success = self.client.post(
            f"/admin/building/inquiries/{inquiry_id}/schedule-tour",
            headers=self.browser_headers,
            data=form_data,
            follow_redirects=False,
        )
        self.assertEqual(success.status_code, 303, success.text)
        self.assertIn("notice=", success.headers["location"])


if __name__ == "__main__":
    unittest.main()
