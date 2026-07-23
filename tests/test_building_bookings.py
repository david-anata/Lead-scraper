from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/building_booking_boot.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAgreement,
        BuildingAvailabilityBlock,
        BuildingDepositEvidence,
        BuildingReservation,
    )
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingBookingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_booking_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-test-key"}
        cls.start = datetime.now(timezone.utc) + timedelta(days=14)
        response = cls.client.put(
            "/api/internal/building/spaces/arena",
            headers=cls.headers,
            json={
                "id": "arena",
                "slug": "the-arena",
                "name": "The Arena",
                "space_type": "event",
                "capacity": 120,
                "status": "available",
                "is_public": True,
            },
        )
        if response.status_code != 200:
            raise AssertionError(response.text)

    def _create(self, reservation_id: str, *, attendance: int = 80, deposit_required: bool = True):
        return self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "id": reservation_id,
                "kind": "event",
                "space_id": "arena",
                "starts_at": self.start.isoformat(),
                "ends_at": (self.start + timedelta(hours=4)).isoformat(),
                "attendance": attendance,
                "deposit_required": deposit_required,
                "actor": "operator@example.com",
            },
        )

    def _transition(self, reservation_id: str, status: str, **extra):
        return self.client.post(
            f"/api/internal/building/bookings/{reservation_id}/transition",
            headers=self.headers,
            json={
                "target_status": status,
                "actor": "operator@example.com",
                **extra,
            },
        )

    def test_00_capacity_and_time_window_are_enforced(self) -> None:
        too_large = self._create("event-too-large", attendance=121)
        self.assertEqual(too_large.status_code, 422)
        invalid = self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "kind": "event",
                "space_id": "arena",
                "starts_at": self.start.isoformat(),
                "ends_at": (self.start - timedelta(hours=1)).isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(invalid.status_code, 422)

    def test_01_event_hold_rejects_conflicts(self) -> None:
        first = self._create("event-one")
        second = self._create("event-two")
        self.assertEqual(first.status_code, 201, first.text)
        self.assertEqual(second.status_code, 201, second.text)
        self.assertEqual(
            self._transition("event-one", "requirements_review").status_code, 200
        )
        hold = self._transition(
            "event-one",
            "soft_hold",
            hold_expires_at=(datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
        )
        self.assertEqual(hold.status_code, 200, hold.text)
        self.assertEqual(
            self._transition("event-two", "requirements_review").status_code, 200
        )
        conflict = self._transition(
            "event-two",
            "soft_hold",
            hold_expires_at=(datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        )
        self.assertEqual(conflict.status_code, 409)
        with self.factory() as session:
            block = session.query(BuildingAvailabilityBlock).one()
            self.assertEqual(block.state, "soft_hold")

    def test_02_confirmation_requires_agreement_and_deposit_evidence(self) -> None:
        self.assertEqual(self._transition("event-one", "quote_sent").status_code, 200)
        self.assertEqual(self._transition("event-one", "contract_pending").status_code, 200)
        blocked = self._transition("event-one", "confirmed")
        self.assertEqual(blocked.status_code, 409)
        no_evidence = self.client.post(
            "/api/internal/building/bookings/event-one/agreements",
            headers=self.headers,
            json={
                "status": "signed",
                "version": 1,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(no_evidence.status_code, 422)
        agreement = self.client.post(
            "/api/internal/building/bookings/event-one/agreements",
            headers=self.headers,
            json={
                "status": "signed",
                "version": 1,
                "provider": "test-sign",
                "provider_reference": "agreement-123",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(agreement.status_code, 201, agreement.text)
        self.assertEqual(self._transition("event-one", "deposit_due").status_code, 200)
        still_blocked = self._transition("event-one", "confirmed")
        self.assertEqual(still_blocked.status_code, 409)
        deposit = self.client.post(
            "/api/internal/building/bookings/event-one/deposit-evidence",
            headers=self.headers,
            json={
                "status": "paid",
                "amount_cents": 50000,
                "provider": "stripe",
                "provider_reference": "pi_test_123",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(deposit.status_code, 201, deposit.text)
        confirmed = self._transition("event-one", "confirmed")
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        self.assertEqual(confirmed.json()["reservation"]["status"], "confirmed")
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "event-one")
            self.assertEqual(reservation.agreement_status, "signed")
            self.assertEqual(reservation.deposit_status, "paid")
            self.assertEqual(session.query(BuildingAgreement).count(), 1)
            self.assertEqual(session.query(BuildingDepositEvidence).count(), 1)
            self.assertEqual(session.query(BuildingAvailabilityBlock).one().state, "booked")

    def test_03_cancellation_releases_inventory(self) -> None:
        cancelled = self._transition("event-one", "cancelled", reason="Customer cancelled")
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        with self.factory() as session:
            self.assertEqual(session.query(BuildingAvailabilityBlock).count(), 0)
        replacement_hold = self._transition(
            "event-two",
            "soft_hold",
            hold_expires_at=(datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        )
        self.assertEqual(replacement_hold.status_code, 200, replacement_hold.text)

    def test_04_invalid_transition_cannot_skip_workflow(self) -> None:
        third = self._create("event-three")
        self.assertEqual(third.status_code, 201, third.text)
        skipped = self._transition("event-three", "confirmed")
        self.assertEqual(skipped.status_code, 409)
