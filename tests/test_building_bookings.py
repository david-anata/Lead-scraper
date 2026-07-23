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
        BuildingProposal,
        BuildingReservation,
        BuildingTour,
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
        response = cls.client.put(
            "/api/internal/building/offerings/arena-events",
            headers=cls.headers,
            json={
                "id": "arena-events",
                "slug": "arena-events",
                "name": "Arena events",
                "offering_type": "event",
                "space_id": "arena",
                "is_published": True,
            },
        )
        if response.status_code != 200:
            raise AssertionError(response.text)
        response = cls.client.put(
            "/api/internal/building/offerings/arena-events/rate-plans/arena-rate-v1",
            headers=cls.headers,
            json={
                "id": "arena-rate-v1",
                "version": 1,
                "name": "Arena event rate",
                "status": "approved",
                "unit_amount_cents": 250000,
                "public_price_display": "From $2,500",
                "booking_unit": "event",
                "deposit_type": "percent",
                "deposit_percent_bps": 5000,
                "cancellation_policy": "Deposit is non-refundable within 30 days.",
                "effective_from": "2020-01-01",
                "approved_by": "approver@example.com",
                "actor": "operator@example.com",
            },
        )
        if response.status_code != 200:
            raise AssertionError(response.text)
        response = cls.client.put(
            "/api/internal/building/spaces/tour-office",
            headers=cls.headers,
            json={
                "id": "tour-office",
                "slug": "tour-office",
                "name": "Tour Office",
                "space_type": "private_office",
                "capacity": 6,
                "status": "available",
                "is_public": False,
            },
        )
        if response.status_code != 200:
            raise AssertionError(response.text)

    def _create(self, reservation_id: str, *, attendance: int = 80, deposit_required: bool = True, offering_id: str | None = None):
        return self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "id": reservation_id,
                "kind": "event",
                "space_id": "arena",
                "offering_id": offering_id,
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

    def _proposal(self, reservation_id: str, status: str, *, version: int = 1, amount_cents: int = 250000, document_url: str = "https://example.com/quote-v1.pdf", rate_plan_id: str | None = None):
        return self.client.post(
            f"/api/internal/building/bookings/{reservation_id}/proposals",
            headers=self.headers,
            json={
                "version": version,
                "status": status,
                "proposal_type": "quote",
                "amount_cents": amount_cents,
                "rate_plan_id": rate_plan_id,
                "line_items": [{"description": "Event package", "amount_cents": amount_cents}],
                "terms_summary": "Four-hour event package.",
                "valid_until": (self.start.date() - timedelta(days=1)).isoformat(),
                "document_url": document_url,
                "approved_by": "approver@example.com" if status == "approved" else "",
                "actor": "operator@example.com",
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
        blocked_quote = self._transition("event-one", "quote_sent")
        self.assertEqual(blocked_quote.status_code, 409)
        self.assertEqual(self._proposal("event-one", "draft").status_code, 201)
        self.assertEqual(self._proposal("event-one", "approved").status_code, 201)
        self.assertEqual(self._proposal("event-one", "sent").status_code, 201)
        self.assertEqual(self._transition("event-one", "quote_sent").status_code, 200)
        blocked_contract = self._transition("event-one", "contract_pending")
        self.assertEqual(blocked_contract.status_code, 409)
        self.assertEqual(self._proposal("event-one", "accepted").status_code, 201)
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
            self.assertEqual(session.query(BuildingProposal).count(), 1)
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

    def test_05_analytics_uses_stage_history_after_booking_moves_on(self) -> None:
        response = self.client.get(
            "/api/internal/building/analytics",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        analytics = response.json()
        self.assertEqual(analytics["event_funnel"]["holds"], 2)
        self.assertEqual(analytics["event_funnel"]["quotes"], 1)
        self.assertEqual(analytics["event_funnel"]["signed"], 1)
        self.assertEqual(analytics["event_funnel"]["deposits"], 1)
        self.assertEqual(analytics["event_funnel"]["confirmed"], 1)
        self.assertEqual(analytics["operations"]["holds_started"], 2)

    def test_06_sent_proposal_content_is_immutable_and_revisions_use_new_version(self) -> None:
        changed = self._proposal(
            "event-one", "accepted", amount_cents=300000
        )
        self.assertEqual(changed.status_code, 409)
        new_draft = self._proposal(
            "event-one",
            "draft",
            version=2,
            amount_cents=300000,
            document_url="https://example.com/quote-v2.pdf",
        )
        self.assertEqual(new_draft.status_code, 201, new_draft.text)
        response = self.client.get(
            "/api/internal/building/bookings/event-one/proposals",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([item["version"] for item in response.json()["proposals"]], [2, 1])

    def test_07_tour_requires_evidence_and_never_blocks_inventory(self) -> None:
        created = self.client.post(
            "/api/internal/building/bookings",
            headers=self.headers,
            json={
                "id": "workspace-tour",
                "kind": "workspace",
                "space_id": "tour-office",
                "starts_at": self.start.isoformat(),
                "ends_at": (self.start + timedelta(days=365)).isoformat(),
                "deposit_required": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(created.status_code, 201, created.text)
        self.assertEqual(self._transition("workspace-tour", "qualified").status_code, 200)
        blocked = self._transition("workspace-tour", "tour_scheduled")
        self.assertEqual(blocked.status_code, 409)
        with self.factory() as session:
            block_count = session.query(BuildingAvailabilityBlock).count()
        scheduled_at = datetime.now(timezone.utc) + timedelta(days=3)
        scheduled = self.client.post(
            "/api/internal/building/bookings/workspace-tour/tours",
            headers=self.headers,
            json={
                "id": "tour-one",
                "scheduled_at": scheduled_at.isoformat(),
                "duration_minutes": 45,
                "host": "host@example.com",
                "meeting_location": "Front lobby",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(scheduled.status_code, 201, scheduled.text)
        self.assertEqual(self._transition("workspace-tour", "tour_scheduled").status_code, 200)
        with self.factory() as session:
            self.assertEqual(session.query(BuildingAvailabilityBlock).count(), block_count)
        missing_reason = self.client.put(
            "/api/internal/building/bookings/tours/tour-one",
            headers=self.headers,
            json={
                "scheduled_at": (scheduled_at + timedelta(days=1)).isoformat(),
                "duration_minutes": 45,
                "status": "scheduled",
                "host": "host@example.com",
                "meeting_location": "Front lobby",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(missing_reason.status_code, 422)
        rescheduled_at = scheduled_at + timedelta(days=1)
        rescheduled = self.client.put(
            "/api/internal/building/bookings/tours/tour-one",
            headers=self.headers,
            json={
                "scheduled_at": rescheduled_at.isoformat(),
                "duration_minutes": 45,
                "status": "scheduled",
                "host": "host@example.com",
                "meeting_location": "Front lobby",
                "reason": "Customer requested a later date",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(rescheduled.status_code, 200, rescheduled.text)
        incomplete = self.client.put(
            "/api/internal/building/bookings/tours/tour-one",
            headers=self.headers,
            json={
                "scheduled_at": rescheduled_at.isoformat(),
                "duration_minutes": 45,
                "status": "completed",
                "host": "host@example.com",
                "meeting_location": "Front lobby",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(incomplete.status_code, 422)
        completed = self.client.put(
            "/api/internal/building/bookings/tours/tour-one",
            headers=self.headers,
            json={
                "scheduled_at": rescheduled_at.isoformat(),
                "duration_minutes": 45,
                "status": "completed",
                "host": "host@example.com",
                "meeting_location": "Front lobby",
                "outcome": "good_fit",
                "next_step": "Prepare office proposal",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(completed.status_code, 200, completed.text)
        workflow = self._transition("workspace-tour", "tour_completed")
        self.assertEqual(workflow.status_code, 200, workflow.text)
        terminal_edit = self.client.put(
            "/api/internal/building/bookings/tours/tour-one",
            headers=self.headers,
            json={
                "scheduled_at": rescheduled_at.isoformat(),
                "duration_minutes": 30,
                "status": "scheduled",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(terminal_edit.status_code, 409)
        with self.factory() as session:
            tour = session.get(BuildingTour, "tour-one")
            self.assertEqual(tour.outcome, "good_fit")

    def test_08_proposal_snapshots_the_effective_approved_rate_plan(self) -> None:
        created = self._create("event-rate-snapshot", offering_id="arena-events")
        self.assertEqual(created.status_code, 201, created.text)
        draft = self._proposal(
            "event-rate-snapshot",
            "draft",
            rate_plan_id="arena-rate-v1",
        )
        self.assertEqual(draft.status_code, 201, draft.text)
        with self.factory() as session:
            proposal = session.query(BuildingProposal).filter(
                BuildingProposal.reservation_id == "event-rate-snapshot"
            ).one()
            self.assertEqual(proposal.rate_plan_id, "arena-rate-v1")
            self.assertEqual(
                proposal.rate_plan_snapshot_json["deposit_percent_bps"],
                5000,
            )
            self.assertEqual(
                proposal.rate_plan_snapshot_json["cancellation_policy"],
                "Deposit is non-refundable within 30 days.",
            )
