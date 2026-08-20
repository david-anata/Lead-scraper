from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_booking_workspace_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-booking-workspace-session",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingCalendarProjection,
    BuildingContact,
    BuildingInquiry,
    BuildingOperationalChecklist,
    BuildingOperationalChecklistItem,
    BuildingProposal,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token
from sales_support_agent.services.building_booking_workspace import _build_phases


class BuildingBookingWorkspaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            f"building_booking_workspace_{uuid.uuid4().hex}.db",
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="booking-workspace-internal",
            building_campaign_token_secret="booking-workspace-csrf",
        )
        cls.client = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(app.state.agent_settings.admin_cookie_name, token)
        now = datetime.now(timezone.utc)
        with cls.factory() as session:
            session.add(BuildingSpace(
                id="guided-arena",
                slug="guided-arena",
                name="The Arena",
                space_type="event",
                capacity=200,
                status="available",
            ))
            session.add(BuildingContact(
                id="guided-host",
                email="jordan@example.com",
                full_name="Jordan Rivera",
                status="active",
            ))
            session.add(BuildingContact(
                id="unrelated-contact",
                email="private@example.com",
                full_name="Unrelated Private Contact",
                status="active",
            ))
            session.add(BuildingInquiry(
                id="guided-inquiry",
                idempotency_key="guided-inquiry-key",
                kind="event",
                name="Jordan Rivera",
                email="jordan@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            session.add(BuildingReservation(
                id="guided-event",
                kind="event",
                status="soft_hold",
                inquiry_id="guided-inquiry",
                contact_id="guided-host",
                space_id="guided-arena",
                starts_at=now + timedelta(days=30),
                guest_starts_at=now + timedelta(days=30, hours=2),
                guest_ends_at=now + timedelta(days=30, hours=6),
                ends_at=now + timedelta(days=30, hours=8),
                hold_expires_at=now + timedelta(days=3),
                attendance=80,
                agreement_status="not_started",
                deposit_status="not_started",
                deposit_required=True,
                assigned_owner="operator@anatainc.com",
                source="website",
                source_reference="public-event-inquiry",
            ))
            session.add(BuildingProposal(
                id="guided-quote",
                reservation_id="guided-event",
                version=1,
                proposal_type="quote",
                status="draft",
                currency="USD",
                amount_cents=265000,
                line_items_json=[
                    {"type": "pricing_subtotal", "description": "Arena event package", "amount_cents": 250000},
                    {"type": "tax", "description": "Lehi, Utah sales tax", "amount_cents": 15000},
                ],
                rate_plan_id="guided-rate",
                rate_plan_snapshot_json={
                    "name": "Arena standard",
                    "version": 1,
                    "tax_rate_bps": 600,
                    "transaction_date": (now + timedelta(days=30)).date().isoformat(),
                    "pricing_adjustment": {
                        "pricing_subtotal_cents": 250000,
                        "discount_cents": 0,
                        "tax_rate_bps": 600,
                    },
                },
                terms_summary="Frozen reviewed terms.",
            ))
            session.add(BuildingCalendarProjection(
                id="guided-calendar",
                reservation_id="guided-event",
                status="pending",
                desired_action="upsert",
            ))
            session.add(BuildingOperationalChecklist(
                id="guided-checklist",
                reservation_id="guided-event",
                checklist_type="event",
                title="Event readiness",
                status="open",
            ))
            session.add(BuildingOperationalChecklistItem(
                id="guided-checklist-item",
                checklist_id="guided-checklist",
                label="Confirm room setup",
                status="pending",
                is_required=True,
            ))
            session.commit()

    def test_workspace_is_customer_named_guided_and_private(self) -> None:
        page = self.client.get("/admin/building/bookings/guided-event")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertEqual(page.headers["cache-control"], "private, no-store")
        self.assertIn("Jordan Rivera · The Arena", page.text)
        self.assertIn("Do this next", page.text)
        self.assertIn("Finish the customer quote", page.text)
        self.assertIn("Booking journey", page.text)
        self.assertIn("Customer request", page.text)
        self.assertIn("Date and temporary hold", page.text)
        self.assertIn("Required payment", page.text)
        self.assertIn("Booking confirmation", page.text)
        self.assertIn("Event operations", page.text)
        self.assertIn("Authoritative evidence", page.text)
        self.assertIn("No signature success is claimed", page.text)
        self.assertIn("Readiness is not payment", page.text)
        self.assertIn("Creates a copyable link only. Nothing is sent.", page.text)
        self.assertIn("Quote builder", page.text)
        self.assertIn("Customer preview", page.text)
        self.assertIn("Version comparison", page.text)
        self.assertIn("Arena event package", page.text)
        self.assertIn("Transaction date", page.text)
        self.assertIn("Sent versions stay frozen", page.text)
        self.assertNotIn("Unrelated Private Contact", page.text)
        self.assertEqual(page.text.count("<h1>"), 1)
        self.assertEqual(page.text.count('id="agent-main-content"'), 1)

    def test_missing_and_unauthenticated_records_fail_closed(self) -> None:
        missing = self.client.get("/admin/building/bookings/not-real")
        self.assertEqual(missing.status_code, 404)
        guest = TestClient(app)
        denied = guest.get(
            "/admin/building/bookings/guided-event",
            follow_redirects=False,
        )
        self.assertIn(denied.status_code, {302, 303})
        self.assertEqual(denied.headers["location"], "/admin/login")

    def test_current_step_advances_only_from_authoritative_evidence(self) -> None:
        base = {
            "reservation": {
                "id": "guided-event",
                "status": "soft_hold",
                "inquiry_id": "guided-inquiry",
                "contact_id": "guided-host",
                "agreement_status": "not_started",
                "deposit_status": "not_started",
                "deposit_required": True,
            },
            "proposal": {"status": "accepted"},
            "agreement": {"preparation_status": "approved"},
            "payment": {"status": "approved"},
        }
        phases, action = _build_phases(base)
        self.assertEqual(phases[3]["state"], "current")
        self.assertEqual(action["title"], "Prepare or finish the agreement.")

        base["reservation"]["agreement_status"] = "signed"
        phases, action = _build_phases(base)
        self.assertEqual(phases[4]["state"], "current")
        self.assertEqual(action["title"], "Prepare billing from the signed booking.")

        base["reservation"]["deposit_status"] = "paid"
        phases, action = _build_phases(base)
        self.assertEqual(phases[5]["state"], "current")
        self.assertEqual(action["title"], "Run the final confirmation gate.")

        base["reservation"]["status"] = "confirmed"
        phases, action = _build_phases(base)
        self.assertEqual(phases[6]["state"], "done")
        self.assertEqual(action["title"], "Complete event operations.")

    def test_existing_control_room_links_each_booking_to_workspace(self) -> None:
        page = self.client.get("/admin/building/bookings")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn(
            'href="/admin/building/bookings/guided-event">View booking</a>',
            page.text,
        )

    def test_terminal_booking_keeps_quote_history_read_only(self) -> None:
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "guided-event")
            reservation.status = "cancelled"
            session.add(reservation)
            session.commit()
        try:
            page = self.client.get("/admin/building/bookings/guided-event")
            self.assertEqual(page.status_code, 200, page.text)
            self.assertIn("Quote history is read-only", page.text)
            self.assertIn('class="booking-quote-fields" disabled', page.text)
            self.assertIn("Release date from Anata Events", page.text)
            self.assertIn(
                'action="/admin/building/inquiries/guided-inquiry/calendar-sync"',
                page.text,
            )
            self.assertIn(
                'name="confirmation" value="SYNC guided-event"', page.text
            )
        finally:
            with self.factory() as session:
                reservation = session.get(BuildingReservation, "guided-event")
                reservation.status = "soft_hold"
                session.add(reservation)
                session.commit()


if __name__ == "__main__":
    unittest.main()
