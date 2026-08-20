"""Taking a lead off the board without taking a contract with it.

"Delete" has to mean something honest. A lead nobody acted on really goes. A
lead that produced a booking or a contract is the top of a paper trail the
billing and agreement records still read, so it comes off the list and stays in
the ledger. The page says which of the two will happen before you press.
"""

from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/lead_removal_boot.db",
)
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "lead-removal-secret")

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class LeadRemovalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"lead_removal_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_factory = app.state.session_factory
        cls.original_settings = app.state.settings
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="removal-key",
            building_campaign_token_secret="removal-csrf",
        )
        cls.client = TestClient(app)
        cls.client.cookies.set(
            app.state.agent_settings.admin_cookie_name,
            create_user_session_token(
                app.state.agent_settings,
                email="david@anatainc.com",
                name="David",
                role="admin",
            ),
        )
        cls.headers = {"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"}
        now = datetime.now(timezone.utc)
        with cls.factory() as session:
            session.add_all([
                BuildingSpace(
                    id="sp", slug="sp", name="The Arena", space_type="event",
                    capacity=200, status="available",
                ),
                BuildingOffering(
                    id="off", slug="off", name="Event", offering_type="event",
                    space_id="sp",
                ),
                BuildingRatePlan(
                    id="plan-v1", offering_id="off", version=1, name="Standard",
                    status="approved", currency="USD", unit_amount_cents=17_500,
                    minimum_units=6, deposit_type="percent",
                    deposit_percent_bps=5_000, cancellation_policy="NR.",
                    tax_status="non_taxable", effective_from=date.today(),
                    approval_evidence="owner 2026",
                ),
                BuildingAgreementTemplate(
                    id="tpl-v1", template_key="event-agreement", version=1,
                    name="Event agreement", status="approved",
                    contract_type="event", body_markdown="{{customer_name}}",
                    clauses_json=[], merge_fields_json=["customer_name"],
                    approved_by="david@anatainc.com", approved_at=now,
                ),
                BuildingContact(
                    id="c1", email="rosa@example.com", full_name="Rosa Delgado",
                    status="active",
                ),
            ])
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.session_factory = cls.original_factory
        app.state.settings = cls.original_settings

    def _csrf(self) -> str:
        page = self.client.get("/admin/building/sales")
        return re.search(r'name="_csrf_token" value="([^"]+)"', page.text).group(1)

    def _lead(self, lead_id: str, *, name: str, email: str, source: str = "anata-building") -> None:
        with self.factory() as session:
            session.add(BuildingInquiry(
                id=lead_id, idempotency_key=f"{lead_id}-key", kind="event",
                name=name, email=email, source=source,
                preferred_date=date.today() + timedelta(days=60),
                payload_json={"_lifecycle": {"stage": "new"}},
            ))
            session.add(BuildingRelationship(
                id=f"rel-{lead_id}", contact_id="c1", relationship_type="prospect",
                status="active", source_reference=f"inquiry:{lead_id}",
            ))
            session.commit()

    def _remove(self, lead_id: str):
        return self.client.post(
            f"/admin/building/inquiries/{lead_id}/remove",
            headers=self.headers, data={"_csrf_token": self._csrf()},
            follow_redirects=False,
        )

    def test_01_a_lead_with_nothing_attached_is_really_deleted(self) -> None:
        self._lead("junk-1", name="Nobody Acted", email="junk@example.com")
        removed = self._remove("junk-1")
        self.assertEqual(removed.status_code, 303, removed.text)
        self.assertIn("deleted", removed.headers["location"])
        with self.factory() as session:
            self.assertIsNone(session.get(BuildingInquiry, "junk-1"))
            # The record of the deletion outlives the record.
            self.assertEqual(
                session.query(BuildingAuditEvent).filter_by(
                    entity_id="junk-1", action="lead_deleted"
                ).count(),
                1,
            )

    def test_02_a_lead_with_a_booking_is_kept_but_taken_off_the_list(self) -> None:
        """Destroying this would strand the booking that still reads it."""
        self._lead("busy-1", name="Has A Booking", email="busy@example.com")
        starts = datetime.now(timezone.utc) + timedelta(days=60)
        with self.factory() as session:
            session.add(BuildingReservation(
                id="res-busy", kind="event", status="soft_hold",
                inquiry_id="busy-1", contact_id="c1", space_id="sp",
                starts_at=starts, guest_starts_at=starts + timedelta(hours=2),
                guest_ends_at=starts + timedelta(hours=7),
                ends_at=starts + timedelta(hours=9), attendance=100,
                deposit_required=True, created_by="test",
            ))
            session.commit()
        removed = self._remove("busy-1")
        self.assertEqual(removed.status_code, 303, removed.text)
        self.assertIn("off+your+list", removed.headers["location"])
        with self.factory() as session:
            self.assertIsNotNone(session.get(BuildingInquiry, "busy-1"))
            self.assertIsNotNone(session.get(BuildingReservation, "res-busy"))
        listing = self.client.get("/admin/building/sales")
        self.assertNotIn("busy-1", listing.text)

    def test_03_the_page_says_which_it_will_do_before_you_press(self) -> None:
        self._lead("warn-1", name="Nothing Yet", email="warn@example.com")
        page = self.client.get("/admin/building/inquiries/warn-1")
        self.assertIn("Remove this lead", page.text)
        self.assertIn("it is deleted for good", page.text)

    def test_04_a_removed_lead_can_be_put_back(self) -> None:
        self._lead("back-1", name="Comes Back", email="back@example.com")
        starts = datetime.now(timezone.utc) + timedelta(days=75)
        with self.factory() as session:
            session.add(BuildingReservation(
                id="res-back", kind="event", status="soft_hold",
                inquiry_id="back-1", contact_id="c1", space_id="sp",
                starts_at=starts, guest_starts_at=starts + timedelta(hours=2),
                guest_ends_at=starts + timedelta(hours=7),
                ends_at=starts + timedelta(hours=9), attendance=80,
                deposit_required=True, created_by="test",
            ))
            session.commit()
        self._remove("back-1")
        self.assertNotIn("back-1", self.client.get("/admin/building/sales").text)
        restored = self.client.post(
            "/admin/building/inquiries/back-1/restore",
            headers=self.headers, data={"_csrf_token": self._csrf()},
            follow_redirects=False,
        )
        self.assertEqual(restored.status_code, 303, restored.text)
        self.assertNotIn("error=", restored.headers["location"])
        self.assertIn("back-1", self.client.get("/admin/building/sales").text)

    def test_05_removing_twice_is_refused_not_repeated(self) -> None:
        self._lead("twice-1", name="Twice Removed", email="twice@example.com")
        starts = datetime.now(timezone.utc) + timedelta(days=90)
        with self.factory() as session:
            session.add(BuildingReservation(
                id="res-twice", kind="event", status="soft_hold",
                inquiry_id="twice-1", contact_id="c1", space_id="sp",
                starts_at=starts, guest_starts_at=starts + timedelta(hours=2),
                guest_ends_at=starts + timedelta(hours=7),
                ends_at=starts + timedelta(hours=9), attendance=60,
                deposit_required=True, created_by="test",
            ))
            session.commit()
        self._remove("twice-1")
        again = self._remove("twice-1")
        self.assertIn("error=", again.headers["location"])

    def test_06_clearing_test_leads_spares_real_prospects(self) -> None:
        """The one that matters: a sweep must not touch a paying customer."""
        self._lead(
            "qa-1", name="Arena QA", email="building+qa@anatainc.com",
            source="production_qa",
        )
        self._lead(
            "real-1", name="Elisa Edwards", email="qeedwards@gmail.com",
            source="eventective",
        )
        cleared = self.client.post(
            "/admin/building/test-leads/remove",
            headers=self.headers, data={"_csrf_token": self._csrf()},
            follow_redirects=False,
        )
        self.assertEqual(cleared.status_code, 303, cleared.text)
        with self.factory() as session:
            self.assertIsNone(session.get(BuildingInquiry, "qa-1"))
            self.assertIsNotNone(
                session.get(BuildingInquiry, "real-1"),
                "a real prospect must survive a test sweep",
            )


if __name__ == "__main__":
    unittest.main()
