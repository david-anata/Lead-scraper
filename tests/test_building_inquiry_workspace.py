from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from unittest import mock
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_inquiry_workspace_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-inquiry-workspace-session",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
    BuildingRelationship,
    BuildingOffering,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class BuildingInquiryWorkspaceTests(unittest.TestCase):
    @staticmethod
    def _jordan_payload() -> dict:
        return {
            "eventType": "Company celebration",
            "groupSize": "85",
            "alternateDate": "2026-09-19",
            "backupDate2": "2026-09-26",
            "dateFlexibility": "Same month is workable",
            "accessStartTime": "14:00",
            "accessEndTime": "23:00",
            "avNeeds": "Presentation display",
            "accessibilityNeeds": "Step-free guest route",
            "vendorPlan": "Caterer only",
            "tourInterest": "Yes, please contact me",
            "notes": "Need a stage and accessible seating.",
            "_lifecycle": {"stage": "new"},
            "_lead_notification": {"status": "delivered"},
            "_customer_receipt": {"status": "sent"},
        }

    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_inquiry_workspace_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="inquiry-workspace-internal",
            building_campaign_token_secret="inquiry-workspace-csrf",
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
            # The date calendar books against a real event offering, so the
            # workspace needs one to show anything to pick.
            session.add(BuildingSpace(
                id="sp", slug="sp", name="Arena", space_type="event",
                capacity=200, status="available",
            ))
            session.add(BuildingOffering(
                id="off", slug="off", name="Event", offering_type="event",
                space_id="sp",
            ))
            session.add(BuildingContact(
                id="jordan-contact",
                email="jordan@example.com",
                phone="801-555-0142",
                full_name="Jordan Rivera",
            ))
            session.add(BuildingRelationship(
                id="jordan-inquiry-relationship",
                contact_id="jordan-contact",
                relationship_type="event_host",
                source_reference="inquiry:jordan-inquiry",
            ))
            session.add(BuildingInquiry(
                id="jordan-inquiry",
                idempotency_key="jordan-inquiry-key",
                kind="event",
                source="anata-building",
                source_reference="event-inquiry",
                name="Jordan Rivera",
                email="jordan@example.com",
                preferred_date=date.today() + timedelta(days=30),
                assigned_owner="building@anatainc.com",
                response_due_at=now - timedelta(hours=1),
                payload_json=cls._jordan_payload(),
            ))
            session.add(BuildingInquiry(
                id="internal-qa-inquiry",
                idempotency_key="internal-qa-inquiry-key",
                kind="event",
                source="production_qa",
                name="Production QA Test",
                email="building+qa@anatainc.com",
                payload_json={
                    "_lifecycle": {"stage": "new"},
                    "_lead_notification": {"status": "failed"},
                    "_customer_receipt": {"status": "not_configured"},
                },
            ))
            session.add(BuildingAuditEvent(
                entity_type="inquiry",
                entity_id="jordan-inquiry",
                action="inquiry_created",
                actor="public-site",
            ))
            session.commit()

    def setUp(self) -> None:
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "jordan-inquiry")
            inquiry.payload_json = self._jordan_payload()
            session.add(inquiry)
            session.commit()

    def test_workspace_preserves_submission_and_excludes_other_records(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertEqual(page.headers["cache-control"], "private, no-store")
        self.assertIn("Jordan Rivera", page.text)
        self.assertIn("Company celebration", page.text)
        self.assertIn("Estimated attendance", page.text)
        self.assertIn("Third-choice date", page.text)
        self.assertIn("Date flexibility", page.text)
        self.assertIn("Setup or vendor access begins", page.text)
        self.assertIn("Accessibility needs", page.text)
        self.assertIn("Need a stage and accessible seating.", page.text)
        self.assertIn("Do this next", page.text)
        self.assertIn("Record response", page.text)
        self.assertIn('href="mailto:jordan@example.com"', page.text)
        self.assertIn("801-555-0142", page.text)
        self.assertIn("Staff Slack alert", page.text)
        self.assertNotIn("Production QA Test", page.text)

    def test_sales_defaults_to_prospects_and_supports_search_and_test_scope(self) -> None:
        default = self.client.get("/admin/building/sales")
        self.assertEqual(default.status_code, 200, default.text)
        self.assertIn("Lead queue", default.text)
        self.assertIn("Website inquiries and staff-added leads", default.text)
        self.assertIn("Add a lead", default.text)
        self.assertIn("Adds the lead to the queue below", default.text)
        self.assertNotIn("Quick staff inquiry", default.text)
        self.assertNotIn("Start a booking workflow", default.text)
        self.assertIn("Jordan Rivera", default.text)
        self.assertNotIn("Production QA Test", default.text)
        self.assertIn('href="/admin/building/inquiries/jordan-inquiry"', default.text)
        self.assertIn("recognized test records are hidden", default.text)

        tests = self.client.get("/admin/building/sales?lead_scope=test&lead_status=all")
        self.assertIn("Production QA Test", tests.text)
        self.assertNotIn(
            'href="/admin/building/inquiries/jordan-inquiry"', tests.text
        )

        missing = self.client.get("/admin/building/sales?q=not-a-real-prospect")
        self.assertNotIn(
            'href="/admin/building/inquiries/jordan-inquiry"', missing.text
        )

    def test_today_links_directly_to_the_customer_workspace(self) -> None:
        page = self.client.get("/admin/building")
        self.assertIn('href="/admin/building/inquiries/jordan-inquiry"', page.text)

    def test_failed_delivery_evidence_has_permissioned_retry_actions(self) -> None:
        page = self.client.get("/admin/building/inquiries/internal-qa-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Retry staff alert", page.text)
        self.assertIn("Retry acknowledgement", page.text)
        self.assertIn('name="_csrf_token"', page.text)

    def test_event_interview_is_structured_prefilled_and_missing_first(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Event and timing", page.text)
        self.assertIn("People and decision", page.text)
        self.assertIn("Call guide", page.text)
        self.assertIn("2026-09-19; 2026-09-26", page.text)
        self.assertIn("14:00–23:00", page.text)
        self.assertIn("Presentation display", page.text)
        self.assertIn("Step-free guest route", page.text)
        self.assertIn("Caterer only", page.text)
        self.assertIn("is-missing", page.text)
        self.assertIn("Changes save automatically", page.text)

    def test_interview_autosave_persists_without_marking_reviewed(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(token)
        response = self.client.post(
            "/admin/building/inquiries/jordan-inquiry/event-interview",
            data={
                "_csrf_token": token.group(1),
                "event_purpose": "Celebrate the product launch",
            },
            headers={
                "Origin": "http://testserver",
                "X-Requested-With": "building-interview-autosave",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"ok": True, "saved": True, "reviewed": False})
        refreshed = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertIn("Celebrate the product launch", refreshed.text)
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "jordan-inquiry")
            self.assertFalse(inquiry.payload_json["_event_interview_meta"]["reviewed"])
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_id="jordan-inquiry", action="event_interview_autosaved"
            ).order_by(BuildingAuditEvent.id.desc()).first()
            self.assertIsNotNone(audit)

    def test_qualification_lists_exact_missing_answers_then_unlocks(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(token)
        responded = self.client.post(
            "/admin/building/inquiries/jordan-inquiry/lifecycle",
            data={
                "_csrf_token": token.group(1),
                "target_stage": "responded",
                "assigned_owner": "building@anatainc.com",
                "channel": "phone",
                "notes": "Customer replied.",
            },
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
        )
        self.assertEqual(responded.status_code, 303, responded.text)
        incomplete = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertIn("Still needed before qualification", incomplete.text)
        # Only what a contract actually needs blocks qualification now. Purpose,
        # format and next step are still asked for, they just no longer stop
        # work, because the package builder never reads them. On this lead that
        # takes the blocking list from four answers down to one.
        blocking = incomplete.text.split(
            "Still needed before qualification"
        )[1].split("</p>")[0]
        self.assertIn("guest schedule", blocking)
        self.assertNotIn("event purpose", blocking)
        self.assertNotIn("event format", blocking)
        self.assertNotIn("agreed next step", blocking)

        token = re.search(r'name="_csrf_token" value="([^"]+)"', incomplete.text)
        saved = self.client.post(
            "/admin/building/inquiries/jordan-inquiry/event-interview",
            data={
                "_csrf_token": token.group(1),
                "save_mode": "reviewed",
                "event_purpose": "Celebrate the product launch",
                "event_format": "Company celebration",
                "candidate_dates": "2026-09-19; 2026-09-26",
                "guest_schedule": "18:00–22:00",
                "access_schedule": "14:00–23:00",
                "attendance": "85",
                "agreed_next_step": "Send the date review by August 6",
            },
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
        )
        self.assertEqual(saved.status_code, 303, saved.text)
        ready = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertIn("Qualify this event request", ready.text)
        self.assertIn("Qualify for date review", ready.text)
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "jordan-inquiry")
            self.assertTrue(inquiry.payload_json["_event_interview_meta"]["reviewed"])

    def test_qualified_inquiry_compares_prefilled_dates_without_creating_hold(self) -> None:
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "jordan-inquiry")
            payload = dict(inquiry.payload_json or {})
            payload["_lifecycle"] = {"stage": "qualified"}
            inquiry.payload_json = payload
            session.add(inquiry)
            session.commit()
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Pick the date", page.text)
        # Every date the prospect asked for stays on the calendar. Replacing the
        # three-date form must not quietly lose their alternates.
        self.assertIn("2026-09-19", page.text)
        self.assertIn("2026-09-26", page.text)
        self.assertIn("Asked for", page.text)
        calendar = mock.Mock(configured=False)
        with mock.patch(
            "sales_support_agent.api.building_inquiry_workspace_router.BuildingGoogleCalendarClient",
            return_value=calendar,
        ):
            result = self.client.get(
                "/admin/building/inquiries/jordan-inquiry/availability",
                params={"dates": "2026-09-19,2026-09-26"},
            )
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(
            [item["status"] for item in result.json()["dates"]],
            ["unknown", "unknown"],
        )
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="jordan-inquiry"
                ).count(),
                0,
            )

    def test_delivered_staff_alert_retry_is_idempotent(self) -> None:
        page = self.client.get("/admin/building/inquiries/jordan-inquiry")
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(token)
        with mock.patch(
            "sales_support_agent.api.building_admin_operations_router.notify_new_building_lead",
            side_effect=AssertionError("Delivered alerts must not be sent twice."),
        ):
            response = self.client.post(
                "/admin/building/inquiries/jordan-inquiry/notify",
                data={"_csrf_token": token.group(1)},
                headers={
                    "Origin": "http://testserver",
                    "Sec-Fetch-Mode": "navigate",
                },
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("already+delivered", response.headers["location"])

    def test_missing_and_unauthenticated_records_fail_closed(self) -> None:
        self.assertEqual(
            self.client.get("/admin/building/inquiries/not-real").status_code, 404
        )
        guest = TestClient(app)
        denied = guest.get(
            "/admin/building/inquiries/jordan-inquiry", follow_redirects=False
        )
        self.assertIn(denied.status_code, {302, 303})
        self.assertEqual(denied.headers["location"], "/admin/login")

    def test_contract_confirmation_collects_a_number_for_legacy_attendance(self) -> None:
        from sales_support_agent.services.building_inquiry_workspace import (
            _confirm_contract_panel,
        )

        html = _confirm_contract_panel(
            {
                "id": "jordan-inquiry",
                "confirm_contract": {
                    "ready": True,
                    "date": "2027-06-17",
                    "label": "Thursday, June 17, 2027",
                    "setup": "6:00 AM",
                    "teardown": "6:00 PM",
                    "guests": "9:00 AM to 3:00 PM",
                    "guest_start": "09:00",
                    "guest_end": "15:00",
                    "clash": "",
                },
            },
            interview={"attendance": "Event"},
            csrf_token="token",
            blockers=[],
        )
        self.assertIn('name="attendance"', html)
        self.assertIn('type="number"', html)
        self.assertIn("Expected attendance", html)
        self.assertIn("Enter the best current estimate", html)

    def test_event_correction_prefers_canonical_arena_offering(self) -> None:
        from sales_support_agent.api.building_inquiry_workspace_router import (
            _event_offering,
        )

        with self.factory() as session:
            session.add_all([
                BuildingOffering(
                    id="arena-event", slug="arena-event", name="The Arena event booking",
                    offering_type="event", space_id="sp",
                ),
                BuildingOffering(
                    id="arena-events", slug="arena-events", name="The Arena event booking",
                    offering_type="event", space_id="sp",
                ),
            ])
            session.commit()
            selected = _event_offering(session, "arena-event")
            self.assertIsNotNone(selected)
            self.assertEqual(selected.id, "arena-events")
            session.delete(session.get(BuildingOffering, "arena-event"))
            session.delete(session.get(BuildingOffering, "arena-events"))
            session.commit()


if __name__ == "__main__":
    unittest.main()


class InquiryContactLinkingTests(unittest.TestCase):
    """Linking an existing customer must never rewrite their saved details.

    The control-room contact form overwrites name, phone, and company on every
    save, so reusing it to attach an existing customer would quietly erase them.
    """

    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"inquiry_contact_link_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_session_factory = app.state.session_factory
        cls.original_settings = app.state.settings
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="inquiry-link-internal",
            building_campaign_token_secret="inquiry-workspace-csrf",
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
        cls.inquiry_id = "link-inquiry"
        cls.existing_contact_id = "existing-customer"
        cls.other_contact_id = "other-customer"
        with cls.factory() as session:
            session.add_all([
                BuildingContact(
                    id=cls.existing_contact_id,
                    email="rosa@example.com",
                    full_name="Rosa Delgado",
                    phone="801-555-0180",
                    status="active",
                ),
                BuildingContact(
                    id=cls.other_contact_id,
                    email="milo@example.com",
                    full_name="Milo Chen",
                    status="active",
                ),
                BuildingInquiry(
                    id=cls.inquiry_id,
                    idempotency_key="link-inquiry-key",
                    kind="event",
                    name="Rosa Delgado",
                    email="rosa@example.com",
                    payload_json={"_lifecycle": {"stage": "responded"}},
                ),
            ])
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.session_factory = cls.original_session_factory
        app.state.settings = cls.original_settings

    def setUp(self) -> None:
        page = self.client.get(f"/admin/building/inquiries/{self.inquiry_id}")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match)
        self.csrf = match.group(1)

    def test_existing_customer_links_without_changing_their_record(self) -> None:
        with self.factory() as session:
            before = session.get(BuildingContact, self.existing_contact_id)
            original = (before.full_name, before.phone, before.email)

        response = self.client.post(
            f"/admin/building/inquiries/{self.inquiry_id}/link-contact",
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
            data={"_csrf_token": self.csrf, "contact_id": self.existing_contact_id},
        )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("notice=", response.headers["location"])

        with self.factory() as session:
            after = session.get(BuildingContact, self.existing_contact_id)
            self.assertEqual((after.full_name, after.phone, after.email), original)
            link = session.query(BuildingRelationship).filter_by(
                source_reference=f"inquiry:{self.inquiry_id}"
            ).one()
            self.assertEqual(link.contact_id, self.existing_contact_id)

    def test_relinking_a_different_customer_is_refused(self) -> None:
        second = self.client.post(
            f"/admin/building/inquiries/{self.inquiry_id}/link-contact",
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
            data={"_csrf_token": self.csrf, "contact_id": self.other_contact_id},
        )
        self.assertEqual(second.status_code, 303, second.text)
        self.assertIn("error=", second.headers["location"])


class LeadPricingRouteTests(unittest.TestCase):
    """Pricing set on one lead must not reach the standard or another lead."""

    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), f"lead_pricing_{uuid.uuid4().hex}.db")
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_session_factory = app.state.session_factory
        cls.original_settings = app.state.settings
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings, building_campaign_token_secret="inquiry-workspace-csrf"
        )
        cls.client = TestClient(app)
        cls.client.cookies.set(
            app.state.agent_settings.admin_cookie_name,
            create_user_session_token(
                app.state.agent_settings, email="david@anatainc.com",
                name="David", role="admin",
            ),
        )
        from sales_support_agent.models.entities import BuildingRatePlan
        from datetime import date as _date
        with cls.factory() as session:
            session.add(BuildingOffering(
                id="off", slug="off", name="Event", offering_type="event", space_id="sp"))
            session.add(BuildingSpace(
                id="sp", slug="sp", name="Arena", space_type="event",
                capacity=200, status="available"))
            session.add(BuildingRatePlan(
                id="plan-v1", offering_id="off", version=1, name="Standard",
                status="approved", currency="USD", unit_amount_cents=17_500,
                minimum_units=6, deposit_type="percent", deposit_percent_bps=5_000,
                effective_from=_date.today()))
            for ident in ("lead-a", "lead-b"):
                session.add(BuildingInquiry(
                    id=ident, idempotency_key=f"{ident}-key", kind="event",
                    name=ident, email=f"{ident}@example.com",
                    payload_json={"_lifecycle": {"stage": "responded"}}))
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.session_factory = cls.original_session_factory
        app.state.settings = cls.original_settings

    def _csrf(self, lead: str) -> str:
        page = self.client.get(f"/admin/building/inquiries/{lead}")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match)
        return match.group(1)

    def test_a_lead_opens_seeded_from_the_standard_rate(self) -> None:
        page = self.client.get("/admin/building/inquiries/lead-a")
        self.assertIn("Pricing for this event", page.text)
        self.assertIn("175.00", page.text)

    def test_every_total_has_a_hook_the_live_preview_can_update(self) -> None:
        """The totals update as an operator types, from a script that finds each
        figure by name. A total added to compute_totals without a matching hook
        would silently stop updating and quietly show a stale number."""
        from sales_support_agent.services.building_lead_pricing import compute_totals

        page = self.client.get("/admin/building/inquiries/lead-a")
        self.assertIn("data-price-form", page.text)
        hooks = set(re.findall(r'data-total="([a-z_]+)"', page.text))
        expected = {
            key.replace("_cents", "")
            .replace("security_deposit", "security")
            .replace("due_to_book", "due")
            for key in compute_totals({})
        }
        self.assertEqual(
            expected - hooks,
            set(),
            "a figure in compute_totals has no hook, so it would never refresh",
        )

    def test_changing_one_lead_leaves_the_standard_and_other_leads_alone(self) -> None:
        from sales_support_agent.models.entities import BuildingRatePlan
        response = self.client.post(
            "/admin/building/inquiries/lead-a/pricing",
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-a"), "hourly_rate": "200",
                  "hours": "8", "cleaning_fee": "250", "discount": "100",
                  "discount_reason": "Repeat customer", "deposit_percent": "25"},
        )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("notice=", response.headers["location"])

        with self.factory() as session:
            priced = session.get(BuildingInquiry, "lead-a").payload_json["_pricing"]
            self.assertEqual(priced["hourly_rate_cents"], 20_000)
            self.assertEqual(priced["discount_reason"], "Repeat customer")
            # The standard is untouched.
            self.assertEqual(
                session.get(BuildingRatePlan, "plan-v1").unit_amount_cents, 17_500)
            # And so is every other lead.
            self.assertEqual(
                session.get(BuildingInquiry, "lead-b").payload_json.get("_pricing"), None)

    def test_a_discount_without_a_reason_is_refused(self) -> None:
        response = self.client.post(
            "/admin/building/inquiries/lead-b/pricing",
            headers={"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-b"), "hourly_rate": "175",
                  "hours": "6", "cleaning_fee": "250", "discount": "500",
                  "discount_reason": "", "deposit_percent": "50"},
        )
        self.assertEqual(response.status_code, 303, response.text)
        self.assertIn("error=", response.headers["location"])
