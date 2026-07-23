from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_admin_operations_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-admin-operations-test-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.integrations.stripe_billing import StripeBillingClient
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingBillingSchedule,
        BuildingAuditEvent,
        BuildingInquiry,
        BuildingInvoice,
        BuildingReservation,
        BuildingServiceRequest,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingAdminOperationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), "building_admin_operations_isolated.db"
        )
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
            building_site_intake_key="building-assisted-test-key",
            building_campaign_token_secret="building-admin-operations-test-secret",
            stripe_secret_key="sk_test_building",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.internal_headers = {"X-Internal-Api-Key": "internal-test-key"}
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(settings.admin_cookie_name, token)
        page = cls.client.get("/admin/building")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        if match is None:
            raise AssertionError("Building Control did not render a CSRF token.")
        cls.csrf = match.group(1)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        space = cls.client.put(
            "/api/internal/building/spaces/arena-admin",
            headers=cls.internal_headers,
            json={
                "id": "arena-admin",
                "slug": "arena-admin",
                "name": "Admin Arena",
                "space_type": "event",
                "capacity": 100,
                "status": "available",
                "is_public": False,
            },
        )
        if space.status_code != 200:
            raise AssertionError(space.text)

    def _post(self, path: str, data: dict) -> object:
        response = self.client.post(
            path,
            headers=self.browser_headers,
            follow_redirects=False,
            data={"_csrf_token": self.csrf, **data},
        )
        self.assertEqual(response.status_code, 303, response.text)
        return response

    def _assert_notice(self, response) -> None:
        self.assertIn("notice=", response.headers["location"])

    def test_00_assisted_lead_preserves_source_consent_and_deduplication(self) -> None:
        missing_reference = self._post(
            "/admin/building/inquiries",
            {
                "kind": "event",
                "source": "eventective",
                "name": "Assisted Event Lead",
                "email": "assisted-event@example.com",
                "consent_to_contact": "true",
            },
        )
        self.assertIn("error=", missing_reference.headers["location"])
        payload = {
            "kind": "event",
            "source": "eventective",
            "source_reference": "eventective-lead-123",
            "name": "Assisted Event Lead",
            "email": "assisted-event@example.com",
            "phone": "801-555-0199",
            "preferred_date": (date.today() + timedelta(days=45)).isoformat(),
            "details": "Company gathering for 40 people.",
            "consent_to_contact": "true",
        }
        with patch(
            "sales_support_agent.api.building_router.HubSpotClient"
        ) as hubspot:
            hubspot.return_value.is_configured = False
            first = self._post("/admin/building/inquiries", payload)
            second = self._post("/admin/building/inquiries", payload)
        self._assert_notice(first)
        self._assert_notice(second)
        with self.factory() as session:
            inquiries = session.query(BuildingInquiry).filter(
                BuildingInquiry.email == "assisted-event@example.com"
            ).all()
            self.assertEqual(len(inquiries), 1)
            self.assertEqual(inquiries[0].source, "eventective")
            self.assertEqual(
                inquiries[0].source_reference,
                "eventective-lead-123",
            )
            self.assertFalse(inquiries[0].consent_to_marketing)
            audit = session.query(BuildingAuditEvent).filter(
                BuildingAuditEvent.entity_type == "inquiry",
                BuildingAuditEvent.entity_id == inquiries[0].id,
                BuildingAuditEvent.action == "created",
            ).one()
            self.assertEqual(audit.actor, "david@anatainc.com")

    def test_00b_operator_owns_and_resolves_urgent_service_work(self) -> None:
        missing_owner = self._post(
            "/admin/building/service-requests",
            {
                "category": "maintenance",
                "priority": "high",
                "title": "Water observed near utility room",
                "space_id": "arena-admin",
            },
        )
        self.assertIn("error=", missing_owner.headers["location"])
        due = datetime.now() + timedelta(hours=1)
        created = self._post(
            "/admin/building/service-requests",
            {
                "category": "maintenance",
                "priority": "urgent",
                "title": "Water observed near utility room",
                "description": "Inspect the source and protect the affected area.",
                "space_id": "arena-admin",
                "source": "inspection",
                "assigned_owner": "facilities@example.com",
                "due_at": due.strftime("%Y-%m-%dT%H:%M"),
            },
        )
        self._assert_notice(created)
        with self.factory() as session:
            row = session.query(BuildingServiceRequest).one()
            request_id = row.id
            self.assertEqual(row.priority, "urgent")
        for target_status in ("triaged", "in_progress"):
            changed = self._post(
                f"/admin/building/service-requests/{request_id}/transition",
                {
                    "target_status": target_status,
                    "assigned_owner": "facilities@example.com",
                    "reason": "Reviewed onsite by facilities",
                },
            )
            self._assert_notice(changed)
        completed = self._post(
            f"/admin/building/service-requests/{request_id}/transition",
            {
                "target_status": "completed",
                "assigned_owner": "facilities@example.com",
                "resolution": "Area inspected, source corrected, and floor dried.",
                "reason": "Facilities verified the correction",
            },
        )
        self._assert_notice(completed)
        with self.factory() as session:
            row = session.get(BuildingServiceRequest, request_id)
            self.assertEqual(row.status, "completed")
            self.assertTrue(row.resolution)

    def test_00a_operator_records_response_without_hiding_crm_sync_state(self) -> None:
        with self.factory() as session:
            inquiry = session.query(BuildingInquiry).filter(
                BuildingInquiry.email == "assisted-event@example.com"
            ).one()
            inquiry_id = inquiry.id
            crm_status = inquiry.status
        response = self._post(
            f"/admin/building/inquiries/{inquiry_id}/lifecycle",
            {
                "target_stage": "responded",
                "assigned_owner": "events@example.com",
                "channel": "phone",
                "notes": "Confirmed date, attendance, and decision timeline.",
            },
        )
        self._assert_notice(response)
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Building performance", page.text)
        self.assertIn("Posted collected cash", page.text)
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, inquiry_id)
            self.assertEqual(inquiry.status, crm_status)
            self.assertEqual(
                inquiry.payload_json["_lifecycle"]["stage"],
                "responded",
            )
            self.assertEqual(inquiry.assigned_owner, "events@example.com")

    def test_01_operator_completes_guarded_event_booking_evidence(self) -> None:
        starts = datetime.now() + timedelta(days=14)
        created = self._post(
            "/admin/building/reservations",
            {
                "kind": "event",
                "space_id": "arena-admin",
                "starts_at": starts.strftime("%Y-%m-%dT%H:%M"),
                "ends_at": (starts + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M"),
                "attendance": "75",
                "deposit_required": "true",
                "assigned_owner": "operator@example.com",
                "source": "eventective",
                "source_reference": "eventective-pilot",
            },
        )
        self._assert_notice(created)
        with self.factory() as session:
            reservation = session.query(BuildingReservation).one()
            reservation_id = reservation.id
            self.assertEqual(reservation.status, "inquiry")

        for state in ("requirements_review",):
            self._assert_notice(self._post(
                f"/admin/building/reservations/{reservation_id}/transition",
                {"target_status": state, "reason": "Reviewed by operator."},
            ))
        hold_expires = datetime.now() + timedelta(days=2)
        self._assert_notice(self._post(
            f"/admin/building/reservations/{reservation_id}/transition",
            {
                "target_status": "soft_hold",
                "hold_expires_at": hold_expires.strftime("%Y-%m-%dT%H:%M"),
                "reason": "Client requested a short hold.",
            },
        ))
        for state in ("quote_sent", "contract_pending"):
            self._assert_notice(self._post(
                f"/admin/building/reservations/{reservation_id}/transition",
                {"target_status": state, "reason": "Workflow progression."},
            ))
        agreement = self._post(
            f"/admin/building/reservations/{reservation_id}/agreements",
            {
                "version": "1",
                "status": "signed",
                "provider": "test-sign",
                "provider_reference": "agreement-admin-1",
            },
        )
        self._assert_notice(agreement)
        self._assert_notice(self._post(
            f"/admin/building/reservations/{reservation_id}/transition",
            {"target_status": "deposit_due", "reason": "Signed agreement received."},
        ))
        deposit = self._post(
            f"/admin/building/reservations/{reservation_id}/deposits",
            {
                "status": "paid",
                "amount": "500.00",
                "provider": "stripe",
                "provider_reference": "pi_admin_deposit",
            },
        )
        self._assert_notice(deposit)
        confirmed = self._post(
            f"/admin/building/reservations/{reservation_id}/transition",
            {"target_status": "confirmed", "reason": "All gates satisfied."},
        )
        self._assert_notice(confirmed)
        with self.factory() as session:
            reservation = session.get(BuildingReservation, reservation_id)
            self.assertEqual(reservation.status, "confirmed")
            self.assertEqual(reservation.agreement_status, "signed")
            self.assertEqual(reservation.deposit_status, "paid")

    def test_02_operator_approves_schedule_before_idempotent_invoice(self) -> None:
        with self.factory() as session:
            reservation_id = session.query(BuildingReservation).one().id
        self._assert_notice(self._post(
            "/admin/building/billing/accounts",
            {
                "account_id": "admin-account",
                "account_name": "Admin Test Account",
                "billing_email": "billing-admin@example.com",
            },
        ))
        self._assert_notice(self._post(
            "/admin/building/billing/schedules",
            {
                "schedule_id": "admin-deposit",
                "billing_account_id": "admin-account",
                "reservation_id": reservation_id,
                "schedule_type": "deposit",
                "description": "Event deposit",
                "amount": "500.00",
                "collection_method": "send_invoice",
                "days_until_due": "7",
                "starts_on": date.today().isoformat(),
            },
        ))
        wrong = self._post(
            "/admin/building/billing/schedules/admin-deposit/invoice",
            {"confirmation": "INVOICE something-else"},
        )
        self.assertIn("error=", wrong.headers["location"])
        self._assert_notice(self._post(
            "/admin/building/billing/schedules/admin-deposit/approve",
            {},
        ))
        provider_invoice = {
            "id": "in_admin_deposit",
            "status": "open",
            "amount_due": 50000,
            "amount_paid": 0,
            "currency": "usd",
            "hosted_invoice_url": "https://invoice.example/admin",
        }
        with (
            patch.object(
                StripeBillingClient,
                "create_customer",
                return_value={"id": "cus_admin"},
            ),
            patch.object(
                StripeBillingClient,
                "create_invoice",
                return_value=provider_invoice,
            ) as create_invoice,
        ):
            sent = self._post(
                "/admin/building/billing/schedules/admin-deposit/invoice",
                {"confirmation": "INVOICE admin-deposit"},
            )
            self._assert_notice(sent)
            duplicate = self._post(
                "/admin/building/billing/schedules/admin-deposit/invoice",
                {"confirmation": "INVOICE admin-deposit"},
            )
            self._assert_notice(duplicate)
            create_invoice.assert_called_once()
        with self.factory() as session:
            schedule = session.get(BuildingBillingSchedule, "admin-deposit")
            invoice = session.query(BuildingInvoice).one()
            self.assertEqual(schedule.status, "completed")
            self.assertEqual(invoice.accounting_status, "pending_qbo")


if __name__ == "__main__":
    unittest.main()
