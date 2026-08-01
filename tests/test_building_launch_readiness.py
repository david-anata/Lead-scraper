from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import date

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/launch_ready_boot.db")
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "launch-ready-session-secret")

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingLaunchDecision,
    BuildingOffering,
    BuildingRatePlan,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token
from sales_support_agent.services.building_launch_readiness import launch_decision_id


class BuildingLaunchReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), f"launch_ready_{uuid.uuid4().hex}.db")
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="launch-ready-internal",
            building_campaign_token_secret="launch-ready-csrf",
        )
        cls.client = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(app.state.agent_settings.admin_cookie_name, token)
        cls.headers = {"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"}
        with cls.factory() as session:
            session.add(BuildingSpace(
                id="launch-arena",
                slug="launch-arena",
                name="The Arena",
                space_type="event",
                status="available",
                is_public=True,
            ))
            session.add(BuildingOffering(
                id="launch-arena-events",
                slug="launch-arena-events",
                name="Arena events",
                offering_type="event",
                space_id="launch-arena",
                is_published=True,
            ))
            session.add(BuildingOffering(
                id="arena-empty-evidence",
                slug="arena-empty-evidence",
                name="Arena empty evidence",
                offering_type="event",
                space_id="launch-arena",
                is_published=False,
            ))
            session.add(BuildingSpace(
                id="other-event-space",
                slug="other-event-space",
                name="Other Event Space",
                space_type="event",
                status="available",
                is_public=False,
            ))
            session.add(BuildingOffering(
                id="other-event-offering",
                slug="other-event-offering",
                name="Other event offering",
                offering_type="event",
                space_id="other-event-space",
                is_published=False,
            ))
            session.add(BuildingRatePlan(
                id="launch-arena-rate-v1",
                offering_id="launch-arena-events",
                version=1,
                name="Arena reconciled draft",
                status="in_review",
                unit_amount_cents=17500,
                public_price_display="$175/hour",
                booking_unit="hour",
                minimum_units=6,
                deposit_type="percent",
                deposit_percent_bps=5000,
                cancellation_policy="Reviewed cancellation terms.",
                tax_status="review_required",
                tax_note="Tax reviewed in quote.",
                source_evidence_json=[{"source": "Listing Copy Pack"}],
                conflicts_json=[],
                effective_from=date(2026, 1, 1),
                created_by="test",
            ))
            session.add(BuildingRatePlan(
                id="arena-empty-rate-v1",
                offering_id="arena-empty-evidence",
                version=1,
                name="Arena empty evidence plan",
                status="in_review",
                unit_amount_cents=17500,
                public_price_display="$175/hour",
                booking_unit="hour",
                minimum_units=6,
                cancellation_policy="Reviewed cancellation terms.",
                tax_status="review_required",
                tax_note="Tax reviewed in quote.",
                source_evidence_json=[],
                effective_from=date(2026, 1, 1),
                created_by="test",
            ))
            session.commit()

    def _csrf(self) -> str:
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200, page.text)
        return re.search(r'name="_csrf_token" value="([^"]+)"', page.text).group(1)

    def _decide(self, key: str, status: str, value: str = "Reviewed value"):
        return self.client.post(
            f"/admin/building/launch-readiness/decisions/{key}",
            headers=self.headers,
            data={
                "_csrf_token": self._csrf(),
                "offering_id": "launch-arena-events",
                "decision_status": status,
                "value": value,
                "evidence": "Approval record launch-2026",
                "confirmation": "I APPROVE THIS DECISION",
            },
            follow_redirects=False,
        )

    def test_01_page_explains_all_blockers_and_calendar_uncertainty(self) -> None:
        page = self.client.get("/admin/building")
        self.assertIn(
            "What is ready—and what still needs outside approval",
            page.text,
        )
        self.assertIn(
            "This list updates from the real system evidence",
            page.text,
        )
        self.assertIn("Business rules", page.text)
        self.assertIn("Tax determination", page.text)
        self.assertIn("Old booking-page copy", page.text)
        self.assertIn("Electronic signatures", page.text)
        self.assertIn("Customer payments", page.text)
        self.assertIn("Dedicated Arena calendar", page.text)
        self.assertIn("Customer email", page.text)
        self.assertIn("Customer booking launch", page.text)
        self.assertIn(
            "Cards are accepted. ACH or check may be approved",
            page.text,
        )
        self.assertIn(
            "seven additional days early",
            page.text,
        )
        self.assertIn(
            "Owner designated building@anatainc.com",
            page.text,
        )
        self.assertIn(
            "Owner approved $175 per full overtime hour",
            page.text,
        )
        self.assertIn("0/7", page.text)
        self.assertIn(
            "Create a dedicated Arena calendar owned by Anata",
            page.text,
        )
        self.assertIn("Venue payment workflow", page.text)
        self.assertIn("overpayments and third-party vendor payments are not accepted", page.text)

    def test_01b_page_presents_a_scannable_governed_launch_workspace(self) -> None:
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200)
        self.assertIn('aria-label="Building Control sections"', page.text)
        self.assertIn('href="/admin/building/settings"', page.text)
        self.assertIn('class="daily-guide today-only"', page.text)
        self.assertIn("Work the Today queue", page.text)
        self.assertIn("Arena setup and administration", page.text)
        self.assertIn(
            "Open this only to change business rules or finish provider setup.",
            page.text,
        )
        self.assertIn("Arena launch command center", page.text)
        self.assertIn("Private, protected, and not ready to publish", page.text)
        self.assertIn('class="launch-checklist"', page.text)
        self.assertIn("Outside setup", page.text)
        self.assertIn(
            "it does not ask you to repeat decisions",
            page.text,
        )
        self.assertIn('class="decision-list"', page.text)
        self.assertEqual(page.text.count('class="decision-card"'), 10)
        self.assertEqual(
            page.text.count("<summary>Answer this question</summary>"),
            7,
        )
        self.assertEqual(
            page.text.count("<summary>Record completed setup</summary>"),
            1,
        )
        self.assertIn("Review agreement templates", page.text)
        self.assertIn("Agent records the date automatically", page.text)
        self.assertIn(
            "Saving records the approved rule and audit history.",
            page.text,
        )
        self.assertIn("What happens if a customer cancels?", page.text)
        self.assertIn("What remains", page.text)
        self.assertIn("What we need from you", page.text)
        self.assertIn("Outside setup required", page.text)
        self.assertNotIn("The final handoff", page.text)
        self.assertIn("No reusable Arena agreement has been prepared", page.text)
        # The calendar row now names which of the three conditions is missing
        # instead of the old catch-all "dry-run only". In this fixture nothing
        # is configured, so it must say so and not claim to be ready.
        self.assertIn("calendar", page.text.lower())
        self.assertNotIn(
            "The dedicated calendar is verified and production projection is enabled.",
            page.text,
        )
        self.assertIn(
            'href="/admin/building/contracts"',
            page.text,
        )
        # The passphrase box is gone. The decision form itself must still be
        # on the page and still post a status.
        self.assertIn('name="decision_status"', page.text)
        self.assertIn("/launch-readiness/decisions/", page.text)
        self.assertNotIn("<label>Applies to</label>", page.text)
        self.assertIn('id="incoming-inquiries"', page.text)
        self.assertIn('id="bookings-and-holds"', page.text)
        self.assertIn('id="billing-and-collections"', page.text)
        self.assertIn('id="customer-email-list"', page.text)

    def test_02_decision_requires_exact_status_confirmation_and_evidence(self) -> None:
        wrong = self._decide("event_calendar", "accepted_policy")
        self.assertIn("requires+status+provider_verified", wrong.headers["location"])
        recorded = self._decide(
            "event_calendar",
            "provider_verified",
            "calendar-id / owner / service-account verified",
        )
        self.assertEqual(recorded.status_code, 303)
        with self.factory() as session:
            row = session.get(
                BuildingLaunchDecision,
                launch_decision_id("launch-arena-events", "event_calendar"),
            )
            self.assertEqual(row.status, "provider_verified")
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="launch_decision",
                entity_id=row.id,
                action="arena_launch_decision_recorded",
            ).one()
            self.assertFalse(audit.after_json["external_write"])
        page = self.client.get("/admin/building")
        self.assertIn("Answered", page.text)
        self.assertIn("Change this approved answer", page.text)
        self.assertIn(
            "calendar-id / owner / service-account verified",
            page.text,
        )

    def test_03_reconciled_rate_plan_approval_stays_blocked(self) -> None:
        response = self.client.post(
            "/admin/building/rate-plans/launch-arena-rate-v1/approve",
            headers=self.headers,
            data={
                "_csrf_token": self._csrf(),
                "approval_evidence": "pricing approval evidence",
                "confirmation": "APPROVE launch-arena-rate-v1",
            },
            follow_redirects=False,
        )
        self.assertIn("Resolve+Arena+launch+decisions", response.headers["location"])
        with self.factory() as session:
            self.assertEqual(session.get(BuildingRatePlan, "launch-arena-rate-v1").status, "in_review")

    def test_04_commercial_decisions_unlock_only_rate_plan_approval(self) -> None:
        for key in (
            "cancellation_policy",
            "tax_treatment",
            "setup_price",
            "teardown_price",
            "overtime_rate",
        ):
            self.assertEqual(self._decide(key, "accepted_policy").status_code, 303)
        response = self.client.post(
            "/admin/building/rate-plans/launch-arena-rate-v1/approve",
            headers=self.headers,
            data={
                "_csrf_token": self._csrf(),
                "approval_evidence": "pricing approval evidence",
                "confirmation": "APPROVE launch-arena-rate-v1",
            },
            follow_redirects=False,
        )
        self.assertIn("approved+and+locked", response.headers["location"])
        with self.factory() as session:
            self.assertEqual(session.get(BuildingRatePlan, "launch-arena-rate-v1").status, "approved")
            self.assertEqual(session.query(BuildingLaunchDecision).count(), 7)
            effective = session.get(
                BuildingLaunchDecision,
                launch_decision_id("launch-arena-events", "effective_date"),
            )
            self.assertEqual(effective.status, "accepted_policy")
            self.assertIn("2026-01-01", effective.value)
            self.assertIn("launch-arena-rate-v1", effective.evidence)

    def test_05_empty_source_evidence_cannot_bypass_arena_gates(self) -> None:
        control = self.client.post(
            "/admin/building/rate-plans/arena-empty-rate-v1/approve",
            headers=self.headers,
            data={
                "_csrf_token": self._csrf(),
                "approval_evidence": "pricing approval evidence",
                "confirmation": "APPROVE arena-empty-rate-v1",
            },
            follow_redirects=False,
        )
        self.assertIn("Resolve+Arena+launch+decisions", control.headers["location"])

        payload = {
            "id": "arena-empty-rate-v2",
            "version": 2,
            "name": "Arena API empty evidence",
            "status": "approved",
            "unit_amount_cents": 17500,
            "public_price_display": "$175/hour",
            "booking_unit": "hour",
            "minimum_units": 6,
            "cancellation_policy": "Reviewed policy.",
            "tax_status": "review_required",
            "tax_note": "Tax reviewed in quote.",
            "approval_evidence": "api approval evidence",
            "effective_from": "2027-01-01",
            "approved_by": "approver@anatainc.com",
            "actor": "operator@anatainc.com",
            "source_evidence": [],
        }
        arena_api = self.client.put(
            "/api/internal/building/offerings/arena-empty-evidence/"
            "rate-plans/arena-empty-rate-v2",
            headers={"X-Internal-Api-Key": "launch-ready-internal"},
            json=payload,
        )
        self.assertEqual(arena_api.status_code, 409, arena_api.text)
        self.assertIn("Resolve Arena launch decisions", arena_api.text)

        other_payload = dict(payload)
        other_payload.update(
            id="other-event-rate-v1",
            version=1,
            name="Other event approved plan",
        )
        other_api = self.client.put(
            "/api/internal/building/offerings/other-event-offering/"
            "rate-plans/other-event-rate-v1",
            headers={"X-Internal-Api-Key": "launch-ready-internal"},
            json=other_payload,
        )
        self.assertEqual(other_api.status_code, 200, other_api.text)
        self.assertEqual(other_api.json()["rate_plan"]["status"], "approved")
        self.assertLessEqual(
            len(launch_decision_id("x" * 64, "transactional_sender")),
            64,
        )


if __name__ == "__main__":
    unittest.main()
