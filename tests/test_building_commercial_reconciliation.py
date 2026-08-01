from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import date

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_commercial_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-commercial-session-secret",
)

try:
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

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingCommercialReconciliationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_commercial_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="commercial-internal-key",
            building_campaign_token_secret="commercial-csrf-secret",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="david@anatainc.com",
            name="Pricing Admin",
            role="admin",
        )
        cls.client.cookies.set(app.state.agent_settings.admin_cookie_name, token)
        cls.form_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        with factory() as session:
            session.add(BuildingSpace(
                id="commercial-arena",
                slug="commercial-arena",
                name="The Arena",
                space_type="event",
                capacity=200,
                status="available",
                is_public=True,
            ))
            session.add(BuildingOffering(
                id="commercial-arena-events",
                slug="commercial-arena-events",
                name="The Arena events",
                offering_type="event",
                space_id="commercial-arena",
                public_description="A flexible event venue.",
                price_display="Contact us",
                booking_unit="hour",
                call_to_action="request_date",
                is_published=True,
            ))
            session.commit()

    def _csrf(self) -> str:
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200, page.text)
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match, page.text)
        return match.group(1)

    def _prepare(self, confirmation: str = "PREPARE ARENA DRAFT"):
        return self.client.post(
            "/admin/building/rate-plans/arena-commercial-baseline",
            headers=self.form_headers,
            data={
                "_csrf_token": self._csrf(),
                "offering_id": "commercial-arena-events",
                "effective_from": "2026-01-01",
                "confirmation": confirmation,
            },
            follow_redirects=False,
        )

    def _reconcile(self, conflict_id: str, status: str, note: str):
        return self.client.post(
            "/admin/building/rate-plans/"
            "commercial-arena-events-commercial-baseline-v1/"
            "reconcile-source-conflicts",
            headers=self.form_headers,
            data={
                "_csrf_token": self._csrf(),
                "conflict_id": conflict_id,
                "resolution_status": status,
                "resolution_note": note,
                "confirmation": (
                    "RECONCILE commercial-arena-events-commercial-baseline-v1"
                ),
            },
            follow_redirects=False,
        )

    def test_01_control_room_surfaces_verified_terms_and_conflicts(self) -> None:
        page = self.client.get("/admin/building")
        self.assertEqual(page.status_code, 200, page.text)
        for text in (
            "$175/hour",
            "six-hour minimum ($1,050)",
            "$250 routine cleaning",
            "50% booking deposit",
            "$500 refundable security deposit",
            "balance due seven days",
            "$175 per full hour",
            "70% deposit",
            "placeholder payment link",
            "reusable agreement remains under legal review",
        ):
            self.assertIn(text, page.text)

    def test_02_prepare_uses_current_owner_rules_but_remains_a_draft(self) -> None:
        # Passphrase removed: this creates a draft, and approval is a
        # separate checked step. What must stay true is below, that it remains
        # a draft and approves nothing.
        prepared = self._prepare()
        self.assertEqual(prepared.status_code, 303, prepared.text)
        with self.factory() as session:
            row = session.get(
                BuildingRatePlan,
                "commercial-arena-events-commercial-baseline-v1",
            )
            self.assertIsNotNone(row)
            self.assertEqual(row.status, "draft")
            self.assertEqual(row.unit_amount_cents, 17500)
            self.assertEqual(row.minimum_units, 6)
            self.assertEqual(row.deposit_percent_bps, 5000)
            self.assertEqual(
                [item["id"] for item in row.addons_json],
                [
                    "cleaning",
                    "setup-reset-75",
                    "setup-reset-150",
                    "setup-reset-200",
                    "av-technician",
                    "anata-event-labor",
                ],
            )
            self.assertEqual(
                row.commercial_terms_json["overtime"]["amount_cents"],
                17500,
            )
            self.assertEqual(
                row.commercial_terms_json["balance_due_days_before_event"],
                7,
            )
            self.assertEqual(
                row.commercial_terms_json["security_deposit"]["amount_cents"],
                50000,
            )
            self.assertFalse(row.approval_evidence)
            self.assertTrue(
                all(
                    item["status"] == "provider_remediation_required"
                    for item in row.conflicts_json[:3]
                )
            )
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="rate_plan",
                entity_id=row.id,
                action="commercial_baseline_draft_prepared",
            ).one()
            self.assertFalse(audit.after_json["provider_write"])

    def test_03_each_conflict_requires_its_own_valid_disposition(self) -> None:
        recorded_only = self._reconcile(
            "tidycal-deposit",
            "reconciled_in_agent",
            "Agent baseline recorded; TidyCal has not been changed.",
        )
        self.assertEqual(recorded_only.status_code, 303)
        invalid_placeholder = self._reconcile(
            "tidycal-payment-link",
            "accepted_exception",
            "Attempted to accept the placeholder without remediation.",
        )
        self.assertIn(
            "disposition+is+not+valid",
            invalid_placeholder.headers["location"],
        )
        with self.factory() as session:
            row = session.get(
                BuildingRatePlan,
                "commercial-arena-events-commercial-baseline-v1",
            )
            conflicts = {item["id"]: item for item in row.conflicts_json}
            self.assertEqual(
                conflicts["tidycal-deposit"]["status"],
                "reconciled_in_agent",
            )
            self.assertEqual(
                conflicts["tidycal-balance"]["status"],
                "provider_remediation_required",
            )
            self.assertEqual(
                conflicts["tidycal-payment-link"]["status"],
                "provider_remediation_required",
            )

    def test_04_approval_fails_closed_until_every_blocker_is_decided(self) -> None:
        with self.factory() as session:
            row = session.get(
                BuildingRatePlan,
                "commercial-arena-events-commercial-baseline-v1",
            )
            row.status = "in_review"
            row.cancellation_policy = "Reviewed cancellation policy."
            row.tax_note = "Tax treatment remains subject to reviewed quote."
            session.commit()
        blocked = self.client.post(
            "/admin/building/rate-plans/"
            "commercial-arena-events-commercial-baseline-v1/approve",
            headers=self.form_headers,
            data={
                "_csrf_token": self._csrf(),
                "approval_evidence": "pricing committee record 2026-08",
                "confirmation": (
                    "APPROVE commercial-arena-events-commercial-baseline-v1"
                ),
            },
            follow_redirects=False,
        )
        self.assertEqual(blocked.status_code, 303)
        self.assertIn("Resolve+blocking+source+conflicts", blocked.headers["location"])
        with self.factory() as session:
            row = session.get(
                BuildingRatePlan,
                "commercial-arena-events-commercial-baseline-v1",
            )
            self.assertEqual(row.status, "in_review")
            self.assertFalse(row.approval_evidence)

    def test_05_specific_decisions_allow_separate_explicit_approval(self) -> None:
        for conflict_id, status, note in (
            (
                "tidycal-deposit",
                "provider_remediated",
                "Operator verified the stale deposit copy was corrected.",
            ),
            (
                "tidycal-balance",
                "provider_remediated",
                "Operator verified the stale balance deadline was corrected.",
            ),
            (
                "tidycal-payment-link",
                "provider_remediated",
                "Operator verified the placeholder payment link was removed.",
            ),
        ):
            response = self._reconcile(conflict_id, status, note)
            self.assertEqual(response.status_code, 303, response.text)
        with self.factory() as session:
            for key in (
                "cancellation_policy",
                "tax_treatment",
                "setup_price",
                "teardown_price",
                "overtime_rate",
                "effective_date",
            ):
                session.add(BuildingLaunchDecision(
                    id=f"commercial-arena-events:{key}",
                    offering_id="commercial-arena-events",
                    decision_key=key,
                    status="accepted_policy",
                    value="Reviewed decision",
                    evidence="pricing committee record 2026-08",
                    decided_by="pricing-admin@anatainc.com",
                ))
            session.commit()
        approved = self.client.post(
            "/admin/building/rate-plans/"
            "commercial-arena-events-commercial-baseline-v1/approve",
            headers=self.form_headers,
            data={
                "_csrf_token": self._csrf(),
                "approval_evidence": "pricing committee record 2026-08",
                "confirmation": (
                    "APPROVE commercial-arena-events-commercial-baseline-v1"
                ),
            },
            follow_redirects=False,
        )
        self.assertEqual(approved.status_code, 303, approved.text)
        with self.factory() as session:
            row = session.get(
                BuildingRatePlan,
                "commercial-arena-events-commercial-baseline-v1",
            )
            self.assertEqual(row.status, "approved")
            self.assertEqual(row.approved_by, "david@anatainc.com")

    def test_06_public_payload_does_not_leak_governance_metadata(self) -> None:
        public = self.client.get(
            "/api/public/building/offerings/commercial-arena-events"
        )
        self.assertEqual(public.status_code, 200, public.text)
        rate_plan = public.json()["rate_plan"]
        self.assertNotIn("commercial_terms", rate_plan)
        self.assertNotIn("source_evidence", rate_plan)
        self.assertNotIn("conflicts", rate_plan)
        self.assertNotIn("legal_template_status", str(rate_plan))


if __name__ == "__main__":
    unittest.main()
