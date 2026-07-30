from __future__ import annotations

import dataclasses
import hashlib
import os
import re
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_agreement_readiness_boot.db",
)
os.environ.setdefault(
    "BUILDING_CAMPAIGN_TOKEN_SECRET",
    "building-agreement-readiness-csrf-secret",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import (
        create_session_factory,
        init_database,
    )
    from sales_support_agent.models.entities import (
        BuildingAgreement,
        BuildingAgreementTemplate,
        BuildingAuditEvent,
        BuildingAvailabilityBlock,
        BuildingContact,
        BuildingInquiry,
        BuildingPaymentRequestReadiness,
        BuildingProposal,
        BuildingReservation,
        BuildingSpace,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.services.building_holds import expire_building_holds

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingAgreementReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            f"building_agreement_readiness_{uuid.uuid4().hex}.db",
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        token = create_user_session_token(
            app.state.agent_settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(app.state.agent_settings.admin_cookie_name, token)
        cls.headers = {"X-Internal-Api-Key": "internal-test-key"}
        cls.form_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }
        now = datetime.now(timezone.utc)
        cls.start = now + timedelta(days=30)
        with factory() as session:
            session.add(BuildingSpace(
                id="agreement-arena",
                slug="agreement-arena",
                name="Agreement Arena",
                space_type="event",
                capacity=200,
                status="available",
            ))
            session.add(BuildingContact(
                id="agreement-host",
                email="agreement-host@example.com",
                full_name="Agreement Host",
                status="active",
            ))
            session.add(BuildingInquiry(
                id="agreement-inquiry",
                idempotency_key="agreement-inquiry-key",
                kind="event",
                name="Agreement Host",
                email="agreement-host@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            reservation = BuildingReservation(
                id="agreement-event",
                kind="event",
                status="soft_hold",
                inquiry_id="agreement-inquiry",
                contact_id="agreement-host",
                offering_id=None,
                space_id="agreement-arena",
                starts_at=cls.start,
                guest_starts_at=cls.start + timedelta(hours=2),
                guest_ends_at=cls.start + timedelta(hours=6),
                ends_at=cls.start + timedelta(hours=8),
                hold_expires_at=now + timedelta(days=2),
                attendance=80,
                deposit_required=True,
                assigned_owner="operator@example.com",
                created_by="operator@example.com",
            )
            session.add(reservation)
            session.add(BuildingAvailabilityBlock(
                id="agreement-block",
                space_id="agreement-arena",
                state="soft_hold",
                starts_at=reservation.starts_at,
                ends_at=reservation.ends_at,
                expires_at=reservation.hold_expires_at,
                source="agent",
                source_reference="reservation:agreement-event",
            ))
            session.add(BuildingProposal(
                id="agreement-quote",
                reservation_id="agreement-event",
                version=1,
                proposal_type="quote",
                status="draft",
                currency="USD",
                amount_cents=300000,
                line_items_json=[{
                    "type": "base",
                    "name": "Reviewed event package",
                    "quantity": 4,
                    "amount_cents": 300000,
                }],
                rate_plan_id="agreement-rate-v1",
                rate_plan_snapshot_json={
                    "id": "agreement-rate-v1",
                    "version": 1,
                    "deposit_type": "percent",
                    "deposit_percent_bps": 2000,
                    "cancellation_policy": "Reviewed cancellation terms.",
                    "tax_status": "non_taxable",
                    "tax_rate_bps": 0,
                    "tax_note": "Reviewed as non-taxable.",
                    "included": ["Venue access"],
                    "addons": [],
                },
                terms_summary="Frozen reviewed quote terms.",
                created_by="operator@example.com",
            ))
            session.commit()

    def test_00_arena_review_package_is_versioned_audited_and_never_approved(self) -> None:
        guest = TestClient(app)
        denied = guest.get(
            "/admin/building/agreement-readiness/arena-review-package/download",
            follow_redirects=False,
        )
        self.assertEqual(denied.status_code, 302)
        self.assertEqual(denied.headers["location"], "/admin/login")

        page = self.client.get(
            "/admin/building/agreement-readiness",
            follow_redirects=False,
        )
        self.assertEqual(page.status_code, 308, page.text)
        self.assertEqual(page.headers["location"], "/admin/building/contracts")
        templates_page = self.client.get("/admin/building/contracts/templates")
        self.assertEqual(templates_page.status_code, 200, templates_page.text)
        csrf = re.search(
            r'name="_csrf_token" value="([^"]+)"',
            templates_page.text,
        ).group(1)

        download = self.client.get(
            "/admin/building/agreement-readiness/arena-review-package/download"
        )
        self.assertEqual(download.status_code, 200, download.text)
        self.assertIn("Prepared for legal review", download.text)
        self.assertIn("not approved for customer signature", download.text)
        self.assertIn(
            'attachment; filename="anata-arena-agreement-business-terms-v1.md"',
            download.headers["content-disposition"],
        )
        self.assertEqual(download.headers["cache-control"], "private, no-store")
        self.assertEqual(
            download.headers["x-content-sha256"],
            hashlib.sha256(download.content).hexdigest(),
        )

        wrong = self.client.post(
            "/admin/building/agreement-readiness/arena-review-package/prepare",
            headers=self.form_headers,
            data={"_csrf_token": csrf, "confirmation": "approve it"},
            follow_redirects=False,
        )
        self.assertEqual(wrong.status_code, 303)
        self.assertIn("Type+exactly", wrong.headers["location"])

        prepared = self.client.post(
            "/admin/building/agreement-readiness/arena-review-package/prepare",
            headers=self.form_headers,
            data={
                "_csrf_token": csrf,
                "confirmation": "PREPARE ARENA AGREEMENT REVIEW",
            },
            follow_redirects=False,
        )
        self.assertEqual(prepared.status_code, 303, prepared.text)
        self.assertIn("prepared+for+legal+review", prepared.headers["location"])
        with self.factory() as session:
            template = session.get(
                BuildingAgreementTemplate,
                "arena-event-agreement-business-terms-v1",
            )
            self.assertIsNotNone(template)
            self.assertEqual(template.status, "in_review")
            self.assertIsNone(template.approved_at)
            self.assertEqual(template.approval_evidence, "")
            self.assertIn(
                download.headers["x-content-sha256"],
                template.template_reference,
            )
            audits = session.query(BuildingAuditEvent).filter_by(
                entity_type="agreement_template",
                entity_id=template.id,
            ).all()
            self.assertEqual(len(audits), 2)
            self.assertTrue(
                all(
                    event.after_json.get("provider_write") is False
                    and event.after_json.get("customer_delivery") is False
                    for event in audits
                )
            )

        replay = self.client.post(
            "/admin/building/agreement-readiness/arena-review-package/prepare",
            headers=self.form_headers,
            data={
                "_csrf_token": csrf,
                "confirmation": "PREPARE ARENA AGREEMENT REVIEW",
            },
            follow_redirects=False,
        )
        self.assertEqual(replay.status_code, 303)
        self.assertIn("already+in+legal+review", replay.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingAuditEvent).filter_by(
                    entity_type="agreement_template",
                    entity_id="arena-event-agreement-business-terms-v1",
                ).count(),
                2,
            )

    def test_01_template_package_and_payment_readiness_are_guarded(self) -> None:
        invalid = self.client.put(
            "/api/internal/building/agreement-readiness/templates/event-template-v1",
            headers=self.headers,
            json={
                "id": "event-template-v1",
                "template_key": "event-agreement",
                "version": 1,
                "name": "Event agreement",
                "template_reference": "approved-repository:event-agreement-v1",
                "merge_fields": ["customer_name", "private_operator_notes"],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(invalid.status_code, 422)
        created = self.client.put(
            "/api/internal/building/agreement-readiness/templates/event-template-v1",
            headers=self.headers,
            json={
                "id": "event-template-v1",
                "template_key": "event-agreement",
                "version": 1,
                "name": "Event agreement",
                "template_reference": "approved-repository:event-agreement-v1",
                "merge_fields": [
                    "customer_name",
                    "customer_email",
                    "event_space",
                    "setup_starts_at",
                    "guest_starts_at",
                    "guest_ends_at",
                    "teardown_ends_at",
                    "quote_total",
                    "deposit_amount",
                    "cancellation_policy",
                    "tax_terms",
                ],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        wrong = self.client.post(
            "/api/internal/building/agreement-readiness/templates/event-template-v1/transition",
            headers=self.headers,
            json={
                "target_status": "in_review",
                "confirmation": "review it",
                "actor": "reviewer@example.com",
            },
        )
        self.assertEqual(wrong.status_code, 422)
        for target, confirmation, evidence in (
            ("in_review", "IN_REVIEW TEMPLATE event-template-v1", ""),
            (
                "approved",
                "APPROVED TEMPLATE event-template-v1",
                "legal-review-ticket-123",
            ),
        ):
            changed = self.client.post(
                "/api/internal/building/agreement-readiness/templates/event-template-v1/transition",
                headers=self.headers,
                json={
                    "target_status": target,
                    "confirmation": confirmation,
                    "evidence": evidence,
                    "actor": "reviewer@example.com",
                },
            )
            self.assertEqual(changed.status_code, 200, changed.text)

        package_payload = {
            "reservation_id": "agreement-event",
            "quote_id": "agreement-quote",
            "template_id": "event-template-v1",
            "agreement_version": 1,
            "payment_version": 1,
            "actor": "operator@example.com",
        }
        headers = {**self.headers, "Idempotency-Key": "agreement-package-v1"}
        prepared = self.client.post(
            "/api/internal/building/agreement-readiness/packages",
            headers=headers,
            json=package_payload,
        )
        self.assertEqual(prepared.status_code, 201, prepared.text)
        body = prepared.json()
        self.assertFalse(body["replayed"])
        self.assertEqual(body["agreement"]["preparation_status"], "prepared")
        self.assertEqual(body["payment_request"]["amount_cents"], 60000)
        self.assertFalse(body["payment_request"]["provider_object_created"])
        self.assertFalse(body["gates"]["signature_verified"])
        self.assertFalse(body["gates"]["payment_verified"])
        self.assertFalse(body["gates"]["booking_confirmed"])
        self.assertNotIn(
            "private_operator_notes",
            body["agreement"]["snapshot"]["merge_values"],
        )
        replay = self.client.post(
            "/api/internal/building/agreement-readiness/packages",
            headers=headers,
            json=package_payload,
        )
        self.assertEqual(replay.status_code, 201, replay.text)
        self.assertTrue(replay.json()["replayed"])
        agreement_id = body["agreement"]["id"]
        payment_id = body["payment_request"]["id"]
        for target, verb in (("in_review", "REVIEW"), ("approved", "APPROVE")):
            changed = self.client.post(
                f"/api/internal/building/agreement-readiness/packages/{agreement_id}/transition",
                headers=self.headers,
                json={
                    "target_status": target,
                    "confirmation": f"{verb} AGREEMENT {agreement_id}",
                    "actor": "approver@example.com",
                },
            )
            self.assertEqual(changed.status_code, 200, changed.text)
        for target, verb in (("in_review", "REVIEW"), ("approved", "APPROVE")):
            changed = self.client.post(
                f"/api/internal/building/agreement-readiness/payments/{payment_id}/transition",
                headers=self.headers,
                json={
                    "target_status": target,
                    "confirmation": f"{verb} PAYMENT {payment_id}",
                    "actor": "approver@example.com",
                },
            )
            self.assertEqual(changed.status_code, 200, changed.text)
        replayed_approval = self.client.post(
            f"/api/internal/building/agreement-readiness/payments/{payment_id}/transition",
            headers=self.headers,
            json={
                "target_status": "approved",
                "confirmation": f"APPROVE PAYMENT {payment_id}",
                "actor": "approver@example.com",
            },
        )
        self.assertEqual(replayed_approval.status_code, 200, replayed_approval.text)
        with self.factory() as session:
            agreement = session.get(BuildingAgreement, agreement_id)
            payment = session.get(BuildingPaymentRequestReadiness, payment_id)
            self.assertEqual(agreement.status, "draft")
            self.assertEqual(agreement.preparation_status, "approved")
            self.assertEqual(agreement.provider, "")
            self.assertEqual(payment.status, "approved")
            self.assertFalse(payment.metadata_json["provider_object_created"])

    def test_02_expiry_propagates_without_provider_success(self) -> None:
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "agreement-event")
            reservation.hold_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
            session.commit()
        result = expire_building_holds(
            self.factory,
            as_of=datetime.now(timezone.utc),
        )
        self.assertEqual(result["expired_count"], 1)
        with self.factory() as session:
            agreement = session.query(BuildingAgreement).one()
            payment = session.query(BuildingPaymentRequestReadiness).one()
            self.assertEqual(agreement.preparation_status, "expired")
            self.assertEqual(agreement.status, "draft")
            self.assertEqual(payment.status, "expired")
            self.assertFalse(payment.metadata_json["payment_received"])
            template = session.get(BuildingAgreementTemplate, "event-template-v1")
            self.assertEqual(template.status, "approved")


if __name__ == "__main__":
    unittest.main()
