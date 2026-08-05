from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_signature_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-signature-session",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingContact,
    BuildingReservation,
    BuildingSignatureRequestReadiness,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class BuildingSignatureReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            f"building_signature_{uuid.uuid4().hex}.db",
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="signature-internal",
            building_campaign_token_secret="signature-csrf",
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
                id="signature-arena",
                slug="signature-arena",
                name="The Arena",
                space_type="event",
                status="available",
            ))
            session.add(BuildingContact(
                id="signature-contact",
                email="signer@example.com",
                full_name="Taylor Morgan",
                status="active",
            ))
            session.add(BuildingReservation(
                id="signature-reservation",
                kind="event",
                status="soft_hold",
                contact_id="signature-contact",
                space_id="signature-arena",
                starts_at=now + timedelta(days=30),
                ends_at=now + timedelta(days=30, hours=8),
                hold_expires_at=now + timedelta(days=3),
                assigned_owner="events@anatainc.com",
            ))
            session.add(BuildingAgreement(
                id="signature-agreement",
                reservation_id="signature-reservation",
                version=1,
                status="draft",
                preparation_status="approved",
                package_checksum="a" * 64,
                package_snapshot_json={
                    "document": {"text": "Approved agreement", "checksum": "b" * 64},
                    "quote": {"currency": "USD", "amount_cents": 105000},
                },
                approved_by="legal@example.com",
                approved_at=now,
                created_by="operator@example.com",
            ))
            session.commit()
        cls.headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }

    def _csrf(self) -> str:
        page = self.client.get(
            "/admin/building/contracts/signature-agreement"
        )
        match = re.search(
            r'name="_csrf_token" value="([^"]+)"',
            page.text,
        )
        self.assertIsNotNone(match, page.text)
        return match.group(1)

    def _post(self, path: str, data: dict | None = None):
        return self.client.post(
            path,
            headers=self.headers,
            data={"_csrf_token": self._csrf(), **(data or {})},
            follow_redirects=False,
        )

    def test_01_prepare_is_idempotent_and_sends_nothing(self) -> None:
        prepared = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness"
        )
        self.assertEqual(prepared.status_code, 303, prepared.text)
        self.assertIn("nothing+was+sent", prepared.headers["location"])
        replay = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness"
        )
        self.assertEqual(replay.status_code, 303, replay.text)
        self.assertIn("already+exists", replay.headers["location"])
        with self.factory() as session:
            rows = session.query(BuildingSignatureRequestReadiness).all()
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row.status, "prepared")
            self.assertEqual(row.delivery_status, "not_sent")
            self.assertEqual(row.provider, "quickbooks_contract_builder")
            self.assertEqual(row.provider_reference, "")
            self.assertEqual(row.signer_email, "signer@example.com")
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="signature_request_readiness",
                entity_id=row.id,
                action="signature_request_readiness_prepared",
            ).one()
            self.assertFalse(audit.after_json["provider_write"])
            self.assertFalse(audit.after_json["message_sent"])

    def test_02_review_and_approval_require_exact_confirmation(self) -> None:
        with self.factory() as session:
            row = session.query(BuildingSignatureRequestReadiness).one()
            readiness_id = row.id
        wrong = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness/transition",
            {"target_status": "in_review", "confirmation": "REVIEW"},
        )
        self.assertIn("Type+REVIEW+SIGNATURE", wrong.headers["location"])
        reviewed = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness/transition",
            {
                "target_status": "in_review",
                "confirmation": f"REVIEW SIGNATURE {readiness_id}",
            },
        )
        self.assertIn("nothing+was+sent", reviewed.headers["location"])
        approved = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness/transition",
            {
                "target_status": "approved",
                "confirmation": f"APPROVE SIGNATURE {readiness_id}",
            },
        )
        self.assertIn("nothing+was+sent", approved.headers["location"])
        with self.factory() as session:
            row = session.get(BuildingSignatureRequestReadiness, readiness_id)
            self.assertEqual(row.status, "approved")
            self.assertEqual(row.delivery_status, "not_sent")
            self.assertTrue(row.reviewed_by)
            self.assertTrue(row.approved_by)

    def test_03_detail_is_plain_language_and_truthful(self) -> None:
        page = self.client.get(
            "/admin/building/contracts/signature-agreement"
        )
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Signature handoff", page.text)
        self.assertIn("Taylor Morgan", page.text)
        self.assertIn("Not Sent", page.text)
        self.assertIn(
            "does not claim a QuickBooks request exists",
            page.text,
        )
        self.assertIn("QuickBooks Contract Builder", page.text)
        self.assertIn("Copy-ready handoff manifest", page.text)
        self.assertIn("agreement_checksum", page.text)
        self.assertNotIn("Send for signature", page.text)

    def test_03a_failed_handoff_is_explicit_and_retryable_without_delivery(self) -> None:
        failed = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness/recovery",
            {"target_status": "failed", "failure_reason": "QuickBooks upload timed out."},
        )
        self.assertIn("nothing+was+sent", failed.headers["location"])
        with self.factory() as session:
            row = session.query(BuildingSignatureRequestReadiness).one()
            self.assertEqual(row.delivery_status, "failed")
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_id=row.id,
                action="signature_handoff_failed",
            ).one()
            self.assertFalse(audit.after_json["message_sent"])
        retried = self._post(
            "/admin/building/contracts/signature-agreement/signature-readiness/recovery",
            {"target_status": "not_sent"},
        )
        self.assertIn("ready+to+retry", retried.headers["location"])
        with self.factory() as session:
            row = session.query(BuildingSignatureRequestReadiness).one()
            self.assertEqual(row.delivery_status, "not_sent")

    def test_04_csrf_and_auth_fail_closed(self) -> None:
        no_csrf = self.client.post(
            "/admin/building/contracts/signature-agreement/signature-readiness",
            headers=self.headers,
            data={},
            follow_redirects=False,
        )
        self.assertEqual(no_csrf.status_code, 403)
        guest = TestClient(app)
        denied = guest.post(
            "/admin/building/contracts/signature-agreement/signature-readiness",
            follow_redirects=False,
        )
        self.assertIn(denied.status_code, {302, 303})
        self.assertEqual(denied.headers["location"], "/admin/login")

    def test_05_expired_hold_blocks_new_readiness(self) -> None:
        now = datetime.now(timezone.utc)
        with self.factory() as session:
            session.add(BuildingContact(
                id="expired-signature-contact",
                email="expired@example.com",
                full_name="Expired Customer",
                status="active",
            ))
            session.add(BuildingReservation(
                id="expired-signature-reservation",
                kind="event",
                status="soft_hold",
                contact_id="expired-signature-contact",
                space_id="signature-arena",
                starts_at=now + timedelta(days=10),
                ends_at=now + timedelta(days=10, hours=4),
                hold_expires_at=now - timedelta(minutes=1),
            ))
            session.add(BuildingAgreement(
                id="expired-signature-agreement",
                reservation_id="expired-signature-reservation",
                version=1,
                preparation_status="approved",
                package_checksum="c" * 64,
                package_snapshot_json={"document": {"text": "Agreement"}},
            ))
            session.commit()
        response = self._post(
            "/admin/building/contracts/expired-signature-agreement/signature-readiness"
        )
        self.assertIn(
            "active+temporary+hold+is+required",
            response.headers["location"],
        )
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingSignatureRequestReadiness)
                .filter_by(agreement_id="expired-signature-agreement")
                .count(),
                0,
            )

    def test_06_quickbooks_signature_requires_and_records_strong_evidence(self) -> None:
        endpoint = "/api/internal/building/bookings/signature-reservation/agreements"
        base = {
            "id": "signature-agreement",
            "version": 1,
            "status": "signed",
            "provider": "quickbooks_contract_builder",
            "provider_reference": "QB-CONTRACT-1042",
            "document_url": "https://qbo.intuit.com/contracts/QB-CONTRACT-1042",
            "actor": "david@anatainc.com",
        }
        missing_certificate = self.client.post(
            endpoint,
            headers={"X-Internal-API-Key": "signature-internal"},
            json={**base, "evidence": {}},
        )
        self.assertEqual(missing_certificate.status_code, 422)
        recorded = self.client.post(
            endpoint,
            headers={"X-Internal-API-Key": "signature-internal"},
            json={
                **base,
                "evidence": {
                    "esign_certificate_reference": "QB-CERT-1042",
                    "signed_document_checksum": "d" * 64,
                },
            },
        )
        self.assertEqual(recorded.status_code, 201, recorded.text)
        with self.factory() as session:
            agreement = session.get(BuildingAgreement, "signature-agreement")
            readiness = session.query(BuildingSignatureRequestReadiness).filter_by(
                agreement_id="signature-agreement"
            ).one()
            reservation = session.get(BuildingReservation, "signature-reservation")
            self.assertEqual(agreement.status, "signed")
            self.assertEqual(agreement.provider, "quickbooks_contract_builder")
            self.assertEqual(readiness.delivery_status, "completed")
            self.assertEqual(readiness.provider_reference, "QB-CONTRACT-1042")
            self.assertEqual(reservation.agreement_status, "signed")


if __name__ == "__main__":
    unittest.main()
