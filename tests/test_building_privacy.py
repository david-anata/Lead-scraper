from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_privacy_boot.db",
)

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import select

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingPrivacyRequest,
        BuildingSuppression,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingPrivacyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_privacy_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings, internal_api_key="privacy-test-key"
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "privacy-test-key"}
        with factory() as session:
            session.add(BuildingContact(
                id="privacy-contact",
                email="privacy@example.com",
                full_name="Original Name",
                phone="555-0100",
                company_name="Original Company",
                source="test",
                metadata_json={"private_provider_payload": "must-not-export"},
            ))
            session.add(BuildingCommunicationPreference(
                contact_id="privacy-contact",
                marketing_status="subscribed",
                transactional_allowed=True,
            ))
            session.commit()

    def test_export_requires_internal_access_and_is_allow_listed(self) -> None:
        unauthorized = self.client.get(
            "/api/internal/building/privacy/contacts/privacy-contact/export"
        )
        self.assertEqual(unauthorized.status_code, 401)
        response = self.client.get(
            "/api/internal/building/privacy/contacts/privacy-contact/export",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["contact"]["email"], "privacy@example.com")
        self.assertNotIn("metadata_json", response.text)
        self.assertNotIn("must-not-export", response.text)

    def test_correction_requires_reason_and_audits_before_after(self) -> None:
        invalid = self.client.post(
            "/api/internal/building/privacy/contacts/privacy-contact/correct",
            headers=self.headers,
            json={"full_name": "New Name", "reason": "no"},
        )
        self.assertEqual(invalid.status_code, 422)
        response = self.client.post(
            "/api/internal/building/privacy/contacts/privacy-contact/correct",
            headers=self.headers,
            json={"full_name": "Corrected Name", "reason": "Verified by requestor"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        with self.factory() as session:
            contact = session.get(BuildingContact, "privacy-contact")
            self.assertEqual(contact.full_name, "Corrected Name")
            audit = session.execute(
                select(BuildingAuditEvent)
                .where(BuildingAuditEvent.action == "privacy_corrected")
                .order_by(BuildingAuditEvent.id.desc())
            ).scalars().first()
            self.assertEqual(audit.before_json["full_name"], "Original Name")
            self.assertEqual(audit.after_json["full_name"], "Corrected Name")

    def test_marketing_suppression_preserves_transactional_permission(self) -> None:
        response = self.client.post(
            "/api/internal/building/privacy/contacts/privacy-contact/suppress",
            headers=self.headers,
            json={"scope": "marketing", "reason": "Requestor opted out"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        with self.factory() as session:
            suppression = session.get(BuildingSuppression, "privacy@example.com")
            preference = session.get(
                BuildingCommunicationPreference, "privacy-contact"
            )
            self.assertEqual(suppression.scope, "marketing")
            self.assertEqual(preference.marketing_status, "unsubscribed")
            self.assertTrue(preference.transactional_allowed)

    def test_deletion_review_never_deletes_contact_and_closure_needs_evidence(self) -> None:
        created = self.client.post(
            "/api/internal/building/privacy/requests",
            headers=self.headers,
            json={
                "contact_id": "privacy-contact",
                "request_type": "deletion_review",
                "requestor_email": "privacy@example.com",
                "details": "Please review this record.",
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        request_id = created.json()["id"]
        started = self.client.post(
            f"/api/internal/building/privacy/requests/{request_id}/transition",
            headers=self.headers,
            json={"status": "in_review"},
        )
        self.assertEqual(started.status_code, 200, started.text)
        invalid_close = self.client.post(
            f"/api/internal/building/privacy/requests/{request_id}/transition",
            headers=self.headers,
            json={"status": "completed", "resolution": "Reviewed", "evidence": {}},
        )
        self.assertEqual(invalid_close.status_code, 422)
        with self.factory() as session:
            self.assertIsNotNone(session.get(BuildingContact, "privacy-contact"))
            row = session.get(BuildingPrivacyRequest, request_id)
            self.assertEqual(row.status, "in_review")
        closed = self.client.post(
            f"/api/internal/building/privacy/requests/{request_id}/transition",
            headers=self.headers,
            json={
                "status": "completed",
                "resolution": "Record retained for documented contractual obligations.",
                "evidence": {"note": "Operator reviewed active obligations."},
            },
        )
        self.assertEqual(closed.status_code, 200, closed.text)
        with self.factory() as session:
            self.assertIsNotNone(session.get(BuildingContact, "privacy-contact"))
