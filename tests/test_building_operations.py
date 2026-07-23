from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/building_ops_boot.db")

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import BuildingAuditEvent, BuildingInquiry
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingOperationsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_ops_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
            building_site_intake_key="building-test-key",
            hubspot_api_token="",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.internal_headers = {"X-Internal-Api-Key": "internal-test-key"}
        cls.site_headers = {
            "X-Internal-Api-Key": "building-test-key",
            "Idempotency-Key": "inquiry-test-1",
        }

    def test_00_public_catalog_is_empty_before_publishing(self) -> None:
        response = self.client.get("/api/public/building/offerings")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["offerings"], [])

    def test_internal_space_and_offering_publish_safe_projection(self) -> None:
        space = self.client.put(
            "/api/internal/building/spaces/arena",
            headers=self.internal_headers,
            json={
                "id": "arena",
                "slug": "the-arena",
                "name": "The Arena",
                "space_type": "event",
                "capacity": 120,
                "status": "available",
                "public_description": "A flexible gathering space.",
                "internal_notes": "Never public.",
                "features": ["Stage"],
                "media": [{"src": "/media/legacy.webp", "alt": "Legacy unapproved media"}],
                "is_public": True,
            },
        )
        self.assertEqual(space.status_code, 200, space.text)
        offering = self.client.put(
            "/api/internal/building/offerings/arena-events",
            headers=self.internal_headers,
            json={
                "id": "arena-events",
                "slug": "arena-events",
                "name": "Arena events",
                "offering_type": "event",
                "space_id": "arena",
                "public_description": "Host a gathering in The Arena.",
                "price_display": "Contact us",
                "booking_unit": "event",
                "call_to_action": "check_date",
                "features": ["Stage"],
                "is_published": True,
            },
        )
        self.assertEqual(offering.status_code, 200, offering.text)
        public = self.client.get("/api/public/building/offerings").json()
        self.assertEqual(public["offerings"][0]["space"]["availability"], "available")
        self.assertNotIn("internal_notes", public["offerings"][0]["space"])
        self.assertEqual(public["offerings"][0]["space"]["media"], [])

    def test_space_media_requires_review_and_stays_attached_to_exact_space(self) -> None:
        for media_id, approved, alt, order in (
            ("arena-gallery", False, "", 1),
            ("arena-card", True, "Open floor and stage inside The Arena", 0),
        ):
            response = self.client.put(
                f"/api/internal/building/spaces/arena/media/{media_id}",
                headers=self.internal_headers,
                json={
                    "id": media_id,
                    "src": f"/media/{media_id}.webp",
                    "kind": "image",
                    "alt": alt,
                    "placement": "card" if approved else "gallery",
                    "sort_order": order,
                    "approved": approved,
                    "actor": "media-editor@example.com",
                },
            )
            self.assertEqual(response.status_code, 200, response.text)
        invalid = self.client.put(
            "/api/internal/building/spaces/arena/media/missing-alt",
            headers=self.internal_headers,
            json={
                "id": "missing-alt",
                "src": "http://insecure.example/image.jpg",
                "approved": True,
            },
        )
        self.assertEqual(invalid.status_code, 422)

        public = self.client.get("/api/public/building/offerings").json()
        media = public["offerings"][0]["space"]["media"]
        self.assertEqual([item["id"] for item in media], ["arena-card"])
        self.assertEqual(media[0]["placement"], "card")

        removed = self.client.request(
            "DELETE",
            "/api/internal/building/spaces/arena/media/arena-card",
            headers=self.internal_headers,
            json={"actor": "media-editor@example.com", "reason": "Wrong room selected"},
        )
        self.assertEqual(removed.status_code, 200, removed.text)
        public_after = self.client.get("/api/public/building/offerings").json()
        self.assertEqual(public_after["offerings"][0]["space"]["media"], [])
        with self.factory() as session:
            audit = (
                session.query(BuildingAuditEvent)
                .filter(BuildingAuditEvent.entity_id == "arena:arena-card")
                .order_by(BuildingAuditEvent.created_at.desc())
                .first()
            )
            self.assertIsNotNone(audit)
            self.assertEqual(audit.action, "removed")

    def test_inquiry_requires_secret_consent_and_idempotency(self) -> None:
        payload = {
            "kind": "event",
            "name": "Ada Test",
            "email": "ada@example.com",
            "preferred_date": "2026-08-15",
            "consent_to_contact": True,
            "details": {"event_type": "Company gathering"},
        }
        self.assertEqual(
            self.client.post("/api/public/building/inquiries", json=payload).status_code,
            401,
        )
        no_key = dict(self.site_headers)
        no_key.pop("Idempotency-Key")
        self.assertEqual(
            self.client.post(
                "/api/public/building/inquiries", headers=no_key, json=payload
            ).status_code,
            400,
        )
        with mock.patch("sales_support_agent.api.building_router.HubSpotClient") as hubspot:
            hubspot.return_value.is_configured = False
            first = self.client.post(
                "/api/public/building/inquiries", headers=self.site_headers, json=payload
            )
            second = self.client.post(
                "/api/public/building/inquiries", headers=self.site_headers, json=payload
            )
        self.assertEqual(first.status_code, 201, first.text)
        self.assertFalse(first.json()["duplicate"])
        self.assertTrue(second.json()["duplicate"])
        with self.factory() as session:
            self.assertEqual(session.query(BuildingInquiry).count(), 1)

    def test_overlapping_availability_is_rejected(self) -> None:
        start = datetime.now(timezone.utc) + timedelta(days=3)
        payload = {
            "space_id": "arena",
            "state": "booked",
            "starts_at": start.isoformat(),
            "ends_at": (start + timedelta(hours=4)).isoformat(),
            "actor": "test@example.com",
        }
        first = self.client.post(
            "/api/internal/building/availability",
            headers=self.internal_headers,
            json=payload,
        )
        second = self.client.post(
            "/api/internal/building/availability",
            headers=self.internal_headers,
            json=payload,
        )
        self.assertEqual(first.status_code, 201, first.text)
        self.assertEqual(first.json()["space_status"], "available")
        self.assertEqual(second.status_code, 409, second.text)

    def test_partial_hubspot_failure_is_retryable_without_duplicate_contact(self) -> None:
        class FailingNoteClient:
            is_configured = True

            def __init__(self):
                self.created = 0

            def find_contact_by_email(self, email):
                return None

            def create_contact(self, properties):
                self.created += 1
                return {"id": "hs-building-contact"}

            def create_contact_note(self, **kwargs):
                raise RuntimeError("temporary HubSpot note failure")

        failing = FailingNoteClient()
        headers = {
            "X-Internal-Api-Key": "building-test-key",
            "Idempotency-Key": "inquiry-hubspot-retry",
        }
        payload = {
            "kind": "workspace",
            "name": "Retry Prospect",
            "email": "retry-building@example.com",
            "consent_to_contact": True,
            "source": "facebook_marketplace",
            "source_reference": "marketplace-message-123",
        }
        app.state.settings = dataclasses.replace(
            app.state.settings,
            hubspot_api_token="hubspot-test-token",
        )
        with mock.patch(
            "sales_support_agent.api.building_router.HubSpotClient",
            return_value=failing,
        ):
            created = self.client.post(
                "/api/public/building/inquiries",
                headers=headers,
                json=payload,
            )
        self.assertEqual(created.status_code, 201, created.text)
        self.assertEqual(created.json()["status"], "crm_sync_needed")
        inquiry_id = created.json()["inquiry_id"]
        self.assertEqual(failing.created, 1)

        class RetryClient:
            is_configured = True

            def __init__(self):
                self.created = 0
                self.noted = 0

            def find_contact_by_email(self, email):
                raise AssertionError("The stored HubSpot ID should be reused.")

            def create_contact(self, properties):
                self.created += 1
                raise AssertionError("Retry must not create a duplicate contact.")

            def create_contact_note(self, **kwargs):
                self.noted += 1
                return {"id": "hs-note-1"}

        retry_client = RetryClient()
        with mock.patch(
            "sales_support_agent.api.building_router.HubSpotClient",
            return_value=retry_client,
        ):
            retried = self.client.post(
                f"/api/internal/building/inquiries/{inquiry_id}/retry-hubspot",
                headers=self.internal_headers,
                json={"actor": "operator@example.com"},
            )
        self.assertEqual(retried.status_code, 200, retried.text)
        self.assertTrue(retried.json()["ok"])
        self.assertEqual(retried.json()["status"], "new")
        self.assertEqual(retried.json()["attempt_count"], 2)
        self.assertEqual(retry_client.created, 0)
        self.assertEqual(retry_client.noted, 1)
