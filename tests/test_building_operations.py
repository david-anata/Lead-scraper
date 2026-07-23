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
    from sales_support_agent.models.database import (
        _repair_legacy_building_event_inquiries,
        backfill_building_inquiry_assignments,
        create_session_factory,
        init_database,
    )
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingAvailabilityBlock,
        BuildingContact,
        BuildingInquiry,
        BuildingRelationship,
    )
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
            building_default_lead_owner="events@example.com",
            building_response_sla_hours=6,
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

    def test_private_office_public_availability_follows_agent_blocks(self) -> None:
        space = self.client.put(
            "/api/internal/building/spaces/office-availability",
            headers=self.internal_headers,
            json={
                "id": "office-availability",
                "slug": "office-availability",
                "name": "Availability Office",
                "space_type": "private_office",
                "status": "available",
                "is_public": True,
            },
        )
        self.assertEqual(space.status_code, 200, space.text)
        offering = self.client.put(
            "/api/internal/building/offerings/office-availability",
            headers=self.internal_headers,
            json={
                "id": "office-availability",
                "slug": "office-availability",
                "name": "Availability Office",
                "offering_type": "private_office",
                "space_id": "office-availability",
                "is_published": True,
            },
        )
        self.assertEqual(offering.status_code, 200, offering.text)
        baseline = self.client.get(
            "/api/public/building/offerings/office-availability"
        )
        self.assertEqual(baseline.json()["space"]["availability"], "available")

        now = datetime.now(timezone.utc)
        held = self.client.post(
            "/api/internal/building/availability",
            headers=self.internal_headers,
            json={
                "id": "office-availability-block",
                "space_id": "office-availability",
                "state": "soft_hold",
                "starts_at": (now + timedelta(days=7)).isoformat(),
                "ends_at": (now + timedelta(days=37)).isoformat(),
                "expires_at": (now + timedelta(days=1)).isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(held.status_code, 201, held.text)
        held_public = self.client.get(
            "/api/public/building/offerings/office-availability"
        ).json()["space"]
        self.assertEqual(held_public["availability"], "contact")
        self.assertIsNone(held_public["available_from"])

        with self.factory() as session:
            block = session.get(
                BuildingAvailabilityBlock,
                "office-availability-block",
            )
            block.expires_at = now - timedelta(minutes=1)
            session.commit()
        expired_public = self.client.get(
            "/api/public/building/offerings/office-availability"
        ).json()["space"]
        self.assertEqual(expired_public["availability"], "available")

        with self.factory() as session:
            block = session.get(
                BuildingAvailabilityBlock,
                "office-availability-block",
            )
            block.state = "occupied"
            block.expires_at = None
            session.commit()
        occupied_public = self.client.get(
            "/api/public/building/offerings/office-availability"
        ).json()["space"]
        self.assertEqual(occupied_public["availability"], "turnover")
        self.assertEqual(
            occupied_public["available_from"],
            (now + timedelta(days=37)).date().isoformat(),
        )

    def test_rate_plans_are_versioned_approved_and_publicly_redacted(self) -> None:
        invalid = self.client.put(
            "/api/internal/building/offerings/arena-events/rate-plans/arena-v1",
            headers=self.internal_headers,
            json={
                "id": "arena-v1",
                "version": 1,
                "name": "Arena standard",
                "status": "approved",
                "currency": "USD",
                "unit_amount_cents": 250000,
                "public_price_display": "From $2,500",
                "booking_unit": "event",
                "minimum_units": 1,
                "deposit_type": "percent",
                "deposit_percent_bps": 5000,
                "effective_from": "2026-01-01",
                "approved_by": "approver@example.com",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(invalid.status_code, 422, invalid.text)
        approved = self.client.put(
            "/api/internal/building/offerings/arena-events/rate-plans/arena-v1",
            headers=self.internal_headers,
            json={
                "id": "arena-v1",
                "version": 1,
                "name": "Arena standard",
                "status": "approved",
                "currency": "USD",
                "unit_amount_cents": 250000,
                "public_price_display": "From $2,500",
                "booking_unit": "event",
                "minimum_units": 1,
                "deposit_type": "percent",
                "deposit_percent_bps": 5000,
                "cancellation_policy": "Deposit is non-refundable within 30 days.",
                "included": ["Tables", "Chairs"],
                "addons": [{"name": "Extra cleaning", "amount_cents": 15000}],
                "effective_from": "2026-01-01",
                "approved_by": "approver@example.com",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        public = self.client.get("/api/public/building/offerings/arena-events")
        self.assertEqual(public.status_code, 200, public.text)
        rate_plan = public.json()["rate_plan"]
        self.assertEqual(public.json()["price_display"], "From $2,500")
        self.assertEqual(rate_plan["deposit"]["percent"], 50.0)
        self.assertNotIn("unit_amount_cents", rate_plan)
        self.assertNotIn("approved_by", rate_plan)
        overlapping = self.client.put(
            "/api/internal/building/offerings/arena-events/rate-plans/arena-v2",
            headers=self.internal_headers,
            json={
                "id": "arena-v2",
                "version": 2,
                "name": "Arena standard 2027",
                "status": "approved",
                "unit_amount_cents": 275000,
                "public_price_display": "From $2,750",
                "cancellation_policy": "Deposit is non-refundable within 30 days.",
                "effective_from": "2027-01-01",
                "approved_by": "approver@example.com",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(overlapping.status_code, 409, overlapping.text)
        retire = self.client.put(
            "/api/internal/building/offerings/arena-events/rate-plans/arena-v1",
            headers=self.internal_headers,
            json={
                "id": "arena-v1",
                "version": 1,
                "name": "Arena standard",
                "status": "retired",
                "effective_from": "2026-01-01",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(retire.status_code, 200, retire.text)
        self.assertEqual(retire.json()["rate_plan"]["status"], "retired")

    def test_inquiry_requires_secret_consent_and_idempotency(self) -> None:
        payload = {
            "kind": "event",
            "name": "Ada Test",
            "email": "ada@example.com",
            "preferred_date": "2026-08-15",
            "consent_to_contact": True,
            "details": {
                "event_type": "Company gathering",
                "landingPage": "/events?utm_source=google&utm_campaign=summer",
                "utmSource": "google",
                "utmMedium": "paid_search",
                "utmCampaign": "summer",
                "firstLandingPage": "/",
                "firstUtmSource": "direct",
                "firstCapturedAt": "2026-08-01T12:00:00Z",
            },
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
            inquiry = session.query(BuildingInquiry).one()
            contact = session.query(BuildingContact).one()
            self.assertEqual(
                inquiry.payload_json["_attribution"]["campaign"], "summer"
            )
            self.assertEqual(
                contact.metadata_json["_building_attribution"]["first_touch"]["source"],
                "direct",
            )
            self.assertEqual(
                contact.metadata_json["_building_attribution"]["latest_touch"]["source"],
                "anata-building",
            )
            relationship = session.query(BuildingRelationship).one()
            self.assertEqual(relationship.relationship_type, "prospect")
            self.assertEqual(
                relationship.metadata_json["inquiry_kind"],
                "event",
            )
            self.assertEqual(relationship.status, "active")
            self.assertEqual(inquiry.assigned_owner, "events@example.com")
            self.assertAlmostEqual(
                (inquiry.response_due_at - inquiry.created_at).total_seconds(),
                6 * 60 * 60,
                delta=1,
            )
            created_audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="inquiry",
                entity_id=inquiry.id,
                action="created",
            ).one()
            self.assertEqual(
                created_audit.after_json["assigned_owner"],
                "events@example.com",
            )
            inquiry.assigned_owner = ""
            inquiry.response_due_at = None
            session.commit()
        self.assertEqual(
            backfill_building_inquiry_assignments(
                self.factory,
                default_owner="legacy-owner@example.com",
                response_sla_hours=2,
            ),
            1,
        )
        with self.factory() as session:
            inquiry = session.query(BuildingInquiry).one()
            self.assertEqual(inquiry.assigned_owner, "legacy-owner@example.com")
            self.assertAlmostEqual(
                (inquiry.response_due_at - inquiry.created_at).total_seconds(),
                2 * 60 * 60,
                delta=1,
            )

    def test_inquiry_response_and_qualification_are_audited_separately_from_crm_sync(self) -> None:
        with self.factory() as session:
            inquiry = session.query(BuildingInquiry).one()
            inquiry_id = inquiry.id
            sync_status = inquiry.status
        responded = self.client.post(
            f"/api/internal/building/inquiries/{inquiry_id}/lifecycle",
            headers=self.internal_headers,
            json={
                "target_stage": "responded",
                "actor": "operator@example.com",
                "assigned_owner": "operator@example.com",
                "channel": "phone",
                "notes": "Discussed the requested event date.",
            },
        )
        self.assertEqual(responded.status_code, 200, responded.text)
        self.assertEqual(responded.json()["crm_sync_status"], sync_status)
        self.assertTrue(responded.json()["lifecycle"]["first_responded_at"])
        qualified = self.client.post(
            f"/api/internal/building/inquiries/{inquiry_id}/lifecycle",
            headers=self.internal_headers,
            json={
                "target_stage": "qualified",
                "actor": "operator@example.com",
                "channel": "email",
            },
        )
        self.assertEqual(qualified.status_code, 200, qualified.text)
        skipped = self.client.post(
            f"/api/internal/building/inquiries/{inquiry_id}/lifecycle",
            headers=self.internal_headers,
            json={
                "target_stage": "responded",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(skipped.status_code, 409)
        analytics = self.client.get(
            "/api/internal/building/analytics",
            headers=self.internal_headers,
        )
        self.assertEqual(analytics.status_code, 200, analytics.text)
        self.assertEqual(analytics.json()["inquiries"]["responded"], 1)
        self.assertEqual(analytics.json()["inquiries"]["by_source"]["anata-building"], 1)
        won = self.client.post(
            f"/api/internal/building/inquiries/{inquiry_id}/lifecycle",
            headers=self.internal_headers,
            json={
                "target_stage": "closed_won",
                "actor": "operator@example.com",
                "channel": "email",
                "notes": "Converted into the approved event workflow.",
            },
        )
        self.assertEqual(won.status_code, 200, won.text)
        with self.factory() as session:
            relationship = session.query(BuildingRelationship).one()
            self.assertEqual(relationship.status, "inactive")
            self.assertEqual(relationship.metadata_json["outcome"], "won")
            self.assertIsNotNone(relationship.ends_on)
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="relationship",
                entity_id=relationship.id,
                action="prospect_won",
            ).one()
            self.assertEqual(audit.after_json["inquiry_id"], inquiry_id)
            relationship.relationship_type = "event_host"
            relationship.status = "active"
            relationship.ends_on = None
            session.commit()
        self.assertEqual(
            _repair_legacy_building_event_inquiries(self.factory),
            1,
        )
        with self.factory() as session:
            relationship = session.query(BuildingRelationship).one()
            self.assertEqual(relationship.relationship_type, "prospect")
            self.assertEqual(relationship.status, "inactive")
            self.assertEqual(relationship.metadata_json["outcome"], "won")
            self.assertEqual(
                session.query(BuildingAuditEvent).filter_by(
                    entity_type="relationship",
                    entity_id=relationship.id,
                    action="legacy_event_inquiry_reclassified",
                ).count(),
                1,
            )

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
