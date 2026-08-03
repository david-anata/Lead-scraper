from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import date, timedelta

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_content_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import (
        create_session_factory,
        init_database,
    )
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingLifestyleMedia,
        BuildingTenantLogo,
        BuildingTestimonial,
    )
    from sales_support_agent.services.access.catalog import (
        ALL_TOOL_KEYS,
        grants_tool,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingContentPublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_content_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="content-internal-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "content-internal-key"}
        cls.expiry = (date.today() + timedelta(days=180)).isoformat()

    def _save(self, kind: str, record_id: str, **values):
        base = {
            "id": record_id,
            "actor": "content@example.com",
            "source_reference": "drive://reviewed-source",
            "consent_reference": "crm://consent/approved",
            "review_expires_on": self.expiry,
        }
        base.update(values)
        return self.client.put(
            f"/api/internal/building/content/{kind}/{record_id}",
            headers=self.headers,
            json=base,
        )

    def _review(self, kind: str, record_id: str, status: str):
        return self.client.post(
            f"/api/internal/building/content/{kind}/{record_id}/review",
            headers=self.headers,
            json={
                "status": status,
                "actor": "reviewer@example.com",
                "reason": "Evidence reviewed for public use.",
            },
        )

    def test_01_tables_are_created_by_additive_building_schema(self) -> None:
        with self.factory() as session:
            self.assertEqual(session.query(BuildingLifestyleMedia).count(), 0)
            self.assertEqual(session.query(BuildingTenantLogo).count(), 0)
            self.assertEqual(session.query(BuildingTestimonial).count(), 0)
        self.assertIn("building.content.manage", ALL_TOOL_KEYS)
        self.assertIn("building.publish", ALL_TOOL_KEYS)
        self.assertTrue(
            grants_tool({"building.manage"}, "building.content.manage")
        )
        self.assertFalse(
            grants_tool({"building.content.manage"}, "building.publish")
        )
        anonymous = self.client.get(
            "/admin/building/content",
            follow_redirects=False,
        )
        self.assertEqual(anonymous.status_code, 302)
        self.assertEqual(anonymous.headers["location"], "/admin/login")

    def test_02_approved_current_records_project_only_public_fields(self) -> None:
        lifestyle = self._save(
            "lifestyle_media",
            "community-kitchen",
            title="Community kitchen",
            media_url="/media/community-kitchen.webp",
            media_kind="image",
            alt_text="Members sharing lunch around the community table",
            caption="A shared place to recharge.",
            placement="lifestyle",
        )
        self.assertEqual(lifestyle.status_code, 200, lifestyle.text)
        logo = self._save(
            "tenant_logo",
            "northstar-logo",
            tenant_name="Northstar",
            asset_url="https://cdn.example.com/northstar.svg",
            alt_text="Northstar",
            destination_url="https://northstar.example",
        )
        self.assertEqual(logo.status_code, 200, logo.text)
        testimonial = self._save(
            "testimonial",
            "review-one",
            quote="The meeting rooms make client days simple.",
            attribution_name="Taylor R.",
            attribution_title="Founder",
            attribution_company="Northstar",
            rating=5,
        )
        self.assertEqual(testimonial.status_code, 200, testimonial.text)
        for kind, record_id in (
            ("lifestyle_media", "community-kitchen"),
            ("tenant_logo", "northstar-logo"),
            ("testimonial", "review-one"),
        ):
            self.assertEqual(
                self._review(kind, record_id, "needs_review").status_code,
                200,
            )
            approved = self._review(kind, record_id, "approved")
            self.assertEqual(approved.status_code, 200, approved.text)

        projection = self.client.get("/api/public/building/content")
        self.assertEqual(projection.status_code, 200, projection.text)
        body = projection.json()
        self.assertEqual(body["lifestyle_media"][0]["id"], "community-kitchen")
        self.assertEqual(body["tenant_logos"][0]["id"], "northstar-logo")
        self.assertEqual(body["testimonials"][0]["id"], "review-one")
        rendered = projection.text
        self.assertNotIn("consent_reference", rendered)
        self.assertNotIn("source_reference", rendered)
        self.assertNotIn("internal_notes", rendered)
        with self.factory() as session:
            actions = {
                row.action
                for row in session.query(BuildingAuditEvent)
                .filter(BuildingAuditEvent.entity_id == "review-one")
                .all()
            }
        self.assertIn("created", actions)
        self.assertIn("status_approved", actions)

    def test_03_expired_or_unapproved_records_are_not_public(self) -> None:
        draft = self._save(
            "testimonial",
            "draft-review",
            quote="A useful private office.",
            attribution_name="Draft Person",
        )
        self.assertEqual(draft.status_code, 200, draft.text)
        expired = self._save(
            "tenant_logo",
            "expired-logo",
            tenant_name="Former Tenant",
            asset_url="/media/former.svg",
            alt_text="Former Tenant",
        )
        self.assertEqual(expired.status_code, 200, expired.text)
        self.assertEqual(
            self._review("tenant_logo", "expired-logo", "needs_review").status_code,
            200,
        )
        self.assertEqual(
            self._review("tenant_logo", "expired-logo", "approved").status_code,
            200,
        )
        with self.factory() as session:
            row = session.get(BuildingTenantLogo, "expired-logo")
            row.review_expires_on = date.today() - timedelta(days=1)
            session.commit()
        projection = self.client.get("/api/public/building/content").json()
        ids = {
            item["id"]
            for group in ("tenant_logos", "testimonials")
            for item in projection[group]
        }
        self.assertNotIn("draft-review", ids)
        self.assertNotIn("expired-logo", ids)

    def test_04_private_benefit_language_is_rejected_but_boom_branding_is_allowed(self) -> None:
        response = self._save(
            "testimonial",
            "private-benefit-review",
            quote="Tenants receive a complimentary Boom membership here.",
            attribution_name="Internal Draft",
        )
        self.assertEqual(response.status_code, 422, response.text)
        self.assertIn("private-benefit", response.text)
        logo = self._save(
            "tenant_logo",
            "boom-fitness-culture-logo",
            tenant_name="Boom Fitness Culture",
            asset_url="/brand/boom-fitness-culture.png",
            alt_text="Boom Fitness Culture",
            destination_url="https://boomfitnessculture.com/",
        )
        self.assertEqual(logo.status_code, 200, logo.text)

    def test_05_offering_publish_is_blocked_until_ready(self) -> None:
        space = self.client.put(
            "/api/internal/building/spaces/readiness-room",
            headers=self.headers,
            json={
                "id": "readiness-room",
                "slug": "readiness-room",
                "name": "Readiness room",
                "space_type": "conference",
                "status": "available",
                "is_public": True,
            },
        )
        self.assertEqual(space.status_code, 200, space.text)
        blocked = self.client.put(
            "/api/internal/building/offerings/readiness-room",
            headers=self.headers,
            json={
                "id": "readiness-room",
                "slug": "readiness-room",
                "name": "Readiness room",
                "offering_type": "meeting_room",
                "space_id": "readiness-room",
                "is_published": True,
            },
        )
        self.assertEqual(blocked.status_code, 422, blocked.text)
        fields = {item["field"] for item in blocked.json()["detail"]["blockers"]}
        self.assertTrue({"public_description", "media", "pricing"}.issubset(fields))
        draft = self.client.put(
            "/api/internal/building/offerings/readiness-room",
            headers=self.headers,
            json={
                "id": "readiness-room",
                "slug": "readiness-room",
                "name": "Readiness room",
                "offering_type": "meeting_room",
                "space_id": "readiness-room",
                "is_published": False,
            },
        )
        self.assertEqual(draft.status_code, 200, draft.text)
        readiness = self.client.get(
            "/api/internal/building/offerings/readiness-room/publication-readiness",
            headers=self.headers,
        )
        self.assertEqual(readiness.status_code, 200, readiness.text)
        self.assertFalse(readiness.json()["ready"])
