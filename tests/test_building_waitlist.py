from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_waitlist_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingRelationship,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingWaitlistTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_waitlist_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="waitlist-internal-key",
            building_site_intake_key="waitlist-site-key",
            building_default_lead_owner="leasing@example.com",
            hubspot_api_token="",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.internal_headers = {"X-Internal-Api-Key": "waitlist-internal-key"}
        space = cls.client.put(
            "/api/internal/building/spaces/waitlist-office",
            headers=cls.internal_headers,
            json={
                "id": "waitlist-office",
                "slug": "waitlist-office",
                "name": "Waitlist Office",
                "space_type": "private_office",
                "status": "turnover",
                "is_public": True,
            },
        )
        if space.status_code != 200:
            raise AssertionError(space.text)
        offering = cls.client.put(
            "/api/internal/building/offerings/waitlist-office",
            headers=cls.internal_headers,
            json={
                "id": "waitlist-office",
                "slug": "waitlist-office",
                "name": "Waitlist Office",
                "offering_type": "private_office",
                "space_id": "waitlist-office",
                "is_published": True,
            },
        )
        if offering.status_code != 200:
            raise AssertionError(offering.text)

    def test_waitlist_intent_creates_and_closes_waitlist_relationship(self) -> None:
        with mock.patch(
            "sales_support_agent.api.building_router.HubSpotClient"
        ) as hubspot:
            hubspot.return_value.is_configured = False
            created = self.client.post(
                "/api/public/building/inquiries",
                headers={
                    "X-Internal-Api-Key": "waitlist-site-key",
                    "Idempotency-Key": "waitlist-request-1",
                },
                json={
                    "kind": "workspace",
                    "name": "Waiting Tenant",
                    "email": "waiting@example.com",
                    "offering_id": "waitlist-office",
                    "consent_to_contact": True,
                    "details": {"intent": "waitlist", "teamSize": "3"},
                },
            )
        self.assertEqual(created.status_code, 201, created.text)
        inquiry_id = created.json()["inquiry_id"]
        with self.factory() as session:
            relationship = session.query(BuildingRelationship).one()
            self.assertEqual(relationship.relationship_type, "waitlist")
            self.assertEqual(relationship.status, "active")
            self.assertEqual(relationship.metadata_json["intent"], "waitlist")

        for target in ("responded", "qualified", "closed_lost"):
            response = self.client.post(
                f"/api/internal/building/inquiries/{inquiry_id}/lifecycle",
                headers=self.internal_headers,
                json={
                    "target_stage": target,
                    "actor": "leasing@example.com",
                    "channel": "email",
                },
            )
            self.assertEqual(response.status_code, 200, response.text)
        with self.factory() as session:
            relationship = session.query(BuildingRelationship).one()
            self.assertEqual(relationship.status, "inactive")
            self.assertEqual(relationship.metadata_json["outcome"], "lost")
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="relationship",
                entity_id=relationship.id,
                action="waitlist_lost",
            ).one()
            self.assertEqual(audit.after_json["inquiry_id"], inquiry_id)
