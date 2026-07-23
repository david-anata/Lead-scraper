from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_contact_merge_boot.db",
)

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import select

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingCampaign,
        BuildingCampaignRecipient,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingContactMerge,
        BuildingRelationship,
        BuildingReservation,
        BuildingSegment,
        BuildingSpace,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingContactMergeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_contact_merge_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings, internal_api_key="merge-test-key"
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "merge-test-key"}
        now = datetime.now(timezone.utc)
        with factory() as session:
            session.add_all([
                BuildingContact(
                    id="merge-survivor", email="survivor@example.com",
                    full_name="Taylor Morgan", source="operator",
                    hubspot_contact_id="hs-survivor",
                ),
                BuildingContact(
                    id="merge-duplicate", email="duplicate@example.com",
                    full_name="", phone="555-0101", company_name="Merged Company",
                    source="import", hubspot_contact_id="hs-duplicate",
                ),
                BuildingCommunicationPreference(
                    contact_id="merge-survivor", marketing_status="subscribed",
                    transactional_allowed=True,
                ),
                BuildingCommunicationPreference(
                    contact_id="merge-duplicate", marketing_status="unsubscribed",
                    transactional_allowed=False,
                ),
                BuildingRelationship(
                    id="survivor-lease", contact_id="merge-survivor",
                    relationship_type="tenant", source_reference="lease-1",
                ),
                BuildingRelationship(
                    id="duplicate-lease", contact_id="merge-duplicate",
                    relationship_type="tenant", source_reference="lease-1",
                ),
                BuildingRelationship(
                    id="duplicate-event", contact_id="merge-duplicate",
                    relationship_type="event_host", source_reference="event-1",
                ),
                BuildingSpace(
                    id="merge-office", slug="merge-office", name="Merge Office",
                    space_type="private_office", status="available",
                ),
                BuildingReservation(
                    id="merge-reservation", kind="workspace", status="inquiry",
                    contact_id="merge-duplicate", space_id="merge-office",
                    starts_at=now + timedelta(days=30),
                    ends_at=now + timedelta(days=395),
                ),
                BuildingSegment(
                    id="merge-segment", name="Merge Segment", rules_json={},
                ),
                BuildingCampaign(
                    id="merge-campaign", name="Historical Campaign",
                    segment_id="merge-segment", subject="History",
                    body_text="Historical message", status="sent",
                ),
                BuildingCampaignRecipient(
                    campaign_id="merge-campaign", contact_id="merge-duplicate",
                    email="duplicate@example.com", full_name="Duplicate",
                    status="sent",
                ),
            ])
            session.commit()

    def test_00_preview_is_protected_and_explains_preserved_history(self) -> None:
        unauthorized = self.client.post(
            "/api/internal/building/crm/contacts/merge/preview",
            params={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
            },
        )
        self.assertEqual(unauthorized.status_code, 401)
        response = self.client.post(
            "/api/internal/building/crm/contacts/merge/preview",
            headers=self.headers,
            params={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        preview = response.json()
        self.__class__.preview_hash = preview["preview_hash"]
        self.assertEqual(preview["counts"]["relationships_to_move"], 1)
        self.assertEqual(preview["counts"]["duplicate_relationships_preserved"], 1)
        self.assertEqual(preview["counts"]["campaign_snapshots_preserved"], 1)
        self.assertEqual(preview["consent_result"]["marketing_status"], "unsubscribed")
        self.assertFalse(preview["consent_result"]["transactional_allowed"])
        self.assertTrue(preview["conflicts"])

    def test_01_confirmation_and_fresh_preview_are_required(self) -> None:
        wrong = self.client.post(
            "/api/internal/building/crm/contacts/merge",
            headers=self.headers,
            json={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
                "preview_hash": self.preview_hash,
                "confirmation": "MERGE",
                "reason": "Imported duplicate confirmed by operator.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(wrong.status_code, 422)
        with self.factory() as session:
            survivor = session.get(BuildingContact, "merge-survivor")
            survivor.phone = "555-9999"
            survivor.updated_at = datetime.now(timezone.utc)
            session.commit()
        stale = self.client.post(
            "/api/internal/building/crm/contacts/merge",
            headers=self.headers,
            json={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
                "preview_hash": self.preview_hash,
                "confirmation": "MERGE merge-duplicate INTO merge-survivor",
                "reason": "Imported duplicate confirmed by operator.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(stale.status_code, 409)

    def test_02_merge_moves_operations_but_preserves_historical_snapshots(self) -> None:
        preview = self.client.post(
            "/api/internal/building/crm/contacts/merge/preview",
            headers=self.headers,
            params={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
            },
        ).json()
        response = self.client.post(
            "/api/internal/building/crm/contacts/merge",
            headers=self.headers,
            json={
                "survivor_contact_id": "merge-survivor",
                "merged_contact_id": "merge-duplicate",
                "preview_hash": preview["preview_hash"],
                "confirmation": "MERGE merge-duplicate INTO merge-survivor",
                "reason": "Imported duplicate confirmed by operator.",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        with self.factory() as session:
            survivor = session.get(BuildingContact, "merge-survivor")
            duplicate = session.get(BuildingContact, "merge-duplicate")
            preference = session.get(
                BuildingCommunicationPreference, "merge-survivor"
            )
            reservation = session.get(BuildingReservation, "merge-reservation")
            recipient = session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.campaign_id == "merge-campaign"
                )
            ).scalar_one()
            self.assertEqual(duplicate.status, "merged")
            self.assertEqual(duplicate.metadata_json["_merged_into_contact_id"], survivor.id)
            self.assertEqual(survivor.company_name, "Merged Company")
            self.assertEqual(survivor.hubspot_contact_id, "hs-survivor")
            self.assertIn("hs-duplicate", survivor.metadata_json["_merged_hubspot_contact_ids"])
            self.assertEqual(preference.marketing_status, "unsubscribed")
            self.assertFalse(preference.transactional_allowed)
            self.assertEqual(reservation.contact_id, survivor.id)
            self.assertEqual(recipient.contact_id, duplicate.id)
            moved_relationship = session.get(BuildingRelationship, "duplicate-event")
            duplicate_relationship = session.get(BuildingRelationship, "duplicate-lease")
            self.assertEqual(moved_relationship.contact_id, survivor.id)
            self.assertEqual(duplicate_relationship.contact_id, duplicate.id)
            self.assertEqual(session.query(BuildingContactMerge).count(), 1)
            audit = session.execute(
                select(BuildingAuditEvent).where(
                    BuildingAuditEvent.action == "contacts_merged"
                )
            ).scalar_one()
            self.assertEqual(
                audit.after_json["consent_result"]["marketing_status"],
                "unsubscribed",
            )


if __name__ == "__main__":
    unittest.main()
