from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/tenant_campaign_boot.db",
)

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import select

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingCampaignRecipient,
        BuildingCommunicationPreference,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class TenantCampaignFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"tenant_campaign_{uuid.uuid4().hex}.db"
        )
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="tenant-campaign-test-key",
            resend_from="Anata Building <hello@example.com>",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "tenant-campaign-test-key"}

    def contact(self, contact_id: str, email: str) -> None:
        response = self.client.put(
            f"/api/internal/building/crm/contacts/{contact_id}",
            headers=self.headers,
            json={
                "email": email,
                "full_name": contact_id,
                "source": "test",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def relationship(
        self,
        contact_id: str,
        relationship_type: str,
        *,
        inquiry_kind: str | None = None,
    ) -> None:
        response = self.client.post(
            f"/api/internal/building/crm/contacts/{contact_id}/relationships",
            headers=self.headers,
            json={
                "relationship_type": relationship_type,
                "source_reference": f"test:{contact_id}",
                "metadata": (
                    {"inquiry_kind": inquiry_kind} if inquiry_kind else {}
                ),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 201, response.text)

    def marketing(self, contact_id: str, status: str = "subscribed") -> None:
        response = self.client.put(
            f"/api/internal/building/crm/contacts/{contact_id}/preference",
            headers=self.headers,
            json={
                "marketing_status": status,
                "source": "documented-test-consent",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def operational(self, contact_id: str) -> None:
        response = self.client.put(
            f"/api/internal/building/crm/contacts/{contact_id}/operational-preference",
            headers=self.headers,
            json={
                "transactional_allowed": True,
                "source": "documented-tenant-operations",
                "evidence_reference": f"lease:{contact_id}",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def test_canonical_segments_are_empty_safe_and_distinguish_prospects(self) -> None:
        bootstrap = self.client.post(
            "/api/internal/building/crm/segments/bootstrap", headers=self.headers
        )
        self.assertEqual(bootstrap.status_code, 200, bootstrap.text)
        self.assertEqual(bootstrap.json()["created"], 4)
        replay = self.client.post(
            "/api/internal/building/crm/segments/bootstrap", headers=self.headers
        )
        self.assertEqual(replay.json()["created"], 0)

        self.contact("workspace-lead", "workspace@example.com")
        self.relationship("workspace-lead", "prospect", inquiry_kind="workspace")
        self.marketing("workspace-lead")
        self.contact("event-lead", "event@example.com")
        self.relationship("event-lead", "prospect", inquiry_kind="event")
        self.marketing("event-lead")

        workspace = self.client.get(
            "/api/internal/building/crm/segments/workspace-prospects/preview",
            headers=self.headers,
        ).json()
        rows = {row["contact_id"]: row for row in workspace["contacts"]}
        self.assertTrue(rows["workspace-lead"]["included"])
        self.assertFalse(rows["event-lead"]["included"])
        self.assertEqual(workspace["included_count"], 1)
        self.assertFalse(workspace["empty"])

    def test_manual_lists_require_explicit_contacts_and_approval_evidence(self) -> None:
        self.contact("manual-contact", "manual@example.com")
        self.marketing("manual-contact")
        rejected = self.client.put(
            "/api/internal/building/crm/segments/manual-board",
            headers=self.headers,
            json={
                "id": "manual-board",
                "name": "Manual approved list",
                "segment_type": "manual_approved_list",
                "manual_contact_ids": ["manual-contact"],
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(rejected.status_code, 422, rejected.text)
        accepted = self.client.put(
            "/api/internal/building/crm/segments/manual-board",
            headers=self.headers,
            json={
                "id": "manual-board",
                "name": "Manual approved list",
                "segment_type": "manual_approved_list",
                "manual_contact_ids": ["manual-contact", "manual-contact"],
                "approval_evidence": "Approved list ticket CRM-42",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.assertEqual(accepted.json()["included_count"], 1)

    def test_operational_permission_is_separate_from_marketing(self) -> None:
        self.contact("operational-contact", "operational@example.com")
        self.marketing("operational-contact")
        response = self.client.put(
            "/api/internal/building/crm/contacts/operational-contact/operational-preference",
            headers=self.headers,
            json={
                "transactional_allowed": False,
                "source": "operator-review",
                "evidence_reference": "support-case-10",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        contact = self.client.get(
            "/api/internal/building/crm/contacts/operational-contact",
            headers=self.headers,
        ).json()["contact"]
        self.assertEqual(contact["marketing_status"], "subscribed")
        self.assertFalse(contact["operational_allowed"])
        self.assertEqual(
            contact["operational_evidence_reference"], "support-case-10"
        )

    def test_provider_free_review_approval_freezes_schedule_ready_outbox(self) -> None:
        self.contact("tenant-one", "tenant@example.com")
        self.relationship("tenant-one", "tenant")
        self.marketing("tenant-one")
        segment = self.client.put(
            "/api/internal/building/crm/segments/tenant-ops",
            headers=self.headers,
            json={
                "id": "tenant-ops",
                "name": "Tenant operations",
                "relationship_types": ["tenant"],
                "purpose_scope": "operational",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(segment.status_code, 200, segment.text)
        legacy_preview = self.client.get(
            "/api/internal/building/crm/segments/tenant-ops/preview",
            headers=self.headers,
        )
        self.assertEqual(legacy_preview.status_code, 200, legacy_preview.text)
        tenant_row = next(
            row
            for row in legacy_preview.json()["contacts"]
            if row["contact_id"] == "tenant-one"
        )
        self.assertFalse(tenant_row["included"])
        self.assertIn("authority evidence is incomplete", tenant_row["reason"])
        self.operational("tenant-one")
        draft_payload = {
            "id": "ops-1",
            "name": "Operations notice",
            "segment_id": "tenant-ops",
            "communication_class": "operational",
            "subject": "Building access",
            "body_text": "The west entrance will be unavailable.",
            "template_reference": "tenant-ops-v1",
            "actor": "operator@example.com",
        }
        draft = self.client.put(
            "/api/internal/building/crm/campaigns/ops-1",
            headers=self.headers,
            json=draft_payload,
        )
        self.assertEqual(draft.status_code, 200, draft.text)
        replay = self.client.put(
            "/api/internal/building/crm/campaigns/ops-1",
            headers=self.headers,
            json=draft_payload,
        )
        self.assertTrue(replay.json()["replayed"])
        preview_response = self.client.post(
            "/api/internal/building/crm/campaigns/ops-1/preview",
            headers=self.headers,
        )
        self.assertEqual(preview_response.status_code, 200, preview_response.text)
        preview = preview_response.json()
        reviewed = self.client.post(
            "/api/internal/building/crm/campaigns/ops-1/review",
            headers=self.headers,
            json={
                "preview_hash": preview["preview_hash"],
                "confirmation": "REVIEW CAMPAIGN ops-1",
                "actor": "reviewer@example.com",
            },
        )
        self.assertEqual(reviewed.status_code, 200, reviewed.text)
        approved = self.client.post(
            "/api/internal/building/crm/campaigns/ops-1/approve",
            headers=self.headers,
            json={
                "preview_hash": preview["preview_hash"],
                "confirmation": "APPROVE CAMPAIGN ops-1",
                "actor": "approver@example.com",
            },
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        self.assertEqual(approved.json()["outbox_status"], "schedule_ready")
        with self.factory() as session:
            recipients = session.execute(
                select(BuildingCampaignRecipient).where(
                    BuildingCampaignRecipient.campaign_id == "ops-1"
                )
            ).scalars().all()
            self.assertEqual(len(recipients), 1)
            self.assertEqual(recipients[0].communication_class, "operational")
            self.assertTrue(recipients[0].content_checksum)
            self.assertTrue(recipients[0].recipient_checksum)
            self.assertEqual(
                recipients[0].permission_snapshot_json["operational_allowed"], True
            )

    def test_private_tenant_content_never_becomes_marketing(self) -> None:
        rejected = self.client.put(
            "/api/internal/building/crm/campaigns/private-marketing",
            headers=self.headers,
            json={
                "id": "private-marketing",
                "name": "Private benefit",
                "segment_id": "current-tenants",
                "communication_class": "marketing",
                "subject": "Private tenant benefit",
                "body_text": "Private details",
                "content_classification": "tenant_private",
                "private_content_approval_evidence": "OPS-7",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(rejected.status_code, 422, rejected.text)
