from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_campaign_scheduler_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-campaign-scheduler-test-secret",
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
        BuildingCampaign,
        BuildingCampaignRecipient,
        BuildingCommunicationPreference,
        BuildingContact,
        BuildingRelationship,
        BuildingSegment,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingCampaignSchedulerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(),
            "building_campaign_scheduler_isolated.db",
        )
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="scheduler-test-key",
            building_campaign_token_secret="scheduler-token-secret",
            resend_api_key="resend-test-key",
            resend_from="Anata Building <hello@example.com>",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "scheduler-test-key"}

    def setUp(self) -> None:
        with self.factory() as session:
            for contact_id, email in (
                ("scheduled-contact-1", "one@example.com"),
                ("scheduled-contact-2", "two@example.com"),
            ):
                session.add(
                    BuildingContact(
                        id=contact_id,
                        email=email,
                        full_name=contact_id,
                        source="scheduler-test",
                    )
                )
                session.add(
                    BuildingRelationship(
                        id=f"relationship-{contact_id}",
                        contact_id=contact_id,
                        relationship_type="tenant",
                        source_reference=f"lease:{contact_id}",
                    )
                )
                session.add(
                    BuildingCommunicationPreference(
                        contact_id=contact_id,
                        marketing_status="subscribed",
                        marketing_source="test",
                        updated_by="test",
                    )
                )
            session.add(
                BuildingSegment(
                    id="scheduled-tenants",
                    name="Scheduled tenants",
                    rules_json={
                        "relationship_types": ["tenant"],
                        "relationship_status": "active",
                        "marketing_statuses": ["subscribed"],
                    },
                    is_active=True,
                    created_by="test",
                )
            )
            session.add(
                BuildingCampaign(
                    id="scheduled-news",
                    name="Scheduled news",
                    segment_id="scheduled-tenants",
                    communication_class="marketing",
                    subject="A scheduled building update",
                    body_text="Here is the latest building news.",
                    status="approved",
                    preview_hash="a" * 64,
                    approved_by="approver@example.com",
                    approved_at=datetime.now(timezone.utc),
                    created_by="operator@example.com",
                )
            )
            session.flush()
            for contact_id, email in (
                ("scheduled-contact-1", "one@example.com"),
                ("scheduled-contact-2", "two@example.com"),
            ):
                session.add(
                    BuildingCampaignRecipient(
                        campaign_id="scheduled-news",
                        contact_id=contact_id,
                        email=email,
                        full_name=contact_id,
                        inclusion_reason="Active tenant with permission.",
                    )
                )
            session.commit()

    def tearDown(self) -> None:
        with self.factory() as session:
            session.query(BuildingAuditEvent).delete()
            session.query(BuildingCampaignRecipient).delete()
            session.query(BuildingCampaign).delete()
            session.query(BuildingSegment).delete()
            session.query(BuildingCommunicationPreference).delete()
            session.query(BuildingRelationship).delete()
            session.query(BuildingContact).delete()
            session.commit()

    def test_schedule_cancel_and_protected_worker(self) -> None:
        future = datetime.now(timezone.utc) + timedelta(days=1)
        scheduled = self.client.post(
            "/api/internal/building/crm/campaigns/scheduled-news/schedule",
            headers=self.headers,
            json={
                "scheduled_at": future.isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(scheduled.status_code, 200, scheduled.text)
        self.assertEqual(scheduled.json()["status"], "scheduled")

        immediate = self.client.post(
            "/api/internal/building/crm/campaigns/scheduled-news/send",
            headers=self.headers,
            json={"actor": "operator@example.com"},
        )
        self.assertEqual(immediate.status_code, 409)

        cancelled = self.client.post(
            "/api/internal/building/crm/campaigns/scheduled-news/unschedule",
            headers=self.headers,
            json={"actor": "operator@example.com"},
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.text)
        self.assertEqual(cancelled.json()["status"], "approved")

        no_timezone = self.client.post(
            "/api/internal/building/crm/campaigns/scheduled-news/schedule",
            headers=self.headers,
            json={
                "scheduled_at": "2099-01-01T09:00:00",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(no_timezone.status_code, 422)

        unauthenticated = self.client.post(
            "/api/internal/building/crm/scheduled-campaigns/run",
            json={"dry_run": True},
        )
        self.assertEqual(unauthenticated.status_code, 401)

    def test_due_worker_rechecks_permission_and_is_idempotent(self) -> None:
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        scheduled = self.client.post(
            "/api/internal/building/crm/campaigns/scheduled-news/schedule",
            headers=self.headers,
            json={
                "scheduled_at": future.isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(scheduled.status_code, 200, scheduled.text)
        with self.factory() as session:
            campaign = session.get(BuildingCampaign, "scheduled-news")
            campaign.scheduled_at = datetime.now(timezone.utc) - timedelta(minutes=1)
            preference = session.get(
                BuildingCommunicationPreference,
                "scheduled-contact-2",
            )
            preference.marketing_status = "unsubscribed"
            session.commit()

        dry_run = self.client.post(
            "/api/internal/building/crm/scheduled-campaigns/run",
            headers=self.headers,
            json={"dry_run": True, "max_campaigns": 10},
        )
        self.assertEqual(dry_run.status_code, 200, dry_run.text)
        self.assertEqual(dry_run.json()["due_count"], 1)
        with self.factory() as session:
            self.assertEqual(
                session.get(BuildingCampaign, "scheduled-news").status,
                "scheduled",
            )

        with mock.patch(
            "sales_support_agent.api.building_crm_router.ResendClient"
        ) as client:
            client.return_value.is_configured.return_value = True
            client.return_value.send_message.return_value = "provider-message-1"
            delivered = self.client.post(
                "/api/internal/building/crm/scheduled-campaigns/run",
                headers=self.headers,
                json={
                    "dry_run": False,
                    "max_campaigns": 10,
                    "actor": "job:test-scheduler",
                },
            )
        self.assertEqual(delivered.status_code, 200, delivered.text)
        self.assertEqual(delivered.json()["sent"], 1)
        self.assertEqual(delivered.json()["suppressed"], 1)
        send_kwargs = client.return_value.send_message.call_args.kwargs
        self.assertEqual(
            send_kwargs["idempotency_key"],
            "building-campaign/scheduled-news/1",
        )

        with self.factory() as session:
            campaign = session.get(BuildingCampaign, "scheduled-news")
            self.assertEqual(campaign.status, "sent")
            self.assertIsNotNone(campaign.scheduled_at)
            self.assertEqual(campaign.scheduled_by, "operator@example.com")
            recipients = {
                row.contact_id: row
                for row in session.query(BuildingCampaignRecipient).all()
            }
            self.assertEqual(recipients["scheduled-contact-1"].status, "sent")
            self.assertEqual(
                recipients["scheduled-contact-2"].status,
                "suppressed",
            )
            actions = {
                row.action
                for row in session.query(BuildingAuditEvent).all()
            }
            self.assertIn("scheduled_send_completed", actions)

        second_run = self.client.post(
            "/api/internal/building/crm/scheduled-campaigns/run",
            headers=self.headers,
            json={"dry_run": False, "max_campaigns": 10},
        )
        self.assertEqual(second_run.status_code, 200, second_run.text)
        self.assertEqual(second_run.json()["due_count"], 0)
