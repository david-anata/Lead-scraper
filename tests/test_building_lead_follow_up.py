from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_lead_follow_up_boot.db",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import BuildingAuditEvent, BuildingInquiry
from sales_support_agent.services.building_lead_intake import build_follow_up_sequence


class BuildingLeadFollowUpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_lead_follow_up_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        cls.original_settings = app.state.settings
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="building-follow-up-key",
            slack_bot_token="xoxb-building-follow-up",
            slack_channel_id="C-BUILDING",
        )
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "building-follow-up-key"}
        now = datetime.now(timezone.utc)
        with cls.factory() as session:
            session.add_all(
                [
                    BuildingInquiry(
                        id="overdue-new-lead",
                        idempotency_key="overdue-new-lead-key",
                        kind="event",
                        name="Overdue Prospect",
                        email="overdue@example.com",
                        assigned_owner="building@anatainc.com",
                        response_due_at=now - timedelta(hours=2),
                        payload_json={
                            "_lifecycle": {"stage": "new"},
                            "_follow_up_sequence": build_follow_up_sequence(
                                now - timedelta(days=2), 4
                            ),
                        },
                    ),
                    BuildingInquiry(
                        id="responded-lead",
                        idempotency_key="responded-lead-key",
                        kind="event",
                        name="Responded Prospect",
                        email="responded@example.com",
                        response_due_at=now - timedelta(hours=3),
                        payload_json={"_lifecycle": {"stage": "responded"}},
                    ),
                    BuildingInquiry(
                        id="future-lead",
                        idempotency_key="future-lead-key",
                        kind="tour",
                        name="Future Prospect",
                        email="future@example.com",
                        response_due_at=now + timedelta(hours=3),
                        payload_json={"_lifecycle": {"stage": "new"}},
                    ),
                    BuildingInquiry(
                        id="qa-overdue-lead",
                        idempotency_key="qa-overdue-lead-key",
                        kind="event",
                        source="production_qa",
                        name="Production QA",
                        email="building+follow-up-qa@anatainc.com",
                        response_due_at=now - timedelta(hours=5),
                        payload_json={"_lifecycle": {"stage": "new"}},
                    ),
                ]
            )
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.settings = cls.original_settings

    def test_job_is_authenticated_previewable_and_idempotent(self) -> None:
        denied = self.client.post("/api/jobs/building-leads/run", json={})
        self.assertEqual(denied.status_code, 401)

        preview = self.client.post(
            "/api/jobs/building-leads/run",
            headers=self.headers,
            json={"dry_run": True},
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["details"]["inquiry_ids"], ["overdue-new-lead"])
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "overdue-new-lead")
            self.assertNotIn("_lead_escalation", inquiry.payload_json)
            self.assertTrue(
                any(
                    step["status"] in {"queued", "due"}
                    for step in inquiry.payload_json["_follow_up_sequence"]
                )
            )

        with mock.patch(
            "sales_support_agent.services.building_lead_follow_up.SlackClient.post_message",
            return_value={"ok": True, "ts": "digest.123"},
        ) as sender:
            first = self.client.post(
                "/api/jobs/building-leads/run",
                headers=self.headers,
                json={"dry_run": False},
            )
            second = self.client.post(
                "/api/jobs/building-leads/run",
                headers=self.headers,
                json={"dry_run": False},
            )
        self.assertEqual(first.json()["details"]["status"], "delivered")
        self.assertEqual(second.json()["details"]["overdue_count"], 0)
        sender.assert_called_once()
        blocks = sender.call_args.kwargs["blocks"]
        rendered = str(blocks)
        self.assertIn("/admin/building/inquiries/overdue-new-lead", rendered)
        self.assertNotIn("responded-lead", rendered)
        self.assertNotIn("future-lead", rendered)
        self.assertNotIn("qa-overdue-lead", rendered)

        with self.factory() as session:
            overdue = session.get(BuildingInquiry, "overdue-new-lead")
            responded = session.get(BuildingInquiry, "responded-lead")
            self.assertEqual(
                overdue.payload_json["_lead_escalation"]["status"], "delivered"
            )
            self.assertNotIn("_lead_escalation", responded.payload_json)
            self.assertGreaterEqual(
                sum(
                    step["status"] == "overdue"
                    for step in overdue.payload_json["_follow_up_sequence"]
                ),
                3,
            )
            audit = session.query(BuildingAuditEvent).filter_by(
                entity_type="inquiry",
                entity_id="overdue-new-lead",
                action="lead_overdue_escalation_delivered",
            ).one()
            self.assertFalse(audit.after_json["customer_contacted"])


if __name__ == "__main__":
    unittest.main()
