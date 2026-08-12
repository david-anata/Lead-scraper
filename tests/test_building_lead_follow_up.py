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
from sales_support_agent.services.building_lead_follow_up import (
    ESCALATION_WINDOW_DAYS,
    SEQUENCE_LOOKBACK_DAYS,
    process_building_lead_follow_up,
)


class BuildingLeadFollowUpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_lead_follow_up_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_session_factory = app.state.session_factory
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
        # Restore the shared factory too. Leaving it pointed at this class's
        # database made later files run against the wrong one — reproducible on
        # main as `pytest test_building_lead_follow_up.py test_hr_section.py`.
        app.state.session_factory = cls.original_session_factory

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



class BuildingLeadFollowUpCoverageTests(unittest.TestCase):
    """Coverage guarantees for the escalation scan.

    The scan used to be ``order_by(created_at).limit(500)``, which froze the
    window on the oldest 500 inquiries ever created: once that many existed, no
    new lead was ever escalated again. These tests pin the behaviour that
    replaced it.
    """

    def setUp(self) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_lead_coverage_{uuid.uuid4().hex}.db"
        )
        self.factory = create_session_factory("sqlite:///" + path)
        init_database(self.factory)
        self.settings = dataclasses.replace(
            app.state.settings,
            slack_bot_token="xoxb-coverage",
            slack_channel_id="C-COVERAGE",
        )
        self.now = datetime(2026, 5, 20, 15, 0, tzinfo=timezone.utc)

    def _add_overdue(self, session, *, index: int, created_at: datetime) -> None:
        session.add(
            BuildingInquiry(
                id=f"lead-{index}",
                idempotency_key=f"lead-{index}-key",
                kind="event",
                name=f"Prospect {index}",
                email=f"prospect{index}@example.com",
                assigned_owner="building@anatainc.com",
                created_at=created_at,
                response_due_at=created_at + timedelta(hours=4),
                payload_json={"_lifecycle": {"stage": "new"}},
            )
        )

    def _run(self, *, now: datetime, dry_run: bool = False):
        with mock.patch(
            "sales_support_agent.services.building_lead_follow_up.SlackClient.post_message",
            return_value={"ok": True, "ts": "digest.coverage"},
        ) as sender:
            result = process_building_lead_follow_up(
                self.factory, settings=self.settings, dry_run=dry_run, now=now
            )
        return result, sender

    def test_a_recent_lead_is_escalated_behind_a_deep_history(self) -> None:
        with self.factory() as session:
            # More history than the old hard limit, all of it older than the
            # recent lead so the previous query would never have reached it.
            for index in range(520):
                self._add_overdue(
                    session,
                    index=index,
                    created_at=self.now - timedelta(days=200 + index),
                )
            self._add_overdue(
                session, index=9999, created_at=self.now - timedelta(hours=6)
            )
            session.commit()

        result, sender = self._run(now=self.now)

        self.assertIn("lead-9999", result["inquiry_ids"])
        self.assertEqual(result["status"], "delivered")
        sender.assert_called_once()
        # Ancient history is outside the escalation window and stays out of it.
        self.assertNotIn("lead-0", result["inquiry_ids"])

    def test_escalation_repeats_daily_until_the_lead_is_answered(self) -> None:
        with self.factory() as session:
            self._add_overdue(
                session, index=1, created_at=self.now - timedelta(hours=6)
            )
            session.commit()

        first, first_sender = self._run(now=self.now)
        same_day, same_day_sender = self._run(now=self.now + timedelta(hours=3))
        next_day, next_day_sender = self._run(now=self.now + timedelta(days=1))

        self.assertEqual(first["overdue_count"], 1)
        first_sender.assert_called_once()
        # Twice in one day is still one nudge.
        self.assertEqual(same_day["overdue_count"], 0)
        same_day_sender.assert_not_called()
        # The next day it chases again, because nobody answered.
        self.assertEqual(next_day["overdue_count"], 1)
        next_day_sender.assert_called_once()

        with self.factory() as session:
            escalation = session.get(BuildingInquiry, "lead-1").payload_json[
                "_lead_escalation"
            ]
            self.assertEqual(escalation["attempt_count"], 2)
            self.assertEqual(escalation["status"], "delivered")

    def test_digest_names_the_hidden_remainder(self) -> None:
        with self.factory() as session:
            for index in range(25):
                self._add_overdue(
                    session,
                    index=index,
                    created_at=self.now - timedelta(hours=6, minutes=index),
                )
            session.commit()

        result, sender = self._run(now=self.now)

        self.assertEqual(result["overdue_count"], 25)
        rendered = str(sender.call_args.kwargs["blocks"])
        self.assertIn("and 5 more", rendered)

    def test_result_states_its_own_coverage(self) -> None:
        with self.factory() as session:
            self._add_overdue(
                session, index=1, created_at=self.now - timedelta(hours=6)
            )
            session.commit()

        result, _ = self._run(now=self.now, dry_run=True)

        self.assertEqual(result["scanned"], 1)
        self.assertFalse(result["scan_truncated"])
        self.assertEqual(result["sequence_lookback_days"], SEQUENCE_LOOKBACK_DAYS)
        self.assertEqual(result["escalation_window_days"], ESCALATION_WINDOW_DAYS)


if __name__ == "__main__":
    unittest.main()
