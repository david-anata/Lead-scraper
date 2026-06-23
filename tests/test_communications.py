from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sales_support_agent import config
from sales_support_agent.config import Settings

try:
    from sales_support_agent.models import database as database_module
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import CommunicationEvent
    from sales_support_agent.models.schemas import CommunicationEventRequest
    from sales_support_agent.services.communications import CommunicationService

    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _FakeClickUpClient:
    def __init__(self) -> None:
        self.created_comments: list[tuple[str, str]] = []

    def get_task(self, task_id: str) -> dict[str, object]:
        return {
            "id": task_id,
            "name": "Example Lead",
            "url": f"https://app.clickup.com/t/{task_id}",
            "status": {"status": "CONTACTED WARM"},
            "assignees": [{"id": "owner-1", "username": "Valeria Morales"}],
            "custom_fields": [],
        }

    def create_task_comment(self, task_id: str, comment_text: str) -> dict[str, object]:
        self.created_comments.append((task_id, comment_text))
        return {"id": f"comment-{len(self.created_comments)}"}

    def get_accessible_custom_fields(self, list_id: str) -> list[dict[str, object]]:
        return []

    def set_custom_field_value(self, task_id: str, field_id: str, value: object) -> dict[str, object]:
        return {"task_id": task_id, "field_id": field_id, "value": value}

    def update_task(self, task_id: str, payload: dict[str, object]) -> dict[str, object]:
        return {"task_id": task_id, "payload": payload}


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def post_message(self, **payload: object) -> dict[str, object]:
        self.messages.append(dict(payload))
        return {"ok": True}


def _build_settings(database_path: Path) -> Settings:
    """Build a complete Settings via load_settings() with a patched environment.

    The Settings dataclass has many required fields; rather than enumerate every
    one here (and drift as it grows), patch the env vars this test relies on and
    let load_settings() fill the rest with its defaults.
    """
    env = {
        "CLICKUP_API_TOKEN": "token",
        "CLICKUP_LIST_ID": "list-123",
        "SLACK_BOT_TOKEN": "slack-token",
        "SLACK_CHANNEL_ID": "channel-123",
        "SALES_AGENT_DB_URL": f"sqlite:///{database_path}",
        "SALES_AGENT_INTERNAL_API_KEY": "internal-key",
    }
    with patch.dict("os.environ", env, clear=False):
        return config.load_settings()


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for communication tests")
class CommunicationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.tempdir.name) / "sales-support-agent.sqlite3"
        self.settings = _build_settings(self.database_path)
        # create_session_factory reassigns the module-level `database.engine`
        # global. Remember the prior engine so tearDown can restore it — otherwise
        # we leave it pointing at our temp DB, which we then delete, breaking any
        # later test that writes through get_engine() ("readonly database").
        self._prev_engine = database_module.engine
        self.session_factory = create_session_factory(self.settings.sales_agent_db_url)
        init_database(self.session_factory)

    def tearDown(self) -> None:
        if database_module.engine is not None:
            database_module.engine.dispose()
        database_module.engine = self._prev_engine
        self.tempdir.cleanup()

    def test_duplicate_payload_signature_skips_comment_and_slack(self) -> None:
        session = self.session_factory()
        clickup_client = _FakeClickUpClient()
        slack_client = _FakeSlackClient()
        payload_one = CommunicationEventRequest(
            task_id="task-123",
            event_type="inbound_reply_received",
            external_event_key="evt-1",
            occurred_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
            summary="Prospect replied and asked for pricing.",
            recommended_next_action="Send pricing and confirm timeline.",
            suggested_reply_draft="Thanks for the reply. I can send pricing today.",
            source="gmail",
            metadata={"classification": "pricing_or_offer_request"},
        )
        payload_two = payload_one.model_copy(update={"external_event_key": "evt-2"})

        try:
            with patch("sales_support_agent.services.communications.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_task.return_value = SimpleNamespace(status="CONTACTED WARM")

                service = CommunicationService(self.settings, clickup_client, slack_client, session)
                first = service.process_event(payload_one)
                second = service.process_event(payload_two)

            self.assertTrue(first["comment_posted"])
            self.assertFalse(second["comment_posted"])
            self.assertEqual(second["slack_notification"]["reason"], "duplicate_event_notification")
            self.assertEqual(len(clickup_client.created_comments), 1)
            self.assertEqual(len(slack_client.messages), 1)
            self.assertEqual(session.query(CommunicationEvent).count(), 2)
        finally:
            session.close()

    def test_communication_outputs_split_slack_and_clickup_voice(self) -> None:
        session = self.session_factory()
        clickup_client = _FakeClickUpClient()
        slack_client = _FakeSlackClient()
        payload = CommunicationEventRequest(
            task_id="task-123",
            event_type="inbound_reply_received",
            external_event_key="evt-voice",
            occurred_at=datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
            summary="Prospect replied and asked for pricing.",
            recommended_next_action="Send pricing and confirm timeline.",
            suggested_reply_draft="Thanks for the reply. I can send pricing today.",
            source="gmail",
            metadata={"classification": "pricing_or_offer_request"},
        )

        try:
            with patch("sales_support_agent.services.communications.ClickUpSyncService") as sync_service_cls:
                sync_service_cls.return_value.sync_task.return_value = SimpleNamespace(status="CONTACTED WARM")

                service = CommunicationService(self.settings, clickup_client, slack_client, session)
                service.process_event(payload)

            self.assertEqual(len(clickup_client.created_comments), 1)
            self.assertEqual(len(slack_client.messages), 1)

            comment_text = clickup_client.created_comments[0][1]
            slack_text = str(slack_client.messages[0]["text"])

            self.assertIn("[Sales Support Agent] Activity logged.", comment_text)
            self.assertIn("Recommended next step:", comment_text)
            self.assertNotIn("Suggested reply:", comment_text)

            self.assertIn("you have a new reply", slack_text.lower())
            self.assertIn("Best next move:", slack_text)
            self.assertIn("Draft idea:", slack_text)
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
