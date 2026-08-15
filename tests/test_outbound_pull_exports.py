"""Bulk recent-pull export and delivery behavior."""

from __future__ import annotations

import csv
import io
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine

from sales_support_agent.api import outbound_router
from sales_support_agent.services import outbound_memory
from sales_support_agent.services.outbound_delivery import deliver_completed_pull


def _engine():
    return create_engine("sqlite://", future=True)


def _seed_run(engine, recipe: str, leads: list[dict]) -> int:
    run_id = outbound_memory.record_run(engine, recipe=recipe, scanned=10,
                                        matched=len(leads), fresh=len(leads), skipped_seen=0)
    outbound_memory.record_run_leads(engine, run_id, leads)
    return run_id


def test_bulk_export_deduplicates_by_domain_and_keeps_provenance():
    engine = _engine()
    first = _seed_run(engine, "plan_upgrade", [{"domain": "same.com", "brand": "Same"}])
    second = _seed_run(engine, "social_surge", [
        {"domain": "same.com", "brand": "Same"}, {"domain": "new.com", "brand": "New"},
    ])
    with (
        patch("sales_support_agent.models.database.get_engine", return_value=engine),
        patch("sales_support_agent.api.outbound_router.get_current_user", return_value={"email": "david@anatainc.com"}),
    ):
        response = outbound_router.outbound_pulls_csv(None, f"{first},{second}")
    rows = list(csv.DictReader(io.StringIO(response.body.decode("utf-8"))))
    assert {row["Domain"] for row in rows} == {"same.com", "new.com"}
    same = next(row for row in rows if row["Domain"] == "same.com")
    assert set(same["source_pulls"].split(",")) == {str(first), str(second)}
    assert outbound_memory.load_contacted(engine) == set()
    assert outbound_memory.load_exports(engine)[0]["duplicates_removed"] == 1


def test_preview_reports_unavailable_legacy_pull():
    engine = _engine()
    run_id = outbound_memory.record_run(engine, recipe="legacy", scanned=20,
                                        matched=5, fresh=5, skipped_seen=0)
    with patch("sales_support_agent.models.database.get_engine", return_value=engine):
        response = outbound_router.outbound_pulls_preview(None, str(run_id))
    assert b'"unavailable_pulls":1' in response.body.replace(b" ", b"")


def test_delivery_respects_disabled_setting():
    engine = _engine()
    with patch("sales_support_agent.config.load_settings") as load_settings:
        result = deliver_completed_pull(engine, {"recipe": "x", "fresh": 2})
    assert result["sent"] == 0
    load_settings.assert_not_called()


def test_delivery_can_send_email_and_slack_without_changing_company_memory():
    engine = _engine()
    outbound_memory.save_delivery_settings(engine, {
        "enabled": "1", "email_enabled": "1", "slack_enabled": "1",
        "frequency": "every_pull", "email_recipients": "david@anatainc.com",
        "content_mode": "link",
    })
    fake_slack = MagicMock()
    fake_slack.post_message.return_value = {"ok": True}
    with (
        patch("sales_support_agent.config.load_settings", return_value=SimpleNamespace()),
        patch("sales_support_agent.services.access.notify._send", return_value=True),
        patch("sales_support_agent.integrations.slack.SlackClient", return_value=fake_slack),
    ):
        before = outbound_memory.load_contacted(engine)
        result = deliver_completed_pull(engine, {"recipe": "social_surge", "fresh": 3})
    assert result == {"email": "sent", "slack": "sent", "sent": 2}
    assert outbound_memory.load_contacted(engine) == before
    history = outbound_memory.load_delivery_history(engine)
    assert {item["destination"] for item in history} == {"email", "slack"}
    assert all(item["status"] == "sent" for item in history)
