"""Tests for sales/slack_alerts.py."""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import HubSpotDeal
from sales_support_agent.services.sales.slack_alerts import (
    ALERT_COOLDOWN_KEY,
    build_alert_batch,
    send_critical_deal_alerts,
)


def _add_deal(sf, did, email, amount_cents=5000, close_date_iso=None, touch_dt=None, is_closed=False):
    with session_scope(sf) as s:
        d = HubSpotDeal(
            hubspot_deal_id=did,
            deal_name=f"Deal {did}",
            amount_cents=amount_cents,
            deal_stage="qualifiedtobuy",
            deal_stage_label="Qualified",
            pipeline="default",
            owner_email=email,
            is_closed=is_closed,
            is_won=False,
        )
        if close_date_iso:
            d.close_date = datetime.fromisoformat(close_date_iso.replace("Z", "+00:00"))
        if touch_dt:
            d.last_meaningful_touch_at = touch_dt
        s.add(d)


class FakeSlack:
    def __init__(self):
        self.calls: list[dict] = []
        self.is_configured = True

    def post_message(self, *, text, blocks=None):
        self.calls.append({"text": text, "blocks": blocks})
        return {}


class TestBuildAlertBatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            s.flush()

    def test_overdue_deal_included(self):
        _add_deal(self.sf, "ov1", "rep@ex.com", close_date_iso="2020-01-01T00:00:00Z")
        with session_scope(self.sf) as s:
            batch = build_alert_batch(s, as_of=datetime.now(timezone.utc))
        self.assertEqual(len(batch.alerts), 1)
        self.assertTrue(any("overdue" in issue for issue in batch.alerts[0].issues))

    def test_stale_deal_included(self):
        old_touch = datetime.now(timezone.utc) - timedelta(days=30)
        _add_deal(self.sf, "st1", "rep@ex.com", touch_dt=old_touch, close_date_iso="2099-01-01T00:00:00Z")
        with session_scope(self.sf) as s:
            batch = build_alert_batch(s, as_of=datetime.now(timezone.utc), stale_days=14)
        self.assertEqual(len(batch.alerts), 1)
        self.assertTrue(any("no touch" in issue for issue in batch.alerts[0].issues))

    def test_healthy_deal_excluded(self):
        now = datetime.now(timezone.utc)
        _add_deal(
            self.sf, "ok1", "rep@ex.com",
            amount_cents=10000,
            close_date_iso="2099-01-01T00:00:00Z",
            touch_dt=now - timedelta(days=3),
        )
        with session_scope(self.sf) as s:
            batch = build_alert_batch(s, as_of=now, stale_days=14)
        self.assertEqual(batch.alerts, [])

    def test_groups_by_rep(self):
        _add_deal(self.sf, "g1", "alice@ex.com", amount_cents=0)
        _add_deal(self.sf, "g2", "alice@ex.com", amount_cents=0)
        _add_deal(self.sf, "g3", "bob@ex.com", amount_cents=0)
        with session_scope(self.sf) as s:
            batch = build_alert_batch(s)
        self.assertIn("alice@ex.com", batch.by_rep)
        self.assertIn("bob@ex.com", batch.by_rep)
        self.assertEqual(len(batch.by_rep["alice@ex.com"]), 2)

    def test_closed_deal_excluded(self):
        _add_deal(self.sf, "cl1", "rep@ex.com", amount_cents=0, is_closed=True)
        with session_scope(self.sf) as s:
            batch = build_alert_batch(s)
        self.assertEqual(batch.alerts, [])


class TestSendCriticalDealAlerts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sf = create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        init_database(cls.sf)

    def setUp(self):
        with session_scope(self.sf) as s:
            for row in s.query(HubSpotDeal).all():
                s.delete(row)
            s.flush()
        # Clear cooldown before each test.
        from sales_support_agent.models.database import kv_set_json
        kv_set_json(ALERT_COOLDOWN_KEY, None)

    def test_no_critical_deals_skips_send(self):
        now = datetime.now(timezone.utc)
        with session_scope(self.sf) as s:
            result = send_critical_deal_alerts(
                s, SimpleNamespace(slack_bot_token="x", slack_channel_id="C1"),
                as_of=now, force=True,
            )
        self.assertFalse(result.get("sent"))
        self.assertTrue(result.get("skipped"))

    def test_slack_not_configured_skips_send(self):
        _add_deal(self.sf, "nc1", "rep@ex.com", amount_cents=0)
        with session_scope(self.sf) as s:
            result = send_critical_deal_alerts(
                s, SimpleNamespace(slack_bot_token="", slack_channel_id=""),
                force=True,
            )
        self.assertFalse(result.get("sent"))

    def test_cooldown_respected(self):
        from sales_support_agent.models.database import kv_set_json
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        kv_set_json(ALERT_COOLDOWN_KEY, {"sent_at": recent})
        _add_deal(self.sf, "cd1", "rep@ex.com", amount_cents=0)
        with session_scope(self.sf) as s:
            result = send_critical_deal_alerts(
                s, SimpleNamespace(slack_bot_token="x", slack_channel_id="C1"),
            )
        self.assertFalse(result.get("sent"))
        self.assertTrue(result.get("skipped"))

    def test_force_bypasses_cooldown(self):
        from sales_support_agent.models.database import kv_set_json
        from unittest.mock import patch, MagicMock
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        kv_set_json(ALERT_COOLDOWN_KEY, {"sent_at": recent})
        _add_deal(self.sf, "fc1", "rep@ex.com", amount_cents=0)

        mock_client = MagicMock()
        mock_client.is_configured.return_value = True
        mock_client.post_message.return_value = {}

        with patch("sales_support_agent.services.sales.slack_alerts.SlackClient", return_value=mock_client):
            with session_scope(self.sf) as s:
                result = send_critical_deal_alerts(
                    s, SimpleNamespace(slack_bot_token="x", slack_channel_id="C1"),
                    force=True,
                )
        self.assertTrue(result.get("sent"))
        mock_client.post_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
