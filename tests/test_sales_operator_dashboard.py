import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from sales_support_agent.services.sales import operator_dashboard


class SalesOperatorDashboardTests(unittest.TestCase):
    def test_infer_offer_prefers_amazon_when_agency_progress_exists(self):
        deal = {
            "id": "deal-1",
            "properties": {
                "dealname": "Amazon Ads Pilot",
                "service_type": "",
                "agency": "active",
                "fulfillment": "",
                "shipping_os": "",
            },
        }
        company = {"id": "1", "properties": {"name": "Acme", "service_type": "Amazon"}}

        inference = operator_dashboard.infer_offer(deal, company)

        self.assertEqual(inference["primary_offer"], "amazon_marketing_service")
        self.assertEqual(inference["deal_service_type_value"], "Amazon")
        self.assertGreaterEqual(inference["confidence"], 0.85)

    def test_build_suggested_next_step_matches_negotiation_stage(self):
        stage = {"label": "Negotiation"}
        inference = {"primary_offer": "fulfillment", "primary_offer_label": "Fulfillment"}

        suggestion = operator_dashboard.build_suggested_next_step(stage, inference)

        self.assertIn("negotiation points", suggestion["text"])
        self.assertGreaterEqual(suggestion["confidence"], 0.95)

    def test_run_writeback_preview_returns_candidate_actions(self):
        settings = SimpleNamespace(hubspot_sales_pipeline_id="pipeline-1", hubspot_portal_id="48602527")
        pipeline = {
            "id": "pipeline-1",
            "label": "Sales Pipeline",
            "stages": [{"id": "qualified", "label": "Qualified", "metadata": {"probability": "0.2"}}],
        }
        deal = {
            "id": "deal-1",
            "properties": {
                "dealname": "Amazon Ads Pilot",
                "dealstage": "qualified",
                "hubspot_owner_id": "owner-1",
                "service_type": "",
                "agency": "active",
                "fulfillment": "",
                "shipping_os": "",
                "hs_next_step": "",
            },
        }
        company = {"id": "3001", "properties": {"name": "Acme", "service_type": "Amazon"}}

        fake_client = mock.Mock()
        fake_client.settings = settings
        fake_client.batch_read.side_effect = lambda object_type, ids, properties=(): [company] if object_type == "companies" else [deal]
        fake_client.list_associations.side_effect = lambda from_type, from_id, to_type: ["3001"] if to_type == "companies" else []

        with mock.patch.object(operator_dashboard, "HubSpotClient", return_value=fake_client), mock.patch.object(
            operator_dashboard, "_get_primary_pipeline", return_value=pipeline
        ), mock.patch.object(operator_dashboard, "_list_deals", return_value=[deal]):
            result = operator_dashboard.run_writeback(settings, mode="preview", limit=10)

        self.assertEqual(result["mode"], "preview")
        self.assertEqual(result["summary"]["candidateDeals"], 1)
        action_types = [action["type"] for action in result["deals"][0]["actions"]]
        self.assertIn("update_deal_service_type", action_types)
        self.assertIn("update_next_step", action_types)

    def test_build_deal_intelligence_prioritizes_reply_due(self):
        now = datetime(2026, 6, 29, 18, 0, tzinfo=timezone.utc)
        intelligence = operator_dashboard._build_deal_intelligence(
            deal={"id": "deal-1", "properties": {"dealname": "Acme Amazon", "hs_next_step": "Send follow-up"}},
            stage={"label": "Proposal Sent"},
            stage_status="open",
            inference={"primary_offer": "amazon_marketing_service", "primary_offer_label": "Amazon Marketing Service"},
            current_next_step="Send follow-up",
            deal_row=SimpleNamespace(
                last_inbound_at=now - timedelta(hours=2),
                last_outbound_at=now - timedelta(days=2),
                last_meaningful_touch_at=now - timedelta(hours=2),
            ),
            contacts=[SimpleNamespace(first_name="Taylor", last_name="Smith", email="taylor@example.com")],
            assets=[],
            events=[],
            signals=[],
            live_mailbox={"configured": True, "matched": True, "messages": []},
            as_of=now,
        )

        self.assertEqual(intelligence["status"], "reply_due")
        self.assertTrue(intelligence["shouldUpdateNextStep"])
        self.assertIn("Reply to Taylor Smith today", intelligence["recommendedNextStep"])

    def test_build_deal_intelligence_detects_new_asset_ready_to_share(self):
        now = datetime(2026, 6, 29, 18, 0, tzinfo=timezone.utc)
        intelligence = operator_dashboard._build_deal_intelligence(
            deal={"id": "deal-1", "properties": {"dealname": "Acme Fulfillment", "hs_next_step": ""}},
            stage={"label": "Qualified"},
            stage_status="open",
            inference={"primary_offer": "fulfillment", "primary_offer_label": "Fulfillment"},
            current_next_step="",
            deal_row=SimpleNamespace(
                last_inbound_at=None,
                last_outbound_at=now - timedelta(days=5),
                last_meaningful_touch_at=now - timedelta(days=5),
            ),
            contacts=[],
            assets=[
                SimpleNamespace(
                    asset_type="rate_sheet",
                    label="Fulfillment Rate Sheet",
                    url="https://example.com/rate-sheet",
                    linked_at=now - timedelta(days=1),
                )
            ],
            events=[],
            signals=[],
            live_mailbox={"configured": False, "matched": False, "messages": [], "error": ""},
            as_of=now,
        )

        self.assertEqual(intelligence["status"], "asset_ready_to_share")
        self.assertEqual(intelligence["assetState"]["status"], "ready_to_share")
        self.assertEqual(intelligence["assetState"]["reviewState"], "ready_to_share")
        self.assertTrue(intelligence["shouldUpdateNextStep"])
        self.assertIn("Fulfillment Rate Sheet", intelligence["recommendedNextStep"])

    def test_build_deal_intelligence_flags_inbox_ahead_of_mirror(self):
        now = datetime(2026, 6, 29, 18, 0, tzinfo=timezone.utc)
        intelligence = operator_dashboard._build_deal_intelligence(
            deal={"id": "deal-1", "properties": {"dealname": "Acme Amazon", "hs_next_step": "Wait for reply"}},
            stage={"label": "Proposal Sent"},
            stage_status="open",
            inference={"primary_offer": "amazon_marketing_service", "primary_offer_label": "Amazon Marketing Service"},
            current_next_step="Wait for reply",
            deal_row=SimpleNamespace(
                last_inbound_at=now - timedelta(days=4),
                last_outbound_at=now - timedelta(days=3),
                last_meaningful_touch_at=now - timedelta(days=3),
            ),
            contacts=[SimpleNamespace(first_name="Taylor", last_name="Smith", email="taylor@example.com")],
            assets=[],
            events=[],
            signals=[],
            live_mailbox={
                "configured": True,
                "matched": True,
                "messages": [
                    {
                        "direction": "inbound",
                        "occurredAt": (now - timedelta(hours=1)).isoformat(),
                        "subject": "Quick question",
                    }
                ],
                "error": "",
            },
            as_of=now,
        )

        self.assertEqual(intelligence["liveMailboxState"], "ahead_of_mirror")
        self.assertTrue(intelligence["needsInboxSyncReview"])
        self.assertIn("Live Gmail shows a newer message", " ".join(intelligence["reasons"]))

    def test_build_deal_intelligence_flags_asset_refresh_after_reply(self):
        now = datetime(2026, 6, 29, 18, 0, tzinfo=timezone.utc)
        intelligence = operator_dashboard._build_deal_intelligence(
            deal={"id": "deal-1", "properties": {"dealname": "Acme Fulfillment", "hs_next_step": "Review open questions"}},
            stage={"label": "Proposal Sent"},
            stage_status="open",
            inference={"primary_offer": "fulfillment", "primary_offer_label": "Fulfillment"},
            current_next_step="Review open questions",
            deal_row=SimpleNamespace(
                last_inbound_at=now - timedelta(hours=5),
                last_outbound_at=now - timedelta(days=2),
                last_meaningful_touch_at=now - timedelta(hours=5),
            ),
            contacts=[SimpleNamespace(first_name="Jamie", last_name="Lee", email="jamie@example.com")],
            assets=[
                SimpleNamespace(
                    asset_type="deck",
                    label="Fulfillment Sales Deck",
                    url="https://example.com/deck",
                    linked_at=now - timedelta(days=4),
                )
            ],
            events=[],
            signals=[],
            live_mailbox={"configured": True, "matched": False, "messages": [], "error": ""},
            as_of=now,
        )

        self.assertEqual(intelligence["assetState"]["reviewState"], "stale_after_reply")
        self.assertTrue(intelligence["needsAssetRefreshReview"])
        self.assertIn("may need a refresh", " ".join(intelligence["reasons"]))

    def test_build_proposed_actions_marks_send_updated_asset_ready(self):
        actions = operator_dashboard._build_proposed_actions(
            deal_name="Acme Fulfillment",
            stage_status="open",
            primary_offer="Fulfillment",
            company_present=True,
            contact_present=True,
            contact_count=1,
            missing_fields=[],
            intelligence={
                "status": "asset_ready_to_share",
                "recommendedNextStep": "Send updated Fulfillment Rate Sheet",
                "assetState": {
                    "status": "ready_to_share",
                    "latestAssetLabel": "Fulfillment Rate Sheet",
                    "count": 1,
                },
            },
        )

        self.assertEqual(actions[0]["state"], "ready")
        self.assertIn("Send updated Fulfillment Rate Sheet", actions[0]["title"])

    def test_build_proposed_actions_blocks_deck_creation_when_context_missing(self):
        actions = operator_dashboard._build_proposed_actions(
            deal_name="Acme Amazon",
            stage_status="open",
            primary_offer="Unclassified",
            company_present=False,
            contact_present=False,
            contact_count=0,
            missing_fields=["company link", "contact link", "service classification"],
            intelligence={
                "status": "monitor",
                "recommendedNextStep": "",
                "assetState": {"status": "none", "latestAssetLabel": None, "count": 0},
            },
        )

        self.assertEqual(actions[0]["state"], "blocked")
        self.assertIn("Collect the missing info", actions[0]["title"])
        self.assertIn("company link", actions[0]["blockedBy"])

    def test_decorate_automation_schedules_includes_last_run_summary(self):
        rows = operator_dashboard._decorate_automation_schedules(
            {
                "sales_operator_review": {
                    "status": "success",
                    "startedAt": "2026-06-30 10:05 UTC",
                    "completedAt": "2026-06-30 10:06 UTC",
                    "summary": {
                        "candidate_deals": 3,
                        "applied_actions": 2,
                        "deferred_actions": 1,
                        "next_action": "Reply to Acme today",
                    },
                }
            }
        )

        operator_row = next(item for item in rows if item["runType"] == "sales_operator_review")
        self.assertEqual(operator_row["lastRun"]["status"], "success")
        self.assertEqual(operator_row["lastRun"]["summary"]["next_action"], "Reply to Acme today")
