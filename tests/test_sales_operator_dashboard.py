import unittest
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
