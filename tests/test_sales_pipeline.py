"""Tests for get_next_stage — ordered pipeline stage lookup."""

from __future__ import annotations

import unittest

from sales_support_agent.services.sales.pipeline import get_next_stage

SAMPLE_PIPELINE = {
    "pipeline_a": [
        {"id": "stage_1", "label": "Appointment Scheduled"},
        {"id": "stage_2", "label": "Qualified to Buy"},
        {"id": "stage_3", "label": "Presentation Scheduled"},
        {"id": "stage_4", "label": "Contract Sent"},
    ],
    "pipeline_b": [
        {"id": "s1", "label": "Only Stage"},
    ],
}


class TestGetNextStage(unittest.TestCase):
    def test_returns_next_stage_from_middle(self):
        result = get_next_stage("pipeline_a", "stage_2", pipeline_data=SAMPLE_PIPELINE)
        self.assertEqual(result, ("stage_3", "Presentation Scheduled"))

    def test_returns_next_stage_from_first(self):
        result = get_next_stage("pipeline_a", "stage_1", pipeline_data=SAMPLE_PIPELINE)
        self.assertEqual(result, ("stage_2", "Qualified to Buy"))

    def test_last_stage_returns_none(self):
        result = get_next_stage("pipeline_a", "stage_4", pipeline_data=SAMPLE_PIPELINE)
        self.assertIsNone(result)

    def test_single_stage_pipeline_returns_none(self):
        result = get_next_stage("pipeline_b", "s1", pipeline_data=SAMPLE_PIPELINE)
        self.assertIsNone(result)

    def test_unknown_stage_returns_none(self):
        result = get_next_stage("pipeline_a", "not_a_stage", pipeline_data=SAMPLE_PIPELINE)
        self.assertIsNone(result)

    def test_unknown_pipeline_returns_none(self):
        result = get_next_stage("no_such_pipeline", "stage_1", pipeline_data=SAMPLE_PIPELINE)
        self.assertIsNone(result)

    def test_empty_pipeline_data_returns_none(self):
        result = get_next_stage("pipeline_a", "stage_1", pipeline_data={})
        self.assertIsNone(result)

    def test_empty_pipeline_id_returns_none(self):
        result = get_next_stage("", "stage_1", pipeline_data=SAMPLE_PIPELINE)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
