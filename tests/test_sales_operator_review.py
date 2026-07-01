import unittest
from types import SimpleNamespace
from unittest import mock

from sales_support_agent.jobs.sales_operator_review import SalesOperatorReviewJob


class SalesOperatorReviewJobTests(unittest.TestCase):
    def test_run_returns_summary_with_writeback_and_next_action(self) -> None:
        settings = SimpleNamespace()
        session_factory = object()

        session_cm = mock.MagicMock()
        session_cm.__enter__.return_value = mock.Mock()
        session_cm.__exit__.return_value = False

        fake_run = SimpleNamespace(id=1)
        fake_audit = mock.Mock()
        fake_audit.start_run.return_value = fake_run

        with mock.patch(
            "sales_support_agent.jobs.sales_operator_review.session_scope",
            return_value=session_cm,
        ), mock.patch(
            "sales_support_agent.jobs.sales_operator_review.AuditService",
            return_value=fake_audit,
        ), mock.patch(
            "sales_support_agent.jobs.sales_operator_review.sync_hubspot_sales",
            return_value=SimpleNamespace(as_dict=lambda: {"ok": True, "deals": 4}),
        ), mock.patch(
            "sales_support_agent.jobs.sales_operator_review.run_writeback",
            return_value={"summary": {"candidateDeals": 3, "appliedActions": 2, "deferredActions": 1}},
        ), mock.patch(
            "sales_support_agent.jobs.sales_operator_review.get_operator_snapshot",
            return_value={
                "recentDeals": [
                    {
                        "proposedActions": [
                            {"title": "Send updated fulfillment deck"},
                        ]
                    }
                ]
            },
        ):
            result = SalesOperatorReviewJob(settings, session_factory).run(dry_run=False, limit=10)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["candidate_deals"], 3)
        self.assertEqual(result["applied_actions"], 2)
        self.assertEqual(result["next_action"], "Send updated fulfillment deck")
        fake_audit.finish_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
