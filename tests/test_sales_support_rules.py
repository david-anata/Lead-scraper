from __future__ import annotations

import unittest
from datetime import date, datetime

from sales_support_agent.config import DEFAULT_STATUS_POLICIES
from sales_support_agent.rules.business_days import add_business_days
from sales_support_agent.rules.follow_up import assess_status_follow_up


class BusinessDayTests(unittest.TestCase):
    def test_add_business_days_skips_weekend(self) -> None:
        self.assertEqual(add_business_days(date(2026, 3, 13), 1), date(2026, 3, 16))
        self.assertEqual(add_business_days(date(2026, 3, 13), 2), date(2026, 3, 17))


class FollowUpRuleTests(unittest.TestCase):
    def test_contacted_cold_becomes_new_and_untouched_after_one_business_day(self) -> None:
        assessment = assess_status_follow_up(
            status="CONTACTED COLD",
            policy=DEFAULT_STATUS_POLICIES["CONTACTED COLD"],
            created_at=datetime(2026, 3, 13, 9, 0, 0),
            as_of_date=date(2026, 3, 16),
            meaningful_touch_at=None,
            next_follow_up_date=None,
            has_work_signal=False,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "new_and_untouched")

    def test_contacted_cold_becomes_due_after_two_business_days(self) -> None:
        assessment = assess_status_follow_up(
            status="CONTACTED COLD",
            policy=DEFAULT_STATUS_POLICIES["CONTACTED COLD"],
            created_at=datetime(2026, 3, 13, 9, 0, 0),
            as_of_date=date(2026, 3, 17),
            meaningful_touch_at=None,
            next_follow_up_date=None,
            has_work_signal=False,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "follow_up_due")

    def test_contacted_cold_becomes_overdue_after_three_business_days(self) -> None:
        assessment = assess_status_follow_up(
            status="CONTACTED COLD",
            policy=DEFAULT_STATUS_POLICIES["CONTACTED COLD"],
            created_at=datetime(2026, 3, 13, 9, 0, 0),
            as_of_date=date(2026, 3, 18),
            meaningful_touch_at=None,
            next_follow_up_date=None,
            has_work_signal=False,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "overdue")

    def test_follow_up_status_requires_next_step(self) -> None:
        assessment = assess_status_follow_up(
            status="FOLLOW UP",
            policy=DEFAULT_STATUS_POLICIES["FOLLOW UP"],
            created_at=datetime(2026, 3, 13, 9, 0, 0),
            as_of_date=date(2026, 3, 16),
            meaningful_touch_at=datetime(2026, 3, 13, 10, 0, 0),
            next_follow_up_date=None,
            has_work_signal=True,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "missing_next_step")

    def test_working_offered_due_after_four_business_days(self) -> None:
        assessment = assess_status_follow_up(
            status="WORKING OFFERED",
            policy=DEFAULT_STATUS_POLICIES["WORKING OFFERED"],
            created_at=datetime(2026, 3, 9, 9, 0, 0),
            meaningful_touch_at=datetime(2026, 3, 9, 9, 0, 0),
            as_of_date=date(2026, 3, 13),
            next_follow_up_date=None,
            has_work_signal=True,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "follow_up_due")

    def test_new_lead_policy_is_present(self) -> None:
        assessment = assess_status_follow_up(
            status="new lead",
            policy=DEFAULT_STATUS_POLICIES["new lead"],
            created_at=datetime(2026, 3, 13, 9, 0, 0),
            as_of_date=date(2026, 3, 16),
            meaningful_touch_at=None,
            next_follow_up_date=None,
            has_work_signal=False,
        )
        self.assertIsNotNone(assessment)
        self.assertEqual(assessment.state, "new_and_untouched")


if __name__ == "__main__":
    unittest.main()
