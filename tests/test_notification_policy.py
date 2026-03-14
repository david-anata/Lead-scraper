from __future__ import annotations

import unittest

from sales_support_agent.services.notification_policy import (
    STALE_URGENCY_FOLLOW_UP_DUE,
    STALE_URGENCY_IMMEDIATE_REVIEW,
    STALE_URGENCY_OVERDUE,
    build_clickup_owner_reference,
    classify_stale_assessment_state,
    determine_stale_notification_mode,
)


class NotificationPolicyTests(unittest.TestCase):
    def test_classify_overdue_state(self) -> None:
        self.assertEqual(classify_stale_assessment_state("overdue"), STALE_URGENCY_OVERDUE)

    def test_classify_new_and_untouched_as_immediate_review(self) -> None:
        self.assertEqual(classify_stale_assessment_state("new_and_untouched"), STALE_URGENCY_IMMEDIATE_REVIEW)

    def test_classify_missing_next_step_as_immediate_review(self) -> None:
        self.assertEqual(classify_stale_assessment_state("missing_next_step"), STALE_URGENCY_IMMEDIATE_REVIEW)

    def test_classify_follow_up_due_state(self) -> None:
        self.assertEqual(classify_stale_assessment_state("follow_up_due"), STALE_URGENCY_FOLLOW_UP_DUE)

    def test_overdue_is_immediate_alert_when_configured(self) -> None:
        self.assertEqual(determine_stale_notification_mode("overdue", ("overdue",)), "immediate_alert")

    def test_follow_up_due_is_digest_only_by_default(self) -> None:
        self.assertEqual(determine_stale_notification_mode("follow_up_due", ("overdue",)), "digest_only")

    def test_clickup_owner_reference_uses_at_prefix(self) -> None:
        self.assertEqual(build_clickup_owner_reference("Gabe"), "@Gabe")

    def test_clickup_owner_reference_falls_back_when_missing(self) -> None:
        self.assertEqual(build_clickup_owner_reference(""), "Assigned AE")


if __name__ == "__main__":
    unittest.main()
