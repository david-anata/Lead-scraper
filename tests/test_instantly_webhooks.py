from __future__ import annotations

import unittest

from sales_support_agent.integrations.instantly_payloads import (
    SUPPORTED_EVENT_MAP,
    build_external_event_key,
    build_next_follow_up_date,
    build_recommended_next_action,
    build_suggested_reply_draft,
    build_summary,
    extract_email,
)


class InstantlyWebhookHelperTests(unittest.TestCase):
    def test_extract_email_prefers_lead_email(self) -> None:
        payload = {"lead_email": "owner@example.com", "email": "fallback@example.com"}
        self.assertEqual(extract_email(payload), "owner@example.com")

    def test_build_external_event_key_is_stable_for_same_payload(self) -> None:
        payload = {
            "event_type": "reply_received",
            "timestamp": "2026-03-13T10:15:00Z",
            "lead_email": "owner@example.com",
            "email_id": "email-123",
            "campaign_id": "campaign-456",
        }
        self.assertEqual(build_external_event_key(payload), build_external_event_key(payload))

    def test_supported_event_map_covers_core_instantly_events(self) -> None:
        self.assertEqual(SUPPORTED_EVENT_MAP["email_sent"], "outbound_email_sent")
        self.assertEqual(SUPPORTED_EVENT_MAP["reply_received"], "inbound_reply_received")
        self.assertEqual(SUPPORTED_EVENT_MAP["lead_meeting_completed"], "meeting_completed")

    def test_build_summary_for_reply_received_uses_reply_text(self) -> None:
        payload = {"event_type": "reply_received", "reply_text": "We are interested, can you send pricing?"}
        summary = build_summary(payload)
        self.assertIn("Instantly recorded a reply", summary)
        self.assertIn("interested", summary)

    def test_build_next_follow_up_date_parses_meeting_start(self) -> None:
        payload = {"event_type": "lead_meeting_booked", "meeting_start": "2026-03-20T16:00:00Z"}
        self.assertEqual(str(build_next_follow_up_date(payload)), "2026-03-20")

    def test_recommended_next_action_for_not_interested_is_review_based(self) -> None:
        payload = {"event_type": "lead_not_interested"}
        self.assertIn("closed status", build_recommended_next_action(payload))

    def test_build_suggested_reply_draft_for_reply_received_is_non_empty(self) -> None:
        payload = {"event_type": "reply_received"}
        self.assertIn("Thanks for the reply", build_suggested_reply_draft(payload))


if __name__ == "__main__":
    unittest.main()
