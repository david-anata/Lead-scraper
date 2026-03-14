from __future__ import annotations

import base64
import unittest

from sales_support_agent.integrations.gmail_payloads import (
    classify_gmail_message,
    extract_candidate_emails,
    normalize_gmail_message,
)


def _encode(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8").rstrip("=")


def _payload(*, sender: str, subject: str, body: str, message_id: str = "msg-1", thread_id: str = "thread-1") -> dict[str, object]:
    return {
        "id": message_id,
        "threadId": thread_id,
        "internalDate": "1773493200000",
        "snippet": body[:50],
        "payload": {
            "headers": [
                {"name": "From", "value": sender},
                {"name": "Subject", "value": subject},
            ],
            "mimeType": "multipart/alternative",
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _encode(body)},
                }
            ],
        },
    }


class GmailPayloadTests(unittest.TestCase):
    def test_classify_matched_pricing_email_as_offer_request(self) -> None:
        classification = classify_gmail_message(
            sender_domain="example.com",
            subject="Pricing follow-up",
            body_text="Can you send the proposal and pricing?",
            configured_source_domains=("fulfil.com",),
            matched_task=True,
        )
        self.assertEqual(classification, "pricing_or_offer_request")

    def test_classify_unmatched_source_domain_as_lead_source_email(self) -> None:
        classification = classify_gmail_message(
            sender_domain="fulfil.com",
            subject="New lead for your team",
            body_text="A new buyer wants to connect.",
            configured_source_domains=("fulfil.com",),
            matched_task=False,
        )
        self.assertEqual(classification, "lead_source_email")

    def test_extract_candidate_emails_includes_sender_and_body_mentions(self) -> None:
        candidates = extract_candidate_emails(
            "owner@example.com",
            "Intro for buyer@example.org",
            "Please also loop in alt@example.net on the reply.",
        )
        self.assertEqual(
            candidates,
            ("owner@example.com", "buyer@example.org", "alt@example.net"),
        )

    def test_normalize_gmail_message_builds_action_summary_and_reply_draft(self) -> None:
        normalized = normalize_gmail_message(
            _payload(
                sender="Pat Buyer <pat@example.com>",
                subject="Question about pricing",
                body="Can you share pricing and next steps?",
            ),
            configured_source_domains=("fulfil.com",),
            matched_task=True,
        )
        self.assertEqual(normalized.classification, "pricing_or_offer_request")
        self.assertEqual(normalized.urgency, "needs_immediate_review")
        self.assertEqual(normalized.sender_email, "pat@example.com")
        self.assertIn("pricing", normalized.action_summary.lower())
        self.assertTrue(normalized.suggested_reply_draft)


if __name__ == "__main__":
    unittest.main()
