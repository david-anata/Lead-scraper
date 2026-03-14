from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import SimpleNamespace

try:
    from sales_support_agent.services.daily_digest import build_daily_digest_subject, build_daily_digest_text
    SQLALCHEMY_AVAILABLE = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    SQLALCHEMY_AVAILABLE = False


class _Lead:
    def __init__(self, *, task_name: str, status: str, task_url: str) -> None:
        self.task_name = task_name
        self.status = status
        self.task_url = task_url


class _Assessment:
    def __init__(self, *, anchor_date: date) -> None:
        self.anchor_date = anchor_date


class _Evaluation:
    def __init__(self, *, lead: _Lead, assessment: _Assessment) -> None:
        self.lead = lead
        self.assessment = assessment


@dataclass(frozen=True)
class _StaleDigestItem:
    urgency: str
    owner_label: str
    evaluation: _Evaluation
    action_summary: str
    suggested_reply_draft: str


@unittest.skipUnless(SQLALCHEMY_AVAILABLE, "sqlalchemy is required for daily digest tests")
class DailyDigestTests(unittest.TestCase):
    def test_build_daily_digest_subject_uses_prefix_and_date(self) -> None:
        subject = build_daily_digest_subject(prefix="[SDR Support]", as_of_date=date(2026, 3, 14))
        self.assertEqual(subject, "[SDR Support] Daily digest for 2026-03-14")

    def test_build_daily_digest_text_includes_stale_and_mailbox_sections(self) -> None:
        stale_items = [
            _StaleDigestItem(
                urgency="overdue",
                owner_label="@Valeria",
                evaluation=_Evaluation(
                    lead=_Lead(
                        task_name="ACME Corp",
                        status="WORKING QUALIFIED",
                        task_url="https://app.clickup.com/t/acme",
                    ),
                    assessment=_Assessment(anchor_date=date(2026, 3, 14)),
                ),
                action_summary="Reply today and confirm the next meeting time.",
                suggested_reply_draft="Thanks for the note. I can send over times for tomorrow.",
            )
        ]
        mailbox_signals = [
            SimpleNamespace(
                provider="gmail",
                external_message_id="msg-1",
                external_thread_id="thread-1",
                dedupe_key="gmail_message:msg-1",
                matched_task_id="task-1",
                sender_name="Pat Buyer",
                sender_email="pat@example.com",
                sender_domain="example.com",
                subject="Need pricing",
                snippet="Can you send pricing?",
                body_text="Can you send pricing?",
                classification="pricing_or_offer_request",
                urgency="needs_immediate_review",
                owner_id="owner-1",
                owner_name="Valeria Morales",
                task_name="ACME Corp",
                task_url="https://app.clickup.com/t/acme",
                task_status="WORKING QUALIFIED",
                action_summary="Send pricing today and confirm timeline.",
                suggested_reply_draft="Thanks for reaching out. I can send pricing today.",
                received_at=datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc),
                processed_at=datetime(2026, 3, 14, 13, 5, tzinfo=timezone.utc),
                raw_payload={},
            )
        ]

        text = build_daily_digest_text(
            as_of_date=date(2026, 3, 14),
            stale_items=stale_items,
            mailbox_signals=mailbox_signals,
            max_items=10,
        )

        self.assertIn("SDR Support Daily Digest", text)
        self.assertIn("Stale lead follow-up", text)
        self.assertIn("Mailbox findings", text)
        self.assertIn("@Valeria | ACME Corp", text)
        self.assertIn("Valeria Morales | pat@example.com", text)
        self.assertIn("Draft:", text)


if __name__ == "__main__":
    unittest.main()
