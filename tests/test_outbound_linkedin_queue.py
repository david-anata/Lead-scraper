"""Joining the email and LinkedIn channels so they don't arrive as strangers.

The rule these tests exist to protect: once someone has answered on email, no
LinkedIn request follows. A connection request cannot be withdrawn from their
notifications, so a queue that re-arms after a reply is worse than no queue.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine

from sales_support_agent.services import outbound_heyreach as hr
from sales_support_agent.services import outbound_linkedin_queue as q
from sales_support_agent.services import outbound_memory as om

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _engine():
    eng = create_engine("sqlite://")
    om.ensure_contacts_table(eng)
    return eng


class DecideTests(unittest.TestCase):
    def test_a_send_starts_the_clock(self):
        d = q.decide("email_sent", now=NOW)
        self.assertEqual(d["state"], "waiting")
        self.assertTrue(d["eligible_at"].startswith("2026-08-06"))

    def test_every_human_reply_stops_it(self):
        for event in ("reply_received", "lead_interested", "lead_meeting_booked",
                      "lead_meeting_completed", "lead_neutral", "lead_not_interested"):
            with self.subTest(event=event):
                self.assertEqual(q.decide(event, now=NOW)["state"], "stopped")

    def test_only_a_no_closes_the_email_channel_too(self):
        self.assertTrue(q.decide("lead_not_interested", now=NOW)["block_email"])
        self.assertFalse(q.decide("reply_received", now=NOW)["block_email"])

    def test_unknown_events_change_nothing(self):
        self.assertIsNone(q.decide("email_bounced", now=NOW))
        self.assertIsNone(q.decide("", now=NOW))

    def test_wait_is_bounded(self):
        """Zero would fire LinkedIn the same hour as the email."""
        self.assertEqual(q.wait_days({"outbound.linkedin_wait_days": 0}), 1)
        self.assertEqual(q.wait_days({"outbound.linkedin_wait_days": 9999}), 30)
        self.assertEqual(q.wait_days({"outbound.linkedin_wait_days": "junk"}), q.DEFAULT_WAIT_DAYS)
        self.assertEqual(q.wait_days({"outbound.linkedin_wait_days": 3}), 3)


class DueTests(unittest.TestCase):
    def _row(self, **kw):
        base = {"state": "waiting", "linkedin_url": "https://linkedin.com/in/a",
                "eligible_at": (NOW - timedelta(days=1)).isoformat()}
        base.update(kw)
        return base

    def test_due_when_the_gap_has_passed(self):
        self.assertTrue(q.is_due(self._row(), now=NOW))

    def test_not_due_early(self):
        self.assertFalse(q.is_due(
            self._row(eligible_at=(NOW + timedelta(days=2)).isoformat()), now=NOW))

    def test_no_profile_is_never_due(self):
        self.assertFalse(q.is_due(self._row(linkedin_url=""), now=NOW))

    def test_stopped_is_never_due(self):
        self.assertFalse(q.is_due(self._row(state="stopped"), now=NOW))

    def test_a_naive_timestamp_does_not_crash(self):
        self.assertTrue(q.is_due(self._row(eligible_at="2026-07-01T00:00:00"), now=NOW))

    def test_counts_reconcile(self):
        rows = [self._row(), self._row(state="stopped"), self._row(linkedin_url=""),
                self._row(eligible_at=(NOW + timedelta(days=3)).isoformat()),
                self._row(state="sent")]
        s = q.summarise(rows, now=NOW)
        self.assertEqual(s["total"],
                         s["due"] + s["waiting"] + s["stopped"] + s["sent"] + s["no_profile"])
        self.assertEqual(s["due"], 1)


class EndToEndTests(unittest.TestCase):
    ROW = {"Work Email": "jane@rho.co", "LinkedIn": "https://www.linkedin.com/in/jane-doe/",
           "First Name": "Jane", "Surname": "Doe", "Merchant Name": "Rho"}

    def test_upload_then_send_then_reply(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        self.assertEqual(om.load_contacts(eng)[0]["state"], "new")

        om.apply_queue_decision(eng, "jane@rho.co", q.decide("email_sent", now=NOW))
        self.assertEqual(om.load_contacts(eng)[0]["state"], "waiting")

        om.apply_queue_decision(eng, "jane@rho.co", q.decide("reply_received", now=NOW))
        row = om.load_contacts(eng)[0]
        self.assertEqual(row["state"], "stopped")
        self.assertFalse(q.is_due(row, now=NOW))

    def test_a_later_send_cannot_revive_someone_who_replied(self):
        """The whole point. A second campaign must not re-queue a person who
        already answered the first one."""
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        om.apply_queue_decision(eng, "jane@rho.co", q.decide("reply_received", now=NOW))
        self.assertFalse(
            om.apply_queue_decision(eng, "jane@rho.co", q.decide("email_sent", now=NOW)))
        self.assertEqual(om.load_contacts(eng)[0]["state"], "stopped")

    def test_re_uploading_the_file_does_not_reset_state(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        om.apply_queue_decision(eng, "jane@rho.co", q.decide("reply_received", now=NOW))
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        self.assertEqual(om.load_contacts(eng)[0]["state"], "stopped")

    def test_a_no_blocks_the_email_channel(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        om.apply_queue_decision(eng, "jane@rho.co", q.decide("lead_not_interested", now=NOW))
        self.assertTrue(om.load_contacts(eng)[0]["email_blocked"])

    def test_a_webhook_for_someone_unknown_is_still_tracked(self):
        eng = _engine()
        om.apply_queue_decision(eng, "stranger@x.com", q.decide("email_sent", now=NOW))
        rows = om.load_contacts(eng)
        self.assertEqual(len(rows), 1)
        self.assertFalse(q.is_due(rows[0], now=NOW))  # no profile, so never queued

    def test_email_only_contacts_are_stored_but_never_queued(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([{"Work Email": "no@rho.co"}]))
        om.apply_queue_decision(eng, "no@rho.co", q.decide("email_sent", now=NOW))
        row = om.load_contacts(eng)[0]
        self.assertEqual(row["linkedin_url"], "")
        self.assertFalse(q.is_due(row, now=NOW))

    def test_a_company_page_does_not_count_as_a_profile(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from(
            [{"Work Email": "x@rho.co", "LinkedIn": "https://linkedin.com/company/rho"}]))
        self.assertEqual(om.load_contacts(eng)[0]["linkedin_url"], "")

    def test_pushing_to_heyreach_takes_them_out_of_the_queue(self):
        eng = _engine()
        om.record_contacts(eng, hr.contacts_from([self.ROW]))
        om.apply_queue_decision(eng, "jane@rho.co", q.decide("email_sent", now=NOW))
        om.mark_linkedin_sent(eng, ["jane@rho.co"])
        row = om.load_contacts(eng)[0]
        self.assertEqual(row["state"], "sent")
        self.assertFalse(q.is_due(row, now=NOW + timedelta(days=99)))


class DescribeTests(unittest.TestCase):
    def test_reads_as_plain_english(self):
        self.assertEqual(
            q.describe({"state": "waiting", "linkedin_url": "https://linkedin.com/in/a",
                        "eligible_at": (NOW + timedelta(days=3)).isoformat()}, now=NOW),
            "Ready in 3 days.")
        self.assertEqual(
            q.describe({"state": "stopped", "reason": "they replied"}, now=NOW),
            "Stopped: they replied.")
        self.assertEqual(
            q.describe({"state": "waiting", "linkedin_url": ""}, now=NOW),
            "No LinkedIn profile, email only.")


if __name__ == "__main__":
    unittest.main()
