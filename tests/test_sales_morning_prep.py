import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from sales_support_agent.services.sales.call_script import morning_prep as mp
from sales_support_agent.services.sales.call_script.bookings import Booking

NOW = datetime(2026, 11, 2, 14, 0, tzinfo=timezone.utc)  # 7 AM Denver


def _booking(eid, hours):
    start = NOW + timedelta(hours=hours)
    return Booking(eid, "Call", "Nick Jones", "nick@reincorporated.org", "Back to talk pricing",
                   start, start + timedelta(minutes=30), "https://meet.example/x", "calendar")


def _context(task_id="t1", amazon=True):
    return {
        "domain": "reincorporated.org",
        "company": "Embera Bead Project",
        "website": {"verified": True, "url": "https://reincorporated.org/"},
        "links": {"amazon": {"verified": amazon, "urls": ["https://www.amazon.com/dp/B0D5FLCNL7"] if amazon else []}},
        "clickup": {"id": task_id, "status": "working qualified",
                    "comments": [{"by": "David", "text": "First call went well"}]},
        "abby_handled": {"rows": []},
        "outbound_contact": {"row": None},
    }


class MorningPrepTests(unittest.TestCase):
    def test_comment_has_links_history_and_flags(self):
        text = mp.build_comment(_booking("e1", 2), _context(amazon=False))
        self.assertIn("https://reincorporated.org/ (verified)", text)
        self.assertIn("Amazon: not verified", text)
        self.assertIn("First call went well", text)
        self.assertIn("Returning", text)
        self.assertIn("Amazon presence: not verified", text)

    def test_only_today_and_off_by_default_never_posts(self):
        client = MagicMock()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(mp.FLAG_ENV, None)
            out = mp.run_morning_prep(now=NOW, bookings=[_booking("e1", 2), _booking("e2", 30)],
                                      context_for=lambda b: _context(), clickup=client, dry_run=False)
        self.assertEqual(out["count"], 1)
        self.assertTrue(out["dry_run"])
        client.create_task_comment.assert_not_called()

    def test_dry_run_with_flag_on_does_not_post(self):
        client = MagicMock()
        with patch.dict(os.environ, {mp.FLAG_ENV: "true"}):
            out = mp.run_morning_prep(now=NOW, bookings=[_booking("e1", 2)],
                                      context_for=lambda b: _context(), clickup=client, dry_run=True)
        client.create_task_comment.assert_not_called()
        self.assertIn("Morning prep", out["calls"][0]["comment"])

    def test_flag_on_posts_to_matched_task(self):
        client = MagicMock()
        with patch.dict(os.environ, {mp.FLAG_ENV: "true"}):
            out = mp.run_morning_prep(now=NOW, bookings=[_booking("e1", 2)],
                                      context_for=lambda b: _context(), clickup=client, dry_run=False)
        client.create_task_comment.assert_called_once()
        self.assertTrue(out["calls"][0]["posted"])


if __name__ == "__main__":
    unittest.main()
