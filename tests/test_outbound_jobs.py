"""The morning routine that removes the person from the loop.

Lead Ops always displayed a schedule and nothing ever ran it, so every pull and
every Amazon check needed someone to click. These cover the parts that decide
whether a day runs, runs twice, or quietly runs up a bill.
"""

from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

from sales_support_agent.api import outbound_jobs as jobs
from sales_support_agent.services import outbound_memory as m

DENVER = ZoneInfo("America/Denver")


class DailyMarkerTests(unittest.TestCase):
    """The routine takes half an hour. A redeploy partway through must not
    start it again and pay for the same Amazon checks twice."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def test_a_fresh_day_has_not_run(self):
        self.assertFalse(jobs._already_ran_today(self._e(), datetime(2026, 7, 27, 8, tzinfo=DENVER)))

    def test_marking_makes_the_same_day_read_as_done(self):
        e = self._e()
        now = datetime(2026, 7, 27, 8, tzinfo=DENVER)
        jobs._mark_ran(e, now)
        self.assertTrue(jobs._already_ran_today(e, now))

    def test_the_next_day_runs_again(self):
        e = self._e()
        jobs._mark_ran(e, datetime(2026, 7, 27, 8, tzinfo=DENVER))
        self.assertFalse(jobs._already_ran_today(e, datetime(2026, 7, 28, 8, tzinfo=DENVER)))

    def test_the_marker_does_not_bump_the_settings_version(self):
        """A daily marker in the settings store would fill the change log and
        bury the real retunes David needs to compare results against."""
        from sales_support_agent.services import outbound_settings as st
        e = self._e()
        before = st.config_version(e)
        jobs._mark_ran(e, datetime(2026, 7, 27, 8, tzinfo=DENVER))
        self.assertEqual(st.config_version(e), before)

    def test_a_broken_database_does_not_claim_the_day_ran(self):
        """Failing closed here would silently skip outbound for a whole day."""
        self.assertFalse(jobs._already_ran_today(object(), datetime(2026, 7, 27, 8, tzinfo=DENVER)))


class MorningRoutineTests(unittest.TestCase):
    def test_it_stops_without_a_storeleads_key_rather_than_raising(self):
        """This runs on a background thread. An exception there is invisible."""
        import outbound_pipeline as op
        real = op.load_config_from_env
        op.load_config_from_env = lambda: ("", "")
        try:
            out = jobs.run_morning_routine(now=datetime(2026, 7, 27, 8, tzinfo=DENVER))
        finally:
            op.load_config_from_env = real
        self.assertFalse(out["ran"])
        self.assertIn("STORELEADS", out["reason"])

    def test_a_weekend_does_nothing(self):
        """We do not pull on weekends because we do not send on weekends."""
        import outbound_pipeline as op
        import outbound_recipes as rx
        real_cfg, real_day = op.load_config_from_env, rx.recipes_for_day
        op.load_config_from_env = lambda: ("key", "")
        rx.recipes_for_day = lambda weekday, settings=None: []
        try:
            out = jobs.run_morning_routine(now=datetime(2026, 7, 26, 8, tzinfo=DENVER))
        finally:
            op.load_config_from_env, rx.recipes_for_day = real_cfg, real_day
        self.assertFalse(out["ran"])
        self.assertEqual(out["pulled"], 0)

    def test_the_daily_amazon_spend_is_capped(self):
        """Unbounded, this would check every brand we hold every morning and
        bill for all of them."""
        self.assertLessEqual(jobs._SCAN_PER_DAY, 20)
        self.assertGreater(jobs._SCAN_PER_DAY, 0)

    def test_it_runs_before_the_working_day(self):
        self.assertLess(jobs._RUN_HOUR, 9)


if __name__ == "__main__":
    unittest.main()
