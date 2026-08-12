"""The morning routine that removes the person from the loop.

Lead Ops always displayed a schedule and nothing ever ran it, so every pull and
every Amazon check needed someone to click. These cover the parts that decide
whether a day runs, runs twice, or quietly runs up a bill.
"""

from __future__ import annotations

import unittest
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient
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


class TimezoneTests(unittest.TestCase):
    """The database stamps UTC; the schedule thinks in Denver. After ~6pm local
    those are different dates, and matching on the row timestamp made the job
    forget it had run and restart every ten minutes, paying each time."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def test_an_evening_run_is_still_remembered(self):
        e = self._e()
        evening = datetime(2026, 7, 27, 19, 40, tzinfo=DENVER)  # 01:40 UTC next day
        jobs._mark_ran(e, evening)
        self.assertTrue(jobs._already_ran_today(e, evening),
                        "the job forgot it had run and would restart on a loop")

    def test_it_still_runs_the_following_morning(self):
        e = self._e()
        jobs._mark_ran(e, datetime(2026, 7, 27, 19, 40, tzinfo=DENVER))
        self.assertFalse(jobs._already_ran_today(e, datetime(2026, 7, 28, 7, tzinfo=DENVER)))


class MorningDigestTests(unittest.TestCase):
    """The email exists so David never has to go looking for the file."""

    def _e(self):
        return create_engine("sqlite://", future=True)

    def _ready(self, e, domain="rho.com"):
        m.record_leads(e, [{"domain": domain, "brand": "Rho", "tier": "A", "score": 16}])
        m.update_amazon_finding(e, domain, {
            "reason": "There are a handful of other sellers on your NAD+ listing. All authorized?",
            "confidence": "high", "marketplace": "amazon.com",
            "checked_at": "2026-07-27T12:00:00+00:00", "skipped_reason": "",
            "findings": {"absent": False, "sponsored_competitors": [],
                         "listings": [{"title": "Rho NAD+", "brand_price": 50.18,
                                       "cheapest": 48.0, "sellers_unknown": 18}]}})

    def test_only_brands_with_a_real_finding_are_offered(self):
        """A brand we checked and found nothing on still carries the old
        plan-upgrade line, which pitches the previous offer."""
        e = self._e()
        self._ready(e)
        m.record_leads(e, [{"domain": "nothing.com", "brand": "Nothing", "tier": "A"}])
        m.update_amazon_finding(e, "nothing.com", {
            "reason": "", "confidence": "high", "marketplace": "amazon.com",
            "checked_at": "2026-07-27T12:00:00+00:00", "skipped_reason": "",
            "findings": {"absent": False, "listings": [], "sponsored_competitors": []}})
        domains = [l["domain"] for l in jobs._sendable_brands(e)]
        self.assertEqual(domains, ["rho.com"])

    def test_a_skipped_brand_is_never_offered(self):
        e = self._e()
        m.record_leads(e, [{"domain": "skip.com", "brand": "Skip", "tier": "A"}])
        m.update_amazon_finding(e, "skip.com", {
            "reason": "", "confidence": "low", "marketplace": "amazon.com",
            "checked_at": "2026-07-27T12:00:00+00:00",
            "skipped_reason": "no confident match", "findings": {}})
        self.assertEqual(jobs._sendable_brands(e), [])

    def test_no_email_when_there_is_nothing_to_act_on(self):
        """A daily "0 brands" email trains you to ignore the daily email."""
        self.assertFalse(jobs._email_the_batch(self._e(), {"pulled": 0, "scanned": 0}))

    def test_best_brands_lead_the_list(self):
        e = self._e()
        self._ready(e, "low.com")
        self._ready(e, "high.com")
        with __import__("sqlalchemy").orm.Session(e) as s:
            pass
        m.record_leads(e, [{"domain": "top.com", "brand": "Top", "tier": "A", "score": 99}])
        m.update_amazon_finding(e, "top.com", {
            "reason": "Someone is listing your thing below your own price. Authorized?",
            "confidence": "high", "marketplace": "amazon.com",
            "checked_at": "2026-07-27T12:00:00+00:00", "skipped_reason": "",
            "findings": {"absent": False, "sponsored_competitors": [],
                         "listings": [{"title": "T", "brand_price": 10.0,
                                       "cheapest": 8.0, "sellers_unknown": 5}]}})
        self.assertEqual(jobs._sendable_brands(e)[0]["domain"], "top.com")


class ScheduledEndpointTests(unittest.TestCase):
    def _client(self):
        app = FastAPI()
        app.state.settings = SimpleNamespace(internal_api_key="internal-secret")
        app.state.session_factory = SimpleNamespace(
            kw={"bind": create_engine("sqlite://", future=True)}
        )
        app.include_router(jobs.router)
        return TestClient(app)

    def test_scheduler_rejects_missing_credentials(self):
        response = self._client().get("/api/jobs/outbound-morning/run")
        self.assertEqual(response.status_code, 401)

    def test_vercel_cron_stays_disabled_before_cutover(self):
        import os

        previous = os.environ.get("CRON_SECRET")
        previous_writes = os.environ.get("VERCEL_CRON_WRITES_ENABLED")
        os.environ["CRON_SECRET"] = "vercel-secret"
        os.environ["VERCEL_CRON_WRITES_ENABLED"] = "false"
        try:
            response = self._client().get(
                "/api/jobs/outbound-morning/run",
                headers={"Authorization": "Bearer vercel-secret"},
            )
        finally:
            if previous is None:
                os.environ.pop("CRON_SECRET", None)
            else:
                os.environ["CRON_SECRET"] = previous
            if previous_writes is None:
                os.environ.pop("VERCEL_CRON_WRITES_ENABLED", None)
            else:
                os.environ["VERCEL_CRON_WRITES_ENABLED"] = previous_writes
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "disabled")

    def test_staging_digest_link_uses_staging_host(self):
        import os

        previous = os.environ.get("SALES_SUPPORT_AGENT_URL")
        os.environ["SALES_SUPPORT_AGENT_URL"] = "https://agent-staging.anatainc.com/"
        try:
            self.assertTrue(
                jobs._batch_link().startswith("https://agent-staging.anatainc.com/")
            )
        finally:
            if previous is None:
                os.environ.pop("SALES_SUPPORT_AGENT_URL", None)
            else:
                os.environ["SALES_SUPPORT_AGENT_URL"] = previous
