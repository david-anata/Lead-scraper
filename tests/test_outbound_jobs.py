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

    def test_a_weekday_with_no_filler_recipes_still_sources_signal_campaigns(self):
        """Monday has no included filler recipe. amz_on still gets its locked share."""
        import outbound_campaign_source as source
        import outbound_pipeline as op
        from sales_support_agent.api import outbound_router
        from sales_support_agent.models import database as db
        from sales_support_agent.services import outbound_batches, outbound_delivery, outbound_memory

        engine = create_engine("sqlite://", future=True)
        calls = {}

        def fake_source(_engine, **kwargs):
            calls["room"] = kwargs.get("company_room")
            return {
                "recorded": 2,
                "leads": [{"domain": "on.com", "recipe": "amz_on", "brand": "On"}],
                "recipes": ["amz_on"],
                "scanned": 5,
                "skipped_seen": 1,
                "campaigns": [{
                    "recipe_key": "amz_on", "recorded": 2, "scanned": 5,
                    "skipped_seen": 1, "skipped_unfit": 0,
                }],
            }

        real = (
            op.load_config_from_env, db.get_engine, outbound_memory.persistence_health,
            outbound_batches.materialize_recipes, outbound_delivery.deliver_daily_batch,
            source.source_active_campaigns, outbound_router._amazon_checker,
        )
        op.load_config_from_env = lambda: ("storeleads-key", "")
        db.get_engine = lambda: engine
        outbound_memory.persistence_health = lambda _engine: {"ready": True}
        outbound_batches.materialize_recipes = lambda *_args, **_kwargs: []
        outbound_delivery.deliver_daily_batch = lambda *_a, **_k: {"email": "skipped", "slack": "skipped"}
        source.source_active_campaigns = fake_source
        outbound_router._amazon_checker = lambda _settings: None
        try:
            out = jobs.run_morning_routine(now=datetime(2026, 11, 9, 7, tzinfo=DENVER))
        finally:
            (
                op.load_config_from_env, db.get_engine, outbound_memory.persistence_health,
                outbound_batches.materialize_recipes, outbound_delivery.deliver_daily_batch,
                source.source_active_campaigns, outbound_router._amazon_checker,
            ) = real
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(calls.get("room"), 300)
        self.assertEqual(out["pulled"], 2)
        self.assertIn("amz_on", out["recipes"])

    def test_a_held_recipe_does_not_spend_a_storeleads_call(self):
        """churned_tool is on the weekday schedule and still must not be pulled."""
        import outbound_campaign_source as source
        import outbound_campaigns as campaigns
        import outbound_pipeline as op
        from sales_support_agent.api import outbound_router
        from sales_support_agent.models import database as db
        from sales_support_agent.services import outbound_batches, outbound_delivery, outbound_memory

        engine = create_engine("sqlite://", future=True)
        calls = []
        held = SimpleNamespace(key="churned_tool", label="Just dropped a growth tool", cap=lambda _settings: 30)
        allowed = SimpleNamespace(key="social_surge", label="Social following spiking", cap=lambda _settings: 5)

        def pull(**kwargs):
            calls.append(kwargs["recipe"].key)
            return op.PipelineResult(dry_run=True)

        real = (
            op.load_config_from_env, op.run_recipe_pull, db.get_engine,
            outbound_memory.persistence_health, outbound_batches.materialize_recipes,
            outbound_delivery.deliver_daily_batch, source.source_active_campaigns,
            source.source_sales_org, campaigns.list_campaigns, outbound_router._amazon_checker,
        )
        op.load_config_from_env = lambda: ("storeleads-key", "")
        op.run_recipe_pull = pull
        db.get_engine = lambda: engine
        outbound_memory.persistence_health = lambda _engine: {"ready": True}
        outbound_batches.materialize_recipes = lambda *_args, **_kwargs: [held, allowed]
        outbound_delivery.deliver_daily_batch = lambda *_a, **_k: {"email": "skipped", "slack": "skipped"}
        source.source_active_campaigns = lambda *_a, **_k: {
            "recorded": 0, "leads": [], "recipes": [], "campaigns": [],
        }
        source.source_sales_org = lambda *_a, **_k: {"recorded": 0}
        campaigns.list_campaigns = lambda _engine: [{
            "recipe_key": "churned_tool",
            "recipe_aliases": "",
            "status": "cut",
            "daily_cap": 0,
            "campaign_id": "a04a958c-6546-5ba0-a97d-586446d2b580",
            "name": "churned_tool|hold|no_trial_offer_ingest",
            "storeleads_filters": {"f:p": "shopify"},
        }]
        outbound_router._amazon_checker = lambda _settings: None
        try:
            out = jobs.run_morning_routine(now=datetime(2026, 11, 10, 7, tzinfo=DENVER))
        finally:
            (
                op.load_config_from_env, op.run_recipe_pull, db.get_engine,
                outbound_memory.persistence_health, outbound_batches.materialize_recipes,
                outbound_delivery.deliver_daily_batch, source.source_active_campaigns,
                source.source_sales_org, campaigns.list_campaigns, outbound_router._amazon_checker,
            ) = real
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(calls, ["social_surge"])
        self.assertNotIn("churned_tool", calls)

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
        self.assertEqual(out["reason"], "weekend")

    def test_the_daily_amazon_spend_is_capped(self):
        """Unbounded, this would check every brand we hold every morning and
        bill for all of them."""
        self.assertLessEqual(jobs._SCAN_PER_DAY, 20)
        self.assertGreater(jobs._SCAN_PER_DAY, 0)

    def test_it_runs_before_the_working_day(self):
        self.assertLess(jobs._RUN_HOUR, 9)

    def test_one_daily_sheet_has_a_controlled_company_ceiling(self):
        self.assertEqual(jobs._DAILY_COMPANY_CAP, 300)  # raised 2026-10-26 (maximize pulls)

    def _run_with_campaigns(self, *, recipes, catalog, sourced, pull_calls, now, env=None,
                            engine=None, lister=None):
        """Drive one morning routine with the campaign and recipe edges stubbed."""
        import os

        import outbound_campaign_source as source
        import outbound_campaigns as campaigns
        import outbound_pipeline as op
        from sales_support_agent.api import outbound_router
        from sales_support_agent.models import database as db
        from sales_support_agent.services import outbound_batches, outbound_delivery, outbound_memory

        if engine is None:
            engine = create_engine("sqlite://", future=True)

        def pull(**kwargs):
            pull_calls.append(kwargs["recipe"].key)
            return op.PipelineResult(dry_run=True)

        replacements = [
            (op, "load_config_from_env", lambda: ("storeleads-key", "")),
            (op, "run_recipe_pull", pull),
            (db, "get_engine", lambda: engine),
            (outbound_memory, "persistence_health", lambda _engine: {"ready": True}),
            (outbound_batches, "materialize_recipes", lambda *_args, **_kwargs: recipes),
            (outbound_delivery, "deliver_daily_batch", lambda *_a, **_k: {"email": "skipped", "slack": "skipped"}),
            (source, "source_active_campaigns", lambda *_a, **_k: sourced),
            (source, "source_sales_org", lambda *_a, **_k: {"recorded": 0}),
            (campaigns, "list_campaigns", lister or (lambda _engine: catalog)),
            (outbound_router, "_amazon_checker", lambda _settings: None),
        ]
        saved = [(owner, name, getattr(owner, name)) for owner, name, _value in replacements]
        for owner, name, value in replacements:
            setattr(owner, name, value)
        previous_env = os.environ.get("OUTBOUND_PROTECTED_CAMPAIGN_IDS")
        if env is not None:
            os.environ["OUTBOUND_PROTECTED_CAMPAIGN_IDS"] = env
        try:
            result = jobs.run_morning_routine(now=now)
        finally:
            for owner, name, value in saved:
                setattr(owner, name, value)
            if env is None:
                pass
            elif previous_env is None:
                os.environ.pop("OUTBOUND_PROTECTED_CAMPAIGN_IDS", None)
            else:
                os.environ["OUTBOUND_PROTECTED_CAMPAIGN_IDS"] = previous_env
        return engine, result

    def test_campaign_rows_are_not_duplicated_by_standalone_skips(self):
        """2026-11-11: plan_upgrade was inserted twice and the batch stayed running."""
        import outbound_instantly_push as push
        from sales_support_agent.services import outbound_batches

        pulled = []
        plan_id = push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]
        recipes = [
            SimpleNamespace(key="plan_upgrade", label="Upgraded to Shopify Plus", cap=lambda _s: 30),
            SimpleNamespace(key="replatformed", label="Recently replatformed", cap=lambda _s: 25),
            SimpleNamespace(key="churned_tool", label="Just dropped a growth tool", cap=lambda _s: 30),
        ]
        catalog = [
            {"recipe_key": "amz_on", "recipe_aliases": "", "status": "active", "daily_cap": 100,
             "campaign_id": "8e0df427-1f18-43ac-b0c6-e8ea787123d7", "storeleads_filters": {"f:scs": "Amazon"}},
            {"recipe_key": "amz_off", "recipe_aliases": "", "status": "active", "daily_cap": 100,
             "campaign_id": "138a162d-6654-4916-9959-20c11e31214b", "storeleads_filters": {"f:scs": "Amazon"}},
            {"recipe_key": "plan_upgrade", "recipe_aliases": "", "status": "active", "daily_cap": 100,
             "campaign_id": plan_id, "storeleads_filters": {"f:plan": "Shopify Plus"}},
            {"recipe_key": "plan_upgrade", "recipe_aliases": "", "status": "cut", "daily_cap": 30,
             "campaign_id": "1937c21a-89c4-458f-a949-306f90ae6258", "storeleads_filters": {"f:plan": "Shopify Plus"}},
            {"recipe_key": "replatform", "recipe_aliases": "replatformed", "status": "active", "daily_cap": 100,
             "campaign_id": "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c", "storeleads_filters": {"f:p": "shopify"}},
            {"recipe_key": "churned_tool", "recipe_aliases": "", "status": "cut", "daily_cap": 0,
             "campaign_id": "a04a958c-6546-5ba0-a97d-586446d2b580",
             "name": "churned_tool|hold|no_trial_offer_ingest", "storeleads_filters": None},
        ]
        sourced = {
            "recorded": 42,
            "leads": [{"domain": "on.example", "brand": "On", "recipe": "amz_on"}],
            "recipes": ["amz_on"],
            "campaigns": [
                {"recipe_key": "amz_on", "recorded": 42, "scanned": 80, "skipped_seen": 3},
                {"recipe_key": "amz_off", "recorded": 0, "scanned": 20, "skipped_seen": 10},
                {"recipe_key": "plan_upgrade", "recorded": 0, "scanned": 11, "skipped_seen": 4},
                {"recipe_key": "replatform", "recorded": 0, "scanned": 8, "skipped_seen": 2},
            ],
        }
        engine, out = self._run_with_campaigns(
            recipes=recipes, catalog=catalog, sourced=sourced, pull_calls=pulled,
            now=datetime(2026, 11, 11, 9, tzinfo=DENVER), env=plan_id,
        )
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(pulled, [])
        runs = outbound_batches.batch_runs(engine, out["batch_id"])
        by_key = {row["recipe_key"]: row for row in runs}
        self.assertEqual(list(by_key), sorted(by_key))
        self.assertEqual(set(by_key), {"amz_on", "amz_off", "plan_upgrade", "replatform", "churned_tool"})
        self.assertEqual(by_key["plan_upgrade"]["status"], "complete")
        self.assertEqual(by_key["plan_upgrade"]["fresh"], 0)
        self.assertNotIn("replatformed", by_key)
        self.assertEqual(by_key["churned_tool"]["status"], "skipped")
        self.assertEqual(by_key["amz_on"]["fresh"], 42)
        batch = outbound_batches.get_batch(engine, out["batch_id"])
        self.assertEqual(batch.status, "ready")
        self.assertEqual(batch.unique_companies, 1)

    def test_a_missing_catalog_does_not_pull_a_recipe_the_campaign_already_recorded(self):
        from sales_support_agent.services import outbound_batches

        pulled = []
        recipe = [SimpleNamespace(key="plan_upgrade", label="Upgraded to Shopify Plus", cap=lambda _s: 30)]
        sourced = {"recorded": 0, "leads": [], "recipes": [], "campaigns": [
            {"recipe_key": "plan_upgrade", "recorded": 0, "scanned": 4, "skipped_seen": 1},
        ]}
        now = datetime(2026, 11, 11, 9, tzinfo=DENVER)
        engine, out = self._run_with_campaigns(
            recipes=recipe, catalog=[], sourced=sourced, pull_calls=pulled, now=now,
        )
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(pulled, [])
        runs = outbound_batches.batch_runs(engine, out["batch_id"])
        self.assertEqual([row["recipe_key"] for row in runs], ["plan_upgrade"])
        self.assertEqual(runs[0]["status"], "complete")

        def explode(_engine):
            raise RuntimeError("catalog unavailable")

        pulled.clear()
        engine, out = self._run_with_campaigns(
            recipes=recipe, catalog=[], sourced=sourced, pull_calls=pulled, now=now, lister=explode,
        )
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(pulled, [])
        runs = outbound_batches.batch_runs(engine, out["batch_id"])
        self.assertEqual([(row["recipe_key"], row["status"]) for row in runs], [("plan_upgrade", "complete")])

    def test_two_active_campaigns_for_one_recipe_become_one_batch_row(self):
        from sales_support_agent.services import outbound_batches

        _engine, out = self._run_with_campaigns(
            recipes=[],
            catalog=[],
            sourced={"recorded": 45, "leads": [], "recipes": ["plan_upgrade"], "campaigns": [
                {"recipe_key": "plan_upgrade", "recorded": 42, "scanned": 80, "skipped_seen": 1},
                {"recipe_key": "plan_upgrade", "recorded": 3, "scanned": 10, "skipped_seen": 0},
            ]},
            pull_calls=[],
            now=datetime(2026, 11, 11, 9, tzinfo=DENVER),
        )
        runs = outbound_batches.batch_runs(_engine, out["batch_id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["fresh"], 45)
        self.assertEqual(outbound_batches.get_batch(_engine, out["batch_id"]).scanned, 90)

    def test_an_active_hold_campaign_does_not_emit_a_second_skipped_row(self):
        import outbound_instantly_push as push
        from sales_support_agent.services import outbound_batches

        hold_id = "a04a958c-6546-5ba0-a97d-586446d2b580"
        self.assertIn(hold_id, push.HOLD_INGEST_CAMPAIGN_IDS)
        pulled = []
        _engine, out = self._run_with_campaigns(
            recipes=[SimpleNamespace(key="churned_tool", label="Just dropped a growth tool", cap=lambda _s: 30)],
            catalog=[{
                "recipe_key": "churned_tool", "recipe_aliases": "", "status": "active",
                "daily_cap": 10, "campaign_id": hold_id,
                "name": "churned_tool|hold|no_trial_offer_ingest",
                "storeleads_filters": {"f:p": "shopify"},
            }],
            sourced={"recorded": 2, "leads": [], "recipes": ["churned_tool"], "campaigns": [
                {"recipe_key": "churned_tool", "recorded": 2, "scanned": 6, "skipped_seen": 1},
            ]},
            pull_calls=pulled,
            now=datetime(2026, 11, 10, 9, tzinfo=DENVER),
        )
        self.assertEqual(pulled, [])
        runs = outbound_batches.batch_runs(_engine, out["batch_id"])
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "complete")
        self.assertEqual(runs[0]["fresh"], 2)

    def test_a_stuck_batch_is_reopened_and_a_finished_one_is_not(self):
        import outbound_pipeline as op
        from sales_support_agent.services import outbound_batches

        now = datetime(2026, 11, 11, 9, tzinfo=DENVER)
        empty = {"recorded": 0, "leads": [], "recipes": [], "campaigns": []}
        engine = create_engine("sqlite://", future=True)
        outbound_batches.ensure_tables(engine)
        batch_id, _created = outbound_batches.create_batch(
            engine, business_date="2026-11-11", trigger="scheduled",
            recipe_count=2, correlation_id="stuck",
        )
        self.assertEqual(outbound_batches.get_batch(engine, batch_id).status, "running")
        _engine, out = self._run_with_campaigns(
            recipes=[], catalog=[], sourced=empty, pull_calls=[], now=now, engine=engine,
        )
        self.assertIs(_engine, engine)
        self.assertTrue(out["ran"], out.get("reason"))
        self.assertEqual(out["batch_id"], batch_id)
        self.assertEqual(outbound_batches.get_batch(engine, batch_id).status, "ready")

        pulled = []
        blocked_engine, blocked = self._run_with_campaigns(
            recipes=[SimpleNamespace(key="plan_upgrade", label="Upgraded to Shopify Plus", cap=lambda _s: 30)],
            catalog=[],
            sourced={"recorded": 99, "leads": [{"domain": "new.example", "recipe": "plan_upgrade"}],
                     "recipes": ["plan_upgrade"], "campaigns": [
                         {"recipe_key": "plan_upgrade", "recorded": 99, "scanned": 1, "skipped_seen": 0},
                     ]},
            pull_calls=pulled, now=now, engine=engine,
        )
        self.assertIs(blocked_engine, engine)
        self.assertFalse(blocked["ran"])
        self.assertIn("already exists", blocked["reason"])
        self.assertEqual(blocked["batch_id"], batch_id)
        self.assertEqual(pulled, [])
        self.assertEqual(outbound_batches.get_batch(engine, batch_id).unique_companies, 0)

        failed_engine = create_engine("sqlite://", future=True)

        def boom(_leads):
            raise RuntimeError(
                'duplicate key value violates unique constraint "outbound_daily_batch_runs_pkey"'
            )

        saved_csv = op.leads_to_csv
        op.leads_to_csv = boom
        try:
            with self.assertRaises(RuntimeError):
                self._run_with_campaigns(
                    recipes=[], catalog=[], sourced=empty, pull_calls=[], now=now, engine=failed_engine,
                )
        finally:
            op.leads_to_csv = saved_csv
        failed = outbound_batches.load_batches(failed_engine, page=1, per_page=5)[0][0]
        self.assertEqual(failed.status, "failed")
        self.assertIn("outbound_daily_batch_runs_pkey", failed.error)
        _same, retried = self._run_with_campaigns(
            recipes=[], catalog=[], sourced=empty, pull_calls=[], now=now, engine=failed_engine,
        )
        self.assertTrue(retried["ran"], retried.get("reason"))
        self.assertEqual(retried["batch_id"], failed.id)
        self.assertEqual(outbound_batches.get_batch(failed_engine, failed.id).status, "ready")

    def test_a_failed_finalize_rerun_puts_the_42_orphans_on_batch_12(self):
        """2026-11-11: 42 amz_on rows were saved, finalize rolled back, none were pushed."""
        import outbound_pipeline as op
        from sqlalchemy import text

        from sales_support_agent.services import outbound_batches, outbound_memory

        now = datetime(2026, 11, 11, 9, tzinfo=DENVER)
        engine = create_engine("sqlite://", future=True)
        outbound_batches.ensure_tables(engine)
        for day in range(1, 12):
            outbound_batches.create_batch(
                engine, business_date=f"2026-10-{day:02d}", trigger="scheduled",
                recipe_count=0, correlation_id=f"pad-{day}",
            )
        batch_id, created = outbound_batches.create_batch(
            engine, business_date="2026-11-11", trigger="scheduled",
            recipe_count=2, correlation_id="batch-12",
        )
        self.assertTrue(created)
        self.assertEqual(batch_id, 12)
        orphans = [
            {"domain": f"amz-on-{index}.example", "brand": f"Brand {index}", "recipe": "amz_on"}
            for index in range(42)
        ]
        outbound_memory.record_leads(engine, orphans, source="campaign:amz_on")
        outbound_memory.record_leads(engine, [
            {"domain": "old.example", "brand": "Old", "recipe": "amz_on"},
            {"domain": "pushed.example", "brand": "Pushed", "recipe": "amz_on"},
            {"domain": "stolen.example", "brand": "Stolen", "recipe": "amz_on"},
        ], source="campaign:amz_on")
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE outbound_contacted_domains SET first_seen_at='2020-01-01 00:00:00' "
                "WHERE domain='old.example'"
            ))
            conn.execute(text(
                "CREATE TABLE outbound_enriched_contacts ("
                "email TEXT, domain TEXT, pushed_at TEXT, campaign_id TEXT)"
            ))
            conn.execute(text(
                "INSERT INTO outbound_enriched_contacts (email, domain, pushed_at, campaign_id) "
                "VALUES ('a@pushed.example', 'pushed.example', '2026-11-11 14:05:00', "
                "'8e0df427-1f18-43ac-b0c6-e8ea787123d7')"
            ))
        later_id, _later = outbound_batches.create_batch(
            engine, business_date="2026-11-12", trigger="scheduled",
            recipe_count=1, correlation_id="nov-12",
        )
        later = outbound_batches.finalize_batch(
            engine, batch_id=later_id,
            recipe_runs=[{"recipe_key": "amz_on", "status": "complete", "fresh": 1}],
            leads=[{"domain": "stolen.example", "brand": "Stolen", "recipe": "amz_on"}],
            filename="anata-daily-leads-2026-11-12.csv",
        )
        self.assertEqual(later.unique_companies, 1)
        empty = {"recorded": 0, "leads": [], "recipes": [], "campaigns": []}
        saved_csv = op.leads_to_csv

        def boom(_leads):
            raise RuntimeError(
                'duplicate key value violates unique constraint "outbound_daily_batch_runs_pkey"'
            )

        op.leads_to_csv = boom
        try:
            with self.assertRaises(RuntimeError):
                self._run_with_campaigns(
                    recipes=[], catalog=[], sourced=empty, pull_calls=[], now=now, engine=engine,
                )
        finally:
            op.leads_to_csv = saved_csv
        failed = outbound_batches.get_batch(engine, 12)
        self.assertEqual(failed.status, "failed")
        self.assertEqual(outbound_batches.load_batch_leads(engine, [12]), [])
        _same, retried = self._run_with_campaigns(
            recipes=[], catalog=[], sourced=empty, pull_calls=[], now=now, engine=engine,
        )
        self.assertTrue(retried["ran"], retried.get("reason"))
        self.assertEqual(retried["batch_id"], 12)
        self.assertEqual(retried["recovered_companies"], 42)
        finished = outbound_batches.get_batch(engine, 12)
        self.assertEqual(finished.status, "ready")
        self.assertEqual(finished.unique_companies, 42)
        domains = {lead["domain"] for lead in outbound_batches.load_batch_leads(engine, [12])}
        self.assertEqual(domains, {row["domain"] for row in orphans})
        self.assertNotIn("pushed.example", domains)
        self.assertNotIn("old.example", domains)
        self.assertNotIn("stolen.example", domains)
        runs = {row["recipe_key"]: row for row in outbound_batches.batch_runs(engine, 12)}
        self.assertEqual(runs["amz_on"]["fresh"], 42)
        self.assertEqual(runs["amz_on"]["status"], "complete")
        self.assertEqual(outbound_batches.get_batch(engine, later_id).unique_companies, 1)


class ScheduledRouteTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
        app.include_router(jobs.router)
        self.client = TestClient(app)

    def test_vercel_cron_can_reach_the_route_with_get(self):
        response = self.client.get("/api/jobs/outbound-morning/run")
        self.assertEqual(response.status_code, 401)
        self.assertNotEqual(response.status_code, 404)

    def test_internal_scheduler_can_reach_the_route_with_post(self):
        response = self.client.post(
            "/api/jobs/outbound-morning/run",
            headers={"X-Internal-Api-Key": "wrong-key"},
        )
        self.assertEqual(response.status_code, 401)
        self.assertNotEqual(response.status_code, 405)

    def test_cron_bearer_secret_is_accepted(self):
        import os
        from unittest.mock import patch

        class NoonDatetime:
            @classmethod
            def now(cls, tz):
                return datetime(2026, 8, 31, 12, tzinfo=tz)

        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            jobs, "datetime", NoonDatetime
        ):
            response = self.client.get(
                "/api/jobs/outbound-morning/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "skipped")

    def _lease_engine(self):
        from sqlalchemy.pool import StaticPool

        from sales_support_agent.services.job_lease import ensure_job_lease_schema

        # The route runs in another thread. A plain memory engine would give
        # that thread an empty database and the lease table would be missing.
        engine = create_engine(
            "sqlite://", future=True, poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
        ensure_job_lease_schema(engine)
        return engine

    def test_rerun_outside_the_pull_hour_replays_a_failed_lease(self):
        import os
        from unittest.mock import patch

        from sqlalchemy import text

        from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job

        engine = self._lease_engine()
        lease = claim_scheduled_job(
            engine, job_key="outbound_morning", run_key="2026-11-11", lease_minutes=240,
        )
        finish_scheduled_job(engine, lease, status="failed", details={"error": "UniqueViolation"})
        calls = []

        class Afternoon:
            @classmethod
            def now(cls, tz):
                return datetime(2026, 11, 11, 15, 43, tzinfo=tz)

        def ran(**kwargs):
            calls.append(kwargs.get("now"))
            return {"ran": True, "recipes": ["amz_on"], "pulled": 1, "scanned": 0, "reason": ""}

        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            jobs, "datetime", Afternoon
        ), patch(
            "sales_support_agent.models.database.get_engine", return_value=engine
        ), patch.object(jobs, "run_morning_routine", side_effect=ran):
            waiting = self.client.post(
                "/api/jobs/outbound-morning/run",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
            self.assertEqual(waiting.status_code, 200)
            self.assertEqual(waiting.json()["status"], "skipped")
            self.assertIn("waiting", waiting.json()["message"])
            self.assertEqual(calls, [])
            replay = self.client.post(
                "/api/jobs/outbound-morning/run?rerun=1",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json()["status"], "ok")
        self.assertEqual(replay.json()["run_key"], "2026-11-11")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].hour, 15)
        with engine.connect() as conn:
            status = conn.execute(text(
                "SELECT status FROM scheduled_job_runs WHERE job_key='outbound_morning' AND run_key='2026-11-11'"
            )).scalar_one()
        self.assertEqual(status, "succeeded")

    def test_rerun_can_reclaim_a_succeeded_lease_and_a_normal_call_cannot(self):
        import os
        from unittest.mock import patch

        from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job

        engine = self._lease_engine()
        lease = claim_scheduled_job(
            engine, job_key="outbound_morning", run_key="2026-11-11", lease_minutes=240,
        )
        finish_scheduled_job(engine, lease, status="succeeded", details={"ran": True})
        calls = []

        class Seven:
            @classmethod
            def now(cls, tz):
                return datetime(2026, 11, 11, 7, tzinfo=tz)

        def ran(**_kwargs):
            calls.append("ran")
            return {"ran": True, "pulled": 0, "scanned": 0, "reason": ""}

        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            jobs, "datetime", Seven
        ), patch(
            "sales_support_agent.models.database.get_engine", return_value=engine
        ), patch.object(jobs, "run_morning_routine", side_effect=ran):
            blocked = self.client.get(
                "/api/jobs/outbound-morning/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )
            self.assertEqual(blocked.json()["status"], "skipped")
            self.assertIn("already running or complete", blocked.json()["message"])
            self.assertEqual(calls, [])
            replay = self.client.get(
                "/api/jobs/outbound-morning/run?rerun=1",
                headers={"Authorization": "Bearer cron-test-key"},
            )
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json()["status"], "ok")
        self.assertEqual(calls, ["ran"])

    def test_a_failed_finalize_returns_503_and_frees_the_lease(self):
        import os
        from unittest.mock import patch

        from sqlalchemy import text

        engine = self._lease_engine()

        class Seven:
            @classmethod
            def now(cls, tz):
                return datetime(2026, 11, 11, 7, tzinfo=tz)

        def boom(**_kwargs):
            raise RuntimeError("finalize failed")

        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            jobs, "datetime", Seven
        ), patch(
            "sales_support_agent.models.database.get_engine", return_value=engine
        ), patch.object(jobs, "run_morning_routine", side_effect=boom):
            response = self.client.get(
                "/api/jobs/outbound-morning/run?rerun=1",
                headers={"Authorization": "Bearer cron-test-key"},
            )
        self.assertEqual(response.status_code, 503)
        with engine.connect() as conn:
            status = conn.execute(text(
                "SELECT status FROM scheduled_job_runs WHERE job_key='outbound_morning' AND run_key='2026-11-11'"
            )).scalar_one()
        self.assertEqual(status, "failed")

    def test_rerun_can_name_batch_12_and_refuses_a_finished_day(self):
        import os
        from unittest.mock import patch

        from sales_support_agent.services import outbound_batches

        engine = self._lease_engine()
        outbound_batches.ensure_tables(engine)
        batch_id, _created = outbound_batches.create_batch(
            engine, business_date="2026-11-11", trigger="scheduled",
            recipe_count=2, correlation_id="open",
        )
        ready_id, _ready = outbound_batches.create_batch(
            engine, business_date="2026-11-12", trigger="scheduled",
            recipe_count=1, correlation_id="done",
        )
        outbound_batches.finalize_batch(
            engine, batch_id=ready_id,
            recipe_runs=[{"recipe_key": "amz_on", "status": "complete", "fresh": 150}],
            leads=[{"domain": "today.example", "brand": "Today", "recipe": "amz_on"}],
            filename="anata-daily-leads-2026-11-12.csv",
        )
        calls = []

        class Afternoon:
            @classmethod
            def now(cls, tz):
                return datetime(2026, 11, 12, 15, tzinfo=tz)

        def ran(**kwargs):
            calls.append(kwargs.get("now"))
            return {"ran": True, "pulled": 42, "scanned": 0, "reason": "", "batch_id": batch_id}

        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            jobs, "datetime", Afternoon
        ), patch(
            "sales_support_agent.models.database.get_engine", return_value=engine
        ), patch.object(jobs, "run_morning_routine", side_effect=ran):
            bare = self.client.post(
                "/api/jobs/outbound-morning/run?business_date=2026-11-11",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
            self.assertEqual(bare.status_code, 400)
            self.assertEqual(calls, [])
            replay = self.client.post(
                "/api/jobs/outbound-morning/run?rerun=1&business_date=2026-11-11",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
            by_id = self.client.get(
                f"/api/jobs/outbound-morning/run?rerun=1&batch_id={batch_id}",
                headers={"Authorization": "Bearer cron-test-key"},
            )
            finished = self.client.post(
                "/api/jobs/outbound-morning/run?rerun=1&business_date=2026-11-12",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
            mismatch = self.client.post(
                f"/api/jobs/outbound-morning/run?rerun=1&batch_id={batch_id}&business_date=2026-11-12",
                headers={"X-Internal-Api-Key": "internal-test-key"},
            )
        self.assertEqual(replay.status_code, 200, replay.text)
        self.assertEqual(replay.json()["run_key"], "2026-11-11")
        self.assertEqual(calls[0].date().isoformat(), "2026-11-11")
        # The first call already finished the 2026-11-11 lease as succeeded.
        # batch_id still names an unfinished batch, so the lease can be reclaimed.
        self.assertEqual(by_id.status_code, 200, by_id.text)
        self.assertEqual(by_id.json()["run_key"], "2026-11-11")
        self.assertEqual(finished.status_code, 409, finished.text)
        self.assertIn("Only a running or failed batch", finished.json()["detail"])
        self.assertIn(str(ready_id), finished.json()["detail"])
        self.assertEqual(mismatch.status_code, 409, mismatch.text)
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            outbound_batches.get_batch(engine, ready_id).status, "ready",
        )


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
