"""Daily Leads alerts, brand names, counters, the manual pull, and copy."""

from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

import outbound_instantly_push as push
import outbound_openers as openers
import outbound_pipeline as pipeline
from sales_support_agent.api import outbound_jobs as jobs
from sales_support_agent.api.outbound_router import daily_run_control
from sales_support_agent.services import outbound_batches as batches

DENVER = ZoneInfo("America/Denver")


def test_brand_names_drop_legal_store_and_tld_words_and_keep_stylization():
    assert pipeline.clean_brand_name("Acme LLC") == "Acme"
    assert pipeline.clean_brand_name("Acme, Inc.") == "Acme"
    assert pipeline.clean_brand_name("Acme Co.") == "Acme"
    assert pipeline.clean_brand_name("Acme Official Store") == "Acme"
    assert pipeline.clean_brand_name("Acme Shop") == "Acme"
    assert pipeline.clean_brand_name("acme.com") == "Acme"
    assert pipeline.clean_brand_name("ACME | handmade soap") == "Acme"
    assert pipeline.clean_brand_name("REV Gum") == "REV Gum"
    assert pipeline.clean_brand_name("MyBevi") == "MyBevi"
    lead = push.build_lead(
        {"email": "a@acme.com", "first_name": "Ann"},
        {"brand": "ACME OFFICIAL STORE", "domain": "acme.com"},
        "Acme sells salsa.",
    )
    assert lead["company_name"] == "Acme"


def test_zero_company_and_recipe_errors_alert_sales_and_a_good_batch_does_not():
    posted = []

    class Slack:
        def post_message(self, *, text, channel):
            posted.append((channel, text))
            return {"ok": True}

    empty = SimpleNamespace(id=11, business_date="2026-11-10", unique_companies=0)
    out = jobs.post_sales_alert(empty, [], slack=Slack(), held=2)
    assert out["posted"] is True
    assert posted[0][0] == jobs.SALES_SLACK_CHANNEL
    assert "batch 11" in posted[0][1]
    assert "0 companies" in posted[0][1]
    assert "2" in posted[0][1]

    posted.clear()
    full = SimpleNamespace(id=12, business_date="2026-11-10", unique_companies=4)
    quiet = jobs.post_sales_alert(full, [{"recipe_key": "amz_on", "status": "complete"}], slack=Slack())
    assert quiet["posted"] is False and posted == []

    failed = jobs.post_sales_alert(
        full,
        [{"recipe_key": "amz_off", "status": "failed", "error": "StoreLeads returned 429"}],
        slack=Slack(),
    )
    assert failed["posted"] is True
    assert "amz_off" in posted[0][1] and "429" in posted[0][1]
    skipped = jobs.post_sales_alert(
        full,
        [{"recipe_key": "churned_tool", "status": "skipped", "error": "held recipe"}],
        slack=Slack(),
    )
    assert skipped["posted"] is False


def test_batch_counters_match_delivered_companies_and_per_recipe_recorded():
    engine = create_engine("sqlite://")
    batch_id, _created = batches.create_batch(
        engine, business_date="2026-11-10", trigger="scheduled",
        recipe_count=1, correlation_id="counters",
    )
    batch = batches.finalize_batch(
        engine, batch_id=batch_id,
        recipe_runs=[
            {"recipe_key": "amz_on", "recipe_label": "amz_on", "status": "complete",
             "fresh": 2, "scanned": 10, "matched": 2, "skipped_seen": 3},
            {"recipe_key": "amz_off", "recipe_label": "amz_off", "status": "failed",
             "fresh": 0, "scanned": 5, "matched": 0, "error": "StoreLeads returned 429"},
            {"recipe_key": "churned_tool", "recipe_label": "churned", "status": "skipped",
             "fresh": 0, "error": "held recipe has no live campaign"},
        ],
        leads=[
            {"domain": "one.com", "brand": "One", "recipe": "amz_on"},
            {"domain": "two.com", "brand": "Two", "recipe": "amz_on"},
            {"domain": "ONE.com", "brand": "One", "recipe": "amz_on"},
        ],
        filename="anata-daily-leads-2026-11-10.csv",
    )
    assert batch.unique_companies == 2
    assert batch.scanned == 15
    assert batch.matched == 2
    assert batch.completed_count == 1
    assert batch.failed_count == 1
    assert batch.recipe_count == 3
    assert batch.duplicates_removed == 0
    assert "429" in batch.error
    assert "held recipe" not in batch.error
    _filename, csv = batches.batch_artifact(engine, batch_id)
    assert csv.count("one.com") == 1 and "two.com" in csv


def test_manual_pull_reports_running_and_already_finished_without_starting():
    engine = create_engine("sqlite://")
    now = datetime(2026, 11, 10, 9, tzinfo=DENVER)
    jobs._morning_state["running"] = False
    jobs._morning_state["last_status"] = ""
    ready = jobs.morning_pull_status(engine, now)
    assert ready["status"] == "ready"
    assert jobs.begin_morning_run() is True
    assert jobs.morning_pull_status(engine, now)["status"] == "running"
    assert jobs.begin_morning_run() is False
    jobs.finish_morning_run({"ran": False, "reason": "StoreLeads pull failed"})
    failed = jobs.morning_pull_status(engine, now)
    assert failed["status"] == "failed" and "StoreLeads pull failed" in failed["reason"]
    jobs._morning_state["last_status"] = ""
    batch_id, _created = batches.create_batch(
        engine, business_date="2026-11-10", trigger="scheduled",
        recipe_count=1, correlation_id="button",
    )
    batches.finalize_batch(
        engine, batch_id=batch_id,
        recipe_runs=[{"recipe_key": "amz_on", "status": "complete", "fresh": 1, "scanned": 4, "matched": 1}],
        leads=[{"domain": "done.com", "brand": "Done", "recipe": "amz_on"}],
        filename="anata-daily-leads-2026-11-10.csv",
    )
    done = jobs.morning_pull_status(engine, now)
    assert done["already_ran"] is True
    assert done["unique_companies"] == 1
    assert "Finished" in done["reason"] or "finished" in done["reason"]
    control = daily_run_control()
    assert "Run today's pull now" in control
    assert "/admin/api/outbound/run-morning" in control
    assert "confirm(" in control
    assert "Started" in control and "Running" in control and "Finished" in control and "Failed" in control


def test_outbound_subjects_and_openers_do_not_use_em_dashes_or_hyphen_substitutes():
    assert openers.clean("Nice work — really") == "Nice work, really"
    assert " - " not in openers.clean("Nice work — really")
    from sales_support_agent.services.outbound_delivery import deliver_daily_batch

    engine = create_engine("sqlite://")
    from sales_support_agent.services import outbound_memory
    outbound_memory.save_delivery_settings(engine, {
        "enabled": "1", "email_enabled": "1", "slack_enabled": "0",
        "email_recipients": "qa@example.com",
    })
    batch_id, _created = batches.create_batch(
        engine, business_date="2026-11-10", trigger="scheduled",
        recipe_count=1, correlation_id="copy",
    )
    batch = batches.finalize_batch(
        engine, batch_id=batch_id,
        recipe_runs=[{"recipe_key": "amz_on", "status": "complete", "fresh": 1}],
        leads=[{"domain": "copy.com", "brand": "Copy", "recipe": "amz_on"}],
        filename="anata-daily-leads-2026-11-10.csv",
    )
    sent = {}

    def capture(**kwargs):
        sent["subject"] = kwargs["subject"]
        return True

    from unittest.mock import patch
    with patch("sales_support_agent.services.access.notify._send", side_effect=lambda *a, **k: capture(**k)), \
         patch("sales_support_agent.config.load_settings", return_value=SimpleNamespace()):
        deliver_daily_batch(engine, batch, [], test=True)
    assert "—" not in sent["subject"] and "–" not in sent["subject"]
    assert " - " not in sent["subject"]
    assert sent["subject"].startswith("TEST, Daily Leads:")
