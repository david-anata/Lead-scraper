"""Campaign table, StoreLeads sourcing, and the Monday score.

These never call Instantly. Proposals stay status='proposed'.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

import outbound_campaign_score as score
import outbound_campaign_source as source
import outbound_campaigns as campaigns
import outbound_enrich as oe
import outbound_instantly_push as push

DENVER = ZoneInfo("America/Denver")
NOW = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)  # a Monday


@pytest.fixture
def engine():
    return create_engine("sqlite://")


def test_seed_uses_only_confirmed_filters():
    for row in campaigns.SEED_ROWS:
        resolved = campaigns.resolve_filters(row["storeleads_filters"], NOW)
        assert set(resolved) <= campaigns.CONFIRMED_FILTERS
        for key in resolved:
            assert not str(key).startswith("_")


def test_shared_floor_and_the_four_signal_filters():
    by_key = {row["recipe_key"]: row for row in campaigns.SEED_ROWS}
    for key in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        resolved = campaigns.resolve_filters(by_key[key]["storeleads_filters"], NOW)
        assert resolved["f:p"] == "shopify"
        assert resolved["f:ds"] == "Active"
        assert resolved["f:cc"] == "US"
        assert resolved["f:it"] == "email"
        assert resolved["f:ermin"] == 1_000_000
        assert resolved["f:ermax"] == 2_000_000
    assert campaigns.resolve_filters(by_key["amz_on"]["storeleads_filters"], NOW)["f:scs"] == "Amazon"
    off = campaigns.resolve_filters(by_key["amz_off"]["storeleads_filters"], NOW)
    assert off["f:scs"] == "Amazon" and off["f:scs:op"] == "not"
    plan = campaigns.resolve_filters(by_key["plan_upgrade"]["storeleads_filters"], NOW)
    assert plan["f:plan"] == "Shopify Plus"
    plan_min = datetime.strptime(plan["f:last_plan_change_at:min"], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
    assert plan_min == NOW - timedelta(days=90)
    moved = campaigns.resolve_filters(by_key["replatform"]["storeleads_filters"], NOW)
    assert moved["f:last_platform"] == "woocommerce,magento,bigcommerce"
    assert moved["f:last_platform:op"] == "or"
    moved_min = datetime.strptime(moved["f:last_platform_change_at:min"], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
    assert moved_min == NOW - timedelta(days=120)
    assert by_key["abby_reengage"]["storeleads_filters"] is None
    assert by_key["replatform"]["recipe_aliases"] == "replatformed"


def test_unconfirmed_filter_keys_are_dropped():
    resolved = campaigns.resolve_filters(
        {"f:p": "shopify", "f:not_real": "x", "_note": "no"}, NOW,
    )
    assert resolved == {"f:p": "shopify"}


def test_seed_and_alias_routing(engine):
    campaigns.ensure_tables(engine)
    routing = campaigns.load_routing(engine)
    assert routing.available
    assert routing.by_recipe["amz_on"] == "a333e00e-bc3b-4abf-bcbf-4e0eb126090c"
    assert routing.by_recipe["replatformed"] == routing.by_recipe["replatform"]
    assert routing.by_recipe["abby_reengage"] == "8ebf1923-7ba4-4a63-96a8-adb18a58ddf6"
    assert "replatformed" not in routing.blocked


def test_empty_table_is_not_a_routing_source(engine):
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM outbound_campaigns"))
    routing = campaigns.load_routing(engine)
    assert routing.available is False


def test_cut_and_proposed_are_blocked(engine):
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE outbound_campaigns SET status = 'cut' WHERE recipe_key = 'plan_upgrade'"
        ))
    routing = campaigns.load_routing(engine)
    assert "plan_upgrade" in routing.blocked
    assert "plan_upgrade" not in routing.by_recipe
    assert routing.by_recipe["amz_on"]


def test_push_pending_uses_the_table_and_holds_cut_recipes(engine):
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE outbound_campaigns SET campaign_id = 'table-camp' WHERE recipe_key = 'amz_on'"
        ))
        conn.execute(text(
            "UPDATE outbound_campaigns SET status = 'cut' WHERE recipe_key = 'plan_upgrade'"
        ))
    oe.ensure_tables(engine)
    oe._record_person(
        engine,
        SimpleNamespace(email="a@on.com", domain="on.com", first_name="A", last_name="B",
                        title="Founder", linkedin_url="", email_status=""),
        "On sells on Amazon.", "test",
        {"brand": "On", "categories": "", "recipe": "amz_on", "tier": "B"},
    )
    oe._record_person(
        engine,
        SimpleNamespace(email="a@plan.com", domain="plan.com", first_name="A", last_name="B",
                        title="Founder", linkedin_url="", email_status=""),
        "Plan upgraded.", "test",
        {"brand": "Plan", "categories": "", "recipe": "plan_upgrade", "tier": "B"},
    )
    sent = {}

    def record(leads, campaign_id, dry_run=False):
        sent[campaign_id] = [lead["email"] for lead in leads]
        return push.PushOutcome(attempted=len(leads), created=len(leads), dry_run=False)

    out = oe.push_pending(engine, "default-camp", dry_run=False, push=record)
    assert sent == {"table-camp": ["a@on.com"]}
    assert out["held"] == 1
    assert len(oe.unpushed(engine)) == 1


def test_push_pending_falls_back_when_the_table_is_empty(engine):
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM outbound_campaigns"))
    oe.ensure_tables(engine)
    oe._record_person(
        engine,
        SimpleNamespace(email="a@plan.com", domain="plan.com", first_name="A", last_name="B",
                        title="Founder", linkedin_url="", email_status=""),
        "Plan upgraded.", "test",
        {"brand": "Plan", "categories": "", "recipe": "plan_upgrade", "tier": "B"},
    )
    sent = {}

    def record(leads, campaign_id, dry_run=False):
        sent[campaign_id] = [lead["email"] for lead in leads]
        return push.PushOutcome(attempted=len(leads), created=len(leads), dry_run=False)

    oe.push_pending(engine, "default-camp", dry_run=False, push=record)
    assert sent == {push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]: ["a@plan.com"]}


def test_push_pending_falls_back_when_the_table_cannot_be_read(engine, monkeypatch):
    def explode(_engine):
        raise RuntimeError("database down")

    monkeypatch.setattr(campaigns, "load_routing", explode)
    oe.ensure_tables(engine)
    oe._record_person(
        engine,
        SimpleNamespace(email="a@off.com", domain="off.com", first_name="A", last_name="B",
                        title="Founder", linkedin_url="", email_status=""),
        "Not on Amazon.", "test",
        {"brand": "Off", "categories": "", "recipe": "amz_off", "tier": "B"},
    )
    sent = {}

    def record(leads, campaign_id, dry_run=False):
        sent[campaign_id] = [lead["email"] for lead in leads]
        return push.PushOutcome(attempted=len(leads), created=len(leads), dry_run=False)

    oe.push_pending(engine, "", dry_run=False, push=record)
    assert sent == {push.RECIPE_CAMPAIGN_MAP["amz_off"]: ["a@off.com"]}


def test_sourcing_records_the_recipe_and_skips_contacted_domains(engine):
    from sales_support_agent.services.outbound_memory import load_leads, record_leads

    record_leads(engine, [{"domain": "seen.com", "brand": "Seen", "recipe": "old"}])
    calls = []

    def fetch(api_key, **kwargs):
        calls.append(kwargs["extra_params"])
        filters = kwargs["extra_params"]
        if filters.get("f:scs:op") == "not":
            domains = [{"name": "off.com", "merchant_name": "Off", "country_code": "US"}]
        elif filters.get("f:scs") == "Amazon" and "f:plan" not in filters and "f:last_platform" not in filters:
            domains = [
                {"name": "on.com", "merchant_name": "On", "country_code": "US"},
                {"name": "seen.com", "merchant_name": "Seen", "country_code": "US"},
            ]
        elif filters.get("f:plan") == "Shopify Plus":
            domains = [{"name": "plus.com", "merchant_name": "Plus", "country_code": "US"}]
        elif "f:last_platform" in filters:
            domains = [{"name": "moved.com", "merchant_name": "Moved", "country_code": "US"}]
        else:
            domains = []
        return SimpleNamespace(domains=domains, next_cursor=None, error="")

    out = source.source_active_campaigns(
        engine, api_key="storeleads-key", now=NOW, fetch=fetch, company_room=300,
    )
    by_domain = {lead["domain"]: lead["recipe"] for lead in load_leads(engine)}
    assert by_domain["on.com"] == "amz_on"
    assert by_domain["off.com"] == "amz_off"
    assert by_domain["plus.com"] == "plan_upgrade"
    assert by_domain["moved.com"] == "replatform"
    assert by_domain["seen.com"] == "old"
    assert out["skipped_seen"] >= 1
    assert "seen.com" not in {lead["domain"] for lead in out["leads"]}
    sent_keys = set().union(*(call.keys() for call in calls))
    assert sent_keys <= campaigns.CONFIRMED_FILTERS
    assert any(call.get("f:scs:op") == "not" for call in calls)
    assert any(call.get("f:last_platform:op") == "or" for call in calls)
    assert all(call.get("f:ermin") == 1_000_000 for call in calls)


def test_sourcing_stops_at_the_company_and_credit_caps(engine):
    def fetch(api_key, **kwargs):
        return SimpleNamespace(
            domains=[{"name": f"{index}.com", "merchant_name": "Brand"} for index in range(10)],
            next_cursor=None, error="",
        )

    out = source.source_active_campaigns(
        engine, api_key="storeleads-key", now=NOW, fetch=fetch, company_room=1,
    )
    assert out["recorded"] == 1

    oe.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_enriched (domain, status, contacts_found, credits_used) "
            "VALUES ('spent.com', 'found', 1, 1100)"
        ))
    calls = []

    def fetch_again(api_key, **kwargs):
        calls.append(1)
        return SimpleNamespace(domains=[], next_cursor=None, error="")

    stopped = source.source_active_campaigns(
        engine, api_key="storeleads-key", now=NOW, fetch=fetch_again,
    )
    assert stopped["recorded"] == 0
    assert stopped["stopped_reason"] == "daily credit cap reached"
    assert calls == []


def test_judge_requires_150_contacted_and_does_not_change_volume():
    assert campaigns.judge_metrics(149, 5, 1, 1, 0) == (False, "insufficient")
    assert campaigns.judge_metrics(200, None, 1, 1, 0) == (False, "insufficient")
    assert campaigns.judge_metrics(200, 5, 1, 1, 0) == (True, "boost")
    assert campaigns.judge_metrics(200, 5, 1, 10, 0) == (True, "cut")
    assert campaigns.judge_metrics(200, 1, 0.1, 1, 0) == (True, "throttle")
    assert campaigns.judge_metrics(200, 2, 0.2, 1, 0.2) == (True, "hold")


def _daily(engine, rows):
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE instantly_campaign_daily (
                campaign_id TEXT, day TEXT, contacted INTEGER,
                bounced INTEGER, meetings INTEGER
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE abby_handled (
                email_id TEXT PRIMARY KEY, campaign_id TEXT, category TEXT,
                action TEXT, detail TEXT, created_at TEXT
            )
            """
        ))
        for row in rows:
            conn.execute(text(
                "INSERT INTO instantly_campaign_daily (campaign_id, day, contacted, bounced, meetings) "
                "VALUES (:campaign_id, :day, :contacted, :bounced, :meetings)"
            ), row)


def test_weekly_score_proposes_and_does_not_activate_or_change_caps(engine):
    campaigns.ensure_tables(engine)
    before = {
        row["recipe_key"]: (row["status"], row["daily_cap"])
        for row in campaigns.list_campaigns(engine)
    }
    amz = "a333e00e-bc3b-4abf-bcbf-4e0eb126090c"
    _daily(engine, [{
        "campaign_id": amz, "day": "2026-10-20",
        "contacted": 200, "bounced": 2, "meetings": 2,
    }])
    with engine.begin() as conn:
        for index in range(10):
            conn.execute(text(
                "INSERT INTO abby_handled (email_id, campaign_id, category, action, detail, created_at) "
                "VALUES (:id, :campaign, 'interested', 'reply', 'thanks', '2026-10-21 15:00:00')"
            ), {"id": f"email-{index}", "campaign": amz})
    monday = datetime(2026, 10, 26, 7, tzinfo=DENVER)
    result = score.run_weekly_score(engine, now=monday)
    assert result["ran"] is True
    assert result["blocked"] == []
    amz_score = next(item for item in result["scores"] if item["campaign_id"] == amz)
    assert amz_score["contacted"] == 200
    assert amz_score["positive_per_100"] == 5.0
    assert amz_score["meetings_per_100"] == 1.0
    assert amz_score["bounce_pct"] == 1.0
    assert amz_score["opt_out_pct"] == 0.0
    assert amz_score["judged"] == 1
    assert amz_score["action"] == "boost"
    assert result["proposed"]
    assert all(item["action"] == "boost" for item in result["proposed"])
    after = campaigns.list_campaigns(engine)
    for row in after:
        if row["recipe_key"] in before:
            assert (row["status"], row["daily_cap"]) == before[row["recipe_key"]]
        else:
            assert row["status"] == "proposed"
            assert row["daily_cap"] == 0
            assert set(campaigns.resolve_filters(row["storeleads_filters"], monday)) <= campaigns.CONFIRMED_FILTERS
    proposed_ids = {row["campaign_id"] for row in after if row["status"] == "proposed"}
    assert proposed_ids.isdisjoint(set(push.RECIPE_CAMPAIGN_MAP.values()))


def test_weekly_score_reads_the_instantly_daily_columns(engine):
    """The analytics sync stores date, positive, unsubscribed, meetings_booked."""

    campaigns.ensure_tables(engine)
    amz = "a333e00e-bc3b-4abf-bcbf-4e0eb126090c"
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE instantly_campaign_daily (
                date TEXT NOT NULL, campaign_id TEXT NOT NULL, contacted INTEGER,
                positive INTEGER, bounced INTEGER, unsubscribed INTEGER,
                meetings_booked INTEGER
            )
            """
        ))
        conn.execute(text(
            "INSERT INTO instantly_campaign_daily "
            "(date, campaign_id, contacted, positive, bounced, unsubscribed, meetings_booked) "
            "VALUES ('2026-10-20', :campaign, 200, 8, 2, 1, 2)"
        ), {"campaign": amz})
        conn.execute(text(
            """
            CREATE TABLE abby_handled (
                email_id TEXT PRIMARY KEY, campaign_id TEXT, category TEXT,
                action TEXT, detail TEXT, created_at TEXT
            )
            """
        ))
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, campaign_id, category, action, detail, created_at) "
            "VALUES ('one', :campaign, 'interested', 'reply', '', '2026-10-21 15:00:00')"
        ), {"campaign": amz})
    result = score.run_weekly_score(engine, now=datetime(2026, 10, 26, 7, tzinfo=DENVER))
    amz_score = next(item for item in result["scores"] if item["campaign_id"] == amz)
    assert amz_score["positive_per_100"] == 4.0
    assert amz_score["meetings_per_100"] == 1.0
    assert amz_score["bounce_pct"] == 1.0
    assert amz_score["opt_out_pct"] == 0.5
    assert "Abby classified positive=1" in amz_score["detail"]
    assert amz_score["action"] == "hold"


def test_weekly_score_does_not_judge_under_150_or_invent_a_missing_table(engine):
    campaigns.ensure_tables(engine)
    amz = "a333e00e-bc3b-4abf-bcbf-4e0eb126090c"
    _daily(engine, [{
        "campaign_id": amz, "day": "2026-10-20",
        "contacted": 149, "bounced": 0, "meetings": 0,
    }])
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, campaign_id, category, action, detail, created_at) "
            "VALUES ('one', :campaign, 'interested', 'reply', '', '2026-10-21 15:00:00')"
        ), {"campaign": amz})
    result = score.run_weekly_score(engine, now=datetime(2026, 10, 26, 7, tzinfo=DENVER))
    amz_score = next(item for item in result["scores"] if item["campaign_id"] == amz)
    assert amz_score["action"] == "insufficient"
    assert amz_score["judged"] == 0
    assert result["proposed"] == []

    bare = create_engine("sqlite://")
    missing = score.run_weekly_score(bare, now=datetime(2026, 10, 26, 7, tzinfo=DENVER))
    assert missing["ran"] is False
    assert "instantly_campaign_daily" in missing["blocked"]
    assert missing["scores"] == []


def test_weekly_score_notes_a_missing_abby_campaign_column(engine):
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE instantly_campaign_daily (
                campaign_id TEXT, day TEXT, contacted INTEGER, bounced INTEGER, meetings INTEGER
            )
            """
        ))
        conn.execute(text(
            "INSERT INTO instantly_campaign_daily (campaign_id, day, contacted, bounced, meetings) "
            "VALUES ('a333e00e-bc3b-4abf-bcbf-4e0eb126090c', '2026-10-20', 200, 2, 2)"
        ))
        conn.execute(text(
            "CREATE TABLE abby_handled (email_id TEXT PRIMARY KEY, category TEXT, created_at TEXT)"
        ))
    result = score.run_weekly_score(engine, now=datetime(2026, 10, 26, 7, tzinfo=DENVER))
    amz_score = next(item for item in result["scores"] if item["campaign_id"].startswith("a333"))
    assert "abby_handled.campaign_id" in amz_score["detail"]
    assert amz_score["action"] == "insufficient"
    assert result["proposed"] == []


def test_score_route_waits_for_monday_morning_denver():
    from sales_support_agent.api import outbound_campaign_score_job as job

    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.include_router(job.router)
    client = TestClient(app)

    class Tuesday:
        @classmethod
        def now(cls, tz):
            return datetime(2026, 10, 27, 7, tzinfo=tz)

    class MondayAfternoon:
        @classmethod
        def now(cls, tz):
            return datetime(2026, 10, 26, 15, tzinfo=tz)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(job, "datetime", Tuesday)
    response = client.post(
        "/api/jobs/outbound-campaign-score/at-1300",
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "skipped"
    monkey.setattr(job, "datetime", MondayAfternoon)
    response = client.post(
        "/api/jobs/outbound-campaign-score/run",
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert response.json()["status"] == "skipped"
    monkey.undo()


def test_migration_seeds_the_five_campaigns_and_locks_down_the_role():
    sql = (
        Path(__file__).parents[1] / "sales_support_agent/migrations/20261030_outbound_campaigns.sql"
    ).read_text(encoding="utf-8")
    for row in campaigns.SEED_ROWS:
        assert row["campaign_id"] in sql
    assert "replatformed" in sql
    assert "f:scs:op" in sql
    assert "Shopify Plus" in sql
    assert "woocommerce,magento,bigcommerce" in sql
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE outbound_campaigns TO agent_app" in sql
    assert "CREATE POLICY outbound_campaigns_agent_app" in sql
