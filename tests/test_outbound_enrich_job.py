"""Daily contact-finding job: caps, switches, weekend, Slack summary."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine

import outbound_contacts as oc
import outbound_enrich as oe
import outbound_instantly_push as push
from sales_support_agent.api import outbound_enrich_job as job

TZ = ZoneInfo("America/Denver")
WEEKDAY = datetime(2026, 10, 22, 8, 15, tzinfo=TZ)   # Thursday
SATURDAY = datetime(2026, 10, 24, 8, 15, tzinfo=TZ)


@pytest.fixture
def engine():
    return create_engine("sqlite://")


CAMPAIGN = "11111111-1111-4111-8111-111111111111"


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "true")
    monkeypatch.setenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", CAMPAIGN)
    for n in ("OUTBOUND_ENRICH_DAILY_COMPANIES",
              "OUTBOUND_ENRICH_DAILY_CREDITS", "OUTBOUND_ENRICH_PER_RUN",
              "OUTBOUND_ENRICH_MAX_CONTACTS"):
        monkeypatch.delenv(n, raising=False)


def leads(n):
    return [{"domain": f"d{i}.com", "brand": f"D{i}", "categories": "/Apparel"} for i in range(n)]


def finder(domain, *, api_key, max_contacts=2, **kw):
    return oc.FindResult(domain=domain, credits_used=2,
                         contacts=[oc.Contact(domain=domain, first_name="A", last_name="B", email=f"a@{domain}", title="Founder")])


def write(lead, contact=None):
    return f"{lead['brand']} stood out. Is Amazon a real channel for you yet?", "ai"


def ok_push(payload, cid, dry_run=False, **k):
    return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)


def run(engine, n=10, **kw):
    posts = []
    out = job.run_enrich_job(now=kw.pop("now", WEEKDAY), engine=engine, leads=leads(n),
                             find=kw.pop("find", finder), write=write, push=kw.pop("push", ok_push),
                             post_slack=posts.append,
                             in_instantly=kw.pop("in_instantly", lambda d: False), **kw)
    return out, posts


def test_runs_pushes_to_configured_campaign_and_posts_slack(engine):
    seen = {}

    def record(payload, cid, dry_run=False, **k):
        seen["campaign_id"] = cid
        return ok_push(payload, cid, dry_run=dry_run)

    out, posts = run(engine, push=record)
    assert out["ran"] and out["pushed"] == 10
    assert out["campaign_id"] == CAMPAIGN
    assert seen["campaign_id"] == CAMPAIGN
    assert push.LIVE_CAMPAIGN_ID not in posts[0]
    assert len(posts) == 1 and "Added to Instantly: 10" in posts[0]


def test_missing_campaign_skips_push_logs_and_alerts(engine, monkeypatch, caplog):
    monkeypatch.delenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID")
    called = []
    with caplog.at_level("ERROR"):
        out, posts = run(engine, push=lambda *a, **k: called.append(1))
    assert out["ran"] and out["pushed"] == 0 and out["campaign_missing"]
    assert out["campaign_id"] == ""
    assert called == []
    assert push.LIVE_CAMPAIGN_ID not in "".join(posts)
    assert any(push.MISSING_CAMPAIGN_MESSAGE in post for post in posts)
    assert "OUTBOUND_INSTANTLY_CAMPAIGN_ID is unset" in caplog.text
    assert len(oe.unpushed(engine)) == 10


def test_max_contacts_comes_from_the_env(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_MAX_CONTACTS", "1")
    seen = {}

    def find(domain, *, api_key, max_contacts=2, **kw):
        seen["max"] = max_contacts
        return finder(domain, api_key=api_key, max_contacts=max_contacts)

    run(engine, n=1, find=find)
    assert seen["max"] == 1


def test_per_run_cap(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_PER_RUN", "4")
    out, _ = run(engine, n=10)
    assert out["report"]["companies_tried"] == 4


def test_daily_company_cap_across_runs(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_DAILY_COMPANIES", "6")
    monkeypatch.setenv("OUTBOUND_ENRICH_PER_RUN", "4")
    first, _ = run(engine, n=10)
    second, _ = run(engine, n=10)
    third, posts = run(engine, n=10)
    assert first["report"]["companies_tried"] == 4
    assert second["report"]["companies_tried"] == 2
    assert not third["ran"] and third["reason"] == "daily cap reached" and posts == []


def test_daily_credit_cap(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_DAILY_CREDITS", "6")
    out, _ = run(engine, n=10)
    assert out["report"]["credits_used"] == 6


def test_defaults_are_the_approved_caps():
    assert job.DEFAULT_DAILY_COMPANIES == 120
    assert job.DEFAULT_DAILY_CREDITS == 250
    assert job.DEFAULT_PER_RUN == 40
    assert job.DEFAULT_MAX_CONTACTS == 2


def test_kill_switch(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "false")
    out, posts = run(engine)
    assert not out["ran"] and posts == []


def test_weekend_skips(engine):
    out, _ = run(engine, now=SATURDAY)
    assert not out["ran"] and out["reason"] == "weekend"


def test_push_switch_off_finds_but_does_not_add(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "false")
    called = []
    out, posts = run(engine, push=lambda *a, **k: called.append(1))
    assert out["ran"] and out["pushed"] == 0 and not called
    assert "switch is off" in posts[0]
    assert len(oe.unpushed(engine)) == 10


def test_instantly_failure_reported_and_contacts_kept(engine):
    bad = lambda payload, cid, dry_run=False, **k: push.PushOutcome(  # noqa: E731
        attempted=len(payload), dry_run=False, errors=["Instantly returned 500"])
    out, posts = run(engine, push=bad)
    assert out["pushed"] == 0 and out["push_problem"]
    assert "Instantly problem" in posts[0]
    assert len(oe.unpushed(engine)) == 10


def test_no_key_does_nothing(engine, monkeypatch):
    monkeypatch.delenv("PROSPEO_API_KEY")
    out, posts = run(engine)
    assert not out["ran"] and posts == []


def test_enrich_run_route_is_one_get_and_one_post():
    """GET and POST are separate routes so they do not share one operation id."""

    matches = [
        route for route in job.router.routes
        if getattr(route, "path", "") == "/api/jobs/outbound-enrich/run"
    ]
    gets = [route for route in matches if "GET" in route.methods]
    posts = [route for route in matches if "POST" in route.methods]
    assert len(gets) == 1 and len(posts) == 1
    assert gets[0] is not posts[0]


def test_missed_enrich_alerts_when_today_is_empty(engine):
    posts = []
    out = job.run_missed_enrich_check(now=WEEKDAY, engine=engine, post_slack=posts.append)
    assert out["alerted"] is True and out["companies"] == 0
    assert posts and "outbound_enriched" in posts[0]


def test_missed_enrich_is_quiet_when_a_row_exists(engine):
    from sqlalchemy import text
    oe.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_enriched (domain, status, contacts_found, credits_used, reason, enriched_at) "
            "VALUES ('a.com', 'found', 1, 1, '', :t)"
        ), {"t": job._start_of_day_utc(WEEKDAY)})
    posts = []
    out = job.run_missed_enrich_check(now=WEEKDAY, engine=engine, post_slack=posts.append)
    assert out["alerted"] is False and out["companies"] == 1 and posts == []


def test_missed_enrich_skips_the_weekend(engine):
    posts = []
    out = job.run_missed_enrich_check(now=SATURDAY, engine=engine, post_slack=posts.append)
    assert out["alerted"] is False and out["reason"] == "weekend" and posts == []


def test_route_requires_scheduler_auth(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from types import SimpleNamespace
    monkeypatch.setenv("CRON_SECRET", "s" * 16)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(job.router)
    called = []
    monkeypatch.setattr(job, "run_enrich_job", lambda: called.append(1) or {"ran": False})
    c = TestClient(app)
    assert c.get("/api/jobs/outbound-enrich/run").status_code == 401
    r = c.get("/api/jobs/outbound-enrich/run", headers={"Authorization": "Bearer " + "s" * 16})
    assert r.status_code == 200 and called == [1]
    assert c.get("/api/jobs/outbound-enrich/missed-run").status_code == 401
    watched = []
    monkeypatch.setattr(job, "run_missed_enrich_check", lambda: watched.append(1) or {"alerted": False})
    missed = c.get(
        "/api/jobs/outbound-enrich/missed-run",
        headers={"Authorization": "Bearer " + "s" * 16},
    )
    assert missed.status_code == 200 and watched == [1]


def test_deadline_stops_run(engine):
    ticks = iter([0, 0, 100, 100, 100, 100])
    rep = oe.run_enrichment(engine, leads(5), api_key="k", find=finder, write=write,
                            deadline=50, clock=lambda: next(ticks))
    assert rep.companies_tried == 2 and "Time budget" in rep.stopped_reason


def test_companies_already_in_instantly_are_skipped_and_cost_nothing(engine):
    out, posts = run(engine, n=6, in_instantly=lambda d: d in {"d0.com", "d1.com"})
    rep = out["report"]
    assert rep["skipped_in_instantly"] == 2 and rep["companies_tried"] == 4
    assert rep["credits_used"] == 8 and out["pushed"] == 4
    assert "already in Instantly: 2" in posts[0]
    # and they are remembered, so the next run does not re-check them
    again, _ = run(engine, n=6, in_instantly=lambda d: (_ for _ in ()).throw(AssertionError(d)))
    assert again["report"]["skipped_already_enriched"] == 6


def test_failed_instantly_check_never_contacts(engine):
    out, _ = run(engine, n=8, in_instantly=lambda d: None)
    rep = out["report"]
    assert rep["companies_tried"] == 0 and out["pushed"] == 0
    assert "Could not check Instantly" in rep["stopped_reason"]


def test_off_unless_explicitly_on(engine, monkeypatch):
    monkeypatch.delenv("OUTBOUND_AUTO_ENRICH")
    out, posts = run(engine)
    assert not out["ran"] and posts == []
