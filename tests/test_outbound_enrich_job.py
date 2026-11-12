"""Daily contact-finding job: caps, switches, weekend, Slack summary."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

import outbound_campaigns as campaigns
import outbound_contacts as oc
import outbound_enrich as oe
import outbound_instantly_push as push
from sales_support_agent.api import outbound_enrich_job as job
from sales_support_agent.services import outbound_memory as memory

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


def leads(n, recipe="amz_on"):
    return [{"domain": f"d{i}.com", "brand": f"D{i}", "categories": "/Apparel", "recipe": recipe} for i in range(n)]


def finder(domain, *, api_key, max_contacts=2, **kw):
    return oc.FindResult(domain=domain, credits_used=2,
                         contacts=[oc.Contact(domain=domain, first_name="A", last_name="B", email=f"a@{domain}", title="Founder")])


def write(lead, contact=None):
    return f"{lead['brand']} stood out. Is Amazon a real channel for you yet?", "ai"


def ok_push(payload, cid, dry_run=False, **k):
    return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)


def run(engine, n=10, **kw):
    posts = []
    rows = kw.pop("lead_rows", None)
    out = job.run_enrich_job(now=kw.pop("now", WEEKDAY), engine=engine, leads=rows if rows is not None else leads(n),
                             find=kw.pop("find", finder), write=write, push=kw.pop("push", ok_push),
                             post_slack=posts.append,
                             in_instantly=kw.pop("in_instantly", lambda d: False), **kw)
    return out, posts


def test_runs_pushes_to_the_v2_recipe_and_posts_slack(engine):
    seen = {}

    def record(payload, cid, dry_run=False, **k):
        seen["campaign_id"] = cid
        return ok_push(payload, cid, dry_run=dry_run)

    out, posts = run(engine, push=record)
    assert out["ran"] and out["pushed"] == 10
    assert out["campaign_id"] == ""
    assert out["default_campaign_ignored"] is True
    assert seen["campaign_id"] == push.RECIPE_CAMPAIGN_MAP["amz_on"]
    text = job.summary_text(out)
    assert posts == []
    assert CAMPAIGN not in text
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in text
    assert push.LIVE_CAMPAIGN_ID not in text
    assert "Added to Instantly: 10" in text


def test_missing_default_still_pushes_a_mapped_recipe(engine, monkeypatch):
    monkeypatch.delenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID")
    seen = []

    def record(payload, cid, dry_run=False, **k):
        seen.append(cid)
        return ok_push(payload, cid, dry_run=dry_run)

    posts = []
    out = job.run_enrich_job(
        now=WEEKDAY, engine=engine,
        leads=[{"domain": "plus.com", "brand": "Plus", "categories": "/Apparel", "recipe": "plan_upgrade"}],
        find=finder, write=write, push=record, post_slack=posts.append,
        in_instantly=lambda d: False,
    )
    assert out["ran"] and out["pushed"] == 1 and out["campaign_missing"] is False
    assert seen == [push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]]
    text = job.summary_text(out)
    assert posts == []
    assert push.MISSING_CAMPAIGN_MESSAGE not in text
    assert push.LIVE_CAMPAIGN_ID not in text
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in text


def test_stale_trial_default_does_not_receive_filler_leads(engine, monkeypatch, caplog):
    monkeypatch.setenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", push.LEGACY_TRIAL_CAMPAIGN_ID)
    called = []
    with caplog.at_level("ERROR"):
        out, posts = run(
            engine,
            lead_rows=[{"domain": f"d{i}.com", "brand": f"D{i}", "categories": "/Apparel", "recipe": "icp_baseline"} for i in range(10)],
            push=lambda *a, **k: called.append(a[1] if len(a) > 1 else k.get("campaign_id")),
        )
    assert out["ran"] and out["pushed"] == 0 and out["campaign_missing"] is False
    assert out["campaign_id"] == ""
    assert called == []
    text = job.summary_text(out)
    assert posts == []
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in text
    assert "Queued, no matching v2 campaign: 10" in text
    assert "OUTBOUND_INSTANTLY_CAMPAIGN_ID is unset" not in caplog.text
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
    assert posts == [] and "switch is off" in job.summary_text(out)
    assert len(oe.unpushed(engine)) == 10


def test_instantly_failure_reported_and_contacts_kept(engine):
    bad = lambda payload, cid, dry_run=False, **k: push.PushOutcome(  # noqa: E731
        attempted=len(payload), dry_run=False, errors=["Instantly returned 500"])
    out, posts = run(engine, push=bad)
    assert out["pushed"] == 0 and out["push_problem"]
    assert posts == [] and "Instantly problem" in job.summary_text(out)
    assert len(oe.unpushed(engine)) == 10


def test_no_key_does_nothing(engine, monkeypatch):
    monkeypatch.delenv("PROSPEO_API_KEY")
    out, posts = run(engine)
    assert not out["ran"] and posts == []


def test_enrich_run_paths_each_accept_get_like_abby():
    """Every cron path has one GET route and one POST route, not two GETs."""

    wanted = {
        "/api/jobs/outbound-enrich/run",
        "/api/jobs/outbound-enrich/at-1415",
        "/api/jobs/outbound-enrich/at-1515",
        "/api/jobs/outbound-enrich/at-1615",
        "/api/jobs/outbound-enrich/at-1715",
    }
    found = [
        route for route in job.router.routes
        if getattr(route, "path", "") in wanted
    ]
    by_path: dict[str, list] = {}
    for route in found:
        by_path.setdefault(route.path, []).append(route)
    assert set(by_path) == wanted
    for path, routes in by_path.items():
        assert len(routes) == 2, path
        assert sum("GET" in route.methods for route in routes) == 1
        assert sum("POST" in route.methods for route in routes) == 1
        assert len({route.operation_id for route in routes}) == 2


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


def _cron_client(monkeypatch):
    import warnings
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    monkeypatch.setenv("CRON_SECRET", "s" * 16)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.include_router(job.router)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        app.openapi()
    duplicates = [str(item.message) for item in caught if "Duplicate Operation ID" in str(item.message)]
    assert duplicates == []
    return TestClient(app)


def test_route_requires_scheduler_auth(monkeypatch):
    called = []
    monkeypatch.setattr(
        job, "maybe_run_enrich_from_abby", lambda: called.append(1) or {"ran": False},
    )
    c = _cron_client(monkeypatch)
    secret = "s" * 16
    bearer = {"Authorization": "Bearer " + secret}
    assert c.get("/api/jobs/outbound-enrich/run").status_code == 401
    assert c.get("/api/jobs/outbound-enrich/run", headers={"Authorization": "Bearer wrong"}).status_code == 401
    for path in (
        "/api/jobs/outbound-enrich/run",
        "/api/jobs/outbound-enrich/at-1415",
        "/api/jobs/outbound-enrich/at-1515",
        "/api/jobs/outbound-enrich/at-1615",
        "/api/jobs/outbound-enrich/at-1715",
    ):
        response = c.get(path, headers=bearer)
        assert response.status_code == 200, path
        posted = c.post(path, headers=bearer)
        assert posted.status_code == 200, path
    assert len(called) == 10
    assert c.get("/api/jobs/outbound-enrich/missed-run").status_code == 401
    watched = []
    monkeypatch.setattr(job, "run_missed_enrich_check", lambda: watched.append(1) or {"alerted": False})
    missed = c.get(
        "/api/jobs/outbound-enrich/missed-run",
        headers=bearer,
    )
    assert missed.status_code == 200 and watched == [1]


def test_enabled_jobs_allowlist_does_not_block_enrich(monkeypatch):
    """VERCEL_CRON_ENABLED_JOBS is not a gate. Any value still reaches the job."""

    monkeypatch.setenv("VERCEL_CRON_ENABLED_JOBS", "abby")
    called = []
    monkeypatch.setattr(
        job, "maybe_run_enrich_from_abby", lambda: called.append(1) or {"ran": False},
    )
    response = _cron_client(monkeypatch).get(
        "/api/jobs/outbound-enrich/at-1415",
        headers={"Authorization": "Bearer " + "s" * 16},
    )
    assert response.status_code == 200 and called == [1]


def test_trailing_slash_redirects_the_same_way_as_abby(monkeypatch):
    """Vercel Cron does not follow redirects. The cron path is the one without a slash."""

    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from sales_support_agent.api.abby_job import router as abby_router
    from sales_support_agent.api.outbound_jobs import _require_scheduler_auth
    from sales_support_agent.api import abby_job

    assert abby_job._require_scheduler_auth is _require_scheduler_auth
    assert job._require_scheduler_auth is _require_scheduler_auth
    monkeypatch.setenv("CRON_SECRET", "s" * 16)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(job.router)
    app.include_router(abby_router)
    client = TestClient(app, follow_redirects=False)
    enrich = client.get("/api/jobs/outbound-enrich/run/")
    abby = client.get("/api/jobs/abby/run/")
    assert enrich.status_code == abby.status_code == 307
    assert enrich.headers["location"].endswith("/api/jobs/outbound-enrich/run")
    assert abby.headers["location"].endswith("/api/jobs/abby/run")
    direct = client.get("/api/jobs/outbound-enrich/run")
    assert direct.status_code == 401


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
    assert posts == [] and "already in Instantly: 2" in job.summary_text(out)
    # and they are remembered, so the next run does not re-check them
    again, _ = run(engine, n=6, in_instantly=lambda d: (_ for _ in ()).throw(AssertionError(d)))
    assert again["report"]["skipped_already_enriched"] == 6


def test_failed_instantly_check_never_contacts(engine):
    out, _ = run(engine, n=8, in_instantly=lambda d: None)
    rep = out["report"]
    assert rep["companies_tried"] == 0 and out["pushed"] == 0
    assert "Could not check Instantly" in rep["stopped_reason"]


def _stamp(engine, domain, when):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE outbound_contacted_domains SET first_seen_at = :t WHERE domain = :d"),
            {"t": when, "d": domain},
        )


def _store_recipe(engine, recipe, count, day):
    slug = {
        "amz_on": "amzon",
        "amz_off": "amzoff",
        "plan_upgrade": "planup",
        "replatform": "replat",
        "replatformed": "replated",
        "icp_baseline": "baseline",
        "social_surge": "social",
    }[recipe]
    rows = [
        {"domain": f"{slug}{index}.com", "brand": slug, "recipe": recipe, "categories": "Apparel"}
        for index in range(count)
    ]
    memory.record_leads(engine, rows)
    for index, row in enumerate(rows):
        _stamp(engine, row["domain"], f"{day} 08:{index:02d}:00")
    return [row["domain"] for row in rows]


def _cap(engine, recipe, cap):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE outbound_campaigns SET daily_cap = :cap WHERE recipe_key = :recipe"),
            {"cap": cap, "recipe": recipe},
        )


def _run_loaded(engine, **kw):
    posts = []
    out = job.run_enrich_job(
        now=kw.pop("now", WEEKDAY), engine=engine,
        find=kw.pop("find", finder), write=write, push=kw.pop("push", ok_push),
        post_slack=posts.append, in_instantly=kw.pop("in_instantly", lambda d: False),
        **kw,
    )
    return out, posts


def test_enrich_job_tries_campaign_recipes_before_newer_baseline(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_PER_RUN", "5")
    campaigns.ensure_tables(engine)
    for recipe in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        _cap(engine, recipe, 1)
        _store_recipe(engine, recipe, 3, "2026-10-01")
    _store_recipe(engine, "social_surge", 8, "2026-11-04")
    baseline = _store_recipe(engine, "icp_baseline", 8, "2026-11-05")
    tried = []
    sent = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        tried.append(domain)
        return finder(domain, api_key=api_key, max_contacts=max_contacts)

    def record(payload, cid, dry_run=False, **k):
        sent.append((cid, [item["email"] for item in payload]))
        return ok_push(payload, cid, dry_run=dry_run)

    out, _posts = _run_loaded(engine, find=find, push=record)
    assert out["ran"]
    assert tried == [
        "amzon0.com", "amzoff0.com", "planup0.com", "replat0.com", baseline[-1],
    ]
    by_campaign = {cid: emails for cid, emails in sent}
    assert by_campaign[push.RECIPE_CAMPAIGN_MAP["amz_on"]] == ["a@amzon0.com"]
    assert by_campaign[push.RECIPE_CAMPAIGN_MAP["amz_off"]] == ["a@amzoff0.com"]
    assert by_campaign[push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]] == ["a@planup0.com"]
    assert by_campaign[push.RECIPE_CAMPAIGN_MAP["replatform"]] == ["a@replat0.com"]
    assert CAMPAIGN not in by_campaign
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in by_campaign
    sent_emails = [email for emails in by_campaign.values() for email in emails]
    assert f"a@{baseline[-1]}" not in sent_emails
    assert set(by_campaign) == {
        push.RECIPE_CAMPAIGN_MAP["amz_on"],
        push.RECIPE_CAMPAIGN_MAP["amz_off"],
        push.RECIPE_CAMPAIGN_MAP["plan_upgrade"],
        push.RECIPE_CAMPAIGN_MAP["replatform"],
    }


def test_enrich_job_reaches_signal_brands_outside_the_newest_page(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_PER_RUN", "2")
    campaigns.ensure_tables(engine)
    for recipe in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        _cap(engine, recipe, 100)
    _store_recipe(engine, "amz_on", 2, "2026-10-01")
    _store_recipe(engine, "replatform", 2, "2026-10-01")
    _store_recipe(engine, "icp_baseline", 6, "2026-11-05")
    _store_recipe(engine, "social_surge", 6, "2026-11-04")

    original = memory.load_leads

    def only_newer_recipes(engine, limit=500):
        rows = original(engine, limit=limit)
        return [row for row in rows if row.get("recipe") in {"icp_baseline", "social_surge"}]

    monkeypatch.setattr(memory, "load_leads", only_newer_recipes)
    tried = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        tried.append(domain)
        return finder(domain, api_key=api_key, max_contacts=max_contacts)

    out, _posts = _run_loaded(engine, find=find)
    assert out["ran"]
    assert tried == ["amzon0.com", "replat0.com"]


def test_campaign_cap_already_used_today_lets_baseline_follow(engine, monkeypatch):
    monkeypatch.setenv("OUTBOUND_ENRICH_PER_RUN", "3")
    campaigns.ensure_tables(engine)
    for recipe in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        _cap(engine, recipe, 0)
    _cap(engine, "amz_on", 2)
    domains = _store_recipe(engine, "amz_on", 4, "2026-10-01")
    baseline = _store_recipe(engine, "icp_baseline", 4, "2026-11-05")
    oe.ensure_tables(engine)
    oe._record_domain(engine, domains[0], "found", 1, 1, "already enriched")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE outbound_enriched SET enriched_at = '2026-10-22 16:00:00' WHERE domain = :d"),
            {"d": domains[0]},
        )
    tried = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        tried.append(domain)
        return finder(domain, api_key=api_key, max_contacts=max_contacts)

    out, _posts = _run_loaded(engine, find=find)
    assert out["ran"]
    assert tried == ["amzon1.com", baseline[-1], baseline[-2]]


def test_replatformed_alias_counts_against_the_replatform_cap(engine):
    campaigns.ensure_tables(engine)
    for recipe in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        _cap(engine, recipe, 0)
    _cap(engine, "replatform", 1)
    memory.record_leads(engine, [
        {"domain": "oldmove.com", "brand": "Old", "recipe": "replatformed"},
        {"domain": "newmove.com", "brand": "New", "recipe": "replatform"},
        {"domain": "base.com", "brand": "Base", "recipe": "icp_baseline"},
    ])
    _stamp(engine, "oldmove.com", "2026-10-01 08:00:00")
    _stamp(engine, "newmove.com", "2026-10-01 09:00:00")
    _stamp(engine, "base.com", "2026-11-05 12:00:00")
    oe.ensure_tables(engine)
    oe._record_domain(engine, "oldmove.com", "found", 1, 1, "done")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE outbound_enriched SET enriched_at = '2026-10-22 16:00:00' WHERE domain = 'oldmove.com'")
        )
    ordered = oe.order_leads_for_enrich(
        engine, memory.load_leads(engine, limit=1000),
        since_utc=datetime(2026, 10, 22, 6, 0, 0),
    )
    assert [lead["domain"] for lead in ordered][0] == "base.com"
    assert "newmove.com" in [lead["domain"] for lead in ordered]


def test_cut_campaign_stays_behind_baseline(engine):
    campaigns.ensure_tables(engine)
    for recipe in ("amz_on", "amz_off", "plan_upgrade", "replatform"):
        _cap(engine, recipe, 100)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE outbound_campaigns SET status = 'cut' WHERE recipe_key = 'plan_upgrade'"
        ))
    _store_recipe(engine, "plan_upgrade", 2, "2026-10-01")
    baseline = _store_recipe(engine, "icp_baseline", 1, "2026-11-05")
    ordered = oe.order_leads_for_enrich(
        engine, memory.load_leads(engine, limit=1000),
        since_utc=datetime(2026, 10, 22, 6, 0, 0),
    )
    assert ordered[0]["domain"] == baseline[0]


def test_off_unless_explicitly_on(engine, monkeypatch):
    monkeypatch.delenv("OUTBOUND_AUTO_ENRICH")
    out, posts = run(engine)
    assert not out["ran"] and posts == []
