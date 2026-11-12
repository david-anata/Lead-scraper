"""Fixes from the contact-finding and OOO follow-up audits."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import create_engine, text

import abby_runner as runner
import outbound_contacts as oc
import outbound_enrich as oe
from sales_support_agent.services.job_lease import ensure_job_lease_schema

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20261112170000_finish_invalid_filter_enrichment.sql"
NOW = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)


def _engine():
    engine = create_engine("sqlite://")
    oe.ensure_tables(engine)
    return engine


def _no_sleep(_seconds):
    return None


def test_platform_subdomain_is_not_sent_to_prospeo():
    calls = []

    def post(url, *, key, body, timeout=25):
        calls.append(url)
        return 200, {"results": []}

    result = oc.find_contacts(
        "tattebakery.myshopify.com", api_key="k" * 20, post=post, sleep=_no_sleep,
    )
    assert calls == []
    assert result.credits_used == 0
    assert result.reason == "Platform subdomain is not searchable."
    assert not oc.is_platform_subdomain("petlibro.com")
    assert oc.is_platform_subdomain("foo.shopifypreview.com")


def test_platform_subdomain_is_recorded_finished_without_a_search(engine=None):
    engine = _engine()
    calls = []

    def find(domain, *, api_key, max_contacts=2, **kwargs):
        calls.append(domain)
        raise AssertionError(domain)

    report = oe.run_enrichment(
        engine,
        [{"domain": "tattebakery.myshopify.com", "brand": "Tatte", "recipe": "amz_off", "country": "US"}],
        api_key="k", find=find, write=lambda *_a, **_k: ("line", "ai"),
        already_in_instantly=lambda _d: False,
    )
    assert calls == []
    assert report.companies_tried == 0 and report.skipped_platform == 1
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, reason FROM outbound_enriched WHERE domain = 'tattebakery.myshopify.com'"
        )).one()
    assert row[0] == "platform"
    again = oe.run_enrichment(
        engine,
        [{"domain": "tattebakery.myshopify.com", "brand": "Tatte", "recipe": "amz_off", "country": "US"}],
        api_key="k", find=find, write=lambda *_a, **_k: ("line", "ai"),
        already_in_instantly=lambda _d: False,
    )
    assert again.skipped_already_enriched == 1 and again.skipped_platform == 0


def test_no_results_retries_once_without_the_title_filter_and_charges_one_search():
    calls = []

    def post(url, *, key, body, timeout=25):
        if url == oc.PROSPEO_SEARCH_URL:
            calls.append(body["filters"])
        if url != oc.PROSPEO_SEARCH_URL:
            pid = body["data"]["person_id"]
            if pid != "p1":
                return 400, {"error": True, "error_code": "NO_MATCH"}
            return 200, {
                "person": {
                    "person_id": "p1", "first_name": "Ann", "last_name": "Lee",
                    "current_job_title": "Founder",
                    "email": {"email": "ann@brand.com", "status": "VERIFIED"},
                },
            }
        if "person_job_title" in body["filters"]:
            return 400, {"error": True, "error_code": "NO_RESULTS"}
        return 200, {"results": [
            {"person": {"person_id": "p1", "current_job_title": "Founder"}},
            {"person": {"person_id": "p2", "current_job_title": "Accountant"}},
        ]}

    result = oc.find_contacts("brand.com", api_key="k" * 20, post=post, sleep=_no_sleep)
    assert "person_job_title" in calls[0]
    assert "person_job_title" not in calls[1]
    assert result.credits_used == 2
    assert [item.email for item in result.contacts] == ["ann@brand.com"]


def test_second_no_results_costs_nothing():
    def post(url, *, key, body, timeout=25):
        return 400, {"error": True, "error_code": "NO_RESULTS"}

    result = oc.find_contacts("brand.com", api_key="k" * 20, post=post, sleep=_no_sleep)
    assert result.credits_used == 0
    assert "NO_RESULTS" in result.reason


def test_title_retry_stays_inside_the_credit_budget():
    calls = []

    def post(url, *, key, body, timeout=25):
        calls.append(url)
        if "person_job_title" in body.get("filters", {}):
            return 400, {"error": True, "error_code": "NO_RESULTS"}
        return 200, {"results": [
            {"person": {"person_id": "p1", "current_job_title": "Founder"}},
        ]}

    blocked = oc.find_contacts(
        "brand.com", api_key="k" * 20, post=post, sleep=_no_sleep, credit_budget=0,
    )
    assert blocked.credits_used == 0 and len(calls) == 1
    calls.clear()
    spent = oc.find_contacts(
        "brand.com", api_key="k" * 20, post=post, sleep=_no_sleep, credit_budget=1,
    )
    assert spent.credits_used == 1
    assert calls == [oc.PROSPEO_SEARCH_URL, oc.PROSPEO_SEARCH_URL]
    assert spent.contacts == []
    assert spent.reason == "Credit budget reached."


def test_conflict_update_refreshes_enriched_at():
    engine = _engine()
    oe._record_domain(engine, "old.com", "none", 0, 0, "search returned NO_RESULTS")
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE outbound_enriched SET enriched_at = '2026-10-01 00:00:00' WHERE domain = 'old.com'"
        ))
    oe._record_domain(engine, "old.com", "none", 0, 1, "No decision-maker found at this domain.")
    with engine.connect() as conn:
        stamp, credits = conn.execute(text(
            "SELECT enriched_at, credits_used FROM outbound_enriched WHERE domain = 'old.com'"
        )).one()
    assert str(stamp).startswith("2026-10-01") is False
    assert credits == 1


def test_amz_off_rotates_with_amz_on():
    import outbound_campaigns as campaigns
    from sales_support_agent.services import outbound_memory as memory

    engine = _engine()
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        for recipe, cap in (("amz_on", 2), ("amz_off", 2), ("plan_upgrade", 2)):
            conn.execute(text(
                "UPDATE outbound_campaigns SET status = 'active', daily_cap = :cap "
                "WHERE recipe_key = :recipe"
            ), {"cap": cap, "recipe": recipe})
    memory.record_leads(engine, [
        {"domain": "on-a.com", "brand": "On A", "recipe": "amz_on"},
        {"domain": "on-b.com", "brand": "On B", "recipe": "amz_on"},
        {"domain": "off-a.com", "brand": "Off A", "recipe": "amz_off"},
        {"domain": "off-b.com", "brand": "Off B", "recipe": "amz_off"},
        {"domain": "plan.com", "brand": "Plan", "recipe": "plan_upgrade"},
    ])
    ordered = oe.order_leads_for_enrich(
        engine, [], since_utc=datetime(2026, 11, 12, 7, 0),
    )
    assert [row["domain"] for row in ordered[:5]] == [
        "on-a.com", "off-a.com", "plan.com", "on-b.com", "off-b.com",
    ]


def test_invalid_filter_migration_keeps_history_and_does_not_delete():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "DELETE" not in sql.upper()
    assert "SET status = 'invalid_filters'" in sql
    assert "amz_off" in sql and "plan_upgrade" in sql and "replatformed" in sql
    assert "enriched_at" not in sql.split("SET", 1)[1].split("WHERE", 1)[0]


class _TimeoutHTTP:
    def __init__(self):
        self.calls = 0

    def request(self, *_args, **_kwargs):
        self.calls += 1
        raise requests.exceptions.ReadTimeout("Read timed out for secret-key. (read timeout=30)")


class _Response:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.content = b"{}"

    def json(self):
        return self._payload


def test_reply_retries_a_read_timeout_once_and_keeps_the_error_text():
    http = _TimeoutHTTP()
    client = runner.Instantly("secret-key", session=http)
    status, data = client.reply(
        reply_to="email-1", eaccount="abby@x.com", subject="Re: Hi", body="Hello", cc=[], extra=[],
    )
    assert http.calls == 2
    assert status == 0 and data["timeout"] is True
    assert "Read timed out" in data["error"]
    assert "secret-key" not in data["error"]
    assert "<key>" in data["error"]


def test_scheduled_timeout_returns_to_pending_and_a_4xx_stays_failed():
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    ensure_job_lease_schema(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_followups_scheduled "
            "(lead, reply_to_id, eaccount, subject, body, kind, due_at, status, detail) "
            "VALUES ('tina@silhouetteoutdoor.net', 'reply-1', 'abby.orit@anatagrowthgroup.com', "
            "'Re: Hi', 'Hello', 'out_of_office', :due, 'pending', 'out_of_office')"
        ), {"due": (NOW - timedelta(minutes=1)).replace(tzinfo=None)})
        conn.execute(text(
            "INSERT INTO abby_followups_scheduled "
            "(lead, reply_to_id, eaccount, subject, body, kind, due_at, status, detail) "
            "VALUES ('other@example.com', 'reply-2', 'abby@x.com', "
            "'Re: Hi', 'Hello', 'out_of_office', :due, 'pending', 'out_of_office')"
        ), {"due": NOW.replace(tzinfo=None)})

    class Client:
        def reply(self, **_kwargs):
            if _kwargs["reply_to"] == "reply-1":
                return 0, {"error": "Read timed out. (read timeout=30)", "timeout": True}
            return 400, {"message": "Lead not in campaign"}

    posts = []
    sent = runner.send_due_scheduled(engine, Client(), NOW, "live", posts.append, 5)
    assert sent == 0 and len(posts) == 1
    assert "tina@silhouetteoutdoor.net" in posts[0]
    assert "Read timed out" in posts[0]
    with engine.connect() as conn:
        tina = conn.execute(text(
            "SELECT status, detail, due_at FROM abby_followups_scheduled WHERE lead = 'tina@silhouetteoutdoor.net'"
        )).one()
        other = conn.execute(text(
            "SELECT status, detail FROM abby_followups_scheduled WHERE lead = 'other@example.com'"
        )).one()
    assert tina[0] == "pending"
    assert "Read timed out" in tina[1]
    assert str(tina[2])[:16] >= "2026-11-12T16:00" or str(tina[2])[:16] >= "2026-11-12 16:00"
    assert other[0] == "failed 400"
    assert other[1] == "Lead not in campaign"
    again = []
    runner.send_due_scheduled(
        engine, Client(), NOW + timedelta(hours=2), "live", again.append, 5,
    )
    assert again == []
