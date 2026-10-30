"""Daily Instantly analytics sync: mapping, upsert, auth, and the migration."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from sales_support_agent.api import instantly_analytics_job as job
from sales_support_agent.services import instantly_campaign_analytics as sync


def _engine():
    return create_engine("sqlite://")


def _get(rows):
    calls = []

    def get(path, params):
        calls.append((path, dict(params)))
        if path == sync.CAMPAIGNS_PATH:
            return 200, {"items": [{"id": "camp-1", "name": "Trial Offer"}]}
        return 200, rows

    get.calls = calls
    return get


def test_positive_uses_interested_then_opportunities_and_meetings_stay_optional():
    interested = sync.normalize_daily_row(
        {"date": "2026-10-28", "opportunities": 3, "total_interested": 2,
         "bounced_count": 4, "unsubscribed_count": 1, "meetings_booked": 1, "sent": 9},
        campaign_id="camp-1", campaign_name="Trial", pulled_at="t",
    )
    assert interested["positive"] == 2
    assert interested["opportunities"] == 3
    assert interested["bounced"] == 4
    assert interested["unsubscribed"] == 1
    assert interested["meetings_booked"] == 1
    fallback = sync.normalize_daily_row(
        {"date": "2026-10-28", "opportunities": 3, "sent": 1},
        campaign_id="camp-1", campaign_name="Trial", pulled_at="t",
    )
    assert fallback["positive"] == 3
    assert fallback["meetings_booked"] is None
    assert fallback["bounced"] == 0


def test_sync_upserts_and_a_later_pull_replaces_the_day(monkeypatch):
    engine = _engine()
    first = _get([{"date": "2026-10-30", "sent": 2, "contacted": 2,
                   "new_leads_contacted": 1, "replies": 1, "unique_replies": 1,
                   "opportunities": 1}])
    out = sync.sync_daily_analytics(
        engine, start_date=date(2026, 10, 30), today=date(2026, 10, 30),
        api_key="k", get=first,
    )
    assert out["ok"] and out["upserted"] == 1
    assert first.calls[1][1]["campaign_id"] == "camp-1"
    second = _get([{"date": "2026-10-30", "sent": 5, "contacted": 4,
                    "new_leads_contacted": 1, "replies": 2, "unique_replies": 2,
                    "opportunities": 1}])
    again = sync.sync_daily_analytics(
        engine, start_date=date(2026, 10, 30), today=date(2026, 10, 30),
        api_key="k", get=second,
    )
    assert again["upserted"] == 1
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT campaign_name, sent, positive FROM instantly_campaign_daily"
        )).fetchall()
    assert rows == [("Trial Offer", 5, 1)]


def test_empty_table_backfills_from_october_and_a_full_table_uses_three_days():
    engine = _engine()
    backfill = _get([{"date": "2026-10-02", "sent": 1, "opportunities": 0}])
    first = sync.sync_daily_analytics(
        engine, today=date(2026, 10, 30), api_key="k", get=backfill,
    )
    assert first["start_date"] == "2026-10-01"
    assert backfill.calls[1][1]["start_date"] == "2026-10-01"
    recent = _get([])
    second = sync.sync_daily_analytics(
        engine, today=date(2026, 10, 30), api_key="k", get=recent,
    )
    assert second["start_date"] == "2026-10-28"
    assert second["end_date"] == "2026-10-30"
    skipped = sync.maybe_backfill_if_empty(engine, api_key="k", get=_get([]))
    assert skipped["ran"] is False and skipped["reason"] == "already loaded"


def test_missing_api_key_does_not_call_instantly(monkeypatch):
    monkeypatch.delenv("INSTANTLY_API_KEY", raising=False)
    monkeypatch.delenv("INSTANTLY_AI", raising=False)
    called = []
    out = sync.sync_daily_analytics(
        _engine(), api_key="", get=lambda *a, **k: called.append(1) or (200, []),
    )
    assert out["ok"] is False and "INSTANTLY_API_KEY" in out["reason"]
    assert called == []


def test_route_requires_scheduler_auth(monkeypatch):
    monkeypatch.setattr(job, "run_analytics_sync", lambda **k: {"ok": True, "start_date": k["start_date"]})
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.include_router(job.router)
    client = TestClient(app)
    assert client.get("/api/jobs/instantly/analytics-sync").status_code == 401
    denied = client.get(
        "/api/jobs/instantly/analytics-sync",
        headers={"Authorization": "Bearer wrong"},
    )
    assert denied.status_code == 401
    monkeypatch.setenv("CRON_SECRET", "cron-test-key")
    cron = client.get(
        "/api/jobs/instantly/analytics-sync?start_date=2026-10-01",
        headers={"Authorization": "Bearer cron-test-key"},
    )
    assert cron.status_code == 200 and cron.json()["start_date"] == "2026-10-01"
    internal = client.post(
        "/api/jobs/instantly/analytics-sync",
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert internal.status_code == 200
    bad = client.get(
        "/api/jobs/instantly/analytics-sync?start_date=nope",
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert bad.json()["reason"] == "start_date must be YYYY-MM-DD."


def test_migration_adds_the_campaign_column_and_daily_primary_key():
    sql = Path("sales_support_agent/migrations/20261030_instantly_campaign_daily.sql").read_text()
    assert "ADD COLUMN IF NOT EXISTS campaign_id TEXT" in sql
    assert "CREATE TABLE IF NOT EXISTS instantly_campaign_daily" in sql
    assert "PRIMARY KEY (date, campaign_id)" in sql
    assert "meetings_booked INTEGER" in sql
    for column in (
        "campaign_name", "sent", "contacted", "new_leads_contacted", "replies",
        "unique_replies", "positive", "bounced", "unsubscribed", "opportunities",
        "pulled_at",
    ):
        assert column in sql
