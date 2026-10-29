"""Enrich triggered from Abby's 5-minute cron: window, throttle, isolation."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from sales_support_agent.api import abby_job, outbound_enrich_job as job
from sales_support_agent.services.job_lease import ensure_job_lease_schema

DENVER = ZoneInfo("America/Denver")
# Wednesday 28 Oct 2026, 5:51 PM Denver = 23:51 UTC. Outside 14:15-17:15 UTC.
EVENING_UTC = datetime(2026, 10, 28, 23, 51, tzinfo=timezone.utc)
# Wednesday 7:03 AM Denver = 13:03 UTC, before the old 14:15 UTC cron, minute 3.
EARLY = datetime(2026, 10, 28, 7, 3, tzinfo=DENVER)
SATURDAY_NOON = datetime(2026, 10, 24, 12, 0, tzinfo=DENVER)
SATURDAY_EVENING = datetime(2026, 10, 24, 20, 0, tzinfo=DENVER)


@pytest.fixture
def engine(tmp_path):
    bound = create_engine(f"sqlite:///{tmp_path / 'lease.db'}")
    ensure_job_lease_schema(bound)
    yield bound
    bound.dispose()


@pytest.fixture
def enrich_on(monkeypatch):
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "true")


def _ran(**kwargs):
    return {
        "ran": True,
        "pushed": 2,
        "report": {"companies_tried": 3, "contacts_found": 4},
    }


def _calls():
    seen = []

    def run_job(**kwargs):
        seen.append(kwargs)
        return _ran()

    return seen, run_job


def _insert(engine, *, run_key, status, started_at, lease_expires_at):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO scheduled_job_runs (
                    job_key, run_key, status, owner_token, started_at,
                    lease_expires_at, details_json
                ) VALUES (
                    :job_key, :run_key, :status, 'test', :started_at,
                    :lease_expires_at, '{}'
                )
                """
            ),
            {
                "job_key": job._VIA_ABBY_JOB_KEY,
                "run_key": run_key,
                "status": status,
                "started_at": started_at.astimezone(timezone.utc).isoformat(),
                "lease_expires_at": lease_expires_at.astimezone(timezone.utc).isoformat(),
            },
        )


def test_weekday_window_is_any_denver_hour_including_outside_the_old_crons():
    assert job.enrich_via_abby_window(EVENING_UTC, recent_success=True) == "abby-2026-10-28T17"
    assert job.enrich_via_abby_window(EVENING_UTC, recent_success=False) == "abby-2026-10-28T17"
    assert job.enrich_via_abby_window(EARLY, recent_success=True) == "abby-2026-10-28T07"
    late = datetime(2026, 10, 23, 23, 47, tzinfo=DENVER)  # Friday night, after 18:00
    assert job.enrich_via_abby_window(late, recent_success=False) is None


def test_enrich_window_is_only_hours_7_through_17():
    day = datetime(2026, 10, 28, tzinfo=DENVER)
    six = day.replace(hour=6, minute=10)
    eighteen = day.replace(hour=18, minute=0)
    seven = day.replace(hour=7, minute=0)
    seventeen = day.replace(hour=17, minute=30)
    assert job.enrich_via_abby_window(six, recent_success=False) is None
    assert job.enrich_via_abby_window(eighteen, recent_success=False) is None
    assert job.enrich_via_abby_window(seven, recent_success=False) == "abby-2026-10-28T07"
    assert job.enrich_via_abby_window(seventeen, recent_success=False) == "abby-2026-10-28T17"


def test_scheduled_enrich_skips_at_6(monkeypatch):
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "true")
    six = datetime(2026, 10, 28, 6, 5, tzinfo=DENVER)
    out = job.run_enrich_job(now=six)
    assert out["ran"] is False
    assert out["reason"] == "outside enrich hours"


def test_weekend_window_is_a_single_catchup_only_without_recent_success():
    assert job.enrich_via_abby_window(SATURDAY_NOON, recent_success=False) == "abby-catchup-2026-10-24"
    assert job.enrich_via_abby_window(SATURDAY_NOON, recent_success=True) is None
    sunday = datetime(2026, 10, 25, 9, 15, tzinfo=DENVER)
    assert job.enrich_via_abby_window(sunday, recent_success=False) == "abby-catchup-2026-10-25"
    assert job.enrich_via_abby_window(sunday, recent_success=True) is None


def test_recent_success_is_today_or_the_last_six_hours(engine):
    morning = datetime(2026, 10, 24, 8, 0, tzinfo=DENVER)
    _insert(
        engine,
        run_key="prior-morning",
        status="succeeded",
        started_at=morning,
        lease_expires_at=morning + timedelta(hours=1),
    )
    # Same Denver day, more than 6 hours later, still counts.
    assert job._has_recent_enrich_success(engine, SATURDAY_EVENING) is True

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM scheduled_job_runs"))
    friday_evening = datetime(2026, 10, 23, 20, 0, tzinfo=DENVER)
    _insert(
        engine,
        run_key="prior-friday",
        status="succeeded",
        started_at=friday_evening,
        lease_expires_at=friday_evening + timedelta(hours=1),
    )
    assert job._has_recent_enrich_success(engine, SATURDAY_NOON) is False

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM scheduled_job_runs"))
    recent = SATURDAY_NOON - timedelta(hours=2)
    _insert(
        engine,
        run_key="prior-recent",
        status="succeeded",
        started_at=recent,
        lease_expires_at=recent + timedelta(hours=1),
    )
    assert job._has_recent_enrich_success(engine, SATURDAY_NOON) is True


def test_first_tick_runs_once_per_hour_then_again_next_hour(engine, enrich_on, caplog):
    seen, run_job = _calls()
    sixteen = datetime(2026, 10, 28, 16, 51, tzinfo=DENVER)
    with caplog.at_level("INFO"):
        first = job.maybe_run_enrich_from_abby(now=sixteen, engine=engine, run_job=run_job)
        second = job.maybe_run_enrich_from_abby(now=sixteen, engine=engine, run_job=run_job)
        third = job.maybe_run_enrich_from_abby(
            now=sixteen + timedelta(hours=1), engine=engine, run_job=run_job,
        )
    assert first["ran"] is True
    assert first["window"] == "abby-2026-10-28T16"
    assert first["leads_attempted"] == 3
    assert first["enriched"] == 4
    assert first["pushed"] == 2
    assert second["ran"] is False
    assert second["reason"] == "already ran this window"
    assert third["ran"] is True
    assert third["window"] == "abby-2026-10-28T17"
    assert len(seen) == 2
    assert "[enrich-via-abby] start window=abby-2026-10-28T16" in caplog.text
    assert "leads_attempted=3 enriched=4 pushed=2" in caplog.text


def test_early_morning_minute_runs_outside_the_old_cron_window(engine, enrich_on):
    seen, run_job = _calls()
    out = job.maybe_run_enrich_from_abby(now=EARLY, engine=engine, run_job=run_job)
    assert out["ran"] is True
    assert out["window"] == "abby-2026-10-28T07"
    assert len(seen) == 1
    assert seen[0]["now"].hour == 7


def test_gate_off_does_not_run_or_consume_the_hour(engine, monkeypatch, caplog):
    monkeypatch.delenv("OUTBOUND_AUTO_ENRICH", raising=False)
    seen, run_job = _calls()
    with caplog.at_level("INFO"):
        skipped = job.maybe_run_enrich_from_abby(now=EVENING_UTC, engine=engine, run_job=run_job)
    assert skipped["ran"] is False
    assert skipped["reason"] == "OUTBOUND_AUTO_ENRICH is false"
    assert seen == []
    assert "[enrich-via-abby] start" not in caplog.text
    assert "reason=OUTBOUND_AUTO_ENRICH is false" in caplog.text
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "true")
    started = job.maybe_run_enrich_from_abby(now=EVENING_UTC, engine=engine, run_job=run_job)
    assert started["ran"] is True
    assert len(seen) == 1


def test_enrich_failure_is_logged_and_does_not_retry_this_hour(engine, enrich_on, caplog):
    seen = []

    def boom(**kwargs):
        seen.append(kwargs)
        raise RuntimeError("prospeo down")

    with caplog.at_level("INFO"):
        failed = job.maybe_run_enrich_from_abby(now=EARLY, engine=engine, run_job=boom)
        again = job.maybe_run_enrich_from_abby(now=EARLY, engine=engine, run_job=boom)
    assert failed["ran"] is False
    assert failed["reason"] == "enrich failed"
    assert failed["leads_attempted"] == 0
    assert again["reason"] == "already ran this window"
    assert len(seen) == 1
    assert "[enrich-via-abby] enrich failed" in caplog.text
    assert "[enrich-via-abby] result" in caplog.text


def test_in_flight_run_does_not_start_another(engine, enrich_on):
    seen, run_job = _calls()
    _insert(
        engine,
        run_key="abby-other",
        status="running",
        started_at=EARLY,
        lease_expires_at=EARLY + timedelta(minutes=30),
    )
    blocked = job.maybe_run_enrich_from_abby(now=EARLY, engine=engine, run_job=run_job)
    assert blocked["reason"] == "enrich already in progress"
    assert seen == []


def test_missing_lease_table_runs_only_on_the_first_tick_of_the_hour(tmp_path, enrich_on):
    bound = create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    seen, run_job = _calls()
    try:
        first = job.maybe_run_enrich_from_abby(now=EARLY, engine=bound, run_job=run_job)
        skipped = job.maybe_run_enrich_from_abby(now=EVENING_UTC, engine=bound, run_job=run_job)
    finally:
        bound.dispose()
    assert first["ran"] is True
    assert first["window"] == "abby-2026-10-28T07"
    assert skipped["ran"] is False
    assert skipped["reason"] == "could not claim enrich window"
    assert len(seen) == 1


def test_stale_running_lease_does_not_block_a_new_hour(engine, enrich_on):
    seen, run_job = _calls()
    _insert(
        engine,
        run_key="abby-stale",
        status="running",
        started_at=EARLY - timedelta(hours=3),
        lease_expires_at=EARLY - timedelta(minutes=1),
    )
    out = job.maybe_run_enrich_from_abby(now=EARLY, engine=engine, run_job=run_job)
    assert out["ran"] is True
    assert len(seen) == 1


def test_weekend_catchup_runs_once_when_nothing_succeeded_recently(engine, enrich_on):
    seen, run_job = _calls()
    first = job.maybe_run_enrich_from_abby(now=SATURDAY_NOON, engine=engine, run_job=run_job)
    second = job.maybe_run_enrich_from_abby(now=SATURDAY_NOON, engine=engine, run_job=run_job)
    assert first["ran"] is True
    assert first["window"] == "abby-catchup-2026-10-24"
    assert second["ran"] is False
    assert second["reason"] in {"not due", "already ran this window"}
    assert len(seen) == 1


def test_weekend_skips_when_enrich_already_succeeded_today(engine, enrich_on):
    seen, run_job = _calls()
    morning = datetime(2026, 10, 24, 8, 0, tzinfo=DENVER)
    _insert(
        engine,
        run_key="prior-morning",
        status="succeeded",
        started_at=morning,
        lease_expires_at=morning + timedelta(hours=1),
    )
    out = job.maybe_run_enrich_from_abby(now=SATURDAY_EVENING, engine=engine, run_job=run_job)
    assert out["reason"] == "not due"
    assert seen == []


def test_weekend_catchup_still_obeys_the_enrich_weekend_rule(engine, enrich_on, monkeypatch):
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    calls = []

    def wrapped(**kwargs):
        calls.append(kwargs)
        return job.run_enrich_job(**kwargs)

    out = job.maybe_run_enrich_from_abby(now=SATURDAY_NOON, engine=engine, run_job=wrapped)
    assert out["ran"] is False
    assert out["reason"] == "weekend"
    again = job.maybe_run_enrich_from_abby(now=SATURDAY_NOON, engine=engine, run_job=wrapped)
    assert again["ran"] is False
    assert len(calls) == 1


def test_abby_route_runs_enrich_after_abby_and_isolates_failure(monkeypatch, caplog):
    monkeypatch.setenv("CRON_SECRET", "cron-secret-value")
    order = []

    def abby():
        order.append("abby")
        return {"handled": 4}

    def enrich():
        order.append("enrich")
        raise RuntimeError("enrich exploded")

    monkeypatch.setattr(abby_job, "run_abby", abby)
    monkeypatch.setattr(job, "maybe_run_enrich_from_abby", enrich)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(abby_job.router)
    client = TestClient(app)
    with caplog.at_level("ERROR"):
        response = client.get(
            "/api/jobs/abby/run",
            headers={"Authorization": "Bearer cron-secret-value"},
        )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["details"] == {"handled": 4}
    assert body["enrich_via_abby"]["reason"] == "hook failed"
    assert order == ["abby", "enrich"]
    assert "[enrich-via-abby] hook failed" in caplog.text


def test_abby_route_keeps_abby_result_when_enrich_returns(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "cron-secret-value")

    monkeypatch.setattr(abby_job, "run_abby", lambda: {"handled": 1})
    monkeypatch.setattr(
        job,
        "maybe_run_enrich_from_abby",
        lambda: {"ran": True, "pushed": 2, "reason": "", "window": "abby-2026-10-28T17"},
    )
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="")
    app.include_router(abby_job.router)
    response = TestClient(app).get(
        "/api/jobs/abby/run",
        headers={"Authorization": "Bearer cron-secret-value"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["details"] == {"handled": 1}
    assert body["enrich_via_abby"]["ran"] is True
    assert body["enrich_via_abby"]["pushed"] == 2
