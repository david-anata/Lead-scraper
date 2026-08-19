"""The scheduled Finance refresh that replaced Render's background loop.

On Render the finance pipeline ran in a loop started at boot and kept alive by
a long-lived process. Serverless has no such process, so the loop never ran
after the move and two things stopped happening unattended: QuickBooks actuals
stopped arriving, and dated obligations that passed unpaid were never rolled
forward, which quietly inflates what looks owed.

These tests hold the properties that make the replacement trustworthy: it runs
every step even when one feed is down, it says which step failed rather than
reporting a bare success, and it cannot be run twice over itself.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.vercel_cron_router import router

HEADERS = {"Authorization": "Bearer cron-secret"}
PATH = "/api/vercel-cron/finance-sync"


@pytest.fixture(autouse=True)
def _enabled(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "cron-secret")
    monkeypatch.setenv("VERCEL_CRON_WRITES_ENABLED", "true")
    monkeypatch.setenv("VERCEL_CRON_ENABLED_JOBS", "finance-sync")


def _client() -> TestClient:
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal")
    app.state.agent_settings = SimpleNamespace(internal_api_key="internal")
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    app.state.session_factory = sessionmaker(bind=engine)
    app.state.ready = True
    app.state.render_git_commit = "test-commit"
    app.include_router(router)
    return TestClient(app)


def _stub(monkeypatch, **failures):
    """Point every step at a recorder, failing only the named ones."""
    calls: list[str] = []

    def record(name, value):
        def run(*args, **kwargs):
            calls.append(name)
            if name in failures:
                raise failures[name]
            return value
        return run

    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.plaid.stale_connected_item_ids",
        record("plaid", ["item-1"]),
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.plaid.sync_connected_items",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.qbo_sync.sync_qbo_invoices",
        record("qbo_invoices", {"created": 2}),
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.qbo_bank_sync.sync_qbo_bank_transactions",
        record("qbo_bank", {"created": 5}),
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.obligations.generate_upcoming_from_templates",
        record("expand_schedule", [{"id": 1}, {"id": 2}]),
    )
    monkeypatch.setattr(
        "sales_support_agent.services.cashflow.obligations.supersede_stale_template_occurrences",
        record("roll_forward", [{"id": 9}]),
    )
    monkeypatch.setattr(
        "sales_support_agent.api.cashflow_router.clear_finance_brief_cache",
        lambda app: None,
    )
    return calls


def test_it_runs_the_whole_finance_pipeline(monkeypatch) -> None:
    calls = _stub(monkeypatch)

    body = _client().get(PATH, headers=HEADERS).json()

    assert body["status"] == "succeeded"
    assert body["failed_steps"] == []
    assert calls == [
        "plaid",
        "qbo_invoices",
        "qbo_bank",
        "expand_schedule",
        "roll_forward",
    ], "every step the Render loop ran must still run, in an order that plans against settled money"


def test_the_roll_forward_still_runs_when_the_bank_feed_is_down(monkeypatch) -> None:
    """The failure that motivated this: one dead feed used to stop the lot, and
    stale obligations then accumulated as money that looked owed."""
    calls = _stub(monkeypatch, qbo_bank=RuntimeError("QuickBooks token expired"))

    body = _client().get(PATH, headers=HEADERS).json()

    assert "roll_forward" in calls
    assert body["steps"]["roll_forward"]["status"] == "succeeded"
    assert body["steps"]["roll_forward"]["result"] == {"superseded": 1}


def test_a_broken_step_is_named_rather_than_reported_as_success(monkeypatch) -> None:
    """A run that says 'succeeded' while a feed is dead is how stale data goes
    unnoticed for weeks."""
    _stub(monkeypatch, qbo_invoices=RuntimeError("QuickBooks token expired"))

    body = _client().get(PATH, headers=HEADERS).json()

    assert body["status"] == "degraded"
    assert body["failed_steps"] == ["qbo_invoices"]
    assert "token expired" in body["steps"]["qbo_invoices"]["error"]


def test_two_invocations_in_the_same_hour_do_not_both_run(monkeypatch) -> None:
    """Vercel can retry a cron. Expanding the schedule twice over itself is the
    kind of double-count this section has already been burned by."""
    calls = _stub(monkeypatch)
    client = _client()

    first = client.get(PATH, headers=HEADERS).json()
    second = client.get(PATH, headers=HEADERS).json()

    assert first["status"] == "succeeded"
    assert second["status"] == "skipped"
    assert calls.count("expand_schedule") == 1


def test_it_stays_shut_until_the_job_is_allowlisted(monkeypatch) -> None:
    monkeypatch.setenv("VERCEL_CRON_ENABLED_JOBS", "content")

    body = _client().get(PATH, headers=HEADERS).json()

    assert body["status"] == "disabled"


def test_it_refuses_an_unauthenticated_caller() -> None:
    assert _client().get(PATH).status_code == 401


def test_the_schedule_is_actually_wired_into_vercel() -> None:
    """The route existing proves nothing if nothing ever calls it. This is the
    exact gap the move to Vercel left behind."""
    manifest = json.loads(
        Path(__file__).resolve().parents[1]
        .joinpath("vercel.json")
        .read_text(encoding="utf-8")
    )
    paths = {item["path"] for item in manifest["crons"]}

    assert PATH in paths
