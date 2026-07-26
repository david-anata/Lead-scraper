"""The schedule pages as the operator actually reaches them.

Schedules and What is coming were both wired to a redirect, so every click on
the Schedules tab landed back on the finance home and the bills found in the
bank history had nowhere to be answered. These tests walk the real routes.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import unquote

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models import database
from sales_support_agent.models.database import init_database, insert_cash_event
from sales_support_agent.services.cashflow.bill_patterns import bill_pattern_key

VENDOR = "Comcast Cable Comm"
PATTERN_KEY = bill_pattern_key(VENDOR)


@pytest.fixture()
def books(monkeypatch):
    """An empty but real database, shared across threads.

    The pages render on a worker thread, and a plain in-memory SQLite engine
    hands each thread its own empty copy, so the pool has to be shared or every
    assertion here would pass against nothing.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=engine, future=True))
    monkeypatch.setattr(database, "engine", engine)
    return engine


def _bank_payment(engine, event_id: str, vendor: str, cents: int, paid_on: date) -> None:
    """One posted bank outflow, the only history the bill finder reads."""
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=event_id, source="csv", source_id=event_id,
            record_kind="transaction", event_type="outflow", category="utilities",
            name=vendor, vendor_or_customer=vendor, description=vendor,
            amount_cents=cents, due_date=paid_on, status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )


def _monthly_history(engine, vendor: str = VENDOR) -> None:
    """Four months of the same bill: enough for the finder to call it recurring."""
    today = date.today()
    for index, months_ago in enumerate((4, 3, 2, 1)):
        _bank_payment(
            engine, f"pay{index}", vendor, 120_00 + index * 100,
            today - timedelta(days=30 * months_ago),
        )


@contextmanager
def _client(**agent_settings):
    """A TestClient with the finance permission granted and redirects visible."""
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="test-secret",
        admin_cookie_name="admin_session",
        admin_session_ttl_hours=24,
    )
    if agent_settings:
        app.state.agent_settings = SimpleNamespace(**agent_settings)
    app.include_router(cashflow_router)
    user = {"email": "finance@example.com", "is_superadmin": True, "permissions": {"finance"}}
    with patch(
        "sales_support_agent.services.auth_deps.get_session_user_from_request",
        return_value={"email": user["email"]},
    ), patch(
        "sales_support_agent.services.auth_deps.get_current_user", return_value=user,
    ), patch(
        "sales_support_agent.api.cashflow_router.get_current_user", return_value=user,
    ):
        yield TestClient(app, follow_redirects=False)


def _flash(response) -> str:
    """The message an operator would actually read after an action."""
    location = response.headers["location"]
    _, _, query = location.partition("flash=")
    return unquote(query)


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

def test_the_schedules_tab_opens_the_schedules_page(books):
    with _client() as client:
        response = client.get("/admin/finances/recurring")

    assert response.status_code == 200, "the Schedules tab used to bounce to the home page"
    assert "<h1>Schedules</h1>" in response.text
    assert "Nothing repeating here yet." in response.text


def test_a_schedule_shows_up_on_the_page_it_lives_on(books):
    from sales_support_agent.services.cashflow.obligations import create_recurring_template

    create_recurring_template(
        name="Office rent", vendor_or_customer="Boulder Ranch", event_type="outflow",
        category="rent", amount_cents=450_000, frequency="monthly",
        next_due_date=date.today() + timedelta(days=10),
    )

    with _client() as client:
        response = client.get("/admin/finances/recurring")

    assert response.status_code == 200
    assert "Office rent" in response.text
    assert "$4,500 a month going out" in response.text


# ---------------------------------------------------------------------------
# What is coming
# ---------------------------------------------------------------------------

def test_what_is_coming_opens_with_nothing_found(books):
    with _client() as client:
        response = client.get("/admin/finances/whats-coming")

    assert response.status_code == 200
    assert "<h1>What is coming</h1>" in response.text
    assert "no regular payment" in response.text
    assert 'is-active" href="/admin/finances/whats-coming"' in response.text, (
        "the operator has to be able to see which page they are on"
    )


def test_a_bill_found_in_the_bank_history_is_offered_with_its_evidence(books):
    _monthly_history(books)

    with _client() as client:
        response = client.get("/admin/finances/whats-coming")

    assert response.status_code == 200
    page = response.text
    assert VENDOR in page, "the bill the bank history repeats four times is missing"
    assert "Why we think so: paid 4 times" in page
    assert "Past payments:" in page
    assert "Track this" in page and "Not a bill" in page and "Not now" in page
    assert PATTERN_KEY in page


def test_a_bill_already_on_the_schedule_stops_asking_and_folds_away(books):
    from sales_support_agent.services.cashflow.obligations import create_recurring_template

    _monthly_history(books)
    create_recurring_template(
        name="Comcast", vendor_or_customer=VENDOR, event_type="outflow",
        category="utilities", amount_cents=121_50, frequency="monthly",
        next_due_date=date.today() + timedelta(days=5),
    )

    with _client() as client:
        page = client.get("/admin/finances/whats-coming").text

    assert "Already on your schedule (1)" in page
    assert "<details" in page, "a bill already handled should be folded away"
    assert "Track this" not in page, "it is already tracked, so there is nothing to answer"


def test_answering_a_bill_returns_to_the_list_and_not_the_finance_home(books):
    _monthly_history(books)

    with _client() as client:
        response = client.post(
            "/admin/finances/whats-coming/decide",
            data={"pattern_key": PATTERN_KEY, "decision": "track",
                  "return_to": "/admin/finances/whats-coming"},
        )

    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/admin/finances/whats-coming?flash="), (
        "answering one question threw the operator back to the finance home"
    )
    assert "next 14 and 30 days" in _flash(response)


def test_tracking_a_bill_makes_it_count_on_the_page(books):
    _monthly_history(books)

    with _client() as client:
        before = client.get("/admin/finances/whats-coming").text
        client.post(
            "/admin/finances/whats-coming/decide",
            data={"pattern_key": PATTERN_KEY, "decision": "track"},
        )
        after = client.get("/admin/finances/whats-coming").text

    assert "1 bill needs an answer" in before
    assert "You track this one" in after, "the answer changed nothing the operator can see"
    assert "1 you already track" in after


def test_saying_it_is_not_a_bill_takes_it_off_the_list(books):
    _monthly_history(books)

    with _client() as client:
        client.post(
            "/admin/finances/whats-coming/decide",
            data={"pattern_key": PATTERN_KEY, "decision": "not_a_bill"},
        )
        after = client.get("/admin/finances/whats-coming").text

    assert VENDOR not in after, "a dismissed bill kept asking"
    assert "Nothing to add" in after


def test_leaving_a_bill_for_now_stops_it_asking_this_week(books):
    _monthly_history(books)

    with _client() as client:
        response = client.post(
            "/admin/finances/whats-coming/decide",
            data={"pattern_key": PATTERN_KEY, "decision": "snooze"},
        )
        after = client.get("/admin/finances/whats-coming").text

    assert "again in a week" in _flash(response)
    assert VENDOR not in after


@pytest.mark.parametrize("bad", [
    {"pattern_key": PATTERN_KEY, "decision": "maybe"},
    {"pattern_key": PATTERN_KEY, "decision": ""},
    {"pattern_key": "not-a-real-key", "decision": "track"},
    {"pattern_key": "", "decision": "track"},
])
def test_a_nonsense_answer_says_so_instead_of_breaking(books, bad):
    _monthly_history(books)

    with _client() as client:
        response = client.post("/admin/finances/whats-coming/decide", data=bad)
        after = client.get("/admin/finances/whats-coming").text

    assert response.status_code == 303, "a bad answer must never show an error page"
    assert response.headers["location"].startswith("/admin/finances/whats-coming?flash=err")
    assert "nothing changed" in _flash(response)
    assert VENDOR in after, "nothing was recorded, so the bill is still waiting"


def test_the_old_reconcile_link_lands_on_what_is_coming(books):
    with _client() as client:
        response = client.get("/admin/finances/reconcile")

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/finances/whats-coming"


# ---------------------------------------------------------------------------
# Rolling old schedule dates forward
# ---------------------------------------------------------------------------

def _stale_series(engine) -> None:
    """A monthly schedule with two dates already gone by and one still ahead."""
    from sales_support_agent.services.cashflow.obligations import create_recurring_template

    template = create_recurring_template(
        name="Insurance", vendor_or_customer="Acme Mutual", event_type="outflow",
        category="insurance", amount_cents=25_000, frequency="monthly",
        next_due_date=date.today() - timedelta(days=40),
    )
    now = datetime.now(timezone.utc)
    today = date.today()
    for index, offset in enumerate((-40, -10, 20)):
        event_id = f"ins{index}"
        with engine.begin() as connection:
            insert_cash_event(
                connection, id=event_id, source="manual", source_id=event_id,
                record_kind="obligation", event_type="outflow", category="insurance",
                name="Insurance", vendor_or_customer="Acme Mutual", amount_cents=25_000,
                due_date=today + timedelta(days=offset), status="planned",
                confidence="estimated", created_at=now, updated_at=now,
            )
            # insert_cash_event has no template column, and the series is what
            # makes a passed date residue rather than a real unpaid bill.
            connection.execute(
                text("UPDATE cash_events SET recurring_template_id = :tid WHERE id = :id"),
                {"tid": template["id"], "id": event_id},
            )


def _open_plan_cents(engine) -> int:
    """What the plan still says is owed on dates that have already passed."""
    with engine.connect() as connection:
        row = connection.execute(text("""
            SELECT COALESCE(SUM(amount_cents), 0) FROM cash_events
            WHERE recurring_template_id IS NOT NULL
              AND archived_at IS NULL
              AND due_date < :cutoff
        """), {"cutoff": (date.today() - timedelta(days=5)).isoformat()}).fetchone()
    return int(row[0])


def test_rolling_forward_takes_passed_dates_out_of_what_you_owe(books):
    _stale_series(books)
    owed_before = _open_plan_cents(books)

    with _client() as client:
        response = client.post("/admin/finances/recurring/roll-forward", data={})

    assert owed_before > 0, "the test needs a stale date to roll forward"
    assert response.status_code == 303
    flash = _flash(response)
    moved = re.search(r"Moved (\d+) old schedule date", flash)
    assert moved, flash
    assert int(moved.group(1)) > 0
    assert _open_plan_cents(books) == 0, "the passed dates still count as money owed"


def test_rolling_forward_twice_says_there_is_nothing_left(books):
    _stale_series(books)

    with _client() as client:
        client.post("/admin/finances/recurring/roll-forward", data={})
        second = client.post("/admin/finances/recurring/roll-forward", data={})

    assert "Nothing to move on" in _flash(second)


def test_rolling_forward_returns_to_the_schedules_page(books):
    _stale_series(books)

    with _client() as client:
        response = client.post("/admin/finances/recurring/roll-forward", data={})

    assert response.headers["location"].startswith("/admin/finances/recurring?flash=")


# ---------------------------------------------------------------------------
# The old ClickUp bill list
# ---------------------------------------------------------------------------

def test_switching_clickup_off_stops_a_manual_refresh_bringing_it_back(books):
    sync = Mock()
    with patch("sales_support_agent.api.cashflow_router.sync_clickup_finance", sync):
        with _client(disable_clickup_finance_sync=True) as client:
            response = client.post("/admin/finances/sync-clickup")

    assert sync.call_count == 0, "the switch is off and ClickUp was imported anyway"
    assert response.status_code == 303
    flash = _flash(response)
    assert "switched off" in flash
    assert "in charge" in flash


def test_with_clickup_still_on_the_refresh_button_still_works(books):
    sync = Mock(return_value=SimpleNamespace(
        rows_inserted=2, rows_skipped_duplicate=1, source_exceptions=0, errors=[],
    ))
    with patch("sales_support_agent.api.cashflow_router.sync_clickup_finance", sync):
        with _client(disable_clickup_finance_sync=False) as client:
            response = client.post("/admin/finances/sync-clickup")

    assert sync.call_count == 1, "the guard must be the switch, not a dead button"
    assert response.status_code == 303
    assert "2 added" in _flash(response)


def test_the_cutover_page_gives_a_verdict(books):
    with _client() as client:
        response = client.get("/admin/finances/cutover")

    assert response.status_code == 200
    assert "Can you switch the old bill list off?" in response.text
    assert "Switching off the old bill list" in response.text


# --- the add form has to actually save -------------------------------------

def _schedule_form(**overrides) -> dict:
    form = {
        "name": "Rent",
        "vendor_or_customer": "Boulder Ranch",
        "event_type": "outflow",
        "category": "rent",
        "amount_dollars": "21042.00",
        "frequency": "monthly",
        "next_due_date": "2026-08-01",
        "day_of_month": "1",
        "flexibility": "chunkable",
    }
    form.update(overrides)
    return form


def _template_count(engine) -> int:
    with engine.connect() as connection:
        return int(connection.execute(
            text("SELECT COUNT(*) FROM recurring_templates")
        ).scalar() or 0)


def test_adding_a_schedule_actually_saves_it(books):
    """It did not. The form sent a running/paused answer that the create call
    would not accept, so every attempt re-rendered the form and saved nothing."""
    with _client() as client:
        response = client.post("/admin/finances/recurring/new", data=_schedule_form())

    assert _template_count(books) == 1, "the add form must save, not just re-render"
    assert response.status_code == 303
    assert response.headers["location"].startswith("/admin/finances/recurring")


def test_adding_a_paused_schedule_saves_it_paused(books):
    with _client() as client:
        client.post("/admin/finances/recurring/new", data=_schedule_form(is_active="off"))

    with books.connect() as connection:
        active = connection.execute(text("SELECT is_active FROM recurring_templates")).scalar()
    assert not active, "a schedule added as paused must not start forecasting"


def test_a_schedule_that_cannot_be_saved_never_shows_python_wording(books):
    """The operator is not technical. A raw type name tells them nothing they
    can act on, and it used to be printed straight onto the page."""
    with patch(
        "sales_support_agent.api.cashflow_router.create_recurring_template",
        side_effect=TypeError("unexpected keyword argument 'is_active'"),
    ):
        with _client() as client:
            response = client.post("/admin/finances/recurring/new", data=_schedule_form())

    assert "TypeError" not in response.text
    assert "keyword argument" not in response.text
    assert "Nothing was changed" in response.text


def test_saving_and_removing_a_schedule_stay_on_the_schedules_page(books):
    with _client() as client:
        client.post("/admin/finances/recurring/new", data=_schedule_form())
        with books.connect() as connection:
            template_id = connection.execute(text("SELECT id FROM recurring_templates")).scalar()

        saved = client.post(
            f"/admin/finances/recurring/{template_id}/edit",
            data=_schedule_form(name="Rent, revised"),
        )
        removed = client.post(f"/admin/finances/recurring/{template_id}/delete")

    for response in (saved, removed):
        assert response.status_code == 303
        assert response.headers["location"].startswith("/admin/finances/recurring"), (
            "an action must leave you where you were, not on the finance home"
        )


def test_answering_a_bill_that_is_no_longer_listed_says_so(books):
    """A well-formed key can name nothing, which is what a stale tab sends. It
    used to answer "Tracking that one" while tracking nothing at all."""
    _monthly_history(books)
    with _client() as client:
        response = client.post(
            "/admin/finances/whats-coming/decide",
            data={"pattern_key": "0123456789abcdef", "decision": "track"},
        )

    flash = _flash(response)
    assert "no longer in the list" in flash
    assert "Tracking" not in flash, "it must not claim success for a no-op"
