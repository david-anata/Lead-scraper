"""Opening a total, and answering a charge, through the real routes.

The unit tests prove the maths. This proves the page actually loads, which is
where a missing import or a bad redirect shows up and nowhere else.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import unquote

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models import database
from sales_support_agent.models.database import init_database, insert_cash_event

TODAY = date.today()
THIS_MONDAY = TODAY - timedelta(days=TODAY.weekday())
NEXT_DUE_DATE = TODAY + timedelta(days=1)
CHARGE_WEEK = NEXT_DUE_DATE - timedelta(days=NEXT_DUE_DATE.weekday())


@pytest.fixture()
def books(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:", future=True,
        connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=engine, future=True))
    monkeypatch.setattr(database, "engine", engine)
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for index in range(3):
            insert_cash_event(
                connection, id=f"bill{index}", source="clickup", source_id=f"bill{index}",
                record_kind="obligation", event_type="outflow", category="utilities",
                name="Lehi City Power", vendor_or_customer="Lehi City Power",
                description="Lehi City Power", amount_cents=89_000 + index,
                due_date=NEXT_DUE_DATE + timedelta(days=index), status="planned",
                confidence="confirmed", created_at=now, updated_at=now,
            )
    return engine


def _client():
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="s", admin_cookie_name="c", admin_session_ttl_hours=24,
    )
    app.include_router(cashflow_router)
    user = {"email": "d@example.com", "is_superadmin": True, "permissions": {"finance"}}
    ctx = [
        patch("sales_support_agent.services.auth_deps.get_session_user_from_request",
              return_value={"email": user["email"]}),
        patch("sales_support_agent.services.auth_deps.get_current_user", return_value=user),
        patch("sales_support_agent.api.cashflow_router.get_current_user", return_value=user),
    ]
    for item in ctx:
        item.start()
    return TestClient(app, follow_redirects=False), ctx


def _flash(response) -> str:
    return unquote(response.headers.get("location", "")).split("flash=", 1)[-1]


def test_opening_a_total_lists_the_charges_behind_it(books):
    client, ctx = _client()
    try:
        response = client.get(
            f"/admin/finances/calendar/charges?week={CHARGE_WEEK.isoformat()}&state=unpaid"
        )
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 200, response.status_code
    assert "Lehi City Power" in response.text
    for leak in ("Traceback", "NameError", "TypeError", "AttributeError"):
        assert leak not in response.text, f"page leaks {leak}"
    for label in ("Monthly", "Weekly", "One-time", "Not a bill"):
        assert label in response.text, label


def test_an_unreadable_week_says_so_instead_of_failing(books):
    client, ctx = _client()
    try:
        response = client.get("/admin/finances/calendar/charges?week=not-a-date&state=unpaid")
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 303
    assert "could not be read" in _flash(response)


def test_a_nonsense_column_is_refused(books):
    client, ctx = _client()
    try:
        response = client.get(
            f"/admin/finances/calendar/charges?week={THIS_MONDAY.isoformat()}&state=sideways"
        )
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 303
    assert "paid" in _flash(response).lower()


def test_answering_a_charge_reports_what_it_did(books):
    client, ctx = _client()
    try:
        with patch(
            "sales_support_agent.services.cashflow.bill_patterns.record_bill_pattern_decision"
        ) as record:
            response = client.post("/admin/finances/calendar/charges/answer", data={
                "pattern_key": "0123456789abcdef", "cadence": "monthly",
                "vendor": "Lehi City Power",
            })
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 303
    assert record.call_count == 1
    assert record.call_args.args[1] == "track", "monthly must write the same decision as tracking"
    assert "monthly" in _flash(response).lower()


def test_a_bad_answer_does_not_write_anything(books):
    client, ctx = _client()
    try:
        with patch(
            "sales_support_agent.services.cashflow.bill_patterns.record_bill_pattern_decision"
        ) as record:
            response = client.post("/admin/finances/calendar/charges/answer", data={
                "pattern_key": "0123456789abcdef", "cadence": "whenever",
            })
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 303
    assert record.call_count == 0
    assert "could not be saved" in _flash(response)


def test_the_weekly_totals_are_links_that_open_the_charges(books):
    client, ctx = _client()
    try:
        response = client.get("/admin/finances/calendar")
    finally:
        for item in ctx:
            item.stop()

    assert response.status_code == 200
    assert "/admin/finances/calendar/charges?week=" in response.text, (
        "the totals must be openable, which is the whole point"
    )
