"""The bookkeeping page as the operator actually loads it.

The unit tests prove the counts are right. This proves the page they land on
shows those counts, adds up, and does not tell the operator their books are up
to date when nothing is written back to QuickBooks.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from sales_support_agent.api.cashflow_router import router as cashflow_router
from sales_support_agent.models import database
from sales_support_agent.models.database import init_database, insert_cash_event


@pytest.fixture()
def seeded_books(monkeypatch):
    """A realistic mix: booked in QuickBooks, filed here only, unknown, transfers.

    The page renders on a worker thread, and a plain in-memory SQLite engine
    hands each thread its own empty database, so the pool has to be shared or
    the page under test would render against nothing and still pass.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    init_database(sessionmaker(bind=engine, future=True))
    monkeypatch.setattr(database, "engine", engine)
    now = datetime.now(timezone.utc)

    def txn(cid, name, *, source="plaid", category="uncategorized", sub=""):
        with engine.begin() as connection:
            insert_cash_event(
                connection, id=cid, source=source, source_id=cid,
                record_kind="transaction", event_type="outflow", category=category,
                subcategory=sub, name=name, vendor_or_customer=name, description=name,
                amount_cents=100_00, due_date=date(2026, 7, 10), status="posted",
                confidence="confirmed", created_at=now, updated_at=now,
            )

    for index in range(40):
        txn(f"q{index}", "Madison Bicycle Shop", source="qbo_bank",
            category="job materials", sub="Job Expenses:Job Materials")
    for index in range(6):
        txn(f"u{index}", "Weird Vendor", source="qbo_bank",
            category="other", sub="Uncategorized Expense")
    for index in range(9):
        txn(f"b{index}", "SQ *COFFEE SHOP")
    txn("comcast", "COMCAST CABLE COMM", category="utilities")
    for index in range(3):
        txn(f"t{index}", "Home banking Withdrawal Transfer to S0050")
    return engine


def _load_page() -> str:
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        admin_session_secret="test-secret",
        admin_cookie_name="admin_session",
        admin_session_ttl_hours=24,
    )
    app.include_router(cashflow_router)
    client = TestClient(app, follow_redirects=False)
    user = {"email": "finance@example.com", "is_superadmin": True, "permissions": {"finance"}}
    with patch(
        "sales_support_agent.services.auth_deps.get_session_user_from_request",
        return_value={"email": user["email"]},
    ), patch(
        "sales_support_agent.services.auth_deps.get_current_user", return_value=user,
    ), patch(
        "sales_support_agent.api.cashflow_router.get_current_user", return_value=user,
    ):
        response = client.get("/admin/finances/bookkeeping")
    assert response.status_code == 200, response.status_code
    return response.text


def _counter(page: str, label: str) -> int:
    match = re.search(re.escape(label) + r"</span><strong>(\d+)", page)
    assert match, f"the page never showed a {label!r} counter"
    return int(match.group(1))


def test_the_page_shows_where_every_transaction_stands(seeded_books):
    page = _load_page()

    booked = _counter(page, "Already booked in QuickBooks")
    here_only = _counter(page, "Filed here only")
    needs = _counter(page, "Need a decision")

    assert booked == 40, "QuickBooks booked 40 of them and is not asking again"
    assert here_only == 1, "Comcast was filed here, so the real books still lack it"
    assert needs == 15, "9 unseen card charges plus 6 QuickBooks could not place"

    total = re.search(r"Already booked in QuickBooks</span><strong>\d+ of (\d+)", page)
    assert total, "the denominator is missing"
    assert booked + here_only + needs == int(total.group(1)), (
        "three overlapping numbers are three numbers the operator cannot act on"
    )
    assert int(total.group(1)) == 56, "the 3 internal transfers are not bookkeeping"


def test_the_page_does_not_claim_the_books_are_up_to_date(seeded_books):
    page = _load_page()

    assert "QuickBooks runs the books" in page
    assert "Book those in QuickBooks" in page, "the operator needs to be told where to fix it"
    assert "Filed automatically" not in page, (
        "the old wording implied filing here was the end of the job"
    )
