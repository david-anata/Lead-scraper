"""Eight tools lived in one dialog. They now live on pages, exactly once each."""

from datetime import date, datetime, timedelta, timezone

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow.finance_nav import (
    NAV_ITEMS,
    nav_counts,
    render_finance_nav,
)
from sales_support_agent.services.cashflow.finance_pages import (
    render_audit_page,
    render_bookkeeping_page,
    render_collections_page,
    render_setup_page,
)

TODAY = date.today()


def _setup():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    return factory.kw["bind"]


def _txn(engine, cid, name, amount, days_ago, category="software"):
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id=cid, source="plaid", source_id=cid,
            record_kind="transaction", event_type="outflow", category=category,
            name=name, vendor_or_customer=name, description=name,
            amount_cents=amount, due_date=TODAY - timedelta(days=days_ago),
            status="posted", confidence="confirmed", created_at=now, updated_at=now,
        )


def test_nav_shows_every_section_and_marks_the_active_one():
    html = render_finance_nav("audit", counts={})
    for _, label, href in NAV_ITEMS:
        assert label in html
        assert href in html
    assert 'class="finance-nav-link is-active" href="/admin/finances/audit"' in html


def test_a_zero_count_is_hidden_rather_than_shown_as_zero():
    html = render_finance_nav("today", counts={"audit": 0, "review": 3})
    assert ">3<" in html
    assert html.count("finance-nav-count") == 1


def test_counts_reflect_what_is_actually_waiting():
    engine = _setup()
    _txn(engine, "e1", "Eliteworks", 3200_00, 4)
    _txn(engine, "e2", "Eliteworks", 3200_00, 4)          # a real duplicate
    _txn(engine, "u1", "Madison Bicycle Shop", 500_00, 2, "uncategorized")

    counts = nav_counts()
    assert counts["audit"] == 1
    assert counts["bookkeeping"] == 1


def test_an_internal_transfer_is_counted_nowhere():
    engine = _setup()
    _txn(engine, "t1", "Home banking Withdrawal Transfer to S0050", 25000_00, 2, "uncategorized")
    _txn(engine, "t2", "Home banking Withdrawal Transfer to S0050", 25000_00, 2, "uncategorized")

    counts = nav_counts()
    assert counts["audit"] == 0, "moving your own money is not a finding"
    assert counts["bookkeeping"] == 0, "and it is not something to categorise"


def test_each_page_renders_its_own_tool_with_the_nav():
    engine = _setup()
    _txn(engine, "e1", "Eliteworks", 3200_00, 4)
    _txn(engine, "e2", "Eliteworks", 3200_00, 4)
    _txn(engine, "u1", "Madison Bicycle Shop", 500_00, 2, "uncategorized")
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="ar1", source="qbo", source_id="ar1",
            record_kind="obligation", event_type="inflow", category="revenue",
            name="Acme Co", vendor_or_customer="Acme Co", amount_cents=12000_00,
            due_date=TODAY - timedelta(days=41), status="overdue",
            confidence="confirmed", created_at=now, updated_at=now,
        )

    audit = render_audit_page()
    assert "double charge" in audit and "finance-nav" in audit

    books = render_bookkeeping_page()
    assert "Madison Bicycle Shop" in books
    assert "&mdash; choose &mdash;" in books, "no category may be preselected"

    collections = render_collections_page()
    assert "Acme Co" in collections

    assert "Vendors" in render_setup_page()


def test_pages_survive_an_empty_database():
    _setup()
    for render in (render_audit_page, render_bookkeeping_page,
                   render_collections_page, render_setup_page):
        html = render()
        assert "finance-nav" in html
        assert len(html) > 500
