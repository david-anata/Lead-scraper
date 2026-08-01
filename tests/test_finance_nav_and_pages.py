"""Eight tools lived in one dialog. They now live on pages, exactly once each."""

from datetime import date, datetime, timedelta, timezone
from html import escape

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
    html = render_finance_nav("review", counts={})
    for _, label, href in NAV_ITEMS:
        assert escape(label) in html
        assert href in html
    assert 'class="finance-nav-link is-active" href="/admin/finances/review"' in html


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


def test_the_review_page_agrees_with_the_trust_check():
    """The trust check reported 94 items to review while the Review page showed
    nothing, because the flags it reads are computed rather than stored and the
    review page was classifying raw rows. Sharing the classifier is not enough;
    the input has to match."""
    from sales_support_agent.services.cashflow.bulk_resolve import list_review_items
    from sales_support_agent.services.cashflow.control import (
        classify_payable_issues,
        load_annotated_obligations,
    )

    engine = _setup()
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        for index in range(3):
            insert_cash_event(
                connection, id=f"done{index}", source="clickup", source_id=f"done{index}",
                record_kind="obligation", event_type="outflow", category="rent",
                name=f"Marked done {index}", vendor_or_customer="Vendor",
                amount_cents=1200_00, due_date=TODAY - timedelta(days=10),
                status="completed", confidence="confirmed",
                created_at=now, updated_at=now,
            )

    annotated = load_annotated_obligations()
    expected = classify_payable_issues(annotated, as_of=TODAY)
    assert len(expected) == 3, "a completed ClickUp bill with no payment still needs review"

    listed = list_review_items(as_of=TODAY)
    assert listed["total"] == len(expected), "both surfaces must report the same count"
    assert "Marked done, no bank proof" in {group["label"] for group in listed["groups"]}


def test_raw_rows_would_have_missed_those_items():
    """Guards the actual regression: classifying un-annotated rows finds nothing."""
    from sales_support_agent.services.cashflow.control import classify_payable_issues
    from sales_support_agent.services.cashflow.obligations import list_obligations

    engine = _setup()
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="done", source="clickup", source_id="done",
            record_kind="obligation", event_type="outflow", category="rent",
            name="Marked done", vendor_or_customer="Vendor", amount_cents=1200_00,
            due_date=TODAY - timedelta(days=10), status="completed",
            confidence="confirmed", created_at=now, updated_at=now,
        )

    assert classify_payable_issues(list_obligations(limit=100), as_of=TODAY) == []
    from sales_support_agent.services.cashflow.control import load_annotated_obligations
    assert classify_payable_issues(load_annotated_obligations(), as_of=TODAY) != []
