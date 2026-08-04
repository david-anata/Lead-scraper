from datetime import date, timedelta

from sales_support_agent.services.cashflow.cash_calendar import (
    build_cash_calendar,
    render_cash_calendar_page,
)
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav


TODAY = date(2026, 8, 4)


def _transaction(identifier: str, *, days_ago: int, amount: int, name: str = "Vendor", **extra):
    return {
        "id": identifier,
        "record_kind": "transaction",
        "event_type": "outflow",
        "status": "posted",
        "due_date": (TODAY - timedelta(days=days_ago)).isoformat(),
        "amount_cents": amount,
        "name": name,
        "source": "plaid",
        **extra,
    }


def _obligation(identifier: str, *, days_ahead: int, amount: int, name: str = "Bill", **extra):
    return {
        "id": identifier,
        "record_kind": "obligation",
        "event_type": "outflow",
        "status": "planned",
        "due_date": (TODAY + timedelta(days=days_ahead)).isoformat(),
        "amount_cents": amount,
        "name": name,
        **extra,
    }


def _events(calendar, day):
    return next(row["events"] for row in calendar["days"] if row["date"] == day.isoformat())


def test_calendar_has_past_seven_today_and_next_fourteen_days():
    calendar = build_cash_calendar([], as_of=TODAY)

    assert len(calendar["days"]) == 22
    assert calendar["start"] == "2026-07-28"
    assert calendar["end"] == "2026-08-18"
    assert calendar["days"][7]["period"] == "today"
    assert calendar["days"][7]["label"] == "Today"


def test_posted_expenses_are_split_by_linked_plan_evidence():
    rows = [
        _transaction("tx-planned", days_ago=2, amount=12500, name="Known vendor"),
        _transaction("tx-surprise", days_ago=1, amount=4900, name="Surprise vendor"),
    ]
    allocations = [{
        "transaction_event_id": "tx-planned",
        "obligation_event_id": "bill-1",
        "amount_cents": 12500,
        "obligation_name": "Known monthly bill",
    }]

    calendar = build_cash_calendar(rows, allocations=allocations, as_of=TODAY)

    assert _events(calendar, TODAY - timedelta(days=2))[0]["kind"] == "posted_planned"
    assert _events(calendar, TODAY - timedelta(days=1))[0]["kind"] == "posted_unplanned"
    assert calendar["totals"]["posted_cents"] == 17400
    assert calendar["totals"]["unplanned_posted_cents"] == 4900


def test_future_plan_uses_only_remaining_unsettled_amount():
    rows = [
        _obligation("part-paid", days_ahead=3, amount=20000, name="Part-paid bill"),
        _obligation("fully-paid", days_ahead=4, amount=8000, name="Settled bill"),
    ]
    allocations = [
        {"transaction_event_id": "old-1", "obligation_event_id": "part-paid", "amount_cents": 5000},
        {"transaction_event_id": "old-2", "obligation_event_id": "fully-paid", "amount_cents": 8000},
    ]

    calendar = build_cash_calendar(rows, allocations=allocations, as_of=TODAY)

    events = _events(calendar, TODAY + timedelta(days=3))
    assert [(item["kind"], item["amount_cents"]) for item in events] == [("planned", 15000)]
    assert _events(calendar, TODAY + timedelta(days=4)) == []
    assert calendar["totals"]["planned_cents"] == 15000


def test_history_warning_is_not_counted_as_required_cash():
    history = [
        {"id": "maybe", "date": TODAY + timedelta(days=2), "name": "Possible tool", "amount_cents": 9900},
        {"id": "tracked", "date": TODAY + timedelta(days=5), "name": "Tracked tool", "amount_cents": 12000, "confirmed": True},
    ]

    calendar = build_cash_calendar([], historical_events=history, as_of=TODAY)

    assert calendar["totals"]["warning_cents"] == 9900
    assert calendar["totals"]["planned_cents"] == 12000
    assert _events(calendar, TODAY + timedelta(days=2))[0]["state_label"] == "Likely from history · not planned"


def test_canonical_actuals_exclude_internal_transfers_and_mirrored_sources():
    plaid = _transaction("plaid-1", days_ago=1, amount=5000, name="Real charge")
    mirrored = {**plaid, "id": "qbo-1", "source": "qbo"}
    transfer = _transaction(
        "transfer-1", days_ago=1, amount=7000, name="Account transfer", category="internal_transfer"
    )

    calendar = build_cash_calendar([plaid, mirrored, transfer], as_of=TODAY)

    assert calendar["actual_source"] == "plaid"
    assert calendar["totals"]["posted_cents"] == 5000
    assert len(_events(calendar, TODAY - timedelta(days=1))) == 1


def test_calendar_renderer_explains_evidence_and_filters_without_write_controls():
    calendar = build_cash_calendar(
        [_transaction("tx-1", days_ago=1, amount=5000)],
        historical_events=[{
            "id": "maybe", "date": TODAY + timedelta(days=2), "name": "Possible tool", "amount_cents": 9900,
        }],
        as_of=TODAY,
    )

    page = render_cash_calendar_page(calendar)

    assert "Past 7 days · today · next 14 days" in page
    assert "Likely from history · not planned" in page
    assert "not counted as required" in page
    assert "data-calendar-filter=\"attention\"" in page
    assert "Posted source: Plaid" in page
    assert "<form" not in page


def test_calendar_error_state_is_safe_and_actionable():
    page = render_cash_calendar_page({"status": "error", "days": [], "totals": {}})

    assert "The cash calendar could not load" in page
    assert "Nothing was changed" in page
    assert 'href="/admin/finances/accounts"' in page


def test_finance_navigation_includes_calendar_destination():
    nav = render_finance_nav("calendar", counts={})

    assert 'href="/admin/finances/calendar"' in nav
    assert 'class="finance-nav-link is-active" href="/admin/finances/calendar"' in nav
