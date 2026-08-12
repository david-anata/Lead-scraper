from datetime import date, timedelta

from sales_support_agent.services.cashflow.cash_calendar import (
    _operator_today,
    build_cash_calendar,
    overlay_paydown_proposals,
    render_cash_calendar_page,
)
from sales_support_agent.services.cashflow import cash_calendar as cash_calendar_module
from sales_support_agent.services.cashflow.finance_nav import render_finance_nav


TODAY = date(2026, 8, 4)


def test_calendar_business_day_uses_denver_time(monkeypatch):
    class FrozenDateTime:
        @staticmethod
        def now(timezone):
            assert str(timezone) == "America/Denver"
            return __import__("datetime").datetime(2026, 8, 11, 18, 0)

    monkeypatch.setattr(cash_calendar_module, "datetime", FrozenDateTime)

    assert _operator_today() == date(2026, 8, 11)


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


def test_very_likely_recurring_charge_is_recognized_without_becoming_a_settlement():
    occurred = TODAY - timedelta(days=1)
    pattern = {
        "pattern_key": "pattern-1",
        "vendor": "Reliable Software",
        "merchant_key": "reliable software",
        "amount_cents": 4900,
        "confidence_bps": 8800,
        "confidence_label": "Very likely",
        "occurrences": 5,
        "paid_in_pieces": False,
        "evidence": [{
            "due_date": occurred.isoformat(),
            "amount_cents": 4900,
            "raw_descriptor": "Reliable Software",
        }],
    }

    calendar = build_cash_calendar(
        [_transaction("tx-recurring", days_ago=1, amount=4900, name="Reliable Software")],
        historical_patterns=[pattern],
        as_of=TODAY,
    )

    event = _events(calendar, occurred)[0]
    assert event["kind"] == "posted_expected"
    assert event["state_label"] == "Paid · expected from history"
    assert event["payment_status"] == "paid"
    assert event["href"] == "/admin/finances/whats-coming"
    assert calendar["totals"]["expected_posted_cents"] == 4900
    assert calendar["totals"]["unplanned_posted_cents"] == 0


def test_weak_or_aggregate_history_never_hides_a_charge_from_review():
    occurred = TODAY - timedelta(days=1)
    base = {
        "vendor": "Uncertain Software",
        "merchant_key": "uncertain software",
        "confidence_bps": 7400,
        "occurrences": 5,
        "evidence": [{"due_date": occurred.isoformat(), "amount_cents": 4900}],
    }
    calendar = build_cash_calendar(
        [_transaction("tx-uncertain", days_ago=1, amount=4900, name="Uncertain Software")],
        historical_patterns=[base],
        as_of=TODAY,
    )
    assert _events(calendar, occurred)[0]["kind"] == "posted_unplanned"

    aggregate = {**base, "confidence_bps": 9000, "paid_in_pieces": True}
    calendar = build_cash_calendar(
        [_transaction("tx-pieces", days_ago=1, amount=4900, name="Uncertain Software")],
        historical_patterns=[aggregate],
        as_of=TODAY,
    )
    assert _events(calendar, occurred)[0]["kind"] == "posted_unplanned"


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
    assert events[0]["state_label"] == "Partially paid · balance due"
    assert events[0]["payment_status"] == "partially_paid"
    assert "$50 paid; $150 remains" in events[0]["evidence"]
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
    assert _events(calendar, TODAY + timedelta(days=2))[0]["state_label"] == "Unconfirmed · likely from history"


def test_tracked_history_replaces_the_same_warning_under_a_bank_descriptor():
    due = TODAY + timedelta(days=7)
    history = [
        {"id": "tracked", "date": due, "name": "Fora Financial", "amount_cents": 205_600, "confirmed": True},
        {"id": "warning", "date": due, "name": "Forafinancial Merchdebit", "amount_cents": 205_600},
    ]

    calendar = build_cash_calendar([], historical_events=history, as_of=TODAY)

    events = _events(calendar, due)
    assert [(item["kind"], item["name"]) for item in events] == [
        ("history_planned", "Fora Financial")
    ]
    assert calendar["totals"]["planned_cents"] == 205_600
    assert calendar["totals"]["warning_cents"] == 0


def test_same_day_different_amount_history_is_not_hidden():
    due = TODAY + timedelta(days=7)
    history = [
        {"id": "tracked", "date": due, "name": "Fora Financial", "amount_cents": 205_600, "confirmed": True},
        {"id": "warning", "date": due, "name": "Forafinancial Merchdebit", "amount_cents": 100_000},
    ]

    calendar = build_cash_calendar([], historical_events=history, as_of=TODAY)

    assert len(_events(calendar, due)) == 2


def test_weekly_rollup_keeps_paid_unpaid_and_possible_money_separate():
    rows = [
        _transaction("paid", days_ago=1, amount=5000),
        _obligation("unpaid", days_ahead=1, amount=12000),
    ]
    history = [{
        "id": "possible",
        "date": TODAY + timedelta(days=2),
        "name": "Possible tool",
        "amount_cents": 9900,
    }]

    calendar = build_cash_calendar(rows, historical_events=history, as_of=TODAY)
    this_week = next(week for week in calendar["weeks"] if week["label"] == "This week")

    assert this_week["paid_cents"] == 5000
    assert this_week["unpaid_cents"] == 12000
    assert this_week["possible_cents"] == 9900
    assert this_week["paid_count"] == 1
    assert this_week["unpaid_count"] == 1
    assert this_week["possible_count"] == 1


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

    assert "Past 7 days · today · through month-end" in page
    assert "Unconfirmed · likely from history" in page
    assert "not counted as required" in page
    assert "data-calendar-filter=\"attention\"" in page
    assert "Posted source: Plaid" in page
    assert "Recognized automatically" in page
    assert "Weekly roll-up" in page
    assert "Paid from bank" in page
    assert "Unpaid planned" in page
    assert "Possible · unconfirmed" in page
    assert 'data-payment-status="paid"' in page
    assert page.count('data-calendar-date="') == 22
    assert 'class="cash-calendar-date is-selected"' in page
    assert "Showing Today. Choose another day to drill down." in page
    assert "data-calendar-select" in page
    assert "data-calendar-batch-bar" in page
    assert "Review and save" in page
    assert "<form" not in page


def test_rent_proposals_overlay_the_same_daily_and_weekly_calendar():
    calendar = build_cash_calendar([], as_of=TODAY, future_days=27)
    proposal_date = TODAY + timedelta(days=8)
    plan = {
        "status": "ok",
        "vendor": "Boulder Ranch",
        "calculation_id": calendar["calculation_id"],
        "instalments": [{"date": proposal_date, "amount_cents": 825_000}],
    }

    overlaid = overlay_paydown_proposals(calendar, plan)
    event = _events(overlaid, proposal_date)[0]
    week = next(
        item for item in overlaid["weeks"]
        if date.fromisoformat(item["start"]) <= proposal_date <= date.fromisoformat(item["end"])
    )

    assert event["state_label"] == "Proposed · not scheduled"
    assert event["payment_status"] == "unconfirmed"
    assert event["amount_cents"] == 825_000
    assert week["possible_cents"] == 825_000
    assert week["proposed_rent_cents"] == 825_000
    page = render_cash_calendar_page(overlaid, paydown={
        **plan, "monthly_cents": 4_000_000, "paid_this_month_cents": 0,
        "remaining_cents": 3_000_000, "planned_total_cents": 825_000,
        "shortfall_cents": 2_175_000, "reserved_cents": 0,
        "unconfirmed_reserved_cents": 0, "floor_cents": 1_000_000,
        "emergency_floor_cents": 0, "savings_would_unlock_cents": 0,
        "balance_basis": "operator_confirmed", "balance_as_of": TODAY,
    })
    assert "Includes $8,250 proposed rent" in page
    assert "Boulder Ranch — proposed rent payment" in page


def test_overlay_refuses_a_plan_from_a_different_snapshot():
    calendar = build_cash_calendar([], as_of=TODAY)
    overlaid = overlay_paydown_proposals(calendar, {
        "status": "ok", "calculation_id": "different",
        "instalments": [{"date": TODAY, "amount_cents": 100_000}],
    })

    assert not any(
        event.get("kind") == "proposed_rent"
        for bucket in overlaid["days"] for event in bucket["events"]
    )


def test_calendar_error_state_is_safe_and_actionable():
    page = render_cash_calendar_page({"status": "error", "days": [], "totals": {}})

    assert "The cash calendar could not load" in page
    assert "Nothing was changed" in page
    assert 'href="/admin/finances/accounts"' in page


def test_finance_navigation_includes_calendar_destination():
    nav = render_finance_nav("calendar", counts={})

    assert 'href="/admin/finances/calendar"' in nav
    assert 'class="finance-nav-link is-active" href="/admin/finances/calendar"' in nav
