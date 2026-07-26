"""What David sees when some of the money going out is only predicted.

A bill his bank history expects is not a bill anyone has sent him. The Today
card has to keep those two apart, the funding gap has to say how much of itself
is only a guess, and the filing queue has to admit it never covers money coming
in. These tests hold that copy and those figures.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from sales_support_agent.models.database import (
    create_session_factory,
    init_database,
    insert_cash_event,
)
from sales_support_agent.services.cashflow import bill_patterns
from sales_support_agent.services.cashflow.control import _summary_metrics
from sales_support_agent.services.cashflow.finance_nav import (
    NAV_ITEMS,
    nav_counts,
    render_finance_nav,
)
from sales_support_agent.services.cashflow.overview import (
    _fallback_finance_control,
    _money,
    _normalise_renderer_state,
    _predicted_gap_note,
    _render_bookkeeping,
    _render_out_metric,
)

AS_OF = date(2026, 7, 13)
SUMMARY_DAYS = 14
FLOOR_CENTS = 1_000_000


def _payable(event_id: str, *, amount_cents: int, days_out: int) -> dict:
    """A bill somebody has actually sent."""
    return {
        "id": event_id,
        "record_kind": "obligation",
        "source": "clickup",
        "event_type": "outflow",
        "category": "rent",
        "name": event_id,
        "vendor_or_customer": event_id,
        "amount_cents": amount_cents,
        "open_amount_cents": amount_cents,
        "due_date": AS_OF + timedelta(days=days_out),
        "status": "planned",
        "confidence": "confirmed",
        "pay_priority": "must_pay",
        "flexibility": "fixed",
    }


def _predicted_bill(
    event_id: str, *, amount_cents: int, days_out: int, probability_bps: int = 6_000
) -> dict:
    """A bill only the bank history says is coming."""
    return {
        "id": event_id,
        "record_kind": "obligation",
        "source": "bill_trend",
        "event_type": "outflow",
        "category": "utilities",
        "name": event_id,
        "vendor_or_customer": event_id,
        "amount_cents": amount_cents,
        "open_amount_cents": amount_cents,
        "due_date": AS_OF + timedelta(days=days_out),
        "status": "planned",
        "confidence": "medium",
        "probability_bps": probability_bps,
        "trend_inferred": True,
        "read_only": True,
    }


def _cash(rows: list[dict], *, balance_cents: int) -> dict:
    """The cash figures the Today card reads, assembled the way control does.

    The outgoing totals come from the real summary maths rather than numbers
    typed into the test, so a change in how a prediction is weighted shows up
    here instead of passing quietly.
    """
    metrics = _summary_metrics(rows, AS_OF, SUMMARY_DAYS)
    after_required = balance_cents - metrics["required_outgoing_cents"]
    after_expected = after_required - metrics["expected_outgoing_cents"]
    payload = {
        "cash_position": {
            **metrics,
            "cash_on_hand_cents": balance_cents,
            "balance_available": True,
            "floor_cents": FLOOR_CENTS,
            "incoming_confirmed_cents": metrics["confirmed_incoming_cents"],
            "incoming_expected_cents": metrics["expected_incoming_cents"],
            "required_out_cents": metrics["required_outgoing_cents"],
            "expected_out_cents": metrics["expected_outgoing_cents"],
            "exposure_out_cents": metrics["outgoing_exposure_cents"],
            "safe_to_commit_cents": max(0, after_required - FLOOR_CENTS),
            "funding_gap_cents": max(0, FLOOR_CENTS - after_expected),
            "funding_gap_required_only_cents": max(0, FLOOR_CENTS - after_required),
        }
    }
    fallback = _fallback_finance_control([], 0, "", AS_OF)
    return _normalise_renderer_state(payload, fallback)["cash"]


def test_today_card_splits_what_is_owed_from_what_is_predicted():
    cash = _cash(
        [
            _payable("rent", amount_cents=400_000, days_out=3),
            _predicted_bill("power", amount_cents=200_000, days_out=5),
        ],
        balance_cents=500_000,
    )

    card = _render_out_metric(cash)

    assert f'<span>Committed</span><strong class="amount-out">{_money(400_000)}</strong>' in card
    assert f"<span>Expected out</span><strong>{_money(120_000)}</strong>" in card
    assert f"<span>Total out</span><strong>{_money(520_000)}</strong>" in card


def test_total_out_is_the_committed_and_predicted_figures_added():
    cash = _cash(
        [
            _payable("rent", amount_cents=400_000, days_out=3),
            _predicted_bill("power", amount_cents=200_000, days_out=5),
            _predicted_bill("phone", amount_cents=50_000, days_out=9),
        ],
        balance_cents=500_000,
    )
    total = cash["required_out_cents"] + cash["expected_out_cents"]

    card = _render_out_metric(cash)

    assert cash["expected_out_cents"] == 150_000, "each prediction is weighted, not counted whole"
    assert total == 550_000
    assert f"<span>Total out</span><strong>{_money(total)}</strong>" in card


def test_the_gap_says_how_much_of_itself_is_only_predicted():
    cash = _cash(
        [
            _payable("rent", amount_cents=400_000, days_out=3),
            _predicted_bill("power", amount_cents=200_000, days_out=5),
        ],
        balance_cents=500_000,
    )

    note = _predicted_gap_note(cash)

    assert cash["funding_gap_cents"] == 1_020_000
    assert f"{_money(120_000)} of this shortfall" in note
    assert "bills your bank history says are coming" in note
    assert 'href="/admin/finances/whats-coming"' in note


def test_a_prediction_with_no_shortfall_still_says_it_is_set_aside():
    cash = _cash(
        [_predicted_bill("power", amount_cents=200_000, days_out=5)],
        balance_cents=9_000_000,
    )

    note = _predicted_gap_note(cash)

    assert cash["funding_gap_cents"] == 0
    assert f"already sets aside {_money(120_000)}" in note
    assert 'href="/admin/finances/whats-coming"' in note


def test_nothing_extra_is_added_when_no_bill_is_predicted():
    cash = _cash([_payable("rent", amount_cents=400_000, days_out=3)], balance_cents=500_000)

    assert cash["expected_out_cents"] == 0
    assert _predicted_gap_note(cash) == "", "a state he is in today gets no extra line"

    card = _render_out_metric(cash)
    assert f"<span>Expected out</span><strong>{_money(0)}</strong>" in card
    assert f"<span>Total out</span><strong>{_money(400_000)}</strong>" in card


def test_the_new_card_is_the_one_the_page_renders():
    """Guards against the helpers being right while the page still shows the old
    single figure."""
    import inspect

    from sales_support_agent.services.cashflow.overview import render_cashflow_overview_page

    source = inspect.getsource(render_cashflow_overview_page)
    assert "_render_out_metric(" in source
    assert "_predicted_gap_note(" in source

    # The heading stays "Required out 14 days": it is the landmark the operator
    # and several page tests both navigate by. What matters is that the card the
    # page shows is the new three-line one, so assert on the rendered output
    # rather than on the source, which also mentions "Committed" in a chart legend.
    card = _render_out_metric({
        "required_out_cents": 60_673_00,
        "expected_out_cents": 23_120_00,
        "exposure_out_cents": 0,
    })
    assert card.count("Required out 14 days") == 1
    for line in ("Committed", "Expected out", "Total out"):
        assert line in card, line
    assert _money(83_793_00) in card, "the total must be the two added together"


def test_the_nav_offers_the_predicted_bills_page_on_every_finance_page():
    strip = render_finance_nav("today", counts={})

    assert "/admin/finances/whats-coming" in strip
    assert "What is coming" in strip
    assert ("whats_coming", "What is coming", "/admin/finances/whats-coming") in NAV_ITEMS


def test_the_nav_badges_only_the_predicted_bills_still_waiting_on_him():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)

    def fake_patterns(**_kwargs):
        return {
            "patterns": [],
            "tracked": [],
            "counts": {
                "patterns": 4, "tracked": 1, "confirmed": 2,
                "unreviewed": 2, "dismissed": 0, "snoozed": 0,
                "monthly_cost_cents": 0,
            },
        }

    original = bill_patterns.list_bill_patterns
    try:
        bill_patterns.list_bill_patterns = fake_patterns
        counts = nav_counts()
    finally:
        bill_patterns.list_bill_patterns = original

    assert counts["whats_coming"] == 2, "answered predictions must not keep badging"
    assert ">2<" in render_finance_nav("today", counts=counts)


def test_a_broken_count_never_takes_the_nav_down():
    def explode(**_kwargs):
        raise RuntimeError("bank history unavailable")

    original = bill_patterns.list_bill_patterns
    try:
        bill_patterns.list_bill_patterns = explode
        counts = nav_counts()
    finally:
        bill_patterns.list_bill_patterns = original

    assert "whats_coming" not in counts
    strip = render_finance_nav("today", counts=counts)
    assert "/admin/finances/whats-coming" in strip, "the link survives a dead count"


def test_the_filing_queue_says_money_coming_in_is_handled_elsewhere():
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    now = datetime.now(timezone.utc)
    with engine.begin() as connection:
        insert_cash_event(
            connection, id="spend1", source="plaid", source_id="spend1",
            record_kind="transaction", event_type="outflow", category="uncategorized",
            name="Madison Bicycle Shop", vendor_or_customer="Madison Bicycle Shop",
            description="Madison Bicycle Shop", amount_cents=500_00,
            due_date=AS_OF - timedelta(days=2), status="posted",
            confidence="confirmed", created_at=now, updated_at=now,
        )

    section = _render_bookkeeping()

    assert "Only money going out is sorted here." in section
    assert "Money coming in is not in this list" in section
    assert 'href="/admin/finances/collections"' in section, "it says where instead"


def test_the_card_says_when_a_tracked_bill_lands_after_the_fortnight():
    """Boulder Ranch was confirmed and the 14 day figure stayed at zero, so the
    click looked like it had done nothing. The card has to account for it."""
    card = _render_out_metric({
        "required_out_cents": 14_758_00,
        "expected_out_cents": 0,
        "expected_out_later_cents": 38_735_00,
        "exposure_out_cents": 2_284_00,
    })

    assert _money(38_735_00) in card
    assert "lands after the next 14 days" in card


def test_the_card_stays_quiet_when_nothing_is_predicted_later():
    card = _render_out_metric({
        "required_out_cents": 14_758_00,
        "expected_out_cents": 0,
        "expected_out_later_cents": 0,
        "exposure_out_cents": 2_284_00,
    })

    assert "lands after the next 14 days" not in card
