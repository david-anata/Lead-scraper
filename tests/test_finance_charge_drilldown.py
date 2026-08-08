"""Opening a weekly total to see the charges behind it.

The rule that matters: the listed charges must add up to exactly the figure that
was clicked. Two numbers describing the same money is the shape that has
produced every wrong answer in this section.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from sales_support_agent.services.cashflow.charge_drilldown import (
    build_charge_drilldown,
    cadence_to_decision,
    render_charge_panel,
)

MONDAY = date(2026, 8, 10)


def _event(vendor, cents, status, *, kind="planned"):
    return {"name": vendor, "vendor_or_customer": vendor, "amount_cents": cents,
            "payment_status": status, "kind": kind, "category": "utilities",
            "state_label": "Unpaid planned", "href": "", "protected": False}


def _calendar(by_day):
    return {"days": [{"date": day, "events": events} for day, events in by_day.items()]}


def _week(**kw):
    return build_charge_drilldown(
        _calendar(kw.pop("by_day")), week_start=MONDAY, state=kw.pop("state", "unpaid"), **kw
    )


def test_the_charges_add_up_to_the_figure_that_was_clicked():
    view = _week(by_day={
        (MONDAY + timedelta(days=1)).isoformat(): [_event("Lehi City Power", 89_000, "unpaid")],
        (MONDAY + timedelta(days=3)).isoformat(): [_event("Bear River", 56_500, "unpaid")],
        (MONDAY + timedelta(days=5)).isoformat(): [_event("Boulder Ranch", 205_600, "unpaid")],
    })

    assert view["count"] == 3
    assert view["total_cents"] == 89_000 + 56_500 + 205_600
    assert sum(c["amount_cents"] for c in view["charges"]) == view["total_cents"]


def test_each_column_shows_only_its_own_charges():
    by_day = {(MONDAY + timedelta(days=1)).isoformat(): [
        _event("Paid thing", 10_000, "paid"),
        _event("Owed thing", 20_000, "unpaid"),
        _event("Guessed thing", 30_000, "unconfirmed", kind="history_warning"),
    ]}

    assert _week(by_day=by_day, state="paid")["total_cents"] == 10_000
    assert _week(by_day=by_day, state="unpaid")["total_cents"] == 20_000
    assert _week(by_day=by_day, state="possible")["total_cents"] == 30_000


def test_charges_outside_the_week_are_excluded():
    by_day = {
        (MONDAY - timedelta(days=1)).isoformat(): [_event("Last week", 99_000, "unpaid")],
        (MONDAY + timedelta(days=2)).isoformat(): [_event("This week", 11_000, "unpaid")],
        (MONDAY + timedelta(days=7)).isoformat(): [_event("Next week", 99_000, "unpaid")],
    }

    view = _week(by_day=by_day)

    assert view["count"] == 1
    assert view["charges"][0]["vendor"] == "This week"


def test_timestamps_from_the_database_are_handled():
    """The same trap that broke the paydown plan against the live ledger."""
    by_day = {datetime(2026, 8, 12, 9, 30): [_event("Lehi City Power", 89_000, "unpaid")]}

    assert _week(by_day=by_day)["count"] == 1


def test_a_bad_state_is_refused_rather_than_guessed():
    with pytest.raises(ValueError):
        build_charge_drilldown(_calendar({}), week_start=MONDAY, state="whatever")


# --- what the operator can say --------------------------------------------

def test_naming_a_cadence_is_the_same_act_as_tracking_the_bill():
    """One decision store, two places to make it. A separate cadence flag here
    would give two sources of truth about one charge."""
    assert cadence_to_decision("monthly") == "track"
    assert cadence_to_decision("weekly") == "track"


def test_one_time_and_not_a_bill_stay_different():
    """David asked for both. One keeps watching the supplier, the other stops
    suggesting them at all."""
    assert cadence_to_decision("one_time") != cadence_to_decision("not_a_bill")
    assert cadence_to_decision("not_a_bill") == "not_a_bill"


def test_an_unknown_answer_is_refused():
    with pytest.raises(ValueError):
        cadence_to_decision("sometimes")


# --- the panel ------------------------------------------------------------

def test_the_panel_offers_all_four_answers_per_charge():
    view = _week(by_day={
        (MONDAY + timedelta(days=1)).isoformat(): [_event("Lehi City Power", 89_000, "unpaid")]
    })

    panel = render_charge_panel(view, action="/admin/finances/calendar/charges/answer")

    for label in ("Monthly", "Weekly", "One-time", "Not a bill"):
        assert label in panel, label
    assert "$890" in panel
    assert "Lehi City Power" in panel


def test_the_panel_total_is_shown_and_matches():
    view = _week(by_day={
        (MONDAY + timedelta(days=1)).isoformat(): [_event("A", 89_000, "unpaid")],
        (MONDAY + timedelta(days=2)).isoformat(): [_event("B", 11_000, "unpaid")],
    })

    panel = render_charge_panel(view, action="/x")

    assert "$1,000" in panel, "the panel must state the same total it was opened from"


def test_the_panel_says_when_it_knows_nothing_about_a_supplier():
    view = _week(by_day={
        (MONDAY + timedelta(days=1)).isoformat(): [_event("Brand New Vendor", 25_000, "unpaid")]
    })

    panel = render_charge_panel(view, action="/x")

    assert "No pattern found yet" in panel


def test_an_empty_week_says_so_rather_than_rendering_an_empty_list():
    panel = render_charge_panel(_week(by_day={}), action="/x")

    assert "Nothing in this week" in panel


def test_panel_copy_carries_no_em_dashes():
    view = _week(by_day={
        (MONDAY + timedelta(days=1)).isoformat(): [_event("Lehi City Power", 89_000, "unpaid")]
    })

    assert "—" not in render_charge_panel(view, action="/x")
