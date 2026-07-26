"""Predicted bills count against forward cash and never against history.

David's decision: a predicted bill counts against the cash in the forecast, but
history stays on actuals only. These tests hold both halves of that line.
"""

from __future__ import annotations

import sys
import types
from copy import deepcopy
from datetime import date, timedelta

from sales_support_agent.services.cashflow.control import (
    _summary_metrics,
    build_finance_control,
    build_finance_control_state,
    build_forecast_paths,
)


AS_OF = date(2026, 7, 13)
SUMMARY_DAYS = 14
BILL_PATTERNS = "sales_support_agent.services.cashflow.bill_patterns"

# Backward-looking figures. Nothing a prediction does may move any of these.
HISTORY_KEYS = ("historical_backlog_cents", "historical_backlog_count")


def _payable(event_id: str, *, amount_cents: int, days_out: int, **overrides) -> dict:
    row = {
        "id": event_id,
        "record_kind": "obligation",
        "source": "clickup",
        "event_type": "outflow",
        "category": "software",
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
    row.update(overrides)
    return row


def _predicted_bill(
    event_id: str,
    *,
    amount_cents: int,
    days_out: int,
    probability_bps: int = 6_000,
) -> dict:
    """A row shaped like the bill pattern module's output."""
    return {
        "id": event_id,
        "record_kind": "obligation",
        "source": "bill_trend",
        "source_label": "Bill pattern",
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
        "bill_trend": True,
        "read_only": True,
    }


def _bank(event_id: str, days_ago: int, amount: int, **overrides) -> dict:
    row = {
        "id": event_id,
        "record_kind": "transaction",
        "source": "csv",
        "source_id": event_id,
        "event_type": "inflow",
        "category": "revenue",
        "vendor_or_customer": "Acme",
        "amount_cents": amount,
        "due_date": AS_OF - timedelta(days=days_ago),
        "status": "posted",
        "confidence": "confirmed",
    }
    row.update(overrides)
    return row


def _history(balance_cents: int) -> list[dict]:
    rows = []
    for index, days_ago in enumerate((0, 7, 14, 21, 28, 35, 42, 49, 56)):
        rows.append(
            _bank(
                f"bank-{index}",
                days_ago,
                30_000 + index * 100,
                account_balance_cents=balance_cents if index == 0 else None,
                event_type="inflow" if index % 2 == 0 else "outflow",
            )
        )
    return rows


def _install_bill_patterns(monkeypatch, projections) -> None:
    """Stand in for the bill pattern module until it is imported for real."""
    module = types.ModuleType(BILL_PATTERNS)

    def confirmed_bill_projections(*, as_of, horizon_days):
        if callable(projections):
            return projections(as_of=as_of, horizon_days=horizon_days)
        return [deepcopy(row) for row in projections]

    module.confirmed_bill_projections = confirmed_bill_projections
    monkeypatch.setitem(sys.modules, BILL_PATTERNS, module)


def test_predicted_bill_lands_in_expected_and_leaves_required_alone():
    real = _payable("real-rent", amount_cents=400_000, days_out=3)
    predicted = _predicted_bill("predicted-power", amount_cents=200_000, days_out=5)

    without = _summary_metrics([real], AS_OF, SUMMARY_DAYS)
    with_prediction = _summary_metrics([real, predicted], AS_OF, SUMMARY_DAYS)

    assert without["expected_outgoing_cents"] == 0
    assert with_prediction["expected_outgoing_cents"] > 0
    assert with_prediction["required_outgoing_cents"] == without["required_outgoing_cents"] == 400_000


def test_predicted_bill_is_weighted_by_its_chance_not_its_face_value():
    predicted = _predicted_bill(
        "predicted-insurance", amount_cents=500_000, days_out=6, probability_bps=6_000
    )

    metrics = _summary_metrics([predicted], AS_OF, SUMMARY_DAYS)

    assert metrics["expected_outgoing_cents"] == 300_000
    assert metrics["expected_outgoing_cents"] != 500_000
    assert metrics["required_outgoing_cents"] == 0


def test_history_is_byte_identical_with_and_without_predictions():
    rows = [
        _payable("stale-scheduler-entry", amount_cents=750_000, days_out=-200),
        _payable("real-payroll", amount_cents=300_000, days_out=2),
    ]
    predictions = [
        _predicted_bill("predicted-power", amount_cents=200_000, days_out=4),
        _predicted_bill("predicted-rent", amount_cents=900_000, days_out=11),
        # A prediction dated in the deep past is still a prediction, never
        # history, however badly the pattern module dated it.
        _predicted_bill("predicted-misdated", amount_cents=650_000, days_out=-300),
    ]

    without = _summary_metrics(rows, AS_OF, SUMMARY_DAYS)
    with_predictions = _summary_metrics([*rows, *predictions], AS_OF, SUMMARY_DAYS)

    assert without["historical_backlog_cents"] == 750_000
    assert without["historical_backlog_count"] == 1
    for key in HISTORY_KEYS:
        assert with_predictions[key] == without[key], key
    assert with_predictions["expected_outgoing_cents"] > 0


def test_full_build_leaves_every_backward_looking_figure_untouched(monkeypatch):
    rows = [
        *_history(1_000_000),
        _payable("stale-scheduler-entry", amount_cents=750_000, days_out=-200),
        _payable("real-payroll", amount_cents=300_000, days_out=2),
    ]

    baseline = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    _install_bill_patterns(
        monkeypatch,
        [
            _predicted_bill("predicted-power", amount_cents=240_000, days_out=4),
            _predicted_bill("predicted-misdated", amount_cents=650_000, days_out=-300),
        ],
    )
    forecast = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    assert baseline["metrics"]["historical_backlog_cents"] == 750_000
    for key in HISTORY_KEYS:
        assert forecast["metrics"][key] == baseline["metrics"][key], key
    assert forecast["trends"] == baseline["trends"]
    assert forecast["cash_snapshot"] == baseline["cash_snapshot"]
    assert forecast["reconciliation_shadow"] == baseline["reconciliation_shadow"]
    assert forecast["metrics"]["required_outgoing_cents"] == baseline["metrics"]["required_outgoing_cents"]
    assert forecast["metrics"]["expected_outgoing_cents"] > 0


def test_funding_gap_moves_by_the_expected_amount(monkeypatch):
    rows = [
        *_history(500_000),
        _payable("real-payroll", amount_cents=450_000, days_out=1),
    ]

    baseline = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    _install_bill_patterns(
        monkeypatch,
        [
            _predicted_bill(
                "predicted-rent", amount_cents=200_000, days_out=6, probability_bps=7_500
            )
        ],
    )
    forecast = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    expected_out = forecast["metrics"]["expected_outgoing_cents"]
    assert expected_out == 150_000
    assert forecast["metrics"]["funding_gap_cents"] == baseline["metrics"]["funding_gap_cents"] + expected_out
    assert forecast["metrics"]["funding_gap_required_only_cents"] == baseline["metrics"]["funding_gap_cents"]
    assert (
        forecast["metrics"]["cash_after_expected_outgoing_cents"]
        == forecast["metrics"]["cash_after_required_outgoing_cents"] - expected_out
    )


def test_a_plausible_predicted_bill_does_not_produce_an_absurd_total(monkeypatch):
    # One $2,400 bill, 80% likely. The forward numbers must move by $1,920 and
    # by nothing else: an earlier bug in this project shipped 20x figures while
    # every structural check still passed.
    face = 240_000
    rows = [*_history(1_000_000), _payable("real-payroll", amount_cents=300_000, days_out=2)]

    baseline = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    _install_bill_patterns(
        monkeypatch,
        [_predicted_bill("predicted-power", amount_cents=face, days_out=4, probability_bps=8_000)],
    )
    forecast = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    metrics = forecast["metrics"]
    assert metrics["expected_outgoing_cents"] == 192_000
    assert metrics["expected_outgoing_cents"] < face
    assert (
        metrics["cash_after_expected_outgoing_cents"]
        == baseline["metrics"]["cash_after_required_outgoing_cents"] - 192_000
    )
    # The predicted bill is the only thing that changed, so no other outgoing
    # total may grow at all.
    assert metrics["required_outgoing_cents"] == baseline["metrics"]["required_outgoing_cents"]
    assert metrics["outgoing_exposure_cents"] == baseline["metrics"]["outgoing_exposure_cents"]


def test_a_failing_bill_pattern_module_leaves_the_page_working(monkeypatch, caplog):
    rows = [*_history(1_000_000), _payable("real-payroll", amount_cents=300_000, days_out=2)]

    baseline = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    def explode(*, as_of, horizon_days):
        raise RuntimeError("pattern detection failed")

    _install_bill_patterns(monkeypatch, explode)
    with caplog.at_level("WARNING"):
        forecast = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    assert forecast["metrics"]["expected_outgoing_cents"] == 0
    assert forecast["metrics"]["funding_gap_cents"] == baseline["metrics"]["funding_gap_cents"]
    assert forecast["metrics"]["required_outgoing_cents"] == baseline["metrics"]["required_outgoing_cents"]
    assert forecast["queue"]["items"] == baseline["queue"]["items"]
    assert "Predicted bills unavailable" in caplog.text


def test_predicted_bill_misses_committed_line_and_lowers_expected_line():
    real = _payable("real-payroll", amount_cents=300_000, days_out=2)
    predicted = _predicted_bill(
        "predicted-rent", amount_cents=200_000, days_out=5, probability_bps=5_000
    )

    baseline = build_forecast_paths([real], as_of=AS_OF, starting_cash_cents=1_000_000)
    forecast = build_forecast_paths(
        [real, predicted], as_of=AS_OF, starting_cash_cents=1_000_000
    )

    assert forecast["minimum_committed_cash_cents"] == baseline["minimum_committed_cash_cents"]
    assert forecast["minimum_expected_cash_cents"] == baseline["minimum_expected_cash_cents"] - 100_000
    assert forecast["minimum_stress_cash_cents"] == baseline["minimum_stress_cash_cents"] - 200_000


def test_renderer_payload_keeps_fact_and_forecast_apart(monkeypatch):
    rows = [*_history(1_000_000), _payable("real-payroll", amount_cents=300_000, days_out=2)]
    _install_bill_patterns(
        monkeypatch,
        [_predicted_bill("predicted-power", amount_cents=200_000, days_out=4, probability_bps=5_000)],
    )

    state = build_finance_control(rows, as_of=AS_OF, floor_cents=100_000)

    position = state["cash_position"]
    assert position["required_out_cents"] == 300_000
    assert position["expected_out_cents"] == 100_000
    assert position["historical_backlog_cents"] == 0


def test_safe_to_commit_and_the_funding_gap_can_never_both_be_positive(monkeypatch):
    """They were computed from different cash. The page could say $1,000 was safe
    to commit while also saying there was a $4,000 shortfall, and the assistant
    was handed both figures in one packet."""
    rows = [
        *_history(1_100_000),
        _payable("real-bill", amount_cents=100_000, days_out=2),
    ]
    _install_bill_patterns(
        monkeypatch,
        [
            _predicted_bill(
                "predicted-big", amount_cents=500_000, days_out=5, probability_bps=10_000
            )
        ],
    )

    metrics = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=900_000)["metrics"]

    assert metrics["expected_outgoing_cents"] == 500_000, "the prediction must really be counted"
    assert metrics["funding_gap_cents"] > 0, "this scenario is genuinely short"
    assert metrics["safe_to_commit_cents"] == 0, (
        "nothing is safe to commit while the same cash is already short"
    )
    assert not (metrics["safe_to_commit_cents"] > 0 and metrics["funding_gap_cents"] > 0)


def test_what_is_committable_ignoring_predictions_is_still_reported(monkeypatch):
    """Both readings stay available so the page can explain the difference rather
    than silently choosing one."""
    rows = [
        *_history(1_100_000),
        _payable("real-bill", amount_cents=100_000, days_out=2),
    ]
    _install_bill_patterns(
        monkeypatch,
        [
            _predicted_bill(
                "predicted-big", amount_cents=500_000, days_out=5, probability_bps=10_000
            )
        ],
    )

    metrics = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=900_000)["metrics"]

    assert metrics["safe_to_commit_required_only_cents"] == 100_000
    assert metrics["safe_to_commit_cents"] == 0


def test_a_tracked_bill_beyond_the_fortnight_is_reported_not_buried(monkeypatch):
    """David confirmed Boulder Ranch, due 28 days out, and nothing he was looking
    at moved: Expected out stayed at zero and the amount went silently into the
    due-later figure alongside real commitments. A correct result read as a
    broken button."""
    rows = [
        *_history(5_000_000),
        _payable("real-soon", amount_cents=100_000, days_out=3),
    ]
    _install_bill_patterns(
        monkeypatch,
        [
            _predicted_bill(
                "predicted-rent", amount_cents=3_873_500, days_out=28, probability_bps=10_000
            )
        ],
    )

    metrics = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)["metrics"]

    assert metrics["expected_outgoing_cents"] == 0, "28 days out is not inside a fortnight"
    assert metrics["expected_outgoing_later_cents"] == 3_873_500
    assert metrics["outgoing_exposure_cents"] == 0, (
        "a forecast must not be mixed into money that is genuinely owed later"
    )


def test_a_prediction_beyond_the_window_does_not_move_the_funding_gap(monkeypatch):
    """It is not in the fortnight, so it must not change the fortnight's shortfall."""
    rows = [*_history(500_000), _payable("real", amount_cents=450_000, days_out=1)]
    baseline = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    _install_bill_patterns(
        monkeypatch,
        [_predicted_bill("far-off", amount_cents=900_000, days_out=40, probability_bps=10_000)],
    )
    forecast = build_finance_control_state(rows, [], as_of=AS_OF, floor_cents=100_000)

    assert forecast["metrics"]["funding_gap_cents"] == baseline["metrics"]["funding_gap_cents"]
    assert forecast["metrics"]["expected_outgoing_later_cents"] == 900_000
