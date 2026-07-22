from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import re

from sales_support_agent.services.cashflow.control import (
    annotate_open_amounts,
    build_finance_control_state,
    build_finance_control,
    build_forecast_paths,
    build_collections_summary,
    build_queue,
    calculate_csv_trends,
    derive_csv_income_projections,
    quick_action_eligibility,
    resolve_cash_snapshot,
)


AS_OF = date(2026, 7, 13)


def _row(event_id: str, **overrides):
    row = {
        "id": event_id,
        "record_kind": "obligation",
        "source": "manual",
        "event_type": "outflow",
        "category": "software",
        "name": event_id,
        "vendor_or_customer": event_id,
        "amount_cents": 100_00,
        "due_date": AS_OF,
        "status": "planned",
        "confidence": "confirmed",
        "pay_priority": "should_pay",
        "flexibility": "fixed",
    }
    row.update(overrides)
    return row


def _bank(event_id: str, days_ago: int, amount: int = 100_00, **overrides):
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


def _history(balance_cents: int = 200_000):
    rows = []
    for index, days_ago in enumerate((0, 7, 14, 21, 28, 35, 42, 49, 56)):
        rows.append(
            _bank(
                f"bank-{index}",
                days_ago,
                amount=30_000 + index * 100,
                account_balance_cents=balance_cents if index == 0 else None,
                event_type="inflow" if index % 2 == 0 else "outflow",
            )
        )
    return rows


def test_zero_balance_is_available_and_newest_first_same_day_wins():
    rows = [
        _bank("closing", 0, account_balance_cents=0),
        _bank("earlier-same-day", 0, account_balance_cents=99_999),
        _bank("older", 1, account_balance_cents=80_000),
    ]

    snapshot = resolve_cash_snapshot(rows, as_of=AS_OF)

    assert snapshot["available"] is True
    assert snapshot["balance_cents"] == 0
    assert snapshot["as_of_date"] == AS_OF.isoformat()


def test_allocations_derive_open_amount_and_reversal_does_not_close_early():
    rows = [_row("rent", amount_cents=100_000, status="paid")]
    allocations = [
        {"id": "a1", "obligation_event_id": "rent", "amount_cents": 40_000},
        {"id": "r1", "obligation_event_id": "rent", "amount_cents": -40_000, "reversed_allocation_id": "a1"},
        {"id": "a2", "obligation_event_id": "rent", "amount_cents": 20_000},
    ]

    result = annotate_open_amounts(rows, allocations)

    assert result[0]["settled_amount_cents"] == 20_000
    assert result[0]["open_amount_cents"] == 80_000


def test_qbo_open_balance_disagreement_is_resolve_first_and_not_forecast():
    invoice = _row(
        "qbo-invoice",
        source="qbo",
        event_type="inflow",
        amount_cents=100_000,
        source_open_amount_cents=25_000,
        due_date=AS_OF + timedelta(days=1),
    )

    annotated = annotate_open_amounts([invoice], [])
    state = build_finance_control_state(
        _history(200_000) + [invoice],
        settlement_annotations=[],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert annotated[0]["amount_cents"] == 100_000
    assert annotated[0]["local_open_amount_cents"] == 100_000
    assert annotated[0]["source_open_amount_cents"] == 25_000
    assert annotated[0]["source_open_disagreement"] is True
    queue_item = next(item for item in state["queue"]["items"] if item["id"] == "qbo-invoice")
    assert queue_item["group"] == "resolve_first"
    assert queue_item["decision_blocker"] == "source_open_disagreement"
    assert state["metrics"]["confirmed_incoming_cents"] == 0
    assert state["forecast"]["paths"]["committed"][-1]["cash_cents"] == 200_000


def test_qbo_partial_balance_is_forecast_once_allocation_evidence_agrees():
    invoice = _row(
        "qbo-invoice",
        source="qbo",
        event_type="inflow",
        amount_cents=100_000,
        source_open_amount_cents=25_000,
        due_date=AS_OF + timedelta(days=1),
    )

    state = build_finance_control_state(
        _history(200_000) + [invoice],
        settlement_annotations={"qbo-invoice": 75_000},
        as_of=AS_OF,
        floor_cents=100_000,
    )

    queue_item = next(item for item in state["queue"]["items"] if item["id"] == "qbo-invoice")
    assert queue_item["decision_blocker"] is None
    assert queue_item["open_amount_cents"] == 25_000
    assert state["metrics"]["confirmed_incoming_cents"] == 25_000
    assert state["forecast"]["paths"]["committed"][-1]["cash_cents"] == 225_000


def test_qbo_api_and_open_invoices_csv_do_not_double_count_the_same_receivable():
    api_invoice = _row(
        "qbo-inv-42",
        source="qbo",
        source_id="qbo-42",
        event_type="inflow",
        amount_cents=100_000,
        source_open_amount_cents=25_000,
        vendor_or_customer="Acme",
        description="Invoice #INV-42",
        due_date=AS_OF + timedelta(days=1),
    )
    csv_invoice = _row(
        "qbo-csv-42",
        source="qbo-csv",
        source_id="qbo-ar-INV-42",
        event_type="inflow",
        amount_cents=25_000,
        source_open_amount_cents=25_000,
        vendor_or_customer="Acme",
        description="Invoice #INV-42",
        due_date=AS_OF + timedelta(days=1),
    )

    state = build_finance_control_state(
        _history(200_000) + [api_invoice, csv_invoice],
        settlement_annotations={"qbo-inv-42": 75_000},
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["metrics"]["confirmed_incoming_cents"] == 25_000
    assert state["collections"]["collectible_14d_cents"] == 25_000
    assert state["data_quality"]["cross_source_receivable_duplicates"] == 1


def test_qbo_open_invoices_csv_is_visible_in_collections_without_api_data():
    csv_invoice = _row(
        "qbo-csv-43",
        source="qbo-csv",
        source_id="qbo-ar-INV-43",
        event_type="inflow",
        amount_cents=25_000,
        source_open_amount_cents=25_000,
        vendor_or_customer="Acme",
        description="Invoice #INV-43",
        due_date=AS_OF - timedelta(days=2),
    )

    state = build_finance_control_state(
        _history(200_000) + [csv_invoice],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["collections"]["overdue_open_cents"] == 25_000
    assert state["collections"]["targets"][0]["invoice_reference"] == "Invoice #INV-43"


def test_collections_summary_surfaces_only_evidence_safe_qbo_receivables():
    overdue = _row(
        "qbo-inv-overdue",
        source="qbo",
        source_id="qbo-inv-overdue",
        event_type="inflow",
        amount_cents=100_000,
        source_open_amount_cents=40_000,
        due_date=AS_OF - timedelta(days=4),
        description="Invoice #INV-42",
    )
    due_soon = _row(
        "qbo-inv-soon",
        source="qbo",
        source_id="qbo-inv-soon",
        event_type="inflow",
        amount_cents=75_000,
        source_open_amount_cents=75_000,
        due_date=AS_OF + timedelta(days=2),
        description="Invoice #INV-43",
    )
    conflict = _row(
        "qbo-inv-conflict",
        source="qbo",
        event_type="inflow",
        amount_cents=50_000,
        source_open_amount_cents=10_000,
        due_date=AS_OF - timedelta(days=1),
    )
    canonical = annotate_open_amounts(
        [overdue, due_soon, conflict], {"qbo-inv-overdue": 60_000}
    )

    collections = build_collections_summary(
        canonical, as_of=AS_OF, horizon_days=14, funding_gap_cents=80_000
    )

    assert [item["id"] for item in collections["targets"]] == [
        "qbo-inv-overdue", "qbo-inv-soon"
    ]
    assert collections["targets"][0]["invoice_reference"] == "Invoice #INV-42"
    assert collections["overdue_open_cents"] == 40_000
    assert collections["due_soon_open_cents"] == 75_000
    assert collections["due_soon_count"] == 1
    assert collections["collectible_14d_cents"] == 115_000
    assert collections["gap_cover_cents"] == 80_000
    assert collections["remaining_gap_after_collections_cents"] == 0
    assert collections["review_count"] == 1
    assert collections["review_cents"] == 50_000
    assert collections["next_collection"]["id"] == "qbo-inv-overdue"


def test_metrics_reserve_every_bill_due_in_window_and_separate_later_exposure():
    rows = _history(300_000) + [
        _row("confirmed-ar", event_type="inflow", amount_cents=80_000),
        _row("expected-ar", event_type="inflow", amount_cents=60_000, confidence="medium"),
        _row("payroll", amount_cents=90_000, pay_priority="must_pay"),
        _row("software", amount_cents=30_000, pay_priority="can_hold", flexibility="deferrable"),
        _row("later", amount_cents=50_000, due_date=AS_OF + timedelta(days=20)),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)

    assert state["metrics"]["cash_on_hand_cents"] == 300_000
    assert state["metrics"]["confirmed_incoming_cents"] == 80_000
    assert state["metrics"]["expected_incoming_cents"] == 30_000
    assert state["metrics"]["required_outgoing_cents"] == 120_000
    assert state["metrics"]["outgoing_exposure_cents"] == 50_000


def test_forecast_paths_treat_confirmed_expected_and_flexible_cash_differently():
    rows = [
        _row("confirmed", event_type="inflow", amount_cents=100_000, due_date=AS_OF + timedelta(days=1)),
        _row("trend", event_type="inflow", amount_cents=100_000, confidence="medium", probability_bps=5_000, trend_inferred=True),
        _row("must", amount_cents=80_000, pay_priority="must_pay"),
        _row(
            "chunk",
            amount_cents=60_000,
            flexibility="chunkable",
            pay_priority="can_hold",
            payment_installments=[{"amount_cents": 20_000, "due_date": AS_OF, "status": "planned"}],
        ),
    ]

    result = build_forecast_paths(rows, as_of=AS_OF, starting_cash_cents=100_000)

    assert result["paths"]["committed"][-1]["cash_cents"] == 100_000
    assert result["paths"]["expected"][-1]["cash_cents"] == 110_000
    assert result["paths"]["stress"][-1]["cash_cents"] == 100_000


def test_installment_schedule_is_capped_at_open_amount():
    rows = [
        _row(
            "chunk",
            amount_cents=100_000,
            flexibility="chunkable",
            pay_priority="can_hold",
            payment_installments=[
                {"amount_cents": 80_000, "due_date": AS_OF},
                {"amount_cents": 80_000, "due_date": AS_OF + timedelta(days=1)},
            ],
        )
    ]

    result = build_forecast_paths(rows, {"chunk": 25_000}, as_of=AS_OF, starting_cash_cents=200_000)

    assert result["paths"]["committed"][-1]["cash_cents"] == 125_000
    assert result["paths"]["stress"][-1]["cash_cents"] == 125_000


def test_csv_trends_exclude_transfer_duplicate_and_require_three_for_recurrence():
    rows = [
        _bank("r1", 42, vendor_or_customer="Acme", amount=50_000),
        _bank("r2", 21, vendor_or_customer="Acme", amount=51_000),
        _bank("r3", 0, vendor_or_customer="Acme", amount=49_000),
        _bank("transfer", 5, amount=999_000, category="transfer"),
        _bank("duplicate", 4, amount=999_000, probable_duplicate=True),
    ]

    trends = calculate_csv_trends(rows, as_of=AS_OF)

    assert trends["transaction_count"] == 3
    assert trends["excluded_count"] == 2
    assert trends["net_56_cents"] == 150_000
    assert trends["recurring_patterns"][0]["occurrences"] == 3
    assert trends["recurring_patterns"][0]["median_cadence_days"] == 21


def test_stale_balance_gates_recommendations_to_verification_actions():
    rows = _history() + [
        _row("must", amount_cents=50_000, pay_priority="must_pay", due_date=AS_OF),
    ]
    rows[0]["due_date"] = AS_OF - timedelta(days=10)

    state = build_finance_control_state(
        rows, as_of=AS_OF, floor_cents=100_000, balance_stale_after_days=3
    )

    assert state["confidence"]["verification_only"] is True
    assert state["recommendations"][0]["action_type"] == "refresh_cash_balance"
    assert all(
        rec["action_type"] in {"refresh_cash_balance", "review_income_patterns"}
        for rec in state["recommendations"]
    )


def test_missing_settlement_evidence_gates_recommendations_to_verification_actions():
    rows = _history(150_000) + [
        _row(
            "bill",
            event_type="outflow",
            amount_cents=50_000,
            due_date=AS_OF,
            status="planned",
            settlement_evidence_available=False,
        )
    ]

    state = build_finance_control_state(
        rows,
        settlement_annotations=None,
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["confidence"]["verification_only"] is True
    assert "settlement evidence is unavailable" in state["confidence"]["reasons"]
    assert state["recommendations"][0]["action_type"] == "resolve_finance_data"


def test_ranked_recommendations_collect_then_protect_cash():
    rows = _history(150_000) + [
        _row("must", amount_cents=90_000, pay_priority="must_pay", due_date=AS_OF),
        _row("receipt", event_type="inflow", amount_cents=40_000, due_date=AS_OF + timedelta(days=2)),
        _row(
            "flex",
            amount_cents=30_000,
            pay_priority="can_hold",
            flexibility="chunkable",
            payment_installments=[{"amount_cents": 30_000, "due_date": AS_OF}],
        ),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)

    action_types = [item["action_type"] for item in state["recommendations"]]
    assert state["metrics"]["funding_gap_cents"] == 70_000
    assert action_types == ["collect_confirmed_income", "split_or_defer_payable"]
    assert state["recommendations"][0]["before_minimum_cash_cents"] < state["recommendations"][0]["after_minimum_cash_cents"]


def test_safe_to_commit_and_funding_gap_are_never_negative():
    safe = build_finance_control_state(_history(200_000), as_of=AS_OF, floor_cents=100_000)
    gap = build_finance_control_state(_history(50_000), as_of=AS_OF, floor_cents=100_000)

    assert safe["metrics"]["safe_to_commit_cents"] == 100_000
    assert safe["metrics"]["funding_gap_cents"] == 0
    assert gap["metrics"]["safe_to_commit_cents"] == 0
    assert gap["metrics"]["funding_gap_cents"] == 50_000


def test_quick_actions_respect_must_pay_and_flexibility_rules():
    must_actions = {item["action_type"] for item in quick_action_eligibility(_row("must", pay_priority="must_pay", flexibility="chunkable"))}
    flexible_actions = {item["action_type"] for item in quick_action_eligibility(_row("flex", pay_priority="can_hold", flexibility="chunkable"))}
    incoming_actions = {item["action_type"] for item in quick_action_eligibility(_row("ar", event_type="inflow", confidence="estimated"))}

    assert "split_into_installments" in must_actions
    assert "defer_or_change_date" not in must_actions
    assert {"split_into_installments", "defer_or_change_date"} <= flexible_actions
    assert {"mark_received", "match_bank_deposit", "assign_follow_up"} <= incoming_actions
    assert all(item["preview_required"] for item in quick_action_eligibility(_row("flex")))


def test_legacy_clickup_notes_drive_chunkable_actions_and_priority():
    row = _row(
        "legacy-rent",
        notes="priority:can_hold | chunk payable; partial payments accepted",
        flexibility="unknown",
        pay_priority="review",
    )

    actions = {item["action_type"] for item in quick_action_eligibility(row)}
    queue = build_queue([row], as_of=AS_OF, funding_gap_cents=50_000)

    assert "split_into_installments" in actions
    assert "defer_or_change_date" in actions
    assert queue["items"][0]["pay_priority"] == "can_hold"
    assert queue["items"][0]["flexibility"] == "chunkable"


def test_queue_group_order_sorting_and_no_silent_truncation():
    rows = [
        _row("duplicate", probable_duplicate=True, due_date=None),
        _row("collect", event_type="inflow", due_date=AS_OF - timedelta(days=3)),
        _row("tax", category="tax", pay_priority="must_pay", due_date=AS_OF + timedelta(days=1)),
        _row("payroll", category="payroll", pay_priority="must_pay", due_date=AS_OF + timedelta(days=1)),
        _row("protect", flexibility="deferrable", pay_priority="can_hold", due_date=AS_OF + timedelta(days=4)),
        _row("week", due_date=AS_OF + timedelta(days=6)),
        _row("next-a", due_date=AS_OF + timedelta(days=9)),
        _row("next-b", due_date=AS_OF + timedelta(days=10)),
    ]

    queue = build_queue(rows, as_of=AS_OF, horizon_days=14, funding_gap_cents=20_000)

    assert [group["key"] for group in queue["groups"]] == [
        "resolve_first", "collect_now", "pay_now", "protect_cash", "this_week", "next_week"
    ]
    assert [item["id"] for item in queue["groups"][2]["items"]] == []  # funding gap suppresses Pay now
    assert queue["groups"][-1]["collapsed"] is True
    assert queue["groups"][-1]["count"] == 2
    assert len(queue["groups"][-1]["items"]) == 2
    assert "duplicate" not in {item["id"] for item in queue["items"]}
    assert queue["truncated"] is False


def test_control_state_queue_supports_full_forecast_window():
    state = build_finance_control_state(
        [_row("later", due_date=AS_OF + timedelta(days=21))],
        as_of=AS_OF,
        floor_cents=100_000,
        horizon_days=28,
        summary_days=14,
    )

    assert [item["id"] for item in state["queue"]["items"]] == ["later"]


def test_queue_pay_now_sorts_operational_category_before_amount():
    rows = [
        _row("tax", category="tax", amount_cents=500_000, pay_priority="must_pay", due_date=AS_OF),
        _row("payroll", category="payroll", amount_cents=10_000, pay_priority="must_pay", due_date=AS_OF),
    ]

    queue = build_queue(rows, as_of=AS_OF, funding_gap_cents=0)

    assert [item["id"] for item in queue["groups"][2]["items"]] == ["payroll", "tax"]


def test_missing_amount_and_date_are_resolve_first_blockers():
    queue = build_queue(
        [_row("missing-amount", amount_cents=0), _row("missing-date", due_date=None)],
        as_of=AS_OF,
    )

    blockers = {item["decision_blocker"] for item in queue["groups"][0]["items"]}
    assert blockers == {"missing_amount", "missing_date"}


def test_inputs_are_not_mutated():
    rows = [_row("rent", amount_cents=100_000)]
    original = dict(rows[0])

    build_finance_control_state(
        rows, {"rent": 25_000}, as_of=AS_OF, floor_cents=100_000
    )

    assert rows[0] == original
    assert "open_amount_cents" not in rows[0]


def test_renderer_facade_accepts_resolved_balance_and_exposes_compatible_shape():
    control = build_finance_control(
        _history(99_999),
        0,
        AS_OF.isoformat(),
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert control["cash_position"]["cash_on_hand_cents"] == 0
    assert control["cash_position"]["balance_available"] is True
    assert len(control["forecast"]["labels"]) == 28
    assert len(control["forecast"]["stress"]) == 28
    assert set(control["smart_brief"]) == {"happening", "broken", "next"}


def test_csv_income_projection_aggregates_same_day_and_only_changes_expected():
    rows = [
        _bank("a1", 15, amount=40_000, vendor_or_customer="Amazon"),
        _bank("a2", 15, amount=60_000, vendor_or_customer="Amazon"),
        _bank("b1", 8, amount=101_000, vendor_or_customer="Amazon"),
        _bank("c1", 1, amount=99_000, vendor_or_customer="Amazon", account_balance_cents=200_000),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)

    projection = state["income_projection"]
    assert projection["status"] == "inferred_review"
    assert projection["eligible_pattern_count"] == 1
    assert projection["inferred_projection_count"] == 4
    assert state["forecast"]["paths"]["committed"][-1]["cash_cents"] == 200_000
    assert state["forecast"]["paths"]["stress"][-1]["cash_cents"] == 200_000
    assert state["forecast"]["paths"]["expected"][-1]["cash_cents"] == 399_000
    assert state["metrics"]["expected_incoming_cents"] == 99_500
    assert projection["csv_trend_expected_cents"] == 99_500
    inferred = [item for item in state["queue"]["items"] if item["trend_inferred"]]
    assert inferred
    assert inferred[0]["source"] == "csv_trend"
    assert inferred[0]["read_only"] is True
    assert inferred[0]["probability_bps"] == 5_000
    assert inferred[0]["source_evidence"]["median_amount_cents"] == 100_000
    assert inferred[0]["source_evidence"]["projected_amount_cents"] == 99_500
    assert inferred[0]["source_evidence"]["occurrence_dates"] == [
        "2026-06-28",
        "2026-07-05",
        "2026-07-12",
    ]


def test_csv_income_projection_supports_daily_processor_cadence():
    rows = [
        _bank("p1", 4, amount=50_000, vendor_or_customer="Daily Processor"),
        _bank("p2", 3, amount=51_000, vendor_or_customer="Daily Processor"),
        _bank("p3", 2, amount=49_000, vendor_or_customer="Daily Processor"),
        _bank("p4", 1, amount=50_000, vendor_or_customer="Daily Processor"),
    ]

    result = derive_csv_income_projections(rows, as_of=AS_OF, horizon_days=5)

    assert result["eligible_pattern_count"] == 1
    assert [row["due_date"] for row in result["projections"]] == [
        AS_OF + timedelta(days=1),
        AS_OF + timedelta(days=2),
        AS_OF + timedelta(days=3),
        AS_OF + timedelta(days=4),
    ]


def test_csv_income_projection_suppresses_sparse_stale_unstable_and_transfer_like():
    cases = [
        [_bank("s1", 8), _bank("s2", 1)],
        [_bank("t1", 49), _bank("t2", 42), _bank("t3", 35)],
        [_bank("u1", 15, amount=20_000), _bank("u2", 8, amount=100_000), _bank("u3", 1, amount=200_000)],
        [
            _bank("x1", 15, vendor_or_customer="Internal Transfer"),
            _bank("x2", 8, vendor_or_customer="Internal Transfer"),
            _bank("x3", 1, vendor_or_customer="Internal Transfer"),
        ],
        [
            _bank("o1", 15, vendor_or_customer="David Narayan", description="TYPE: TRANSFER FROM SHARE"),
            _bank("o2", 8, vendor_or_customer="David Narayan", description="FROM SHARE CASHOUT"),
            _bank("o3", 1, vendor_or_customer="David Narayan", description="BILL PAYMT REVERSAL"),
        ],
    ]

    for rows in cases:
        assert derive_csv_income_projections(rows, as_of=AS_OF)["projections"] == []


def test_real_receivable_suppresses_equivalent_csv_projection():
    rows = [
        _bank("r1", 15, amount=100_000, vendor_or_customer="Acme"),
        _bank("r2", 8, amount=101_000, vendor_or_customer="Acme"),
        _bank("r3", 1, amount=99_000, vendor_or_customer="Acme"),
        _row(
            "real-ar",
            event_type="inflow",
            vendor_or_customer="Acme",
            amount_cents=100_000,
            due_date=AS_OF + timedelta(days=6),
        ),
    ]

    result = derive_csv_income_projections(
        annotate_open_amounts(rows),
        as_of=AS_OF,
        horizon_days=14,
    )

    assert result["status"] == "configured_real"
    assert all(row["due_date"] != AS_OF + timedelta(days=6) for row in result["projections"])


def test_no_eligible_income_pattern_is_not_a_decision_blocker():
    rows = [
        _bank("income-1", 10, amount=100_000, vendor_or_customer="One Off A"),
        _bank("income-2", 1, amount=80_000, vendor_or_customer="One Off B", account_balance_cents=200_000),
        _row("legacy-zero", amount_cents=0),
    ]

    control = build_finance_control(rows, as_of=AS_OF, floor_cents=100_000)

    assert control["income_projection"]["status"] == "no_eligible_pattern"
    assert control["income_projection"]["ready"] is True
    assert "forecast income is not configured" not in control["confidence"]["reasons"]
    assert control["trust_gate"]["income_ready"] is True
    assert not any(
        item["action_type"] == "configure_income_forecast"
        for item in control["recommendations"]
    )


def test_csv_income_projection_preserves_inputs():
    rows = [
        _bank("r1", 15, amount=100_000),
        _bank("r2", 8, amount=101_000),
        _bank("r3", 1, amount=99_000),
    ]
    originals = [dict(row) for row in rows]

    derive_csv_income_projections(rows, as_of=AS_OF)

    assert rows == originals


def test_missing_cash_outranks_cleanup_when_no_income_pattern_is_eligible():
    rows = [
        _bank("income-1", 20, vendor_or_customer="One Off A"),
        _bank("income-2", 10, vendor_or_customer="One Off B"),
        _row("legacy-zero", amount_cents=0),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)
    actions = [item["action_type"] for item in state["recommendations"]]

    assert actions[0] == "upload_latest_balance"
    assert {"resolve_finance_data", "resolve_missing_amount"} <= set(actions)
    assert "configure_income_forecast" not in actions


def test_csv_income_projection_rejects_discontinuous_recent_history():
    rows = [
        _bank("old-1", 180, vendor_or_customer="Amazon"),
        _bank("old-2", 173, vendor_or_customer="Amazon"),
        _bank("old-3", 166, vendor_or_customer="Amazon"),
        _bank("recent", 1, vendor_or_customer="Amazon"),
    ]

    assert derive_csv_income_projections(rows, as_of=AS_OF)["projections"] == []


def test_csv_income_projection_excludes_category_only_refunds_and_reversals():
    for category in ("refund", "reversal"):
        rows = [
            _bank(f"{category}-1", 15, category=category),
            _bank(f"{category}-2", 8, category=category),
            _bank(f"{category}-3", 1, category=category),
        ]
        assert derive_csv_income_projections(rows, as_of=AS_OF)["projections"] == []


def test_csv_income_projection_uses_non_default_summary_window():
    rows = [
        _bank("a1", 15, amount=100_000, vendor_or_customer="Amazon"),
        _bank("a2", 8, amount=101_000, vendor_or_customer="Amazon"),
        _bank(
            "a3",
            1,
            amount=99_000,
            vendor_or_customer="Amazon",
            account_balance_cents=200_000,
        ),
    ]

    state = build_finance_control_state(
        rows,
        as_of=AS_OF,
        floor_cents=100_000,
        summary_days=7,
    )

    assert state["income_projection"]["csv_trend_expected_cents"] == 49_750
    assert state["metrics"]["expected_incoming_cents"] == 49_750


def test_income_patterns_have_stable_hex_keys_and_apply_operator_decisions():
    rows = [
        _bank("a1", 15, amount=100_000, vendor_or_customer="Amazon"),
        _bank("a2", 8, amount=101_000, vendor_or_customer="Amazon"),
        _bank(
            "a3", 1, amount=99_000, vendor_or_customer="Amazon",
            account_balance_cents=200_000,
        ),
    ]
    originals = deepcopy(rows)

    review = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)
    pattern = review["income_projection"]["patterns"][0]
    pattern_key = pattern["pattern_key"]

    assert re.fullmatch(r"[0-9a-f]{16}", pattern_key)
    assert pattern["decision"] == "review"
    assert pattern["evidence"]["occurrence_dates"] == [
        "2026-06-28", "2026-07-05", "2026-07-12"
    ]
    assert review["metrics"]["expected_incoming_cents"] > 0
    assert review["trust_gate"]["income_ready"] is False

    tracked = build_finance_control_state(
        rows,
        as_of=AS_OF,
        floor_cents=100_000,
        income_decisions={pattern_key: "track_expected"},
    )
    assert tracked["income_projection"]["patterns"][0]["decision"] == "track_expected"
    assert tracked["income_projection"]["operator_reviewed_pattern_count"] == 1
    assert tracked["trust_gate"]["income_ready"] is True
    assert tracked["metrics"]["expected_incoming_cents"] == review["metrics"]["expected_incoming_cents"]

    for decision in ("exclude", "one_time"):
        excluded = build_finance_control_state(
            rows,
            as_of=AS_OF,
            floor_cents=100_000,
            income_decisions={pattern_key: decision},
        )
        assert excluded["income_projection"]["projections"] == []
        assert excluded["income_projection"]["operator_reviewed_pattern_count"] == 0
        assert excluded["metrics"]["expected_incoming_cents"] == 0
    assert rows == originals


def test_real_future_receivable_satisfies_income_gate_with_auto_patterns_present():
    rows = [
        _bank("a1", 15, vendor_or_customer="Amazon"),
        _bank("a2", 8, vendor_or_customer="Amazon"),
        _bank("a3", 1, vendor_or_customer="Amazon", account_balance_cents=200_000),
        _row(
            "real-ar", event_type="inflow", amount_cents=50_000,
            due_date=AS_OF + timedelta(days=3),
        ),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)

    assert state["income_projection"]["real_future_inflow_count"] == 1
    assert state["trust_gate"]["income_ready"] is True


def test_data_quality_quarantines_noise_but_keeps_recent_zero_blocker():
    rows = _history() + [
        _bank("zero-transaction", 2, amount=0),
        _row("duplicate", probable_duplicate=True),
        _row("old-zero", amount_cents=0, due_date=AS_OF - timedelta(days=91)),
        _row("recent-zero", amount_cents=0, due_date=AS_OF - timedelta(days=10)),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)
    quality = state["data_quality"]

    assert quality["probable_duplicate_count"] == 1
    assert quality["zero_transaction_count"] == 1
    assert quality["stale_zero_obligation_count"] == 1
    assert quality["actionable_zero_obligation_count"] == 1
    assert {item["id"] for item in quality["quarantine"]} == {
        "zero-transaction", "duplicate", "old-zero"
    }
    assert [item["id"] for item in state["queue"]["items"] if item["decision_blocker"]] == [
        "recent-zero"
    ]


def test_current_duplicate_missing_amount_blocks_trust_while_old_duplicate_zero_is_quarantined():
    rows = _history() + [
        _row("current-duplicate-zero", amount_cents=0, probable_duplicate=True),
        _row(
            "old-duplicate-zero",
            amount_cents=0,
            probable_duplicate=True,
            due_date=AS_OF - timedelta(days=91),
        ),
    ]

    state = build_finance_control_state(rows, as_of=AS_OF, floor_cents=100_000)

    assert state["trust_gate"]["payables_ready"] is False
    assert {issue["id"] for issue in state["trust_gate"]["payable_issues"]} == {
        "current-duplicate-zero"
    }
    assert {item["id"] for item in state["data_quality"]["quarantine"]} == {
        "old-duplicate-zero"
    }
    assert "current-duplicate-zero" in {
        item["id"] for item in state["queue"]["items"]
    }


def test_stale_payable_source_evidence_blocks_payment_and_defer_recommendations():
    state = build_finance_control_state(
        _history(250_000) + [
            _row(
                "stale-qbo-bill",
                source="qbo",
                source_updated_at=(AS_OF - timedelta(days=8)).isoformat(),
                flexibility="deferrable",
                pay_priority="can_hold",
            )
        ],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["trust_gate"]["payables_ready"] is False
    assert state["trust_gate"]["payable_issues"] == [
        {"id": "stale-qbo-bill", "reason": "stale source evidence"}
    ]
    assert "resolve_finance_data" in {
        item["action_type"] for item in state["recommendations"]
    }
    assert not {
        "split_or_defer_payable", "pay_or_schedule_must_pay"
    } & {item["action_type"] for item in state["recommendations"]}


def test_trust_gate_and_source_status_are_deterministic_and_bank_first():
    rows = [
        _row("conflict", source="qbo", source_conflict=True),
        _row("clickup-bill", source="clickup"),
    ]
    connections = {
        "clickup": {"connected": True, "last_synced_at": AS_OF.isoformat()},
        "quickbooks": {"connected": True, "last_synced_at": AS_OF.isoformat()},
    }
    original_connections = deepcopy(connections)

    state = build_finance_control_state(
        rows,
        as_of=AS_OF,
        floor_cents=100_000,
        source_connections=connections,
    )

    assert state["trust_gate"] == {
        **state["trust_gate"],
        "cash_ready": False,
        "payables_ready": False,
        "income_ready": True,
        "ready": False,
        "next_action": "upload_latest_balance",
    }
    assert state["trust_gate"]["reasons"][0] == "cash balance is missing"
    assert state["recommendations"][0]["action_type"] == "upload_latest_balance"
    statuses = {item["name"]: item["status"] for item in state["source_status"]}
    assert statuses == {
        "Bank CSV": "missing",
        "ClickUp": "current",
        "QBO receivables": "current",
        "QBO payment evidence": "connected",
    }
    assert connections == original_connections


def test_connected_sources_without_rows_are_not_ready():
    state = build_finance_control_state(
        [],
        as_of=AS_OF,
        floor_cents=100_000,
        source_connections={
            "clickup": {"connected": True, "last_synced_at": AS_OF.isoformat()},
            "quickbooks": {"status": "ready", "last_synced_at": AS_OF.isoformat()},
        },
    )

    statuses = state["source_status_by_key"]
    assert statuses["clickup"]["status"] == "connected"
    assert statuses["qbo_receivables"]["status"] == "connected"
    assert statuses["qbo_actuals"]["status"] == "connected"
    assert statuses["clickup"]["ready"] is False
    assert statuses["qbo_receivables"]["ready"] is False
    assert statuses["qbo_actuals"]["ready"] is False


def test_connected_source_with_undated_payable_rows_is_not_ready():
    state = build_finance_control_state(
        [_row("undated-clickup-bill", source="clickup", due_date=None)],
        as_of=AS_OF,
        floor_cents=100_000,
        source_connections={
            "clickup": {"connected": True, "last_synced_at": AS_OF.isoformat()},
        },
    )

    source = state["source_status_by_key"]["clickup"]
    assert source["status"] == "current"
    assert source["ready"] is True


def test_commitment_capacity_excludes_income_and_reserves_installment_remainder():
    expected_income = _row(
        "expected-ar", event_type="inflow", amount_cents=500_000,
        confidence="medium", due_date=AS_OF + timedelta(days=1),
    )
    full_bill = _row(
        "full-chunk", amount_cents=100_000, flexibility="chunkable",
        due_date=AS_OF - timedelta(days=2),
        payment_installments=[{"amount_cents": 20_000, "due_date": AS_OF, "status": "planned"}],
    )
    scheduled_bill = _row(
        "scheduled-chunk", amount_cents=100_000, flexibility="chunkable",
        due_date=AS_OF - timedelta(days=2),
        payment_installments=[
            {"amount_cents": 20_000, "due_date": AS_OF, "status": "confirmed"},
            {"amount_cents": 30_000, "due_date": AS_OF + timedelta(days=20), "status": "confirmed"},
        ],
    )

    full = build_finance_control_state(
        _history(250_000) + [expected_income, full_bill],
        as_of=AS_OF, floor_cents=100_000,
    )
    scheduled = build_finance_control_state(
        _history(250_000) + [expected_income, scheduled_bill],
        as_of=AS_OF, floor_cents=100_000,
    )

    assert full["metrics"]["expected_incoming_cents"] == 250_000
    assert full["metrics"]["required_outgoing_cents"] == 100_000
    assert full["metrics"]["safe_to_commit_cents"] == 50_000
    assert scheduled["metrics"]["required_outgoing_cents"] == 20_000
    assert scheduled["metrics"]["outgoing_exposure_cents"] == 80_000
    assert scheduled["metrics"]["safe_to_commit_cents"] == 130_000


def test_unready_gate_emits_only_trust_resolution_recommendations():
    state = build_finance_control_state(
        _history(50_000) + [_row("bill", flexibility="deferrable", pay_priority="can_hold")],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["trust_gate"]["ready"] is False
    assert not {
        "collect_confirmed_income", "split_or_defer_payable", "pay_or_schedule_must_pay"
    } & {item["action_type"] for item in state["recommendations"]}


def test_source_missing_clickup_payable_stays_reserved_and_blocks_trust():
    state = build_finance_control_state(
        _history(250_000) + [_row(
            "moved-clickup-bill", source="clickup", source_status="source_missing",
            amount_cents=500_000, due_date=AS_OF + timedelta(days=1),
        )],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    assert state["metrics"]["required_outgoing_cents"] == 500_000
    assert state["trust_gate"]["payables_ready"] is False
    assert state["trust_gate"]["payable_issues"] == [
        {"id": "moved-clickup-bill", "reason": "missing from ClickUp source"}
    ]
    assert state["queue"]["items"][0]["decision_blocker"] == "source_missing"


def test_completed_clickup_task_stays_reserved_without_settlement_evidence():
    state = build_finance_control_state(
        _history(250_000) + [_row(
            "just-completed", source="clickup", status="completed",
            source_updated_at=AS_OF.isoformat(), amount_cents=500_000,
            due_date=AS_OF + timedelta(days=1),
        )],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    item = next(item for item in state["queue"]["items"] if item["id"] == "just-completed")
    assert state["metrics"]["required_outgoing_cents"] == 500_000
    assert item["group"] == "resolve_first"
    assert item["decision_blocker"] == "completion_requires_bank_evidence"
    assert state["trust_gate"]["payable_issues"] == [
        {"id": "just-completed", "reason": "ClickUp completion lacks settlement evidence"}
    ]


def test_completed_clickup_task_does_not_release_from_a_newer_bank_snapshot_alone():
    state = build_finance_control_state(
        _history(250_000) + [_row(
            "bank-covered-completion", source="clickup", status="completed",
            source_updated_at=(AS_OF - timedelta(days=1)).isoformat(), amount_cents=500_000,
            due_date=AS_OF + timedelta(days=1),
        )],
        as_of=AS_OF,
        floor_cents=100_000,
    )

    item = next(item for item in state["queue"]["items"] if item["id"] == "bank-covered-completion")
    assert state["metrics"]["required_outgoing_cents"] == 500_000
    assert item["group"] == "resolve_first"
    assert item["completion_requires_bank_evidence"] is True


def test_completed_clickup_task_releases_after_settlement_allocation():
    state = build_finance_control_state(
        _history(250_000) + [_row(
            "settled-completion", source="clickup", status="completed",
            source_updated_at=AS_OF.isoformat(), amount_cents=500_000,
            due_date=AS_OF + timedelta(days=1),
        )],
        settlement_annotations={"settled-completion": 500_000},
        as_of=AS_OF,
        floor_cents=100_000,
    )

    item = next(item for item in state["queue"]["items"] if item["id"] == "settled-completion")
    assert state["metrics"]["required_outgoing_cents"] == 0
    assert item["group"] == "completed"
    assert item["completion_requires_bank_evidence"] is False


def test_plaid_history_replaces_overlapping_csv_without_losing_older_history():
    older_csv = _row(
        "older-csv", record_kind="transaction", source="csv", status="posted",
        event_type="inflow", amount_cents=100_000, due_date=AS_OF - timedelta(days=10),
    )
    overlapping_csv = _row(
        "overlap-csv", record_kind="transaction", source="csv", status="posted",
        event_type="inflow", amount_cents=200_000, due_date=AS_OF - timedelta(days=2),
    )
    plaid = _row(
        "plaid-posted", record_kind="transaction", source="plaid", status="posted",
        event_type="inflow", amount_cents=200_000, due_date=AS_OF - timedelta(days=2),
    )

    trends = calculate_csv_trends([older_csv, overlapping_csv, plaid], as_of=AS_OF)

    assert trends["transaction_count"] == 2
    assert trends["net_28_cents"] == 300_000


def test_pending_plaid_transaction_is_not_bank_history_evidence():
    pending = _row(
        "plaid-pending", record_kind="transaction", source="plaid", status="pending",
        event_type="outflow", amount_cents=999_999, due_date=AS_OF,
    )

    trends = calculate_csv_trends([pending], as_of=AS_OF)

    assert trends["transaction_count"] == 0
    assert trends["net_28_cents"] == 0
