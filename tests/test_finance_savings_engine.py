from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

from sales_support_agent.services.cashflow.savings import build_savings_view_model


AS_OF = date(2026, 7, 14)


def _bank(event_id: str, vendor: str, days_ago: int, amount_cents: int, **overrides):
    row = {
        "id": event_id,
        "source_id": event_id,
        "record_kind": "transaction",
        "source": "csv",
        "event_type": "outflow",
        "category": "software",
        "vendor_or_customer": vendor,
        "amount_cents": amount_cents,
        "due_date": AS_OF - timedelta(days=days_ago),
        "status": "posted",
    }
    row.update(overrides)
    return row


def _obligation(event_id: str, vendor: str, priority: str = "can_hold"):
    return {
        "id": event_id,
        "record_kind": "obligation",
        "source": "clickup",
        "event_type": "outflow",
        "category": "software",
        "vendor_or_customer": vendor,
        "amount_cents": 0,
        "due_date": AS_OF,
        "status": "planned",
        "pay_priority": priority,
    }


def _build(events, **overrides):
    kwargs = {
        "as_of": AS_OF,
        "balance_cents": 50_000,
        "floor_cents": 100_000,
        "source_freshness": {"as_of_date": AS_OF, "coverage_days": 180},
    }
    kwargs.update(overrides)
    return build_savings_view_model(events, **kwargs)


def test_recurring_can_hold_cost_uses_posted_median_and_normalized_horizons():
    rows = [_obligation("plan", "Acme Cloud")]
    for index, days in enumerate((152, 122, 92, 62, 32, 2)):
        rows.append(_bank(f"tx-{index}", "Acme Cloud", days, 20_000))

    result = _build(rows)
    opportunity = next(item for item in result["opportunities"] if item["opportunity_type"] == "recurring_cost")

    assert result["state"] == "ready"
    assert opportunity["cadence"] == "monthly"
    assert opportunity["baseline_amount_cents"] == 20_000
    assert opportunity["monthly_potential_cents"] == 20_000
    assert opportunity["annual_gross_potential_cents"] == 240_000
    assert opportunity["verified_net_potential_cents"] is None
    assert opportunity["scenario_28d_floor_improvement_cents"] == 20_000
    assert opportunity["scenario_28d_funding_gap_offset_cents"] == 20_000


def test_price_increase_compares_latest_three_to_prior_three():
    rows = []
    history = zip(
        (150, 120, 90, 60, 30, 0),
        (10_000, 10_000, 10_000, 12_500, 12_500, 12_500),
    )
    for index, (days, amount) in enumerate(history):
        rows.append(_bank(f"price-{index}", "Design Suite", days, amount))

    result = _build(rows)
    opportunity = next(item for item in result["opportunities"] if item["opportunity_type"] == "price_increase")

    assert opportunity["baseline_amount_cents"] == 10_000
    assert opportunity["current_amount_cents"] == 12_500
    assert opportunity["monthly_potential_cents"] == 2_500
    assert opportunity["annual_gross_potential_cents"] == 30_000
    assert opportunity["reason_codes"] == [
        "stable_recurring_price_increase",
        "latest_three_above_prior_three",
    ]


@pytest.mark.parametrize(
    ("vendor", "days", "expected_cadence", "expected_monthly"),
    [
        ("Weekly Tool", (14, 7, 0), "weekly", 5_200),
        ("Biweekly Tool", (28, 14, 0), "biweekly", 2_600),
        ("Monthly Tool", (60, 30, 0), "monthly", 1_200),
        ("Quarterly Tool", (180, 90, 0), "quarterly", 400),
    ],
)
def test_supported_cadences_use_exact_monthly_normalization(
    vendor, days, expected_cadence, expected_monthly
):
    rows = [_obligation(f"plan-{vendor}", vendor)]
    rows.extend(
        _bank(f"{vendor}-{index}", vendor, days_ago, 1_200)
        for index, days_ago in enumerate(days)
    )

    opportunity = _build(rows)["opportunities"][0]

    assert opportunity["cadence"] == expected_cadence
    assert opportunity["monthly_potential_cents"] == expected_monthly
    assert opportunity["annual_gross_potential_cents"] == expected_monthly * 12


def test_fee_leakage_preserves_90_day_horizon_and_only_annualizes_with_coverage():
    rows = [
        _bank("fee-1", "Bank Service", 70, 5_000, category="fees", description="Wire fee"),
        _bank("fee-2", "Bank Service", 30, 6_000, category="fees", description="Wire fee"),
        _bank("fee-3", "Bank Service", 2, 7_000, category="fees", description="Wire fee"),
    ]

    short = _build(rows, source_freshness={"as_of_date": AS_OF, "coverage_days": 80})
    current = _build(rows, source_freshness={"as_of_date": AS_OF, "coverage_days": 90})
    short_fee = short["opportunities"][0]
    current_fee = current["opportunities"][0]

    assert short_fee["observed_90d_potential_cents"] == 18_000
    assert short_fee["monthly_potential_cents"] is None
    assert short_fee["annual_gross_potential_cents"] is None
    assert current_fee["annual_gross_potential_cents"] == 73_000
    assert current["headline"]["fee_90d_potential_cents"] == 18_000


def test_single_explicit_fee_at_one_hundred_dollars_is_medium_confidence():
    result = _build(
        [_bank("fee", "Bank Service", 2, 10_000, category="fees")]
    )

    assert result["opportunities"][0]["observed_90d_potential_cents"] == 10_000
    assert result["opportunities"][0]["data_confidence"] == "medium"


def test_protected_conflicted_transfer_and_refund_evidence_is_suppressed():
    rows = [
        _obligation("rent-plan", "Landlord", "can_hold"),
        *[_bank(f"rent-{i}", "Landlord", days, 200_000, category="rent") for i, days in enumerate((60, 30, 0))],
        *[
            _bank(
                f"conflict-{i}",
                "Conflict SaaS",
                days,
                10_000,
                probable_duplicate=i == 2,
            )
            for i, days in enumerate((60, 30, 0))
        ],
        _obligation("conflict-plan", "Conflict SaaS"),
        _bank("transfer", "Internal Transfer", 1, 99_000, category="transfer"),
        _bank("refund", "Refund Shop", 1, 4_000, category="refund"),
    ]

    result = _build(rows)

    assert result["opportunities"] == []
    assert result["suppressed_counts"] == {
        "conflict": 1,
        "protected": 3,
        "refund_or_reversal": 1,
        "transfer": 1,
    }


@pytest.mark.parametrize(
    "match_status",
    ("ambiguous", "review", "conflict", "possible", "pending_review", "unresolved"),
)
def test_unresolved_match_state_blocks_the_entire_merchant(match_status):
    rows = [_obligation("plan", "Conflict SaaS")]
    rows.extend(
        _bank(
            f"conflict-{index}",
            "Conflict SaaS",
            days,
            10_000,
            match_status=match_status if index == 2 else "unmatched",
        )
        for index, days in enumerate((60, 30, 0))
    )

    result = _build(rows)

    assert result["opportunities"] == []
    assert result["suppressed_counts"]["conflict"] == 1


def test_unresolved_obligation_evidence_blocks_posted_merchant_history():
    plan = _obligation("plan", "Conflict SaaS")
    plan["classification"] = "conflict"
    rows = [plan]
    rows.extend(
        _bank(f"conflict-{index}", "Conflict SaaS", days, 10_000)
        for index, days in enumerate((60, 30, 0))
    )

    assert _build(rows)["opportunities"] == []


@pytest.mark.parametrize("category", ("credit_card", "credit_card_payment", "bank_transfer"))
def test_credit_card_payments_and_transfers_never_become_savings(category):
    rows = [_obligation("plan", "Card Account")]
    rows.extend(
        _bank(f"card-{index}", "Card Account", days, 25_000, category=category)
        for index, days in enumerate((60, 30, 0))
    )

    result = _build(rows)

    assert result["opportunities"] == []
    assert result["suppressed_counts"]["transfer"] == 3


@pytest.mark.parametrize("category", ["credit_card", "credit_card_payment", "card_payment"])
def test_card_payment_evidence_blocks_the_entire_merchant(category):
    rows = [_obligation("plan", "Card Provider")]
    rows.extend(
        _bank(f"tx-{index}", "Card Provider", days, 10_000)
        for index, days in enumerate((90, 60, 30))
    )
    rows.append(_bank("payment", "Card Provider", 0, 50_000, category=category))

    result = _build(rows)

    assert result["opportunities"] == []
    assert result["suppressed_counts"]["transfer"] == 1


def test_future_obligations_drive_the_28_day_funding_gap_offset():
    rows = [_obligation("plan", "Acme Cloud")]
    rows.extend(
        _bank(f"tx-{index}", "Acme Cloud", days, 20_000)
        for index, days in enumerate((62, 32, 2))
    )
    rows.append(
        {
            "id": "future-payroll",
            "record_kind": "obligation",
            "source": "clickup",
            "event_type": "outflow",
            "vendor_or_customer": "Payroll",
            "amount_cents": 100_000,
            "due_date": AS_OF + timedelta(days=10),
            "status": "planned",
            "pay_priority": "must_pay",
        }
    )

    result = _build(rows, balance_cents=150_000, floor_cents=100_000)
    opportunity = result["opportunities"][0]

    assert opportunity["scenario_funding_gap_cents"] == 50_000
    assert opportunity["scenario_28d_funding_gap_offset_cents"] == 20_000


def test_stale_source_fails_closed_before_calculating_candidates():
    rows = [_obligation("plan", "Acme Cloud")]
    rows.extend(_bank(f"tx-{index}", "Acme Cloud", days, 20_000) for index, days in enumerate((60, 30, 0)))

    result = _build(rows, source_freshness={"as_of_date": AS_OF - timedelta(days=4), "coverage_days": 90})

    assert result["state"] == "stale"
    assert result["opportunities"] == []
    assert result["headline"]["recurring_monthly_potential_cents"] == 0


def test_missing_settlement_evidence_suppresses_savings_review():
    rows = [
        _obligation("plan", "Acme Cloud"),
        *[
            _bank(f"tx-{index}", "Acme Cloud", days, 20_000)
            for index, days in enumerate((60, 30, 0))
        ],
    ]
    rows[0]["settlement_evidence_available"] = False

    result = _build(rows)

    assert result["state"] == "error"
    assert result["opportunities"] == []


def test_missing_balance_or_floor_suppresses_scenario_cash_effect():
    rows = [_obligation("plan", "Acme Cloud")]
    rows.extend(
        _bank(f"tx-{index}", "Acme Cloud", days, 20_000)
        for index, days in enumerate((62, 32, 2))
    )

    result = _build(rows, balance_cents=None)
    opportunity = result["opportunities"][0]

    assert opportunity["scenario_28d_floor_improvement_cents"] is None
    assert opportunity["scenario_28d_funding_gap_offset_cents"] is None
    assert opportunity["scenario_funding_gap_cents"] is None


def test_funding_gap_offset_uses_28_day_forecast_stress_minimum():
    rows = [_obligation("plan", "Acme Cloud")]
    rows.extend(
        _bank(f"tx-{index}", "Acme Cloud", days, 20_000)
        for index, days in enumerate((62, 32, 2))
    )
    no_future_bill = _build(rows, balance_cents=200_000, floor_cents=100_000)
    rows.append(
        {
            "id": "future-bill",
            "record_kind": "obligation",
            "source": "clickup",
            "event_type": "outflow",
            "category": "inventory",
            "vendor_or_customer": "Inventory Vendor",
            "amount_cents": 180_000,
            "open_amount_cents": 180_000,
            "due_date": AS_OF + timedelta(days=14),
            "status": "planned",
        }
    )
    stressed = _build(rows, balance_cents=200_000, floor_cents=100_000)

    baseline_opportunity = no_future_bill["opportunities"][0]
    stressed_opportunity = stressed["opportunities"][0]
    assert baseline_opportunity["scenario_28d_funding_gap_offset_cents"] == 0
    assert stressed_opportunity["scenario_28d_stress_minimum_balance_cents"] == 20_000
    assert stressed_opportunity["scenario_funding_gap_cents"] == 80_000
    assert stressed_opportunity["scenario_28d_funding_gap_offset_cents"] == 20_000


def test_same_inputs_produce_byte_stable_output_and_input_is_not_mutated():
    rows = [_obligation("plan", "Acme Cloud")]
    rows.extend(_bank(f"tx-{index}", "Acme Cloud", days, 20_000) for index, days in enumerate((90, 60, 30, 0)))
    original = json.dumps(rows, default=str, sort_keys=True)

    first = _build(rows)
    second = _build(list(reversed(rows)))

    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    assert json.dumps(rows, default=str, sort_keys=True) == original
    assert len(first["opportunities"][0]["opportunity_key"]) == 64
    assert len(first["opportunities"][0]["evidence_hash"]) == 64


def test_overlapping_price_increase_is_not_double_counted_in_headline():
    rows = [_obligation("plan", "Acme Cloud")]
    history = zip(
        (150, 120, 90, 60, 30, 0),
        (10_000, 10_000, 10_000, 12_000, 12_000, 12_000),
    )
    for index, (days, amount) in enumerate(history):
        rows.append(_bank(f"tx-{index}", "Acme Cloud", days, amount))

    result = _build(rows)
    recurring = next(item for item in result["opportunities"] if item["opportunity_type"] == "recurring_cost")
    increase = next(item for item in result["opportunities"] if item["opportunity_type"] == "price_increase")

    assert recurring["included_in_headline"] is True
    assert increase["included_in_headline"] is False
    assert result["headline"]["recurring_monthly_potential_cents"] == 11_000
