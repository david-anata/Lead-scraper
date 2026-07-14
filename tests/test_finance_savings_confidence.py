from __future__ import annotations

from datetime import date, timedelta

from sales_support_agent.services.cashflow.savings import build_savings_view_model


AS_OF = date(2026, 7, 14)


def _bank(event_id: str, vendor: str, days_ago: int, amount: int, **overrides):
    row = {
        "id": event_id,
        "source_id": event_id,
        "record_kind": "transaction",
        "source": "csv",
        "event_type": "outflow",
        "category": "software",
        "vendor_or_customer": vendor,
        "amount_cents": amount,
        "due_date": AS_OF - timedelta(days=days_ago),
        "status": "posted",
    }
    row.update(overrides)
    return row


def _plan(vendor: str):
    return {
        "id": f"plan-{vendor}",
        "record_kind": "obligation",
        "source": "clickup",
        "event_type": "outflow",
        "vendor_or_customer": vendor,
        "pay_priority": "can_hold",
    }


def _build(rows):
    return build_savings_view_model(
        rows,
        as_of=AS_OF,
        balance_cents=20_000,
        floor_cents=100_000,
        source_freshness={"as_of_date": AS_OF, "coverage_days": 180},
    )


def test_three_stable_monthly_charges_are_medium_not_high():
    rows = [_plan("Three Co")]
    rows.extend(_bank(f"three-{index}", "Three Co", days, 10_000) for index, days in enumerate((60, 30, 0)))

    opportunity = _build(rows)["opportunities"][0]

    assert opportunity["data_confidence"] == "medium"
    assert 6_500 <= opportunity["confidence_bps"] <= 8_499


def test_five_stable_consistent_charges_are_high():
    rows = [_plan("Five Co")]
    rows.extend(_bank(f"five-{index}", "Five Co", days, 10_000) for index, days in enumerate((120, 90, 60, 30, 0)))

    opportunity = _build(rows)["opportunities"][0]

    assert opportunity["data_confidence"] == "high"
    assert opportunity["confidence_bps"] >= 8_500


def test_inconsistent_cadence_and_amounts_do_not_enter_results():
    rows = [_plan("Noisy Co")]
    for index, (days, amount) in enumerate(zip((100, 65, 30, 0), (10_000, 17_000, 8_000, 20_000))):
        rows.append(_bank(f"noisy-{index}", "Noisy Co", days, amount))

    assert _build(rows)["opportunities"] == []


def test_price_increase_must_reach_ten_percent_and_ten_dollars():
    rows = []
    history = zip(
        (150, 120, 90, 60, 30, 0),
        (20_000, 20_000, 20_000, 21_900, 21_900, 21_900),
    )
    for index, (days, amount) in enumerate(history):
        rows.append(_bank(f"threshold-{index}", "Threshold Co", days, amount))

    assert _build(rows)["opportunities"] == []


def test_fee_confidence_requires_four_events_and_90_day_coverage_for_high():
    medium_rows = [
        _bank("m-1", "Bank", 60, 5_000, category="fees"),
        _bank("m-2", "Bank", 1, 6_000, category="fees"),
    ]
    high_rows = [
        _bank(f"h-{index}", "Bank", days, 3_000, category="fees")
        for index, days in enumerate((80, 55, 30, 1))
    ]

    assert _build(medium_rows)["opportunities"][0]["data_confidence"] == "medium"
    assert _build(high_rows)["opportunities"][0]["data_confidence"] == "high"


def test_ranking_is_confidence_then_gap_offset_then_date_then_amount():
    rows = []
    rows.append(_plan("High Later"))
    rows.extend(_bank(f"high-{index}", "High Later", days, 5_000) for index, days in enumerate((120, 90, 60, 30, 0)))
    rows.append(_plan("Medium Larger"))
    rows.extend(_bank(f"medium-{index}", "Medium Larger", days, 50_000) for index, days in enumerate((60, 30, 0)))

    result = _build(rows)

    assert [item["normalized_merchant"] for item in result["opportunities"]] == ["high_later", "medium_larger"]


def test_evidence_hash_changes_with_source_freshness_but_key_does_not():
    rows = [_plan("Acme")]
    rows.extend(_bank(f"tx-{index}", "Acme", days, 10_000) for index, days in enumerate((60, 30, 0)))
    current = _build(rows)["opportunities"][0]
    older = build_savings_view_model(
        rows,
        as_of=AS_OF,
        balance_cents=20_000,
        floor_cents=100_000,
        source_freshness={"as_of_date": AS_OF - timedelta(days=1), "coverage_days": 180},
    )["opportunities"][0]

    assert current["opportunity_key"] == older["opportunity_key"]
    assert current["evidence_hash"] != older["evidence_hash"]
