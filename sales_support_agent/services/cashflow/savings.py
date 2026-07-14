"""Deterministic, read-only savings opportunity detection.

The public entry point is :func:`build_savings_view_model`.  It accepts plain
canonical cash-event mappings and returns JSON-serializable dictionaries.  The
module performs no database access, AI calls, writes, or forecast mutation.

Bank CSV transactions are the only monetary evidence.  Obligation mappings may
only supply the explicit ``pay_priority=can_hold`` or ``must_pay`` context used
to gate recurring-cost candidates.
"""

from __future__ import annotations

import hashlib
import json
import re
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence


CONTRACT_VERSION = "finance-savings-v1"
RULE_VERSION = "phase-1a.1"

PROTECTED_CATEGORIES = {
    "critical_utilities",
    "debt",
    "debt_service",
    "insurance",
    "payroll",
    "rent",
    "tax",
    "taxes",
    "utilities",
}
TRANSFER_CATEGORIES = {
    "account_transfer",
    "bank_transfer",
    "card_payment",
    "credit_card",
    "credit_card_payment",
    "credit_card_payments",
    "interaccount_transfer",
    "internal_transfer",
    "transfer",
    "transfers",
}
REFUND_CATEGORIES = {"refund", "reversal", "reimbursements"}
FEE_TOKENS = (
    "bank fee",
    "late fee",
    "monthly fee",
    "maintenance fee",
    "merchant fee",
    "overdraft",
    "service charge",
    "wire fee",
    "finance charge",
)

_CADENCES: tuple[tuple[str, int, int, int, int], ...] = (
    # name, minimum interval, maximum interval, annual numerator, denominator
    ("weekly", 5, 9, 52, 12),
    ("biweekly", 12, 17, 26, 12),
    ("monthly", 25, 35, 1, 1),
    ("quarterly", 80, 100, 1, 3),
)
_CONFLICT_FLAGS = (
    "conflicted",
    "source_conflict",
    "import_conflict",
    "probable_duplicate",
    "unresolved_duplicate",
    "unresolved_match",
    "ambiguous_match",
    "needs_action",
)
_REFUND_FLAGS = ("is_refund", "is_reversal", "reversed")
_TRANSFER_FLAGS = ("is_transfer",)
_UNRESOLVED_MATCH_STATES = {
    "ambiguous",
    "conflict",
    "conflicted",
    "duplicate",
    "needs_action",
    "pending_review",
    "possible",
    "probable_duplicate",
    "review",
    "source_conflict",
    "unresolved",
}
_MATCH_STATE_FIELDS = (
    "classification",
    "match_status",
    "matching_status",
    "reconciliation_status",
    "resolution_status",
)
_CLOSED_FORECAST_STATUSES = {
    "cancelled",
    "canceled",
    "closed",
    "matched",
    "paid",
    "posted",
    "resolved",
    "settled",
    "void",
    "voided",
}


def build_savings_view_model(
    events: Iterable[Mapping[str, Any]],
    *,
    as_of: date,
    balance_cents: int | None,
    floor_cents: int | None,
    source_freshness: Mapping[str, Any] | date | datetime | str | None,
    account_scope: str = "default",
    stale_after_days: int = 3,
) -> dict[str, Any]:
    """Build the complete Phase 1A Savings radar view model.

    Args:
        events: Canonical event mappings.  Posted/matched CSV outflow
            transactions provide evidence; obligations provide explicit
            ``pay_priority`` context only.
        as_of: Deterministic evaluation date.
        balance_cents: Current posted cash balance for scenario-only impact.
        floor_cents: Operator cash floor for scenario-only impact.
        source_freshness: A date/datetime/ISO string, or a mapping containing
            ``as_of_date``/``latest_date`` and optional ``coverage_days``.
        account_scope: Stable account/workspace scope used in opportunity keys.
        stale_after_days: Maximum source age before all candidates fail closed.

    Returns:
        A JSON-safe dictionary containing section state, deterministically
        ranked opportunity dictionaries, separate-horizon headline totals, and
        source freshness.  Inputs are never modified.
    """
    if not isinstance(as_of, date):
        raise TypeError("as_of must be a date")
    if stale_after_days < 0:
        raise ValueError("stale_after_days must be non-negative")

    rows = [dict(event) for event in events]
    source = _source_state(rows, as_of, source_freshness, stale_after_days)
    base = {
        "contract_version": CONTRACT_VERSION,
        "rule_version": RULE_VERSION,
        "as_of_date": as_of.isoformat(),
        "source_freshness": source,
        "opportunities": [],
        "headline": _empty_headline(),
        "suppressed_counts": {
            "conflict": 0,
            "protected": 0,
            "refund_or_reversal": 0,
            "transfer": 0,
        },
    }

    if any(
        not _is_bank_transaction(row) and row.get("settlement_evidence_available") is False
        for row in rows
    ):
        base["state"] = "error"
        base["message"] = "Savings review is unavailable until settlement evidence can be loaded."
        return base

    posted, suppressed, blocked_merchants = _eligible_posted_outflows(rows, as_of)
    blocked_merchants.update(_unresolved_merchants(rows))
    base["suppressed_counts"] = suppressed
    if source["stale"]:
        base["state"] = "stale"
        base["message"] = "Savings estimates are unavailable until posted cash data is refreshed."
        return base
    if not posted:
        base["state"] = "insufficient_history"
        base["message"] = "Upload posted bank history to find evidence-backed savings opportunities."
        return base

    can_hold, must_pay = _priority_merchants(rows)
    forecast_stress_minimum_cents = _forecast_stress_minimum_cents(
        rows, as_of=as_of, balance_cents=balance_cents
    )
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in posted:
        merchant = row["_normalized_merchant"]
        if merchant not in blocked_merchants and merchant not in must_pay:
            groups[merchant].append(row)

    opportunities: list[dict[str, Any]] = []
    for merchant in sorted(groups):
        merchant_rows = sorted(groups[merchant], key=_event_sort_key)
        recurring = _recurring_facts(merchant_rows)
        if recurring:
            if merchant in can_hold:
                opportunities.append(
                    _build_recurring_cost(
                        merchant_rows,
                        recurring,
                        account_scope=account_scope,
                        as_of=as_of,
                        forecast_stress_minimum_cents=forecast_stress_minimum_cents,
                        floor_cents=floor_cents,
                        source=source,
                    )
                )
            price_increase = _build_price_increase(
                merchant_rows,
                recurring,
                account_scope=account_scope,
                as_of=as_of,
                forecast_stress_minimum_cents=forecast_stress_minimum_cents,
                floor_cents=floor_cents,
                source=source,
            )
            if price_increase is not None:
                opportunities.append(price_increase)

        fee = _build_fee_leakage(
            merchant_rows,
            account_scope=account_scope,
            as_of=as_of,
            forecast_stress_minimum_cents=forecast_stress_minimum_cents,
            floor_cents=floor_cents,
            source=source,
        )
        if fee is not None:
            opportunities.append(fee)

    opportunities = [
        item for item in opportunities if item["data_confidence"] in {"high", "medium"}
    ]
    opportunities.sort(key=_ranking_key)
    _mark_headline_overlaps(opportunities)

    base["opportunities"] = opportunities
    base["headline"] = _headline(opportunities)
    if opportunities:
        base["state"] = "ready"
        base["message"] = f"{len(opportunities)} evidence-backed savings opportunities need review."
    elif len(posted) < 3 or source["coverage_days"] < 30:
        base["state"] = "insufficient_history"
        base["message"] = "More posted history is needed to establish comparable costs."
    else:
        base["state"] = "empty"
        base["message"] = "No evidence-backed savings opportunities need review."
    return base


def detect_savings_opportunities(
    events: Iterable[Mapping[str, Any]],
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Return only ranked opportunities; see :func:`build_savings_view_model`."""
    return build_savings_view_model(events, **kwargs)["opportunities"]


def _source_state(
    rows: Sequence[Mapping[str, Any]],
    as_of: date,
    raw: Mapping[str, Any] | date | datetime | str | None,
    stale_after_days: int,
) -> dict[str, Any]:
    coverage_override: int | None = None
    source_date: date | None = None
    if isinstance(raw, Mapping):
        source_date = _to_date(
            raw.get("as_of_date") or raw.get("latest_date") or raw.get("updated_at")
        )
        if raw.get("coverage_days") is not None:
            coverage_override = max(0, int(raw["coverage_days"]))
    else:
        source_date = _to_date(raw)

    bank_dates = [
        parsed
        for row in rows
        if _is_bank_transaction(row)
        for parsed in [_event_date(row)]
        if parsed is not None and parsed <= as_of
    ]
    if source_date is None and bank_dates:
        source_date = max(bank_dates)
    coverage_days = coverage_override
    if coverage_days is None:
        coverage_days = (max(bank_dates) - min(bank_dates)).days + 1 if bank_dates else 0
    age_days = (as_of - source_date).days if source_date is not None else None
    stale = source_date is None or age_days is None or age_days < 0 or age_days > stale_after_days
    return {
        "as_of_date": source_date.isoformat() if source_date else None,
        "age_days": age_days,
        "coverage_days": coverage_days,
        "stale": stale,
        "stale_after_days": stale_after_days,
    }


def _eligible_posted_outflows(
    rows: Sequence[Mapping[str, Any]], as_of: date
) -> tuple[list[dict[str, Any]], dict[str, int], set[str]]:
    eligible: list[dict[str, Any]] = []
    suppressed = {"conflict": 0, "protected": 0, "refund_or_reversal": 0, "transfer": 0}
    blocked_merchants: set[str] = set()
    for source_row in rows:
        if (
            not _is_bank_transaction(source_row)
            or str(source_row.get("event_type", "")).lower() != "outflow"
        ):
            continue
        event_date = _event_date(source_row)
        if event_date is None or event_date > as_of or int(source_row.get("amount_cents") or 0) <= 0:
            continue
        row = dict(source_row)
        merchant = _merchant(row)
        if not merchant:
            continue
        row["_normalized_merchant"] = merchant
        row["_event_date"] = event_date
        category = _slug(row.get("category"))
        text = _event_text(row)
        if _has_unresolved_match_or_conflict(row):
            suppressed["conflict"] += 1
            blocked_merchants.add(merchant)
            continue
        if (
            category in PROTECTED_CATEGORIES
            or str(row.get("pay_priority", "")).lower() == "must_pay"
        ):
            suppressed["protected"] += 1
            blocked_merchants.add(merchant)
            continue
        if (
            category in TRANSFER_CATEGORIES
            or _has_any_flag(row, _TRANSFER_FLAGS)
            or "account transfer" in text
            or "credit card payment" in text
        ):
            suppressed["transfer"] += 1
            blocked_merchants.add(merchant)
            continue
        if category in REFUND_CATEGORIES or _has_any_flag(row, _REFUND_FLAGS) or text.startswith("refund "):
            suppressed["refund_or_reversal"] += 1
            continue
        eligible.append(row)
    return eligible, suppressed, blocked_merchants


def _unresolved_merchants(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        merchant
        for row in rows
        if _has_unresolved_match_or_conflict(row)
        for merchant in [_merchant(row)]
        if merchant
    }


def _forecast_stress_minimum_cents(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: date,
    balance_cents: int | None,
) -> int | None:
    """Return the lowest projected cash balance through the next 28 days.

    Posted transaction history is already reflected in ``balance_cents`` and
    is never replayed. Open obligations are grouped by effective date so row
    ordering cannot create an artificial intraday low. Conflicted, duplicate,
    and transfer records are excluded from ranking evidence.
    """
    if balance_cents is None:
        return None

    horizon = as_of + timedelta(days=28)
    daily_net: dict[date, int] = defaultdict(int)
    for row in rows:
        if str(row.get("record_kind") or "").lower() == "transaction":
            continue
        status = _slug(row.get("status"))
        if status in _CLOSED_FORECAST_STATUSES:
            continue
        if _has_unresolved_match_or_conflict(row):
            continue
        if _slug(row.get("classification")) in {"conflict", "duplicate"}:
            continue
        category = _slug(row.get("category"))
        if category in TRANSFER_CATEGORIES or _has_any_flag(row, _TRANSFER_FLAGS):
            continue
        event_date = _event_date(row)
        if event_date is None or event_date > horizon:
            continue
        effective_date = max(as_of, event_date)
        raw_open = row.get("open_amount_cents")
        amount = int(raw_open if raw_open is not None else row.get("amount_cents") or 0)
        if amount <= 0:
            continue
        direction = 1 if str(row.get("event_type") or "").lower() == "inflow" else -1
        daily_net[effective_date] += direction * amount

    running = int(balance_cents)
    minimum = running
    for event_date in sorted(daily_net):
        running += daily_net[event_date]
        minimum = min(minimum, running)
    return minimum


def _priority_merchants(rows: Sequence[Mapping[str, Any]]) -> tuple[set[str], set[str]]:
    can_hold: set[str] = set()
    must_pay: set[str] = set()
    for row in rows:
        merchant = _merchant(row)
        if not merchant:
            continue
        priority = str(row.get("pay_priority") or "").strip().lower()
        if priority == "can_hold":
            can_hold.add(merchant)
        elif priority == "must_pay":
            must_pay.add(merchant)
    return can_hold, must_pay


def _recurring_facts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if len(rows) < 3:
        return None
    dates = [row["_event_date"] for row in rows]
    gaps = [(dates[index] - dates[index - 1]).days for index in range(1, len(dates))]
    median_gap = float(statistics.median(gaps))
    cadence = next((item for item in _CADENCES if item[1] <= median_gap <= item[2]), None)
    if cadence is None:
        return None
    _, minimum, maximum, annual_numerator, monthly_denominator = cadence
    consistent = sum(1 for gap in gaps if minimum <= gap <= maximum)
    consistency = consistent / len(gaps)
    amounts = [int(row["amount_cents"]) for row in rows]
    selected = amounts[-min(6, len(amounts)) :]
    current = _median_cents(selected)
    cv = _coefficient_of_variation(selected)
    rounded_gap = int(
        Decimal(str(median_gap)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    next_expected = dates[-1] + timedelta(days=rounded_gap)
    return {
        "cadence": cadence[0],
        "median_gap_days": median_gap,
        "cadence_consistency": consistency,
        "amount_cv": cv,
        "current_amount_cents": current,
        "next_expected": next_expected,
        "annual_numerator": annual_numerator,
        "monthly_denominator": monthly_denominator,
    }


def _build_recurring_cost(
    rows: Sequence[Mapping[str, Any]],
    facts: Mapping[str, Any],
    *,
    account_scope: str,
    as_of: date,
    forecast_stress_minimum_cents: int | None,
    floor_cents: int | None,
    source: Mapping[str, Any],
) -> dict[str, Any]:
    current = int(facts["current_amount_cents"])
    monthly = _normalize_monthly(current, str(facts["cadence"]))
    scenario = _scenario_improvement(current, facts, as_of)
    confidence, bps = _recurring_confidence(rows, facts, source, explicit_corroboration=True)
    return _opportunity(
        rows,
        opportunity_type="recurring_cost",
        account_scope=account_scope,
        cadence=str(facts["cadence"]),
        baseline_amount_cents=current,
        current_amount_cents=current,
        next_expected=facts["next_expected"],
        monthly_potential_cents=monthly,
        annual_gross_potential_cents=monthly * 12,
        observed_90d_potential_cents=None,
        scenario_improvement_cents=scenario,
        forecast_stress_minimum_cents=forecast_stress_minimum_cents,
        floor_cents=floor_cents,
        confidence=confidence,
        confidence_bps=bps,
        source=source,
        reason_codes=["explicit_can_hold", "recurring_posted_outflows"],
        limitations=["Usage and contract terms are not available.", "Net savings are unverified."],
        downside="The cost may support an active workflow; verify necessity and terms before acting.",
        facts=facts,
    )


def _build_price_increase(
    rows: Sequence[Mapping[str, Any]],
    facts: Mapping[str, Any],
    *,
    account_scope: str,
    as_of: date,
    forecast_stress_minimum_cents: int | None,
    floor_cents: int | None,
    source: Mapping[str, Any],
) -> dict[str, Any] | None:
    if len(rows) < 6 or (rows[-1]["_event_date"] - rows[0]["_event_date"]).days < 90:
        return None
    previous = [int(row["amount_cents"]) for row in rows[-6:-3]]
    latest = [int(row["amount_cents"]) for row in rows[-3:]]
    baseline = _median_cents(previous)
    current = _median_cents(latest)
    delta = current - baseline
    if baseline <= 0 or delta < max(1_000, _round_ratio(baseline, 10, 100)):
        return None
    window_cv = max(_coefficient_of_variation(previous), _coefficient_of_variation(latest))
    price_facts = dict(facts)
    price_facts["amount_cv"] = window_cv
    monthly = _normalize_monthly(delta, str(facts["cadence"]))
    scenario = _scenario_improvement(delta, facts, as_of)
    confidence, bps = _recurring_confidence(rows, price_facts, source, explicit_corroboration=False)
    return _opportunity(
        rows,
        opportunity_type="price_increase",
        account_scope=account_scope,
        cadence=str(facts["cadence"]),
        baseline_amount_cents=baseline,
        current_amount_cents=current,
        next_expected=facts["next_expected"],
        monthly_potential_cents=monthly,
        annual_gross_potential_cents=monthly * 12,
        observed_90d_potential_cents=None,
        scenario_improvement_cents=scenario,
        forecast_stress_minimum_cents=forecast_stress_minimum_cents,
        floor_cents=floor_cents,
        confidence=confidence,
        confidence_bps=bps,
        source=source,
        reason_codes=["stable_recurring_price_increase", "latest_three_above_prior_three"],
        limitations=["The increase may reflect a contract or usage change.", "Net savings are unverified."],
        downside="A rollback may change service level; verify the cause of the increase first.",
        facts=price_facts,
    )


def _build_fee_leakage(
    rows: Sequence[Mapping[str, Any]],
    *,
    account_scope: str,
    as_of: date,
    forecast_stress_minimum_cents: int | None,
    floor_cents: int | None,
    source: Mapping[str, Any],
) -> dict[str, Any] | None:
    cutoff = as_of - timedelta(days=89)
    fees = [row for row in rows if row["_event_date"] >= cutoff and _is_explicit_fee(row)]
    total = sum(int(row["amount_cents"]) for row in fees)
    if len(fees) < 2 and total < 10_000:
        return None
    confidence, bps = _fee_confidence(fees, total, source)
    annual = _round_ratio(total, 365, 90) if int(source["coverage_days"]) >= 90 else None
    return _opportunity(
        fees,
        opportunity_type="avoidable_fee",
        account_scope=account_scope,
        cadence="90_day",
        baseline_amount_cents=None,
        current_amount_cents=None,
        next_expected=None,
        monthly_potential_cents=None,
        annual_gross_potential_cents=annual,
        observed_90d_potential_cents=total,
        scenario_improvement_cents=None,
        forecast_stress_minimum_cents=forecast_stress_minimum_cents,
        floor_cents=floor_cents,
        confidence=confidence,
        confidence_bps=bps,
        source=source,
        reason_codes=["explicit_fee_description", "posted_fee_leakage_90d"],
        limitations=["Fee avoidance depends on the account or payment behavior.", "Net savings are unverified."],
        downside="Changing banking or payment behavior may add operational constraints.",
        facts={
            "cadence": "90_day",
            "median_gap_days": None,
            "cadence_consistency": None,
            "amount_cv": _coefficient_of_variation([int(row["amount_cents"]) for row in fees]),
        },
    )


def _opportunity(
    rows: Sequence[Mapping[str, Any]],
    *,
    opportunity_type: str,
    account_scope: str,
    cadence: str,
    baseline_amount_cents: int | None,
    current_amount_cents: int | None,
    next_expected: date | None,
    monthly_potential_cents: int | None,
    annual_gross_potential_cents: int | None,
    observed_90d_potential_cents: int | None,
    scenario_improvement_cents: int | None,
    forecast_stress_minimum_cents: int | None,
    floor_cents: int | None,
    confidence: str,
    confidence_bps: int,
    source: Mapping[str, Any],
    reason_codes: list[str],
    limitations: list[str],
    downside: str,
    facts: Mapping[str, Any],
) -> dict[str, Any]:
    merchant = rows[0]["_normalized_merchant"]
    display_name = _display_name(rows[-1])
    category = _slug(rows[-1].get("category")) or "uncategorized"
    key_input = f"{CONTRACT_VERSION}|{account_scope}|{merchant}|{cadence}|{opportunity_type}|outflow"
    opportunity_key = hashlib.sha256(key_input.encode()).hexdigest()
    evidence = sorted(
        (
            str(row.get("source_id") or row.get("id") or ""),
            row["_event_date"].isoformat(),
            -abs(int(row["amount_cents"])),
        )
        for row in rows
    )
    evidence_payload = {
        "contract_version": CONTRACT_VERSION,
        "rule_version": RULE_VERSION,
        "source_updated_at": source["as_of_date"],
        "transactions": evidence,
    }
    evidence_hash = hashlib.sha256(
        json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    gap = None
    offset = None
    visible_scenario = None
    if forecast_stress_minimum_cents is not None and floor_cents is not None:
        gap = max(0, int(floor_cents) - int(forecast_stress_minimum_cents))
        if scenario_improvement_cents is not None:
            visible_scenario = scenario_improvement_cents
            offset = min(gap, scenario_improvement_cents)
    return {
        "opportunity_key": opportunity_key,
        "opportunity_type": opportunity_type,
        "normalized_merchant": merchant,
        "display_name": display_name,
        "category": category,
        "cadence": cadence,
        "occurrence_count": len(rows),
        "evidence_transaction_ids": [item[0] for item in evidence],
        "evidence_dates": [item[1] for item in evidence],
        "evidence_amounts_cents": [abs(item[2]) for item in evidence],
        "baseline_amount_cents": baseline_amount_cents,
        "current_amount_cents": current_amount_cents,
        "next_expected_date": next_expected.isoformat() if next_expected else None,
        "one_time_potential_cents": None,
        "observed_90d_potential_cents": observed_90d_potential_cents,
        "monthly_potential_cents": monthly_potential_cents,
        "annual_gross_potential_cents": annual_gross_potential_cents,
        "verified_net_potential_cents": None,
        "scenario_28d_floor_improvement_cents": visible_scenario,
        "scenario_28d_funding_gap_offset_cents": offset,
        "scenario_funding_gap_cents": gap,
        "scenario_28d_stress_minimum_balance_cents": forecast_stress_minimum_cents,
        "data_confidence": confidence,
        "confidence_bps": confidence_bps,
        "decision_confidence": "unknown",
        "source_freshness": dict(source),
        "evidence_hash": evidence_hash,
        "reason_codes": reason_codes,
        "limitations": limitations,
        "downside": downside,
        "protected": False,
        "conflicted": False,
        "included_in_headline": True,
        "calculation": {
            "formula_id": f"{opportunity_type}:{RULE_VERSION}",
            "median_gap_days": facts.get("median_gap_days"),
            "cadence_consistency_bps": _ratio_bps(facts.get("cadence_consistency")),
            "amount_cv_bps": _ratio_bps(facts.get("amount_cv")),
        },
    }


def _recurring_confidence(
    rows: Sequence[Mapping[str, Any]],
    facts: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    explicit_corroboration: bool,
) -> tuple[str, int]:
    count = len(rows)
    consistency = float(facts["cadence_consistency"])
    cv = float(facts["amount_cv"])
    occurrence_score = 2_000 if count >= 5 else 1_500 if count == 4 else 1_000
    bps = (
        3_000
        + occurrence_score
        + _round_ratio(int(round(consistency * 10_000)), 2_000, 10_000)
        + _round_ratio(max(0, 10_000 - int(round(min(cv, 1.0) * 10_000))), 1_500, 10_000)
        + (1_000 if explicit_corroboration else 0)
        + (500 if not source["stale"] else 0)
    )
    bps = min(10_000, bps)
    if count >= 5 and cv <= 0.10 and consistency >= 0.80:
        return "high", max(8_500, bps)
    if count >= 3 and cv <= 0.25 and consistency >= 0.60:
        return "medium", min(8_499, max(6_500, bps))
    return "low", min(6_499, bps)


def _fee_confidence(
    rows: Sequence[Mapping[str, Any]], total_cents: int, source: Mapping[str, Any]
) -> tuple[str, int]:
    bps = (
        7_000
        + (1_000 if len(rows) >= 2 else 0)
        + 1_500
        + (500 if not source["stale"] else 0)
    )
    if len(rows) >= 4 and int(source["coverage_days"]) >= 90:
        return "high", max(8_500, min(10_000, bps))
    if len(rows) in {2, 3} or total_cents >= 10_000:
        return "medium", min(8_499, max(6_500, bps))
    return "low", min(6_499, bps)


def _mark_headline_overlaps(opportunities: Sequence[dict[str, Any]]) -> None:
    by_subject: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in opportunities:
        if item["opportunity_type"] != "avoidable_fee":
            by_subject[(item["normalized_merchant"], item["cadence"])].append(item)
    for items in by_subject.values():
        if len(items) < 2:
            continue
        winner = max(
            items,
            key=lambda item: (
                int(item["monthly_potential_cents"] or 0),
                item["opportunity_key"],
            ),
        )
        for item in items:
            if item is not winner:
                item["included_in_headline"] = False
                item["reason_codes"] = [
                    *item["reason_codes"],
                    "overlaps_larger_recurring_opportunity",
                ]


def _headline(opportunities: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    included = [item for item in opportunities if item["included_in_headline"]]
    return {
        "opportunity_count": len(opportunities),
        "headline_opportunity_count": len(included),
        "recurring_monthly_potential_cents": sum(
            int(item["monthly_potential_cents"] or 0) for item in included
        ),
        "recurring_annual_gross_potential_cents": sum(
            int(item["annual_gross_potential_cents"] or 0)
            for item in included
            if item["opportunity_type"] != "avoidable_fee"
        ),
        "fee_90d_potential_cents": sum(
            int(item["observed_90d_potential_cents"] or 0) for item in included
        ),
    }


def _empty_headline() -> dict[str, int]:
    return {
        "opportunity_count": 0,
        "headline_opportunity_count": 0,
        "recurring_monthly_potential_cents": 0,
        "recurring_annual_gross_potential_cents": 0,
        "fee_90d_potential_cents": 0,
    }


def _ranking_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    confidence = 0 if item["data_confidence"] == "high" else 1
    next_expected = item["next_expected_date"] or "9999-12-31"
    amount = item["monthly_potential_cents"] or item["observed_90d_potential_cents"] or 0
    return (
        confidence,
        -int(item["scenario_28d_funding_gap_offset_cents"] or 0),
        next_expected,
        -int(amount),
        item["opportunity_key"],
    )


def _scenario_improvement(amount_cents: int, facts: Mapping[str, Any], as_of: date) -> int:
    next_expected = facts["next_expected"]
    horizon = as_of + timedelta(days=28)
    if next_expected > horizon:
        return 0
    gap = max(
        1,
        int(
            Decimal(str(facts["median_gap_days"])).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
        ),
    )
    count = 1 + (horizon - next_expected).days // gap
    return amount_cents * count


def _normalize_monthly(amount_cents: int, cadence: str) -> int:
    cadence_def = next(item for item in _CADENCES if item[0] == cadence)
    return _round_ratio(amount_cents, cadence_def[3], cadence_def[4])


def _is_bank_transaction(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("record_kind") or "").lower() == "transaction"
        and str(row.get("source") or "").lower() == "csv"
        and str(row.get("status") or "").lower() in {"posted", "matched"}
    )


def _is_explicit_fee(row: Mapping[str, Any]) -> bool:
    if _slug(row.get("category")) in {"fee", "fees", "bank_fees"}:
        return True
    text = _event_text(row)
    return any(token in text for token in FEE_TOKENS)


def _merchant(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("normalized_merchant") or "").strip()
    if explicit:
        return _slug(explicit)
    raw = str(
        row.get("vendor_or_customer")
        or row.get("name")
        or row.get("description")
        or ""
    ).lower()
    raw = re.sub(r"\b(?:ach|debit|purchase|pos|recurring|withdrawal)\b", " ", raw)
    raw = re.sub(r"\b\d{4,}\b", " ", raw)
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    return _slug(" ".join(raw.split()))


def _display_name(row: Mapping[str, Any]) -> str:
    return str(
        row.get("friendly_name")
        or row.get("vendor_or_customer")
        or row.get("name")
        or row.get("description")
        or "Unknown merchant"
    ).strip()


def _event_text(row: Mapping[str, Any]) -> str:
    return " ".join(
        str(row.get(field) or "").strip().lower()
        for field in ("category", "name", "vendor_or_customer", "description")
    )


def _event_date(row: Mapping[str, Any]) -> date | None:
    for field in ("posted_date", "effective_date", "due_date", "transaction_date"):
        parsed = _to_date(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _event_sort_key(row: Mapping[str, Any]) -> tuple[date, str]:
    return row["_event_date"], str(row.get("source_id") or row.get("id") or "")


def _median_cents(values: Sequence[int]) -> int:
    return int(
        Decimal(str(statistics.median(values))).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )


def _coefficient_of_variation(values: Sequence[int]) -> float:
    if not values:
        return 1.0
    mean = statistics.mean(values)
    return statistics.pstdev(values) / mean if mean else 1.0


def _round_ratio(value: int, numerator: int, denominator: int) -> int:
    return int(
        (Decimal(value) * Decimal(numerator) / Decimal(denominator)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )


def _ratio_bps(value: Any) -> int | None:
    if value is None:
        return None
    return int(round(float(value) * 10_000))


def _has_any_flag(row: Mapping[str, Any], names: Sequence[str]) -> bool:
    return any(_truthy(row.get(name)) for name in names)


def _has_unresolved_match_state(row: Mapping[str, Any]) -> bool:
    return any(
        _slug(row.get(field)) in _UNRESOLVED_MATCH_STATES
        for field in _MATCH_STATE_FIELDS
    )


def _has_unresolved_match_or_conflict(row: Mapping[str, Any]) -> bool:
    return _has_any_flag(row, _CONFLICT_FLAGS) or _has_unresolved_match_state(row)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized not in {"", "0", "false", "no", "n", "none", "resolved"}
    return bool(value)


def _slug(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")
