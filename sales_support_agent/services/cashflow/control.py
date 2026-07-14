"""Pure Finance Control V2 state and recommendation engine.

The engine accepts row dictionaries so callers can use ORM projections, import
preview rows, or fixtures without coupling calculations to persistence. Amounts
are integer cents and all returned collections have deterministic ordering.
"""

from __future__ import annotations

import re
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence


ACTIVE_STATUSES = {"planned", "pending", "overdue", "open", "due"}
TERMINAL_STATUSES = {"paid", "matched", "cancelled", "canceled", "void"}
TRANSACTION_STATUSES = {"posted", "matched"}
GROUP_ORDER = (
    "resolve_first",
    "collect_now",
    "pay_now",
    "protect_cash",
    "this_week",
    "next_week",
)
GROUP_LABELS = {
    "resolve_first": "Resolve first",
    "collect_now": "Collect now",
    "pay_now": "Pay now",
    "protect_cash": "Protect cash",
    "this_week": "This week",
    "next_week": "Next week",
}
_PRIORITY_ORDER = {"must_pay": 0, "should_pay": 1, "review": 2, "can_hold": 3}
_CATEGORY_ORDER = {
    "payroll": 0,
    "tax": 1,
    "rent": 2,
    "debt": 3,
    "insurance": 4,
    "utilities": 5,
    "revenue": 6,
}


def _as_date(value: Any) -> date | None:
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


def _amount(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _money(cents: int) -> str:
    return f"${int(cents) / 100:,.0f}"


def _flexibility(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("flexibility") or "unknown").lower()
    if explicit not in {"", "unknown"}:
        return explicit
    evidence = " ".join(str(row.get(field) or "") for field in ("name", "description", "notes")).lower()
    if any(token in evidence for token in ("chunk", "partial", "installment")):
        return "chunkable"
    if any(token in evidence for token in ("defer", "flexible")):
        return "deferrable"
    return "unknown"


def _pay_priority(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("pay_priority") or "review").lower()
    notes = str(row.get("notes") or "").lower()
    match = re.search(r"priority:(must_pay|should_pay|review|can_hold)", notes)
    return match.group(1) if match else explicit


def _row_id(row: Mapping[str, Any], index: int = 0) -> str:
    return str(row.get("id") or row.get("source_id") or f"row-{index}")


def _event_date(row: Mapping[str, Any]) -> date | None:
    fields = (
        ("expected_date", "due_date", "effective_date")
        if row.get("event_type") == "inflow"
        else ("due_date", "expected_date", "effective_date")
    )
    for field in fields:
        parsed = _as_date(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _actual_date(row: Mapping[str, Any]) -> date | None:
    for field in ("posting_date", "transaction_date", "effective_date", "due_date", "date"):
        parsed = _as_date(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _is_transaction(row: Mapping[str, Any]) -> bool:
    kind = str(row.get("record_kind") or "").lower()
    if kind:
        return kind == "transaction"
    return str(row.get("source") or "").lower() == "csv" or str(row.get("status") or "").lower() in TRANSACTION_STATUSES


def _is_active_obligation(row: Mapping[str, Any]) -> bool:
    if _is_transaction(row):
        return False
    status = str(row.get("status") or "planned").lower()
    if status in {"cancelled", "canceled", "void"}:
        return False
    # Allocation-derived open balance wins over a stale legacy paid flag.
    if status in {"paid", "matched"}:
        return _amount(row.get("open_amount_cents")) > 0
    return True


def _is_duplicate(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("probable_duplicate")
        or row.get("is_duplicate")
        or str(row.get("classification") or "").lower() in {"duplicate", "conflict"}
    )


def _needs_match_review(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("possible_match")
        or row.get("match_candidates")
        or str(row.get("match_status") or "").lower() in {"possible", "ambiguous", "review"}
    )


def _normalise_allocations(
    annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None,
) -> dict[str, int]:
    """Return active allocated cents by obligation id.

    A mapping may be ``{obligation_id: cents}``, ``{id: [allocations]}``, or an
    object containing an ``allocations`` list. Reversal rows deactivate the
    allocation referenced by ``reversed_allocation_id``.
    """
    if not annotations:
        return {}
    if isinstance(annotations, Mapping) and isinstance(annotations.get("allocations"), Sequence):
        items: list[Any] = list(annotations["allocations"])
    elif isinstance(annotations, Mapping):
        items = []
        for obligation_id, value in annotations.items():
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                for allocation in value:
                    if isinstance(allocation, Mapping):
                        items.append({"obligation_event_id": obligation_id, **allocation})
            elif isinstance(value, Mapping):
                items.append({"obligation_event_id": obligation_id, **value})
            else:
                items.append({"obligation_event_id": obligation_id, "amount_cents": value})
    else:
        items = list(annotations)

    reversed_ids = {
        str(item.get("reversed_allocation_id"))
        for item in items
        if isinstance(item, Mapping) and item.get("reversed_allocation_id")
    }
    totals: dict[str, int] = defaultdict(int)
    for item in items:
        if not isinstance(item, Mapping):
            continue
        allocation_id = str(item.get("id") or item.get("allocation_id") or "")
        if allocation_id in reversed_ids:
            continue
        if item.get("reversed_allocation_id"):
            continue
        if item.get("reversed") or item.get("is_reversed") or item.get("active") is False:
            continue
        obligation_id = str(item.get("obligation_event_id") or item.get("obligation_id") or item.get("event_id") or "")
        if obligation_id:
            totals[obligation_id] += int(item.get("amount_cents") or item.get("settled_amount_cents") or 0)
    return {key: max(0, value) for key, value in totals.items()}


def annotate_open_amounts(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Copy rows and derive settled/open amounts without mutating the input."""
    allocated = _normalise_allocations(settlement_annotations)
    result: list[dict[str, Any]] = []
    for index, source_row in enumerate(rows):
        row = dict(source_row)
        event_id = _row_id(row, index)
        face = _amount(row.get("amount_cents"))
        embedded = _amount(row.get("settled_amount_cents"))
        settled = allocated.get(event_id, embedded)
        local_open = max(0, face - settled)
        source_open_value = row.get("source_open_amount_cents")
        source_open = None if source_open_value is None else _amount(source_open_value)
        row["id"] = event_id
        row["settled_amount_cents"] = min(face, settled)
        row["open_amount_cents"] = local_open
        row["local_open_amount_cents"] = local_open
        row["source_open_disagreement"] = bool(
            not _is_transaction(row)
            and source_open is not None
            and source_open != local_open
        )
        if row["source_open_disagreement"]:
            row["source_conflict"] = True
        result.append(row)
    return result


def resolve_cash_snapshot(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: date,
    stale_after_days: int = 3,
) -> dict[str, Any]:
    """Resolve the latest CSV balance; the first same-day row is closing cash."""
    candidates: list[tuple[int, Mapping[str, Any], date]] = []
    for index, row in enumerate(rows):
        if str(row.get("source") or "").lower() != "csv" or row.get("account_balance_cents") is None:
            continue
        row_date = _actual_date(row)
        if row_date is not None and row_date <= as_of:
            candidates.append((index, row, row_date))
    if not candidates:
        return {"available": False, "balance_cents": None, "as_of_date": None, "source": None, "stale": True, "age_days": None}
    _, latest, latest_date = max(candidates, key=lambda item: item[2])
    age_days = (as_of - latest_date).days
    return {
        "available": True,
        "balance_cents": int(latest["account_balance_cents"]),
        "as_of_date": latest_date.isoformat(),
        "source": str(latest.get("source") or "csv"),
        "stale": age_days > stale_after_days,
        "age_days": age_days,
    }


def _probability_bps(row: Mapping[str, Any]) -> int:
    if str(row.get("confidence") or "").lower() == "confirmed" and not row.get("trend_inferred"):
        return 10_000
    if row.get("probability_bps") is not None:
        return min(10_000, max(0, int(row["probability_bps"])))
    if row.get("probability") is not None:
        value = float(row["probability"])
        return min(10_000, max(0, round(value * (100 if value > 1 else 10_000))))
    return {"high": 7_500, "medium": 5_000, "estimated": 5_000, "low": 2_500}.get(
        str(row.get("confidence") or "estimated").lower(), 5_000
    )


def _installments(row: Mapping[str, Any], as_of: date, horizon_end: date) -> list[tuple[date, int]]:
    items = row.get("payment_installments") or row.get("installments") or []
    result: list[tuple[date, int]] = []
    for item in items if isinstance(items, Sequence) else []:
        if not isinstance(item, Mapping) or str(item.get("status") or "planned").lower() in TERMINAL_STATUSES:
            continue
        due = _as_date(item.get("due_date"))
        if due is not None and due <= horizon_end:
            result.append((max(as_of, due), _amount(item.get("amount_cents"))))
    return result


def _outflow_schedule(row: Mapping[str, Any], path: str, as_of: date, horizon_end: date) -> list[tuple[date, int]]:
    open_amount = _amount(row.get("open_amount_cents"))
    if not open_amount:
        return []
    due = _event_date(row)
    if due is None:
        return []
    due = max(as_of, due)
    installments = _installments(row, as_of, horizon_end)
    scheduled = min(open_amount, sum(amount for _, amount in installments))
    flexibility = _flexibility(row)
    priority = _pay_priority(row)

    if path == "expected":
        return [(due, open_amount)]
    if path == "stress":
        earliest = _as_date(row.get("earliest_plausible_date")) or due
        if priority == "must_pay" or flexibility in {"fixed", "unknown"}:
            return [(max(as_of, earliest), open_amount)]
        return _cap_schedule(installments, open_amount)
    if priority == "must_pay" or flexibility in {"fixed", "unknown"}:
        return [(due, open_amount)]
    if scheduled:
        return _cap_schedule(installments, open_amount)
    return []


def _cap_schedule(schedule: Iterable[tuple[date, int]], cap: int) -> list[tuple[date, int]]:
    result: list[tuple[date, int]] = []
    remaining = cap
    for due, amount in sorted(schedule):
        used = min(remaining, amount)
        if used:
            result.append((due, used))
            remaining -= used
        if remaining <= 0:
            break
    return result


def build_forecast_paths(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    as_of: date,
    starting_cash_cents: int,
    horizon_days: int = 28,
) -> dict[str, Any]:
    """Build Committed, Expected, and Stress daily paths."""
    canonical = annotate_open_amounts(rows, settlement_annotations)
    horizon_days = max(28, horizon_days)
    horizon_end = as_of + timedelta(days=horizon_days - 1)
    changes: dict[str, dict[date, int]] = {name: defaultdict(int) for name in ("committed", "expected", "stress")}

    for row in canonical:
        if not _is_active_obligation(row) or not row.get("open_amount_cents"):
            continue
        if row.get("source_open_disagreement"):
            # Conflicting provider and settlement evidence is not forecastable.
            # Keep the face/open amount visible in Resolve first instead.
            continue
        event_type = str(row.get("event_type") or "outflow").lower()
        due = _event_date(row)
        if due is None or due > horizon_end:
            continue
        if event_type == "outflow":
            for path in changes:
                for event_date, amount in _outflow_schedule(row, path, as_of, horizon_end):
                    if event_date <= horizon_end:
                        changes[path][event_date] -= amount
            continue

        open_amount = _amount(row.get("open_amount_cents"))
        confirmed = str(row.get("confidence") or "").lower() == "confirmed" and not row.get("trend_inferred")
        expected_date = due + timedelta(days=max(0, int(row.get("median_payment_lag_days") or 0)))
        stress_date = _as_date(row.get("p80_date")) or due + timedelta(days=max(0, int(row.get("p80_lag_days") or 0)))
        if confirmed:
            changes["committed"][max(as_of, due)] += open_amount
            if stress_date <= horizon_end:
                changes["stress"][max(as_of, stress_date)] += open_amount
        weighted = open_amount * _probability_bps(row) // 10_000
        if expected_date <= horizon_end:
            changes["expected"][max(as_of, expected_date)] += weighted

    paths: dict[str, list[dict[str, Any]]] = {}
    minima: dict[str, int] = {}
    for path_name, path_changes in changes.items():
        running = int(starting_cash_cents)
        points: list[dict[str, Any]] = []
        for offset in range(horizon_days):
            day = as_of + timedelta(days=offset)
            delta = path_changes.get(day, 0)
            running += delta
            points.append({"date": day.isoformat(), "change_cents": delta, "cash_cents": running})
        paths[path_name] = points
        minima[path_name] = min([starting_cash_cents, *(point["cash_cents"] for point in points)])
    return {
        "as_of_date": as_of.isoformat(),
        "horizon_days": horizon_days,
        "paths": paths,
        "minimum_committed_cash_cents": minima["committed"],
        "minimum_expected_cash_cents": minima["expected"],
        "minimum_stress_cash_cents": minima["stress"],
    }


def calculate_csv_trends(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: date,
) -> dict[str, Any]:
    """Calculate bank-history trends after transfer and duplicate exclusion."""
    actuals: list[tuple[Mapping[str, Any], date]] = []
    excluded = 0
    for row in rows:
        row_date = _actual_date(row)
        if not _is_transaction(row) or str(row.get("source") or "").lower() != "csv" or row_date is None or row_date > as_of:
            continue
        if str(row.get("category") or "").lower() == "transfer" or row.get("is_transfer") or _is_duplicate(row):
            excluded += 1
            continue
        actuals.append((row, row_date))

    def signed_sum(start: date, end: date) -> int:
        return sum(
            _amount(row.get("amount_cents")) * (1 if row.get("event_type") == "inflow" else -1)
            for row, row_date in actuals
            if start <= row_date <= end
        )

    net_28 = signed_sum(as_of - timedelta(days=27), as_of)
    net_56 = signed_sum(as_of - timedelta(days=55), as_of)
    prior_28 = net_56 - net_28
    tolerance = max(10_000, abs(prior_28) // 20)
    direction = "flat" if abs(net_28 - prior_28) <= tolerance else ("improving" if net_28 > prior_28 else "declining")

    weekly: dict[date, dict[str, int]] = defaultdict(lambda: {"inflow": 0, "outflow": 0})
    for row, row_date in actuals:
        if row_date < as_of - timedelta(days=55):
            continue
        week = row_date - timedelta(days=row_date.weekday())
        event_type = "inflow" if row.get("event_type") == "inflow" else "outflow"
        weekly[week][event_type] += _amount(row.get("amount_cents"))
    week_values = [weekly[key] for key in sorted(weekly)]
    inflows = [value["inflow"] for value in week_values]
    outflows = [value["outflow"] for value in week_values]
    weekly_nets = [value["inflow"] - value["outflow"] for value in week_values]
    median_in = int(statistics.median(inflows)) if inflows else 0
    median_out = int(statistics.median(outflows)) if outflows else 0

    recurring = _recurring_patterns(actuals)
    receipt_lags = _receipt_lags(actuals)
    span_days = (max((item[1] for item in actuals), default=as_of) - min((item[1] for item in actuals), default=as_of)).days
    confidence = "high" if len(actuals) >= 8 and span_days >= 56 else "medium" if len(actuals) >= 3 and span_days >= 28 else "low"
    return {
        "transaction_count": len(actuals),
        "excluded_count": excluded,
        "history_span_days": span_days,
        "confidence": confidence,
        "net_28_cents": net_28,
        "net_56_cents": net_56,
        "prior_28_net_cents": prior_28,
        "net_cash_direction": direction,
        "median_weekly_inflow_cents": median_in,
        "median_weekly_outflow_cents": median_out,
        "weekly_burn_cents": max(0, median_out - median_in),
        "weekly_net_volatility_cents": int(statistics.pstdev(weekly_nets)) if len(weekly_nets) > 1 else 0,
        "recurring_patterns": recurring,
        "customer_receipt_lag_days": receipt_lags,
    }


def _party_key(row: Mapping[str, Any]) -> str:
    raw = str(row.get("vendor_or_customer") or row.get("name") or row.get("description") or "unknown").lower()
    return re.sub(r"[^a-z0-9]+", " ", raw).strip() or "unknown"


def _recurring_patterns(actuals: Sequence[tuple[Mapping[str, Any], date]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[tuple[Mapping[str, Any], date]]] = defaultdict(list)
    for row, row_date in actuals:
        grouped[(str(row.get("event_type") or "outflow"), _party_key(row))].append((row, row_date))
    result: list[dict[str, Any]] = []
    for (event_type, party), occurrences in sorted(grouped.items()):
        if len(occurrences) < 3:
            continue
        ordered = sorted(occurrences, key=lambda item: item[1])
        gaps = [(right[1] - left[1]).days for left, right in zip(ordered, ordered[1:])]
        cadence = int(statistics.median(gaps)) if gaps else 0
        result.append(
            {
                "event_type": event_type,
                "party": party,
                "occurrences": len(ordered),
                "median_amount_cents": int(statistics.median(_amount(item[0].get("amount_cents")) for item in ordered)),
                "median_cadence_days": cadence,
                "confidence": "high" if len(ordered) >= 5 else "medium",
                "partial_payment_pattern": any(bool(item[0].get("partial_payment")) for item in ordered),
            }
        )
    return result


def _receipt_lags(actuals: Sequence[tuple[Mapping[str, Any], date]]) -> dict[str, int]:
    lags: dict[str, list[int]] = defaultdict(list)
    for row, actual_date in actuals:
        if row.get("event_type") != "inflow" or not (row.get("matched_to_id") or row.get("obligation_event_id")):
            continue
        due = _as_date(row.get("invoice_due_date") or row.get("obligation_due_date"))
        if due is not None:
            lags[_party_key(row)].append((actual_date - due).days)
    return {party: int(statistics.median(values)) for party, values in sorted(lags.items()) if len(values) >= 3}


def assess_confidence(snapshot: Mapping[str, Any], trends: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    reasons: list[str] = []
    settlement_evidence_missing = any(
        not _is_transaction(row) and row.get("settlement_evidence_available") is False
        for row in rows
    )
    if not snapshot.get("available"):
        reasons.append("cash balance is missing")
    elif snapshot.get("stale"):
        reasons.append("cash balance is stale")
    if settlement_evidence_missing:
        reasons.append("settlement evidence is unavailable")
    if trends.get("confidence") == "low":
        reasons.append("less than 28 days of comparable bank history")
    if any(_is_duplicate(row) or _needs_match_review(row) for row in rows):
        reasons.append("unresolved duplicate or match candidates")
    level = (
        "low"
        if not snapshot.get("available") or snapshot.get("stale") or settlement_evidence_missing
        else ("medium" if reasons else str(trends.get("confidence") or "medium"))
    )
    return {"level": level, "verification_only": level == "low", "reasons": reasons}


def _summary_metrics(canonical: Sequence[Mapping[str, Any]], as_of: date, window_days: int) -> dict[str, int]:
    end = as_of + timedelta(days=window_days - 1)
    confirmed_in = expected_in = required_out = exposure_out = 0
    for row in canonical:
        if not _is_active_obligation(row):
            continue
        if row.get("source_open_disagreement"):
            continue
        due = _event_date(row)
        open_amount = _amount(row.get("open_amount_cents"))
        if row.get("event_type") == "inflow":
            if due is None or due > end:
                continue
            if str(row.get("confidence") or "").lower() == "confirmed" and not row.get("trend_inferred"):
                confirmed_in += open_amount
            else:
                expected_in += open_amount * _probability_bps(row) // 10_000
        else:
            # The operator rule is deliberately conservative: every open bill
            # overdue or due through day 14 is reserved in full. Chunkability
            # changes the recommended action, not the stated obligation.
            if due is not None and due <= end:
                required_out += open_amount
            else:
                exposure_out += open_amount
    return {
        "confirmed_incoming_cents": confirmed_in,
        "expected_incoming_cents": expected_in,
        "required_outgoing_cents": required_out,
        "outgoing_exposure_cents": exposure_out,
    }


def quick_action_eligibility(
    row: Mapping[str, Any],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """Return ordered quick actions; every write is preview-only."""
    del as_of  # Reserved for date-sensitive policy without reading a clock.
    row = annotate_open_amounts([row], settlement_annotations)[0]
    is_closed = not _is_transaction(row) and not _amount(row.get("open_amount_cents"))
    if _is_transaction(row):
        actions = ["preview_cash_impact"]
        if row.get("event_type") == "inflow":
            actions.append("match_bank_deposit")
        else:
            actions.append("match_bank_transaction")
        actions.append("flag_duplicate")
    elif row.get("event_type") == "inflow":
        actions = ["preview_cash_impact"]
        if is_closed:
            actions = ["preview_cash_impact"]
        elif _event_date(row) is None or str(row.get("confidence") or "").lower() != "confirmed":
            actions.append("confirm_expected_date")
        if not is_closed:
            actions.extend(["mark_received", "match_bank_deposit", "change_confidence", "assign_follow_up"])
    else:
        actions = ["preview_cash_impact"]
        if not is_closed:
            actions.append("record_partial_payment")
        flexibility = _flexibility(row)
        priority = _pay_priority(row)
        if not is_closed and flexibility == "chunkable":
            actions.append("split_into_installments")
        if not is_closed and flexibility in {"chunkable", "deferrable"} and priority != "must_pay":
            actions.append("defer_or_change_date")
        if not is_closed:
            actions.extend(["match_bank_transaction", "mark_paid", "flag_duplicate"])
    if str(row.get("source") or "").lower() in {"clickup", "qbo", "quickbooks"} or row.get("source_url"):
        actions.append("open_source")
    return [
        {"action_type": action, "eligible": True, "preview_required": action != "open_source", "confirmation_required": action != "open_source"}
        for action in actions
    ]


def _blocker(row: Mapping[str, Any], *, as_of: date, stale_source_days: int = 7) -> str | None:
    if row.get("source_open_disagreement"):
        return "source_open_disagreement"
    if _is_duplicate(row):
        return "probable_duplicate"
    if _needs_match_review(row):
        return "possible_actual_match"
    if _amount(row.get("amount_cents")) == 0:
        return "missing_amount"
    if _event_date(row) is None and not _is_transaction(row):
        return "missing_date"
    updated = _as_date(row.get("source_updated_at") or row.get("updated_at"))
    if row.get("source_stale") or (updated is not None and (as_of - updated).days > stale_source_days and row.get("source") in {"clickup", "qbo"}):
        return "stale_source"
    return None


def build_queue(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    as_of: date,
    horizon_days: int = 14,
    funding_gap_cents: int = 0,
) -> dict[str, Any]:
    """Build complete ordered queue groups without truncating collapsed rows."""
    canonical = annotate_open_amounts(rows, settlement_annotations)
    groups: dict[str, list[dict[str, Any]]] = {key: [] for key in GROUP_ORDER}
    for row in canonical:
        blocker = _blocker(row, as_of=as_of)
        if not blocker and (not _is_active_obligation(row) or not row.get("open_amount_cents")):
            continue
        due = _event_date(row)
        days_until = (due - as_of).days if due else None
        if blocker:
            group = "resolve_first"
        elif (
            row.get("event_type") == "inflow"
            and str(row.get("confidence") or "").lower() == "confirmed"
            and (days_until is not None and days_until <= 2 or str(row.get("status")) == "overdue")
        ):
            group = "collect_now"
        elif row.get("event_type") == "outflow" and days_until is not None and days_until < 0:
            group = "pay_now"
        elif row.get("event_type") == "outflow" and _pay_priority(row) == "must_pay" and days_until is not None and days_until <= 2 and funding_gap_cents == 0:
            group = "pay_now"
        elif row.get("event_type") == "outflow" and _flexibility(row) in {"chunkable", "deferrable"} and funding_gap_cents > 0:
            group = "protect_cash"
        elif days_until is not None and days_until <= 7:
            group = "this_week"
        elif days_until is not None and days_until <= horizon_days:
            group = "next_week"
        else:
            continue
        overdue_days = max(0, -days_until) if days_until is not None else 0
        open_amount = _amount(row.get("open_amount_cents"))
        item = {
            "id": row["id"],
            "group": group,
            "event_type": str(row.get("event_type") or "outflow"),
            "confidence": str(row.get("confidence") or "estimated"),
            "party": str(row.get("vendor_or_customer") or row.get("name") or row.get("description") or ""),
            "due_date": due.isoformat() if due else None,
            "days_until_due": days_until,
            "overdue_days": overdue_days,
            "open_amount_cents": open_amount,
            "decision_blocker": blocker,
            "pay_priority": _pay_priority(row),
            "flexibility": _flexibility(row),
            "category": str(row.get("category") or "uncategorized"),
            "floor_impact_cents": min(open_amount, max(0, funding_gap_cents)),
            "needs_action": group not in {"this_week", "next_week"},
            "action_label": "Review overdue" if days_until is not None and days_until < 0 else {
                "resolve_first": "Resolve first",
                "collect_now": "Collect now",
                "pay_now": "Pay now",
                "protect_cash": "Protect cash",
                "this_week": "Review this week",
                "next_week": "Plan next week",
            }[group],
            "quick_actions": quick_action_eligibility(row, as_of=as_of),
        }
        groups[group].append(item)

    def sort_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            0 if item["decision_blocker"] else 1,
            _PRIORITY_ORDER.get(str(item["pay_priority"]), 9),
            _CATEGORY_ORDER.get(str(item["category"]), 99),
            -int(item["overdue_days"]),
            -int(item["floor_impact_cents"]),
            item["due_date"] or "9999-12-31",
            -int(item["open_amount_cents"]),
            str(item["id"]),
        )

    result_groups = []
    all_items: list[dict[str, Any]] = []
    for key in GROUP_ORDER:
        items = sorted(groups[key], key=sort_key)
        all_items.extend(items)
        result_groups.append(
            {
                "key": key,
                "label": GROUP_LABELS[key],
                "collapsed": key == "next_week",
                "count": len(items),
                "subtotal_cents": sum(item["open_amount_cents"] for item in items),
                "items": items,
            }
        )
    return {"groups": result_groups, "items": all_items, "count": len(all_items), "truncated": False}


def build_recommendations(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return ranked, explainable recommendation candidates from Finance state."""
    as_of = _as_date(state.get("as_of_date")) or date.min
    confidence = state.get("confidence") or {}
    forecast = state.get("forecast") or {}
    metrics = state.get("metrics") or {}
    queue = state.get("queue") or {}
    minimum = int(forecast.get("minimum_stress_cash_cents") or 0)
    limitations = list(confidence.get("reasons") or [])
    expires = (as_of + timedelta(days=1)).isoformat()
    candidates: list[dict[str, Any]] = []

    def add(
        rank: int,
        action: str,
        item: Mapping[str, Any] | None,
        facts: list[str],
        before: int,
        after: int,
        downside: str,
        *,
        eligible_amount_cents: int | None = None,
        recommendation_confidence: str | None = None,
    ) -> None:
        target = None
        if item is not None:
            open_amount = _amount(item.get("open_amount_cents"))
            target = {
                "id": item.get("id"),
                "event_type": item.get("event_type"),
                "amount_cents": open_amount,
                "eligible_amount_cents": open_amount if eligible_amount_cents is None else eligible_amount_cents,
            }
        candidates.append(
            {
                "rank": rank,
                "action_type": action,
                "target": target,
                "triggering_facts": facts,
                "before_minimum_cash_cents": before,
                "after_minimum_cash_cents": after,
                "dependencies": [],
                "excluded_income_cents": int(metrics.get("expected_incoming_cents") or 0),
                "confidence": recommendation_confidence or str(confidence.get("level") or "medium"),
                "limitations": limitations,
                "downside": downside,
                "expires_on": expires,
                "preview_required": action != "no_action",
            }
        )

    snapshot = state.get("cash_snapshot") or {}
    if not snapshot.get("available"):
        add(1, "upload_latest_balance", None, ["Cash balance is missing."], minimum, minimum, "Recommendations remain unavailable until cash is verified.", recommendation_confidence="high")
    elif snapshot.get("stale"):
        add(1, "refresh_cash_balance", None, [f"Cash balance is {snapshot.get('age_days')} days old."], minimum, minimum, "Payment decisions use stale cash until refreshed.", recommendation_confidence="high")

    for item in queue.get("items") or []:
        blocker = item.get("decision_blocker")
        if blocker:
            add(2 if blocker in {"probable_duplicate", "possible_actual_match"} else 1, f"resolve_{blocker}", item, [str(blocker).replace("_", " ").capitalize()], minimum, minimum, "Resolving evidence may change open cash exposure.")

    if confidence.get("verification_only"):
        if not candidates:
            add(1, "verify_finance_data", None, limitations or ["Finance confidence is low."], minimum, minimum, "Action recommendations stay suppressed.")
        return _rank(candidates)

    funding_gap = int(metrics.get("funding_gap_cents") or 0)
    if funding_gap:
        incoming = [item for item in queue.get("items") or [] if item.get("group") == "collect_now"]
        for item in incoming:
            impact = min(_amount(item.get("open_amount_cents")), funding_gap)
            add(3, "collect_confirmed_income", item, [f"Confirmed receipt covers {impact} cents of the floor breach."], minimum - impact, minimum, "Receipt timing may slip despite confirmation.")
        protected = [item for item in queue.get("items") or [] if item.get("group") == "protect_cash"]
        for item in protected:
            impact = min(_amount(item.get("open_amount_cents")), funding_gap)
            add(4, "split_or_defer_payable", item, [f"Flexible payment contributes up to {impact} cents to the breach."], minimum, minimum + impact, "The unpaid balance remains due and may require vendor agreement.")
    else:
        safe_to_commit = int(metrics.get("safe_to_commit_cents") or 0)
        for item in queue.get("items") or []:
            eligible = min(_amount(item.get("open_amount_cents")), safe_to_commit)
            if item.get("group") == "pay_now" and item.get("pay_priority") == "must_pay" and eligible:
                add(
                    5,
                    "pay_or_schedule_must_pay",
                    item,
                    ["Stress cash remains above the configured floor."],
                    minimum,
                    minimum,
                    "Cash falls if other expected obligations are missing.",
                    eligible_amount_cents=eligible,
                )
    if not candidates:
        add(6, "no_action", None, ["Stress cash remains above the floor and no decision blockers are open."], minimum, minimum, "Recalculate when money data changes.", recommendation_confidence="high")
    return _rank(candidates)


def _rank(candidates: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(candidates, key=lambda item: (item["rank"], str((item.get("target") or {}).get("id") or ""), item["action_type"]))
    for position, item in enumerate(ordered, 1):
        item["position"] = position
    return ordered


def build_finance_control_state(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    as_of: date | None = None,
    floor_cents: int | None = None,
    horizon_days: int = 28,
    summary_days: int = 14,
    balance_stale_after_days: int = 3,
) -> dict[str, Any]:
    """Build the complete deterministic Finance Control V2 read model."""
    from sales_support_agent.services.cashflow.settings import resolve_cash_floor_cents

    effective_date = as_of or date.today()
    floor_cents = resolve_cash_floor_cents(floor_cents)
    canonical = annotate_open_amounts(rows, settlement_annotations)
    snapshot = resolve_cash_snapshot(rows, as_of=effective_date, stale_after_days=balance_stale_after_days)
    starting_cash = int(snapshot["balance_cents"] or 0)
    forecast = build_forecast_paths(
        canonical,
        as_of=effective_date,
        starting_cash_cents=starting_cash,
        horizon_days=horizon_days,
    )
    trends = calculate_csv_trends(rows, as_of=effective_date)
    confidence = assess_confidence(snapshot, trends, canonical)
    metrics: dict[str, Any] = _summary_metrics(canonical, effective_date, summary_days)
    minimum_stress = int(forecast["minimum_stress_cash_cents"])
    metrics.update(
        {
            "cash_on_hand_cents": snapshot["balance_cents"],
            "cash_available": bool(snapshot["available"]),
            "floor_cents": int(floor_cents),
            "minimum_stress_cash_cents": minimum_stress if snapshot["available"] else None,
            "safe_to_commit_cents": max(0, minimum_stress - floor_cents) if snapshot["available"] else None,
            "funding_gap_cents": max(0, floor_cents - minimum_stress) if snapshot["available"] else None,
        }
    )
    queue = build_queue(
        canonical,
        as_of=effective_date,
        horizon_days=max(summary_days, horizon_days),
        funding_gap_cents=int(metrics.get("funding_gap_cents") or 0),
    )
    state: dict[str, Any] = {
        "as_of_date": effective_date.isoformat(),
        "cash_snapshot": snapshot,
        "metrics": metrics,
        "forecast": forecast,
        "trends": trends,
        "confidence": confidence,
        "queue": queue,
    }
    state["recommendations"] = build_recommendations(state)
    return state


def build_cash_metrics(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    as_of: date | None = None,
    floor_cents: int | None = None,
    horizon_days: int = 28,
    summary_days: int = 14,
    balance_stale_after_days: int = 3,
) -> dict[str, Any]:
    """Build only the cash/income/outgoing metric portion of Finance state."""
    return build_finance_control_state(
        rows,
        settlement_annotations,
        as_of=as_of,
        floor_cents=floor_cents,
        horizon_days=horizon_days,
        summary_days=summary_days,
        balance_stale_after_days=balance_stale_after_days,
    )["metrics"]


def build_finance_control(
    rows: Sequence[Mapping[str, Any]],
    balance_cents: int | Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    balance_as_of: str | date | None = None,
    *,
    smart_mode: bool = True,
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    as_of: date | None = None,
    floor_cents: int | None = None,
) -> dict[str, Any]:
    """Renderer-friendly facade over :func:`build_finance_control_state`.

    The legacy renderer passes a separately resolved balance as positional
    arguments. Direct service callers may instead pass settlement annotations
    as the second argument and omit ``balance_as_of``.
    """
    if balance_cents is not None and not isinstance(balance_cents, int):
        if settlement_annotations is not None:
            raise ValueError("settlement annotations were supplied twice")
        settlement_annotations = balance_cents
        balance_cents = None

    from sales_support_agent.services.cashflow.settings import resolve_cash_floor_cents

    effective_date = as_of or date.today()
    floor_cents = resolve_cash_floor_cents(floor_cents)
    working_rows = list(rows)
    override_date = _as_date(balance_as_of)
    if balance_cents is not None and override_date is not None:
        # A transfer-classified zero-amount row supplies cash without changing
        # transaction trends or mutating the caller's rows.
        working_rows.insert(
            0,
            {
                "id": "finance-control-balance-override",
                "record_kind": "transaction",
                "source": "csv",
                "source_id": "finance-control-balance-override",
                "event_type": "inflow",
                "category": "transfer",
                "amount_cents": 0,
                "due_date": override_date,
                "status": "posted",
                "confidence": "confirmed",
                "account_balance_cents": balance_cents,
            },
        )

    state = build_finance_control_state(
        working_rows,
        settlement_annotations,
        as_of=effective_date,
        floor_cents=floor_cents,
    )
    metrics = state["metrics"]
    forecast = state["forecast"]
    path_data = forecast["paths"]
    labels = [point["date"] for point in path_data["committed"]]
    state["forecast"] = {
        **forecast,
        "labels": labels,
        "actual": [metrics["cash_on_hand_cents"], *([None] * (len(labels) - 1))],
        "committed": [point["cash_cents"] for point in path_data["committed"]],
        "expected": [point["cash_cents"] for point in path_data["expected"]],
        "stress": [point["cash_cents"] for point in path_data["stress"]],
        "floor_cents": floor_cents,
    }
    state["cash_position"] = {
        **metrics,
        "balance_available": metrics["cash_available"],
        "incoming_confirmed_cents": metrics["confirmed_incoming_cents"],
        "incoming_expected_cents": metrics["expected_incoming_cents"],
        "required_out_cents": metrics["required_outgoing_cents"],
        "exposure_out_cents": metrics["outgoing_exposure_cents"],
    }
    state["smart_brief"] = _smart_brief(state)
    top = state["recommendations"][0] if smart_mode and state["recommendations"] else None
    state["recommendation"] = _renderer_recommendation(top)
    if not smart_mode:
        state["recommendations"] = []
    return state


def _smart_brief(state: Mapping[str, Any]) -> dict[str, str]:
    metrics = state["metrics"]
    trends = state["trends"]
    confidence = state["confidence"]
    queue = state["queue"]
    happening = (
        f"Cash is {trends['net_cash_direction']} over 28 days; "
        f"{_money(metrics['confirmed_incoming_cents'])} confirmed in and "
        f"{_money(metrics['required_outgoing_cents'])} required out are due in 14 days."
    )
    blockers = sum(1 for item in queue["items"] if item.get("decision_blocker"))
    if confidence["reasons"]:
        broken = "; ".join(str(reason).capitalize() for reason in confidence["reasons"]) + "."
    elif blockers:
        broken = f"{blockers} money items block a safe decision."
    else:
        broken = "No material decision blockers are open."
    recommendation = state["recommendations"][0] if state["recommendations"] else None
    next_action = (
        str(recommendation["action_type"]).replace("_", " ").capitalize() + "."
        if recommendation
        else "Refresh money sources before the next review."
    )
    return {"happening": happening, "broken": broken, "next": next_action}


def _renderer_recommendation(recommendation: Mapping[str, Any] | None) -> dict[str, Any]:
    if not recommendation:
        return {}
    facts = recommendation.get("triggering_facts") or []
    dependencies = recommendation.get("dependencies") or []
    limitations = recommendation.get("limitations") or []
    return {
        **recommendation,
        "title": str(recommendation["action_type"]).replace("_", " ").capitalize(),
        "why": " ".join(str(item) for item in facts),
        "depends_on": "; ".join(str(item) for item in dependencies) or "Confirmed income only; expected trend income is excluded.",
        "limitations": "; ".join(str(item) for item in limitations) or "Recalculate when source data changes.",
        "action_label": "Create action preview",
    }


# Short aliases for callers that use service-oriented naming.
build_control_state = build_finance_control_state
build_cashflow_paths = build_forecast_paths
build_trend_metrics = calculate_csv_trends
eligible_quick_actions = quick_action_eligibility


__all__ = [
    "annotate_open_amounts",
    "assess_confidence",
    "build_cashflow_paths",
    "build_cash_metrics",
    "build_control_state",
    "build_finance_control_state",
    "build_finance_control",
    "build_forecast_paths",
    "build_queue",
    "build_recommendations",
    "build_trend_metrics",
    "calculate_csv_trends",
    "eligible_quick_actions",
    "quick_action_eligibility",
    "resolve_cash_snapshot",
]
