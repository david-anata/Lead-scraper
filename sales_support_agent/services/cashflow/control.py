"""Pure Finance Control V2 state and recommendation engine.

The engine accepts row dictionaries so callers can use ORM projections, import
preview rows, or fixtures without coupling calculations to persistence. Amounts
are integer cents and all returned collections have deterministic ordering.
"""

from __future__ import annotations

import re
import statistics
from hashlib import sha256
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, Sequence


ACTIVE_STATUSES = {"planned", "pending", "overdue", "open", "due"}
TERMINAL_STATUSES = {"paid", "matched", "cancelled", "canceled", "void", "completed"}
TRANSACTION_STATUSES = {"posted", "matched"}
INCOME_DECISIONS = {"review", "track_expected", "exclude", "one_time"}
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
    if status == "completed":
        # Operational completion can release a forecast only after a newer
        # bank snapshot has had a chance to confirm the resulting cash state.
        return bool(row.get("completion_requires_bank_evidence"))
    if status in {"cancelled", "canceled", "void"}:
        return False
    # Allocation-derived open balance wins over a stale legacy paid flag.
    if status in {"paid", "matched"}:
        return _amount(row.get("open_amount_cents")) > 0
    return True


def _is_probable_duplicate(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("probable_duplicate")
        or row.get("is_duplicate")
        or str(row.get("classification") or "").lower() == "duplicate"
    )


def _is_duplicate(row: Mapping[str, Any]) -> bool:
    return _is_probable_duplicate(row) or str(row.get("classification") or "").lower() == "conflict"


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


def _annotate_clickup_completion_evidence(
    rows: Sequence[Mapping[str, Any]], snapshot: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Keep a recent ClickUp completion reserved until bank evidence is newer."""
    bank_date = _as_date(snapshot.get("as_of_date"))
    result: list[dict[str, Any]] = []
    for source_row in rows:
        row = dict(source_row)
        if (
            str(row.get("source") or "").lower() == "clickup"
            and str(row.get("status") or "").lower() == "completed"
        ):
            completed_date = _as_date(row.get("source_updated_at"))
            row["completion_requires_bank_evidence"] = bool(
                not snapshot.get("available")
                or snapshot.get("stale")
                or completed_date is None
                or bank_date is None
                or bank_date <= completed_date
            )
        result.append(row)
    return result


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


def _transfer_like(row: Mapping[str, Any]) -> bool:
    evidence = " ".join(
        str(row.get(field) or "")
        for field in ("vendor_or_customer", "name", "description", "memo")
    ).lower()
    return bool(
        str(row.get("category") or "").lower() in {"transfer", "refund", "reversal"}
        or row.get("is_transfer")
        or any(
            token in evidence
            for token in (
                "type: transfer",
                "internal transfer",
                "account transfer",
                "online transfer",
                "credit card payment",
                "card payment",
                "bill paymt",
                "from share",
                "cashout",
                "refund",
                "reversal",
            )
        )
    )


def _income_decision_map(
    decisions: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None,
) -> dict[str, str]:
    if not decisions:
        return {}
    result: dict[str, str] = {}
    if isinstance(decisions, Mapping):
        items = decisions.get("decisions")
        if isinstance(items, Sequence) and not isinstance(items, (str, bytes)):
            iterable: Iterable[Any] = items
        else:
            iterable = (
                {"pattern_key": key, "decision": value}
                if not isinstance(value, Mapping)
                else {"pattern_key": key, **value}
                for key, value in decisions.items()
            )
    else:
        iterable = decisions
    for item in iterable:
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("pattern_key") or item.get("key") or "").strip()
        decision = str(item.get("decision") or "").strip().lower()
        if key and decision in INCOME_DECISIONS:
            result[key] = decision
    return result


def _quarantine_reason(row: Mapping[str, Any], *, as_of: date) -> str | None:
    if _is_probable_duplicate(row):
        # A recent, active payable with no amount is not harmless duplicate noise.
        # Keep it visible so its missing amount can block payment decisions.
        row_date = _event_date(row) or _actual_date(row)
        if (
            not _is_transaction(row)
            and _is_active_obligation(row)
            and not _amount(row.get("amount_cents"))
            and row_date is not None
            and (as_of - row_date).days <= 90
        ):
            return None
        return "probable_duplicate"
    if _amount(row.get("amount_cents")):
        return None
    if _is_transaction(row):
        return "zero_transaction"
    row_date = _event_date(row) or _actual_date(row)
    if row_date is not None and (as_of - row_date).days > 90:
        return "stale_zero_obligation"
    return None


def _partition_data_quality(
    rows: Sequence[Mapping[str, Any]], *, as_of: date
) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    visible: list[Mapping[str, Any]] = []
    quarantined: list[dict[str, str]] = []
    counts = {
        "total": len(rows),
        "visible": 0,
        "quarantined": 0,
        "probable_duplicates": 0,
        "zero_transactions": 0,
        "stale_zero_obligations": 0,
        "actionable_zero_obligations": 0,
    }
    reason_to_count = {
        "probable_duplicate": "probable_duplicates",
        "zero_transaction": "zero_transactions",
        "stale_zero_obligation": "stale_zero_obligations",
    }
    for index, row in enumerate(rows):
        reason = _quarantine_reason(row, as_of=as_of)
        if reason:
            quarantined.append({"id": _row_id(row, index), "reason": reason})
            counts[reason_to_count[reason]] += 1
            continue
        visible.append(row)
        if not _is_transaction(row) and not _amount(row.get("amount_cents")):
            counts["actionable_zero_obligations"] += 1
    counts["visible"] = len(visible)
    counts["quarantined"] = len(quarantined)
    return visible, {
        "counts": counts,
        "total_count": counts["total"],
        "visible_count": counts["visible"],
        "quarantined_count": counts["quarantined"],
        "probable_duplicate_count": counts["probable_duplicates"],
        "zero_transaction_count": counts["zero_transactions"],
        "stale_zero_obligation_count": counts["stale_zero_obligations"],
        "actionable_zero_obligation_count": counts["actionable_zero_obligations"],
        "quarantine": quarantined,
    }


def _connection_for(
    source_connections: Mapping[str, Any] | None, aliases: Sequence[str]
) -> Mapping[str, Any]:
    if not isinstance(source_connections, Mapping):
        return {}
    normalized = {
        re.sub(r"[^a-z0-9]+", "", str(key).lower()): value
        for key, value in source_connections.items()
    }
    for alias in aliases:
        value = normalized.get(re.sub(r"[^a-z0-9]+", "", alias.lower()))
        if isinstance(value, Mapping):
            return value
        if isinstance(value, bool):
            return {"connected": value}
    return {}


def _build_source_status(
    rows: Sequence[Mapping[str, Any]],
    snapshot: Mapping[str, Any],
    *,
    as_of: date,
    source_connections: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    definitions = (
        ("bank_csv", "Bank CSV", ("csv", "bank_csv", "bank csv"), {"csv"}),
        ("clickup", "ClickUp", ("clickup",), {"clickup"}),
        ("quickbooks", "QuickBooks", ("quickbooks", "qbo"), {"quickbooks", "qbo"}),
    )
    result: list[dict[str, Any]] = []
    for key, name, aliases, row_sources in definitions:
        source_rows = [
            row for row in rows if str(row.get("source") or "").lower() in row_sources
        ]
        connection = _connection_for(source_connections, aliases)
        explicit_status = str(connection.get("status") or "").strip().lower()
        connected = connection.get("connected")
        updated = _as_date(
            connection.get("last_synced_at")
            or connection.get("last_success_at")
            or connection.get("updated_at")
        )
        if key == "bank_csv":
            status = "missing" if not snapshot.get("available") else (
                "stale" if snapshot.get("stale") else "current"
            )
            ready = status == "current"
            latest_date = snapshot.get("as_of_date")
        else:
            row_dates = [
                parsed
                for row in source_rows
                if (parsed := _as_date(row.get("source_updated_at") or row.get("updated_at") or _event_date(row)))
            ]
            latest = updated or max(row_dates, default=None)
            stale = bool(
                explicit_status == "stale"
                or any(row.get("source_stale") for row in source_rows)
                or (latest is not None and (as_of - latest).days > 7)
            )
            if connected is False or explicit_status in {"disconnected", "not_connected"}:
                status, ready = "disconnected", False
            elif explicit_status in {"error", "failed"}:
                status, ready = "error", False
            elif stale:
                status, ready = "stale", False
            elif not source_rows or not any(_event_date(row) is not None for row in source_rows):
                # A healthy connection alone does not establish that payable data
                # was actually received and dated for operational use.
                status, ready = "connected", False
            elif source_rows:
                status, ready = "current", True
            else:
                status, ready = "not_connected", False
            latest_date = latest.isoformat() if latest else None
        result.append(
            {
                "key": key,
                "name": name,
                "status": status,
                "ready": ready,
                "row_count": len(source_rows),
                "latest_date": latest_date,
            }
        )
    return result


def _build_trust_gate(
    snapshot: Mapping[str, Any],
    canonical: Sequence[Mapping[str, Any]],
    income_projection: Mapping[str, Any],
    *,
    as_of: date,
) -> dict[str, Any]:
    cash_ready = bool(snapshot.get("available") and not snapshot.get("stale"))
    payable_issues: list[tuple[str, str]] = []
    for row in canonical:
        if not _is_active_obligation(row):
            continue
        row_id = str(row.get("id") or "unknown")
        if not _amount(row.get("amount_cents")):
            payable_issues.append((row_id, "missing amount"))
        elif (
            row.get("source_open_disagreement")
            or row.get("source_conflict")
            or str(row.get("classification") or "").lower() == "conflict"
        ):
            payable_issues.append((row_id, "source conflict"))
        elif str(row.get("source_status") or "").lower() == "source_missing":
            payable_issues.append((row_id, "missing from ClickUp source"))
        elif row.get("completion_requires_bank_evidence"):
            payable_issues.append((row_id, "ClickUp completion is newer than bank evidence"))
        elif _needs_match_review(row):
            payable_issues.append((row_id, "ambiguous match"))
        elif row.get("settlement_evidence_available") is False:
            payable_issues.append((row_id, "missing settlement evidence"))
        elif _event_date(row) is None:
            payable_issues.append((row_id, "missing date"))
        elif _payable_source_is_stale(row, as_of=as_of):
            payable_issues.append((row_id, "stale source evidence"))
    payables_ready = not payable_issues
    income_ready = bool(income_projection.get("ready"))
    reasons: list[str] = []
    if not snapshot.get("available"):
        reasons.append("cash balance is missing")
        next_action = "upload_latest_balance"
    elif snapshot.get("stale"):
        reasons.append("cash balance is stale")
        next_action = "refresh_cash_balance"
    else:
        next_action = ""
    if payable_issues:
        reasons.append(
            f"{len(payable_issues)} active obligation(s) have missing or conflicting evidence"
        )
        if not next_action:
            next_action = "resolve_finance_data"
    if not income_ready:
        reasons.append(str(income_projection.get("reason") or "income evidence is not ready"))
        if not next_action:
            next_action = (
                "review_income_patterns"
                if income_projection.get("review_pattern_count")
                else "configure_income_forecast"
            )
    ready = cash_ready and payables_ready and income_ready
    return {
        "cash_ready": cash_ready,
        "payables_ready": payables_ready,
        "income_ready": income_ready,
        "ready": ready,
        "reasons": reasons,
        "next_action": next_action or "review_cash_plan",
        "payable_issues": [
            {"id": row_id, "reason": reason} for row_id, reason in payable_issues
        ],
    }


def _payable_source_is_stale(
    row: Mapping[str, Any], *, as_of: date, stale_source_days: int = 7
) -> bool:
    if row.get("source_stale"):
        return True
    source = str(row.get("source") or "").lower()
    updated = _as_date(row.get("source_updated_at") or row.get("updated_at"))
    return (
        source in {"clickup", "qbo", "quickbooks"}
        and updated is not None
        and (as_of - updated).days > stale_source_days
    )


def derive_csv_income_projections(
    rows: Sequence[Mapping[str, Any]],
    *,
    as_of: date,
    horizon_days: int = 28,
    summary_days: int = 14,
    income_decisions: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Infer conservative read-only income; never persist or confirm it."""
    horizon_end = as_of + timedelta(days=max(1, horizon_days) - 1)
    historical = [
        (row, row_date)
        for row in rows
        if (row_date := _actual_date(row)) is not None
        and row_date <= as_of
        and _is_transaction(row)
        and str(row.get("source") or "").lower() == "csv"
        and str(row.get("event_type") or "").lower() == "inflow"
        and str(row.get("status") or "posted").lower() in TRANSACTION_STATUSES
        and _amount(row.get("amount_cents"))
        and not _transfer_like(row)
        and not _is_duplicate(row)
        and not _needs_match_review(row)
    ]
    real_future = [
        row
        for row in rows
        if not _is_transaction(row)
        and str(row.get("event_type") or "").lower() == "inflow"
        and _is_active_obligation(row)
        and not row.get("source_open_disagreement")
        and (due := _event_date(row)) is not None
        and as_of <= due <= horizon_end
        and _amount(row.get("open_amount_cents") or row.get("amount_cents"))
    ]
    grouped: dict[str, dict[date, int]] = defaultdict(lambda: defaultdict(int))
    for row, row_date in historical:
        party = _party_key(row)
        if party != "unknown":
            grouped[party][row_date] += _amount(row.get("amount_cents"))

    decisions = _income_decision_map(income_decisions)
    projections: list[dict[str, Any]] = []
    patterns: list[dict[str, Any]] = []
    eligible_patterns = 0
    for party, daily in sorted(grouped.items()):
        occurrences = sorted(daily.items())
        if len(occurrences) < 3:
            continue
        gaps = [
            (right[0] - left[0]).days
            for left, right in zip(occurrences, occurrences[1:])
        ]
        cadence = int(statistics.median(gaps))
        if any(
            gap > max(2 * cadence, cadence + 7)
            for gap in gaps[-4:]
        ):
            continue
        median_amount = int(statistics.median(amount for _, amount in occurrences))
        if not 1 <= cadence <= 45 or not median_amount:
            continue
        median_absolute_deviation = int(statistics.median(
            abs(amount - median_amount) for _, amount in occurrences
        ))
        deviation_bps = median_absolute_deviation * 10_000 // median_amount
        projected_amount = int(statistics.quantiles(
            [amount for _, amount in occurrences],
            n=4,
            method="inclusive",
        )[0])
        if not projected_amount:
            continue
        last_date = occurrences[-1][0]
        if deviation_bps > 2_500 or (as_of - last_date).days > max(2 * cadence, 21):
            continue
        eligible_patterns += 1
        pattern_key = sha256(f"csv-income|{party}".encode("utf-8")).hexdigest()[:16]
        decision = decisions.get(pattern_key, decisions.get(party, "review"))
        evidence = {
            "type": "recurring_csv_income",
            "occurrences": len(occurrences),
            "occurrence_dates": [day.isoformat() for day, _ in occurrences],
            "median_amount_cents": median_amount,
            "projected_amount_cents": projected_amount,
            "median_cadence_days": cadence,
            "last_receipt_date": last_date.isoformat(),
            "median_absolute_deviation_bps": deviation_bps,
            "probability_bps": 5_000,
        }
        pattern = {
            "pattern_key": pattern_key,
            "party": party,
            "decision": decision,
            "operator_reviewed": decision == "track_expected",
            "evidence": evidence,
            "projected_dates": [],
        }
        next_date = last_date + timedelta(days=cadence)
        while next_date <= as_of:
            next_date += timedelta(days=cadence)
        while next_date <= horizon_end:
            duplicate = any(
                (
                    _party_key(real) == party
                    and abs((_event_date(real) - next_date).days) <= max(2, cadence // 3)
                )
                or (
                    abs(_amount(real.get("open_amount_cents") or real.get("amount_cents")) - projected_amount)
                    <= max(100, projected_amount // 10)
                    and abs((_event_date(real) - next_date).days) <= 2
                )
                for real in real_future
            )
            if not duplicate and decision in {"review", "track_expected"}:
                projections.append({
                    "id": f"trend-income:{party}:{next_date.isoformat()}",
                    "record_kind": "obligation",
                    "source": "csv_trend",
                    "source_label": "CSV income trend",
                    "event_type": "inflow",
                    "category": "revenue",
                    "name": party,
                    "vendor_or_customer": party,
                    "amount_cents": projected_amount,
                    "due_date": next_date,
                    "expected_date": next_date,
                    "status": "planned",
                    "confidence": "medium",
                    "probability_bps": 5_000,
                    "trend_inferred": True,
                    "pattern_key": pattern_key,
                    "income_decision": decision,
                    "operator_reviewed": decision == "track_expected",
                    "read_only": True,
                    "source_evidence": evidence,
                })
                pattern["projected_dates"].append(next_date.isoformat())
            next_date += timedelta(days=cadence)
        pattern["projection_count"] = len(pattern["projected_dates"])
        patterns.append(pattern)

    if real_future:
        status, ready, reason = "configured_real", True, "future income is configured from active receivables"
    elif any(pattern["decision"] == "review" for pattern in patterns):
        status, ready, reason = "inferred_review", False, "recurring income patterns require operator review"
    elif any(pattern["decision"] == "track_expected" for pattern in patterns):
        status, ready, reason = "configured_expected", True, "operator-reviewed recurring income is tracked as Expected"
    elif historical:
        status, ready, reason = "not_configured", False, "forecast income is not configured"
    else:
        status, ready, reason = "no_history", False, "no comparable posted CSV inflows are available"
    summary_end = as_of + timedelta(days=max(1, summary_days) - 1)
    csv_trend_expected_cents = sum(
        _amount(row.get("amount_cents")) * _probability_bps(row) // 10_000
        for row in projections
        if (_event_date(row) or date.max) <= summary_end
    )
    return {
        "status": status,
        "ready": ready,
        "reason": reason,
        "historical_inflow_count": len(historical),
        "real_future_inflow_count": len(real_future),
        "eligible_pattern_count": eligible_patterns,
        "operator_reviewed_pattern_count": sum(
            pattern["decision"] == "track_expected" for pattern in patterns
        ),
        "review_pattern_count": sum(pattern["decision"] == "review" for pattern in patterns),
        "inferred_projection_count": len(projections),
        "csv_trend_expected_cents": csv_trend_expected_cents,
        "projections": projections,
        "patterns": patterns,
    }


def assess_confidence(snapshot: Mapping[str, Any], trends: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], income_projection: Mapping[str, Any] | None = None) -> dict[str, Any]:
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
    income_missing = bool(
        income_projection
        and income_projection.get("historical_inflow_count")
        and income_projection.get("status") == "not_configured"
    )
    if income_missing:
        reasons.append("forecast income is not configured")
    level = (
        "low"
        if not snapshot.get("available") or snapshot.get("stale") or settlement_evidence_missing or income_missing
        else ("medium" if reasons else str(trends.get("confidence") or "medium"))
    )
    return {
        "level": level,
        "verification_only": level == "low",
        "reasons": reasons,
        "income_projection": dict(income_projection or {}),
    }


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
            if due is not None and due <= end:
                items = row.get("payment_installments") or row.get("installments") or []
                confirmed_installments = [
                    item
                    for item in items if isinstance(item, Mapping)
                    and str(item.get("status") or "").lower() not in TERMINAL_STATUSES
                    and (
                        row.get("installments_confirmed") is True
                        or item.get("confirmed") is True
                        or str(item.get("confidence") or "").lower() == "confirmed"
                        or str(item.get("status") or "").lower() in {"confirmed", "scheduled", "approved"}
                    )
                ] if isinstance(items, Sequence) else []
                if due < as_of and _flexibility(row) == "chunkable" and confirmed_installments:
                    scheduled_in_window = sum(
                        _amount(item.get("amount_cents"))
                        for item in confirmed_installments
                        if (installment_due := _as_date(item.get("due_date"))) is not None
                        and installment_due <= end
                    )
                    reserved = min(open_amount, scheduled_in_window)
                    required_out += reserved
                    exposure_out += open_amount - reserved
                else:
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
    if str(row.get("source_status") or "").lower() == "source_missing":
        return "source_missing"
    if row.get("completion_requires_bank_evidence"):
        return "completion_requires_bank_evidence"
    if row.get("source_open_disagreement"):
        return "source_open_disagreement"
    if _is_probable_duplicate(row):
        return "probable_duplicate"
    if row.get("source_conflict") or str(row.get("classification") or "").lower() == "conflict":
        return "source_conflict"
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
    visible_rows, _ = _partition_data_quality(rows, as_of=as_of)
    canonical = annotate_open_amounts(visible_rows, settlement_annotations)
    groups: dict[str, list[dict[str, Any]]] = {key: [] for key in GROUP_ORDER}
    for row in canonical:
        if (
            str(row.get("status") or "").lower() == "completed"
            and not row.get("completion_requires_bank_evidence")
        ):
            # Retain operational completion for audit, never as a future cash need.
            due = _event_date(row)
            open_amount = _amount(row.get("open_amount_cents"))
            item = {
                "id": row["id"],
                "group": "completed",
                "event_type": str(row.get("event_type") or "outflow"),
                "confidence": str(row.get("confidence") or "estimated"),
                "party": str(row.get("vendor_or_customer") or row.get("name") or row.get("description") or ""),
                "due_date": due.isoformat() if due else None,
                "days_until_due": (due - as_of).days if due else None,
                "overdue_days": 0,
                "open_amount_cents": open_amount,
                "decision_blocker": None,
                "pay_priority": _pay_priority(row),
                "flexibility": _flexibility(row),
                "category": str(row.get("category") or "uncategorized"),
                "source": str(row.get("source") or "manual"),
                "source_label": str(row.get("source_label") or row.get("source") or "Manual"),
                "source_evidence": dict(row.get("source_evidence") or {}),
                "trend_inferred": bool(row.get("trend_inferred")),
                "probability_bps": _probability_bps(row),
                "read_only": True,
                "floor_impact_cents": 0,
                "needs_action": False,
                "action_label": "Completed in ClickUp",
                "status": "completed",
                "completion_requires_bank_evidence": False,
                "quick_actions": [{"action_type": "preview_cash_impact", "eligible": True, "preview_required": True, "confirmation_required": True}],
            }
            groups.setdefault("completed", []).append(item)
            continue
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
            "source": str(row.get("source") or "manual"),
            "source_label": str(row.get("source_label") or row.get("source") or "Manual"),
            "source_evidence": dict(row.get("source_evidence") or {}),
            "trend_inferred": bool(row.get("trend_inferred")),
            "probability_bps": _probability_bps(row),
            "read_only": bool(row.get("read_only")),
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
            "completion_requires_bank_evidence": bool(row.get("completion_requires_bank_evidence")),
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
    completed_items = sorted(groups.get("completed", []), key=lambda item: (
        item["due_date"] or "9999-12-31", str(item["id"])
    ))
    all_items.extend(completed_items)
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
    income_projection = state.get("income_projection") or {}
    trust_gate = state.get("trust_gate") or {}
    if not snapshot.get("available"):
        add(1, "upload_latest_balance", None, ["Cash balance is missing."], minimum, minimum, "Recommendations remain unavailable until cash is verified.", recommendation_confidence="high")
    elif snapshot.get("stale"):
        add(1, "refresh_cash_balance", None, [f"Cash balance is {snapshot.get('age_days')} days old."], minimum, minimum, "Payment decisions use stale cash until refreshed.", recommendation_confidence="high")

    if not trust_gate.get("payables_ready", True):
        add(
            2,
            "resolve_finance_data",
            None,
            ["Active obligations have missing or conflicting evidence."],
            minimum,
            minimum,
            "Payment and defer actions remain suppressed until obligation evidence is resolved.",
            recommendation_confidence="high",
        )

    if not trust_gate.get("income_ready", income_projection.get("ready", False)):
        action = (
            "review_income_patterns"
            if income_projection.get("review_pattern_count")
            else "configure_income_forecast"
        )
        add(
            3,
            action,
            None,
            [str(income_projection.get("reason") or "Income evidence is not ready.")],
            minimum,
            minimum,
            "Expected cash remains incomplete until income evidence is resolved.",
            recommendation_confidence="high",
        )

    for item in queue.get("items") or []:
        blocker = item.get("decision_blocker")
        if blocker:
            add(4, f"resolve_{blocker}", item, [str(blocker).replace("_", " ").capitalize()], minimum, minimum, "Resolving evidence may change open cash exposure.")

    if not trust_gate.get("ready", not confidence.get("verification_only")):
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
    income_decisions: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    source_connections: Mapping[str, Any] | None = None,
    as_of: date | None = None,
    floor_cents: int | None = None,
    horizon_days: int = 28,
    summary_days: int = 14,
    balance_stale_after_days: int = 3,
) -> dict[str, Any]:
    """Build the complete deterministic Finance Control V2 read model."""
    from sales_support_agent.services.cashflow.settings import resolve_cash_floor_cents
    from sales_support_agent.services.cashflow.reconciliation import build_reconciliation_shadow

    effective_date = as_of or date.today()
    floor_cents = resolve_cash_floor_cents(floor_cents)
    visible_rows, data_quality = _partition_data_quality(rows, as_of=effective_date)
    balance_rows = [row for row in rows if not _is_probable_duplicate(row)]
    snapshot = resolve_cash_snapshot(balance_rows, as_of=effective_date, stale_after_days=balance_stale_after_days)
    visible_rows = _annotate_clickup_completion_evidence(visible_rows, snapshot)
    canonical = annotate_open_amounts(visible_rows, settlement_annotations)
    starting_cash = int(snapshot["balance_cents"] or 0)
    trends = calculate_csv_trends(visible_rows, as_of=effective_date)
    income_projection = derive_csv_income_projections(
        canonical,
        as_of=effective_date,
        horizon_days=horizon_days,
        summary_days=summary_days,
        income_decisions=income_decisions,
    )
    projected_rows = annotate_open_amounts([
        *canonical,
        *income_projection["projections"],
    ])
    forecast = build_forecast_paths(
        projected_rows,
        as_of=effective_date,
        starting_cash_cents=starting_cash,
        horizon_days=horizon_days,
    )
    confidence = assess_confidence(snapshot, trends, canonical, income_projection)
    metrics: dict[str, Any] = _summary_metrics(projected_rows, effective_date, summary_days)
    minimum_stress = int(forecast["minimum_stress_cash_cents"])
    cash_after_required = starting_cash - metrics["required_outgoing_cents"]
    metrics.update(
        {
            "cash_on_hand_cents": snapshot["balance_cents"],
            "cash_available": bool(snapshot["available"]),
            "floor_cents": int(floor_cents),
            "minimum_stress_cash_cents": minimum_stress if snapshot["available"] else None,
            "cash_after_required_outgoing_cents": cash_after_required if snapshot["available"] else None,
            "safe_to_commit_cents": max(0, cash_after_required - floor_cents) if snapshot["available"] else None,
            "funding_gap_cents": max(0, floor_cents - cash_after_required) if snapshot["available"] else None,
        }
    )
    queue = build_queue(
        projected_rows,
        as_of=effective_date,
        horizon_days=max(summary_days, horizon_days),
        funding_gap_cents=int(metrics.get("funding_gap_cents") or 0),
    )
    trust_gate = _build_trust_gate(
        snapshot, canonical, income_projection, as_of=effective_date
    )
    confidence = {
        **confidence,
        "verification_only": not trust_gate["ready"],
        "reasons": list(dict.fromkeys([*trust_gate["reasons"], *confidence["reasons"]])),
    }
    source_status = _build_source_status(
        rows,
        snapshot,
        as_of=effective_date,
        source_connections=source_connections,
    )
    state: dict[str, Any] = {
        "as_of_date": effective_date.isoformat(),
        "cash_snapshot": snapshot,
        "metrics": metrics,
        "forecast": forecast,
        "trends": trends,
        "income_projection": income_projection,
        "data_quality": data_quality,
        "source_status": source_status,
        "source_status_by_key": {item["key"]: item for item in source_status},
        "trust_gate": trust_gate,
        "confidence": confidence,
        "queue": queue,
        # Shadow-only until a record-level backfill delta has been reviewed.
        # It intentionally cannot alter cash metrics or queue membership.
        "reconciliation_shadow": build_reconciliation_shadow(rows, as_of=effective_date),
    }
    state["recommendations"] = build_recommendations(state)
    return state


def build_cash_metrics(
    rows: Sequence[Mapping[str, Any]],
    settlement_annotations: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    *,
    income_decisions: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    source_connections: Mapping[str, Any] | None = None,
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
        income_decisions=income_decisions,
        source_connections=source_connections,
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
    income_decisions: Sequence[Mapping[str, Any]] | Mapping[str, Any] | None = None,
    source_connections: Mapping[str, Any] | None = None,
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
        income_decisions=income_decisions,
        source_connections=source_connections,
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
        f"{_money(metrics['expected_incoming_cents'])} expected in, with "
        f"{_money(metrics['required_outgoing_cents'])} required out due in 14 days."
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
    "derive_csv_income_projections",
    "eligible_quick_actions",
    "quick_action_eligibility",
    "resolve_cash_snapshot",
]
