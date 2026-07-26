"""Predict the bills that are probably coming, from the bank history itself.

A scheduler only knows the bills somebody typed in. The bank knows all of them.
This module reads the recurring outflows already detected in posted bank history
and turns the ones the operator confirms into read-only forecast rows.

How it fits together
--------------------
1. ``trend_detector.detect_recurring_patterns`` does the detection: vendor
   normalisation, grouping, frequency from the median gap, a 0-1 confidence and
   the already-tracked check against recurring templates. This module wraps it
   for outflows only and adds what a bill needs: a safe projected amount, a plain
   English explanation, an operator decision and forecast rows.
2. Decisions are appended to the existing ``finance_action_audit`` table, exactly
   as income pattern decisions are, so predicting bills needs no new table.
3. Projections are synthetic. They are never written to ``cash_events``, which is
   what keeps stored history actuals-only.

The one place this deliberately diverges from the income equivalent is the
projected amount. See ``_projected_bill_amount``.
"""

from __future__ import annotations

import calendar
import json
import re
import statistics
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Mapping, Sequence

from sqlalchemy import text

from sales_support_agent.services.cashflow.categorizer import categorize
from sales_support_agent.services.cashflow.income_decisions import (
    DEFAULT_SCOPE,
    _decode_payload,
    _normalize_evidence,
    _request_identity,
    _serialize_created_at,
    _validate_actor,
    _validate_pattern_key,
    _validate_scope,
)
from sales_support_agent.services.cashflow.obligations import _next_occurrence
from sales_support_agent.services.cashflow.trend_detector import (
    _jaccard,
    _normalize_vendor,
    _to_date,
    detect_recurring_patterns,
)


BILL_PATTERN_ACTION = "bill_pattern_decision_recorded"
BILL_PATTERN_ENTITY = "bill_pattern"
VALID_BILL_DECISIONS = frozenset({"track", "not_a_bill", "snooze"})

DEFAULT_LOOKBACK_DAYS = 180
MIN_OCCURRENCES = 3
SNOOZE_DAYS = 7
# A predicted date and a real bill this close together are the same bill.
DOUBLE_COUNT_WINDOW_DAYS = 3
# Same threshold trend_detector uses for its already-tracked check, so a vendor
# that counts as covered there also counts as covered here.
VENDOR_MATCH_MIN_SIMILARITY = 0.45
# Frequencies obligations._next_occurrence accepts. Anything else must be mapped
# or dropped before it reaches a caller.
ALLOWED_FREQUENCIES = ("weekly", "biweekly", "monthly", "quarterly", "annual")
_ACTIVE_OBLIGATION_STATUSES = ("planned", "pending", "overdue", "open", "due")
_PATTERN_KEY_RE = re.compile(r"^[0-9a-f]{16}$")
# How many of these land in a month, used only to rank by cost.
_MONTHLY_MULTIPLIER = {
    "weekly": 52 / 12,
    "biweekly": 26 / 12,
    "monthly": 1.0,
    "quarterly": 1 / 3,
    "annual": 1 / 12,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_bill_patterns(
    *,
    as_of: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    scope: str = DEFAULT_SCOPE,
) -> dict[str, Any]:
    """Return the bills the bank history says are probably coming.

    ``patterns`` holds the ones that still need a decision or have been
    confirmed, biggest monthly cost first. ``tracked`` holds the ones a recurring
    template already covers, so the page can collapse them instead of asking
    about a bill that is already on the schedule.
    """
    as_of = as_of or datetime.utcnow().date()
    lookback_days = max(1, int(lookback_days))
    history = _load_bill_history(as_of=as_of, lookback_days=lookback_days)
    decisions = _decision_records(scope=scope)

    detected = [
        pattern
        for pattern in detect_recurring_patterns(
            min_occurrences=MIN_OCCURRENCES, lookback_days=lookback_days
        )
        if pattern.event_type == "outflow"
    ]

    patterns: list[dict[str, Any]] = []
    tracked: list[dict[str, Any]] = []
    dismissed = 0
    snoozed = 0
    for detected_pattern in detected:
        occurrences = history.get(detected_pattern.normalized_vendor)
        if not occurrences or len(occurrences) < MIN_OCCURRENCES:
            continue
        built = _build_pattern(
            detected_pattern,
            occurrences=occurrences,
            as_of=as_of,
            decisions=decisions,
        )
        if built is None:
            continue
        if built["already_tracked"]:
            tracked.append(built)
        elif built["decision"] == "not_a_bill":
            dismissed += 1
        elif built["decision"] == "snoozed":
            snoozed += 1
        else:
            patterns.append(built)

    patterns.sort(key=_cost_rank)
    tracked.sort(key=_cost_rank)
    return {
        "patterns": patterns,
        "tracked": tracked,
        "counts": {
            "patterns": len(patterns),
            "tracked": len(tracked),
            "confirmed": sum(1 for row in patterns if row["decision"] == "track"),
            "unreviewed": sum(1 for row in patterns if not row["decision"]),
            "dismissed": dismissed,
            "snoozed": snoozed,
            "monthly_cost_cents": sum(_monthly_cost_cents(row) for row in patterns),
        },
    }


def pattern_exists(
    pattern_key: str,
    *,
    as_of: date | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    scope: str = DEFAULT_SCOPE,
) -> bool:
    """Whether this key names a bill the bank history actually shows.

    The key's shape can be valid while naming nothing, which is what a stale
    bookmark or a re-posted form produces. Without this check the operator gets
    told a bill is now being tracked when nothing was tracked at all.
    """
    try:
        pattern_key = _validate_pattern_key(pattern_key)
    except ValueError:
        return False
    found = list_bill_patterns(as_of=as_of, lookback_days=lookback_days, scope=scope)
    return any(
        str(item.get("pattern_key")) == pattern_key
        for item in [*found.get("patterns", []), *found.get("tracked", [])]
    )


def record_bill_pattern_decision(
    pattern_key: str,
    decision: str,
    *,
    actor: str,
    evidence: Mapping[str, Any] | None = None,
    scope: str = DEFAULT_SCOPE,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Append the operator's answer about one predicted bill to the audit trail."""
    pattern_key = _validate_pattern_key(pattern_key)
    decision = _validate_bill_decision(decision)
    actor = _validate_actor(actor)
    scope = _validate_scope(scope)

    supplied = dict(evidence) if isinstance(evidence, Mapping) else evidence
    if decision == "snooze":
        # Stamp the day it was snoozed so the pattern can come back on its own.
        supplied = dict(supplied or {})
        supplied.setdefault("snoozed_on", datetime.utcnow().date().isoformat())
    normalized_evidence, canonical_evidence = _normalize_evidence(supplied)
    request_identity = _request_identity(
        request_id,
        scope=scope,
        pattern_key=pattern_key,
        decision=decision,
        actor=actor,
        canonical_evidence=canonical_evidence,
    )

    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.now(timezone.utc)
    audit_id = sha256(
        "\x1f".join(("bill-pattern-decision-v1", scope, pattern_key, request_identity)).encode(
            "utf-8"
        )
    ).hexdigest()
    payload = {
        "decision": decision,
        "evidence": normalized_evidence,
        "pattern_key": pattern_key,
        "request_id": request_identity,
    }
    with db_engine.begin() as connection:
        result = connection.execute(
            text("""
                INSERT INTO finance_action_audit (
                    id, scope_key, action_type, entity_type, entity_id,
                    actor, evidence_json, created_at
                ) VALUES (
                    :id, :scope, :action_type, :entity_type, :pattern_key,
                    :actor, :evidence_json, :created_at
                )
                ON CONFLICT(id) DO NOTHING
            """),
            {
                "id": audit_id,
                "scope": scope,
                "action_type": BILL_PATTERN_ACTION,
                "entity_type": BILL_PATTERN_ENTITY,
                "pattern_key": pattern_key,
                "actor": actor,
                "evidence_json": json.dumps(payload, separators=(",", ":"), sort_keys=True),
                "created_at": now,
            },
        )
        if result.rowcount == 0:
            stored_row = connection.execute(
                text("""
                    SELECT id, scope_key, entity_id, actor, evidence_json, created_at
                    FROM finance_action_audit WHERE id = :id
                """),
                {"id": audit_id},
            ).one()
            stored = _decision_from_row(stored_row)
            stored_payload = _decode_payload(stored_row._mapping["evidence_json"])
            if (
                stored is None
                or stored_payload.get("request_id") != request_identity
                or stored["pattern_key"] != pattern_key
                or stored["decision"] != decision
                or stored["actor"] != actor
                or stored["evidence"] != normalized_evidence
            ):
                raise ValueError("request_id is already associated with a different decision")
            return {**stored, "created": False}

    return {
        "pattern_key": pattern_key,
        "decision": decision,
        "actor": actor,
        "evidence": normalized_evidence,
        "scope": scope,
        "audit_id": audit_id,
        "created_at": now.isoformat(),
        "created": True,
    }


def load_bill_pattern_decisions(*, scope: str = DEFAULT_SCOPE) -> dict[str, str]:
    """Return the latest answer recorded for each predicted bill."""
    return {
        pattern_key: record["decision"]
        for pattern_key, record in _decision_records(scope=scope).items()
    }


def confirmed_bill_projections(
    *,
    as_of: date,
    horizon_days: int,
    scope: str = DEFAULT_SCOPE,
) -> list[dict[str, Any]]:
    """Return read-only forecast rows for predicted bills the operator confirmed.

    Only a pattern answered with "track" contributes cash. Silence is not
    consent: an unreviewed or dismissed pattern is worth nothing to the forecast.
    These rows are never persisted.
    """
    horizon_days = max(1, int(horizon_days))
    horizon_end = as_of + timedelta(days=horizon_days)
    listing = list_bill_patterns(as_of=as_of, scope=scope)
    confirmed = [row for row in listing["patterns"] if row["decision"] == "track"]
    if not confirmed:
        return []

    real_bills = _load_real_outflow_obligations(as_of=as_of, horizon_end=horizon_end)
    projections: list[dict[str, Any]] = []
    for pattern in confirmed:
        vendor_tokens = pattern["vendor"].lower().split()
        for due_date in _occurrences_in_window(pattern, as_of=as_of, horizon_end=horizon_end):
            if _already_on_the_schedule(due_date, vendor_tokens, real_bills):
                continue
            projections.append({
                "id": f"bill-trend-{pattern['pattern_key']}-{due_date.isoformat()}",
                "source": "bill_trend",
                "source_label": "Predicted bill",
                "record_kind": "obligation",
                "event_type": "outflow",
                "category": pattern["category"],
                "name": pattern["vendor"],
                "vendor_or_customer": pattern["vendor"],
                "amount_cents": pattern["amount_cents"],
                "open_amount_cents": pattern["amount_cents"],
                "due_date": due_date,
                "expected_date": due_date,
                "status": "planned",
                "confidence": "medium",
                "probability_bps": pattern["confidence_bps"],
                "read_only": True,
                "trend_inferred": True,
                "bill_trend": True,
                "pattern_key": pattern["pattern_key"],
            })
    projections.sort(key=lambda row: (row["due_date"], row["vendor_or_customer"]))
    return projections


# ---------------------------------------------------------------------------
# Pattern assembly
# ---------------------------------------------------------------------------

def _build_pattern(
    detected: Any,
    *,
    occurrences: list[tuple[date, int, str]],
    as_of: date,
    decisions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Turn one detected outflow into the dict the page and forecast consume."""
    dates = [row[0] for row in occurrences]
    amounts = [row[1] for row in occurrences]
    gaps = [(right - left).days for left, right in zip(dates, dates[1:])]
    if not gaps:
        return None
    median_gap = float(statistics.median(gaps))
    frequency = _bill_frequency(detected.frequency, median_gap)
    if frequency is None:
        return None

    amount_cents = _projected_bill_amount(amounts)
    if amount_cents <= 0:
        return None

    vendor = detected.normalized_vendor
    pattern_key = bill_pattern_key(vendor)
    next_due = _next_due_date(dates, frequency=frequency, as_of=as_of)
    confidence_bps = max(0, min(10_000, int(round(float(detected.confidence) * 10_000))))
    return {
        "pattern_key": pattern_key,
        "vendor": vendor,
        "amount_cents": amount_cents,
        "frequency": frequency,
        "next_due": next_due,
        "confidence_bps": confidence_bps,
        "confidence_label": _confidence_label(confidence_bps),
        "occurrences": len(occurrences),
        "evidence": [
            {"due_date": row[0], "amount_cents": row[1]}
            for row in sorted(occurrences, key=lambda row: row[0], reverse=True)[:6]
        ],
        "why": _explain(frequency=frequency, dates=dates, median_gap_days=median_gap),
        "category": _bill_category([row[2] for row in occurrences], vendor),
        "already_tracked": bool(detected.already_tracked),
        "decision": _effective_decision(decisions.get(pattern_key), as_of=as_of),
    }


def bill_pattern_key(vendor: str) -> str:
    """Stable 16 hex character key for a vendor, matching the audit key format."""
    return sha256(f"bill|{vendor}".encode("utf-8")).hexdigest()[:16]


def _projected_bill_amount(amounts: Sequence[int]) -> int:
    """Project a bill at the 75th percentile of what it has actually cost.

    The income equivalent projects the first quartile, which is the right kind of
    caution for money coming in. For money going out the same choice is backwards:
    it hides a shortfall until the payment has already cleared. Leaning high means
    the forecast never understates what is about to leave the account.
    """
    ordered = sorted(int(amount) for amount in amounts)
    if not ordered:
        return 0
    if len(ordered) < 2:
        return ordered[0]
    return int(statistics.quantiles(ordered, n=4, method="inclusive")[2])


def _bill_frequency(detected_frequency: str, median_gap_days: float) -> str | None:
    """Return a frequency the rest of Finance accepts, or None to drop it.

    trend_detector calls anything past a quarter "irregular", which would blow up
    in obligations._next_occurrence. A yearly bill is still a bill, so gaps near
    twelve months become "annual" and everything else genuinely irregular is
    dropped rather than guessed at.
    """
    if detected_frequency in ALLOWED_FREQUENCIES:
        return detected_frequency
    if 300 <= median_gap_days <= 430:
        return "annual"
    return None


def _next_due_date(dates: Sequence[date], *, frequency: str, as_of: date) -> date:
    """Step the calendar forward from the last payment to the first future one."""
    typical_day = _typical_day_of_month(dates)
    cursor = dates[-1]
    for _ in range(600):
        if cursor > as_of:
            return cursor
        cursor = _next_occurrence(cursor, frequency, typical_day)
    return cursor


def _occurrences_in_window(
    pattern: Mapping[str, Any], *, as_of: date, horizon_end: date
) -> list[date]:
    """Every future date this bill is expected inside the forecast window."""
    frequency = str(pattern["frequency"])
    cursor = pattern["next_due"]
    typical_day = cursor.day
    dates: list[date] = []
    for _ in range(600):
        if cursor > horizon_end:
            break
        if cursor > as_of:
            dates.append(cursor)
        cursor = _next_occurrence(cursor, frequency, typical_day)
    return dates


def _already_on_the_schedule(
    due_date: date,
    vendor_tokens: list[str],
    real_bills: Sequence[tuple[list[str], date]],
) -> bool:
    """True when a real bill for this vendor already sits on this date.

    Without this the real bill and the guess both count and the operator sees the
    same payment twice.
    """
    for real_tokens, real_due in real_bills:
        if abs((real_due - due_date).days) > DOUBLE_COUNT_WINDOW_DAYS:
            continue
        if _jaccard(vendor_tokens, real_tokens) >= VENDOR_MATCH_MIN_SIMILARITY:
            return True
    return False


def _cost_rank(pattern: Mapping[str, Any]) -> tuple[int, int, str]:
    return (
        -_monthly_cost_cents(pattern),
        -int(pattern["confidence_bps"]),
        str(pattern["vendor"]),
    )


def _monthly_cost_cents(pattern: Mapping[str, Any]) -> int:
    multiplier = _MONTHLY_MULTIPLIER.get(str(pattern["frequency"]), 1.0)
    return int(round(int(pattern["amount_cents"]) * multiplier))


# ---------------------------------------------------------------------------
# Plain English helpers
# ---------------------------------------------------------------------------

def _confidence_label(confidence_bps: int) -> str:
    if confidence_bps >= 7_500:
        return "Very likely"
    if confidence_bps >= 5_000:
        return "Likely"
    return "Possible"


def _explain(*, frequency: str, dates: Sequence[date], median_gap_days: float) -> str:
    """One sentence a non-technical reader can check against their own memory."""
    count = len(dates)
    opening = f"paid {count} times"
    if frequency in {"monthly", "quarterly", "annual"}:
        typical_day = _typical_day_of_month(dates)
        spread = max(abs(day.day - typical_day) for day in dates)
        if spread == 0:
            return f"{opening}, always on the {_ordinal(typical_day)}"
        if spread <= 4:
            return f"{opening}, always within {spread} days of the {_ordinal(typical_day)}"
    else:
        weekdays = [day.weekday() for day in dates]
        common = Counter(weekdays).most_common(1)[0][0]
        if weekdays.count(common) * 10 >= len(weekdays) * 7:
            return f"{opening}, nearly always on a {calendar.day_name[common]}"
    return f"{opening}, about every {int(round(median_gap_days))} days"


def _typical_day_of_month(dates: Sequence[date]) -> int:
    return int(statistics.median(sorted(day.day for day in dates)))


def _ordinal(day: int) -> str:
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _bill_category(categories: Sequence[str], vendor: str) -> str:
    """Prefer what the history was actually filed under, then the rule guess."""
    counted = Counter(
        str(category).strip().lower()
        for category in categories
        if str(category or "").strip().lower() not in {"", "uncategorized", "unknown", "other"}
    )
    if counted:
        return counted.most_common(1)[0][0]
    guessed = categorize(vendor)
    return guessed if guessed and guessed != "uncategorized" else "other"


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------

def _validate_bill_decision(decision: str) -> str:
    if not isinstance(decision, str) or decision not in VALID_BILL_DECISIONS:
        allowed = ", ".join(sorted(VALID_BILL_DECISIONS))
        raise ValueError(f"decision must be one of: {allowed}")
    return decision


def _decision_from_row(row: Any) -> dict[str, Any] | None:
    values = dict(row._mapping)
    pattern_key = str(values.get("entity_id") or "")
    payload = _decode_payload(values.get("evidence_json"))
    decision = payload.get("decision")
    if not _PATTERN_KEY_RE.fullmatch(pattern_key) or decision not in VALID_BILL_DECISIONS:
        return None
    evidence = payload.get("evidence")
    return {
        "pattern_key": pattern_key,
        "decision": decision,
        "actor": str(values.get("actor") or ""),
        "evidence": dict(evidence) if isinstance(evidence, Mapping) else {},
        "scope": str(values.get("scope_key") or DEFAULT_SCOPE),
        "audit_id": str(values.get("id") or ""),
        "created_at": _serialize_created_at(values.get("created_at")),
    }


def _decision_records(*, scope: str = DEFAULT_SCOPE) -> dict[str, dict[str, Any]]:
    """Latest audit-backed decision per pattern, with the evidence kept."""
    scope = _validate_scope(scope)
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = get_engine()
    ensure_finance_trust_schema(db_engine)
    with db_engine.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT id, scope_key, entity_id, actor, evidence_json, created_at
                FROM finance_action_audit
                WHERE scope_key = :scope
                  AND action_type = :action_type
                  AND entity_type = :entity_type
                ORDER BY created_at DESC, id DESC
            """),
            {
                "scope": scope,
                "action_type": BILL_PATTERN_ACTION,
                "entity_type": BILL_PATTERN_ENTITY,
            },
        ).fetchall()

    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        record = _decision_from_row(row)
        if record is not None and record["pattern_key"] not in latest:
            latest[record["pattern_key"]] = record
    return latest


def _effective_decision(record: Mapping[str, Any] | None, *, as_of: date) -> str:
    """Map a stored answer onto what the page should show today."""
    if not record:
        return ""
    decision = str(record.get("decision") or "")
    if decision in {"track", "not_a_bill"}:
        return decision
    if decision != "snooze":
        return ""
    evidence = record.get("evidence")
    snoozed_on = _to_date((evidence or {}).get("snoozed_on")) or _to_date(record.get("created_at"))
    if snoozed_on is not None and (as_of - snoozed_on).days >= SNOOZE_DAYS:
        # The snooze ran out, so ask about this bill again.
        return ""
    return "snoozed"


# ---------------------------------------------------------------------------
# History and real obligations
# ---------------------------------------------------------------------------

def _load_bill_history(
    *, as_of: date, lookback_days: int
) -> dict[str, list[tuple[date, int, str]]]:
    """Posted bank outflows grouped by the same vendor key the detector uses.

    The detector returns an average and a range but not the payments themselves,
    and a bill needs the payments: for the high quantile amount, the evidence
    list and the explanation.
    """
    from sales_support_agent.models.database import get_engine

    cutoff = (as_of - timedelta(days=lookback_days)).isoformat()
    with get_engine().connect() as connection:
        rows = connection.execute(
            text("""
                SELECT amount_cents, due_date, vendor_or_customer, name, description, category
                FROM cash_events
                WHERE source = 'csv'
                  AND status IN ('posted', 'matched')
                  AND event_type = 'outflow'
                  AND due_date >= :cutoff
                  AND amount_cents > 0
                ORDER BY due_date ASC
            """),
            {"cutoff": cutoff},
        ).fetchall()

    grouped: dict[str, list[tuple[date, int, str]]] = {}
    for row in rows:
        values = dict(row._mapping)
        row_date = _to_date(values.get("due_date"))
        # A payment after the forecast date is not yet evidence.
        if row_date is None or row_date > as_of:
            continue
        vendor = _normalize_vendor(
            str(
                values.get("vendor_or_customer")
                or values.get("name")
                or values.get("description")
                or ""
            )
        )
        if not vendor:
            continue
        grouped.setdefault(vendor, []).append((
            row_date,
            int(values.get("amount_cents") or 0),
            str(values.get("category") or ""),
        ))
    for occurrences in grouped.values():
        occurrences.sort(key=lambda entry: entry[0])
    return grouped


def _load_real_outflow_obligations(
    *, as_of: date, horizon_end: date
) -> list[tuple[list[str], date]]:
    """Live bills already on the schedule, as (vendor tokens, due date)."""
    from sales_support_agent.models.database import get_engine

    window_start = (as_of - timedelta(days=DOUBLE_COUNT_WINDOW_DAYS)).isoformat()
    window_end = (horizon_end + timedelta(days=DOUBLE_COUNT_WINDOW_DAYS + 1)).isoformat()
    statuses = ", ".join(f"'{status}'" for status in _ACTIVE_OBLIGATION_STATUSES)
    with get_engine().connect() as connection:
        rows = connection.execute(
            text(f"""
                SELECT vendor_or_customer, name, description, due_date
                FROM cash_events
                WHERE record_kind = 'obligation'
                  AND event_type = 'outflow'
                  AND status IN ({statuses})
                  AND archived_at IS NULL
                  AND due_date >= :window_start
                  AND due_date <= :window_end
            """),
            {"window_start": window_start, "window_end": window_end},
        ).fetchall()

    real_bills: list[tuple[list[str], date]] = []
    for row in rows:
        values = dict(row._mapping)
        due = _to_date(values.get("due_date"))
        if due is None:
            continue
        vendor = _normalize_vendor(
            str(
                values.get("vendor_or_customer")
                or values.get("name")
                or values.get("description")
                or ""
            )
        )
        if not vendor:
            continue
        real_bills.append((vendor.lower().split(), due))
    return real_bills


__all__ = [
    "BILL_PATTERN_ACTION",
    "BILL_PATTERN_ENTITY",
    "VALID_BILL_DECISIONS",
    "bill_pattern_key",
    "confirmed_bill_projections",
    "list_bill_patterns",
    "load_bill_pattern_decisions",
    "record_bill_pattern_decision",
]
