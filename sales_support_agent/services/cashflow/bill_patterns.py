"""Predict the bills that are probably coming, from the bank history itself.

A scheduler only knows the bills somebody typed in. The bank knows all of them.
This module reads the recurring outflows already detected in posted bank history
and turns the ones the operator confirms into read-only forecast rows.

How it fits together
--------------------
1. Posted bank outflows are grouped by ``bill_merchant_key``, then each group is
   judged: is this a bill arriving on a cycle, and if so at what amount?
2. Decisions are appended to the existing ``finance_action_audit`` table, exactly
   as income pattern decisions are, so predicting bills needs no new table.
3. Projections are synthetic. They are never written to ``cash_events``, which is
   what keeps stored history actuals-only.

What real bank history forced, all found by loading the live page
----------------------------------------------------------------
The obvious version of this offered internal transfers as bills, split one rent
into two, and read a rent paid in instalments as a weekly bill projecting four
times its cost. So, in order:

* Transfers between the operator's own accounts are excluded. Tracking one
  inflates the cash the forecast says is needed.
* Grouping is on two cleaned words, not the raw descriptor, because the bank
  words the same payment differently between runs. The label is fuller than the
  grouping key, since two words reads badly. See ``_display_label``.
* A descriptor with no payee left in it ("Draft", "Check") is dropped.
* Uneven payments landing several times a month are one bill paid in pieces, and
  are added into a monthly figure. Steady ones are left on their own cycle.
  See ``_consolidate_part_payments``.
* A merchant charged almost daily is a pile of charges, not a cycle, and is
  dropped rather than multiplied up.
* An unpredictable amount caps confidence, because the amount is the forecast.

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
from uuid import uuid4
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
)


BILL_PATTERN_ACTION = "bill_pattern_decision_recorded"
BILL_PATTERN_ENTITY = "bill_pattern"
VALID_BILL_DECISIONS = frozenset({"track", "not_a_bill", "snooze", "reset"})

DEFAULT_LOOKBACK_DAYS = 180
MIN_OCCURRENCES = 3
SNOOZE_DAYS = 7
# A predicted date and a real bill this close together are the same bill.
DOUBLE_COUNT_WINDOW_DAYS = 3
# How alike a predicted vendor and a real bill's vendor must read before the two
# are treated as the same payment, so a guess and the invoice cannot both count.
VENDOR_MATCH_MIN_SIMILARITY = 0.45
# Frequencies obligations._next_occurrence accepts. Anything else must be mapped
# or dropped before it reaches a caller.
ALLOWED_FREQUENCIES = ("weekly", "biweekly", "monthly", "quarterly", "annual")
_ACTIVE_OBLIGATION_STATUSES = ("planned", "pending", "overdue", "open", "due")
_PATTERN_KEY_RE = re.compile(r"^[0-9a-f]{16}$")
# Below this a series is not a bill arriving on a cycle, it is a pile of charges
# from one merchant. Real bank history has vendors billing almost daily; calling
# those "every week" and multiplying them up overstates the month enormously.
MIN_BILL_GAP_DAYS = 5
# How much a bill is allowed to vary and still be treated as the same charge
# repeating. Measured as the inter-quartile spread over the median.
STEADY_AMOUNT_SPREAD = 0.25
# What is left after the bank boilerplate is stripped but still says nothing
# about who was paid. Offering these as bills to track is noise.
_NOT_A_MERCHANT = frozenset({
    "check", "draft", "home", "banking", "withdrawal", "deposit", "transfer",
    "payment", "pmts", "debits", "credit", "bill", "sale", "fee", "entry",
    "type", "company", "misc", "ach", "pos", "web", "recurring",
    "purch", "name", "svcs", "autopay", "paymt", "charge",
})
# Memoised answer, keyed by the data fingerprint so a new payment or a recorded
# answer invalidates it immediately rather than after a timeout.
_PATTERN_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
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

    The answer is memoised, because three separate things want it on a single
    page load: the nav badge, the page itself and the cash forecast. Recomputing
    it for each of them made every finance page noticeably slower to use. The
    cache key carries a stamp of the bank rows and the recorded answers, so a new
    payment or a click is reflected at once rather than after a delay.
    """
    as_of = as_of or datetime.utcnow().date()
    lookback_days = max(1, int(lookback_days))
    cache_key = (as_of, lookback_days, scope, _data_stamp())
    cached = _PATTERN_CACHE.get(cache_key)
    if cached is not None:
        return cached
    result = _list_bill_patterns_uncached(
        as_of=as_of, lookback_days=lookback_days, scope=scope
    )
    # One entry is all that is useful, and it keeps a long-lived process from
    # holding every day's answer in memory.
    _PATTERN_CACHE.clear()
    _PATTERN_CACHE[cache_key] = result
    return result


def _data_stamp() -> tuple[Any, ...]:
    """A cheap fingerprint of everything the answer depends on.

    Cheaper than the detection by a wide margin, and exact: if it has not moved,
    nothing that could change the answer has happened.
    """
    from sales_support_agent.models.database import get_engine

    try:
        with get_engine().connect() as connection:
            bank = connection.execute(text("""
                SELECT source, COUNT(*) AS row_count, MAX(updated_at) AS newest
                FROM cash_events
                WHERE source IN ('plaid', 'csv') AND event_type = 'outflow'
                  AND status IN ('posted', 'matched')
                GROUP BY source
                ORDER BY CASE source WHEN 'plaid' THEN 0 ELSE 1 END
                LIMIT 1
            """)).fetchone()
            answers = connection.execute(
                text("SELECT COUNT(*) FROM finance_action_audit WHERE action_type = :action"),
                {"action": BILL_PATTERN_ACTION},
            ).scalar()
            schedules = connection.execute(text(
                "SELECT COUNT(*) FROM recurring_templates WHERE event_type = 'outflow'"
            )).scalar()
            try:
                aliases = connection.execute(text("""
                    SELECT COUNT(*), MAX(created_at), MAX(revoked_at)
                    FROM finance_vendor_aliases
                """)).fetchone()
            except Exception:
                aliases = (0, None, None)
    except Exception:
        # Never let the fingerprint be the thing that breaks the page. A unique
        # value simply means this call is not served from cache.
        return (uuid4().hex,)
    return (
        str(bank[0]) if bank else "unavailable",
        int(bank[1]) if bank else 0,
        str(bank[2]) if bank else "",
        answers,
        schedules,
        *aliases,
    )


def _list_bill_patterns_uncached(
    *, as_of: date, lookback_days: int, scope: str
) -> dict[str, Any]:
    history = _load_bill_history(as_of=as_of, lookback_days=lookback_days)
    decisions = _decision_records(scope=scope)

    # Plaid may word a vendor differently from the legacy CSV archive. Preserve
    # an earlier operator decision only when the two recurring series share
    # strong payment evidence; a similar-looking name alone is not enough.
    legacy_patterns: list[dict[str, Any]] = []
    legacy_history = _load_bill_history(
        as_of=as_of, lookback_days=lookback_days, source="csv",
    )
    for merchant, occurrences in legacy_history.items():
        if len(occurrences) < MIN_OCCURRENCES:
            continue
        legacy = _build_pattern(
            merchant,
            occurrences=occurrences,
            as_of=as_of,
            decisions=decisions,
            already_tracked=False,
        )
        if legacy is not None and legacy.get("decision"):
            legacy_patterns.append(legacy)

    # The history is grouped by the same merchant reader the filing queue uses,
    # so a vendor whose descriptor varies stays one bill. Driving off the
    # detector's own grouping split Boulder Ranch into "Type: Pmts Boulder Ranch"
    # and "Company: Boulder Ranch Entry:" and counted the rent twice.
    tracked_keys = _keys_already_on_a_schedule()

    patterns: list[dict[str, Any]] = []
    tracked: list[dict[str, Any]] = []
    dismissed = 0
    snoozed = 0
    for merchant, occurrences in history.items():
        if len(occurrences) < MIN_OCCURRENCES:
            continue
        built = _build_pattern(
            merchant,
            occurrences=occurrences,
            as_of=as_of,
            decisions=decisions,
            already_tracked=merchant in tracked_keys,
        )
        if built is None:
            continue
        if not built.get("decision"):
            inherited = _matching_legacy_decision(built, legacy_patterns)
            if inherited:
                built["decision"] = inherited["decision"]
                built["decision_inherited_from"] = inherited["pattern_key"]
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

    The next occurrence of a tracked bill is always included, even when it falls
    past the horizon. A monthly bill can easily sit five weeks out, and dropping
    it meant the operator tracked something and then found no trace of it
    anywhere, which is the same silence that made tracking feel broken. It cannot
    distort the fortnight either way, because a date outside the window is only
    ever reported as due later.
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
    merchant: str,
    *,
    occurrences: list[tuple[date, int, str]],
    as_of: date,
    decisions: Mapping[str, Mapping[str, Any]],
    already_tracked: bool,
) -> dict[str, Any] | None:
    """Turn one merchant's payment history into the dict the page consumes.

    Returns None when the history is not a bill on a cycle. Two real shapes have
    to be handled before the amount means anything:

    Paid in pieces. A rent settled with several payments across the month looks
    like a weekly bill of one instalment. Read that way it projects roughly four
    times the rent. When the amounts are uneven and several land in most months,
    the month's payments are added into one monthly bill instead.

    Not a bill at all. A merchant charged almost daily is a pile of charges, not
    something arriving on a cycle, so it is dropped rather than multiplied up.
    """
    label = _display_label(merchant, occurrences)
    original = sorted(occurrences, key=lambda row: row[0])
    series, paid_in_pieces = _consolidate_part_payments(original, as_of=as_of)
    if len(series) < MIN_OCCURRENCES:
        return None

    dates = [row[0] for row in series]
    amounts = [row[1] for row in series]
    gaps = [(right - left).days for left, right in zip(dates, dates[1:])]
    if not gaps:
        return None
    median_gap = float(statistics.median(gaps))
    if median_gap < MIN_BILL_GAP_DAYS:
        return None

    frequency = _bill_frequency(_frequency_from_gap(median_gap), median_gap)
    if frequency is None:
        return None

    amount_cents = _projected_bill_amount(amounts)
    if amount_cents <= 0:
        return None

    pattern_key = bill_pattern_key(merchant)
    next_due = _next_due_date(dates, frequency=frequency, as_of=as_of)
    confidence_bps = _confidence(dates=dates, amounts=amounts, gaps=gaps)
    why = _explain(frequency=frequency, dates=dates, median_gap_days=median_gap)
    if paid_in_pieces:
        why += ", paid in pieces across each month and added up here"
    decision_record = decisions.get(pattern_key) or {}
    decision_evidence = decision_record.get("evidence") or {}
    override_date = _to_date(decision_evidence.get("payment_date"))
    return {
        "pattern_key": pattern_key,
        "vendor": label,
        "merchant_key": merchant,
        "amount_cents": amount_cents,
        "monthly_cost_cents": _monthly_cost_cents({
            "amount_cents": amount_cents, "frequency": frequency,
        }),
        "frequency": frequency,
        "next_due": override_date or next_due,
        "confidence_bps": confidence_bps,
        "confidence_label": _confidence_label(confidence_bps),
        "occurrences": len(series),
        "paid_in_pieces": bool(decision_evidence.get("paid_in_pieces", paid_in_pieces)),
        "evidence": [
            {
                "due_date": row[0],
                "amount_cents": row[1],
                "raw_descriptor": str(row[3] if len(row) > 3 else ""),
            }
            for row in sorted(series, key=lambda row: row[0], reverse=True)[:6]
        ],
        "why": why,
        "category": str(decision_evidence.get("category") or _bill_category(
            [row[2] for row in occurrences], label
        )),
        "already_tracked": already_tracked,
        "decision": _effective_decision(decisions.get(pattern_key), as_of=as_of),
    }


def _amount_spread(amounts: Sequence[int]) -> float:
    """Inter-quartile spread over the median. 0 means every payment is the same."""
    ordered = sorted(int(amount) for amount in amounts)
    median = statistics.median(ordered)
    if median <= 0:
        return 0.0
    if len(ordered) < 4:
        return (ordered[-1] - ordered[0]) / median
    quarters = statistics.quantiles(ordered, n=4, method="inclusive")
    return (quarters[2] - quarters[0]) / median


def _consolidate_part_payments(
    occurrences: Sequence[tuple[date, int, str]],
    *,
    as_of: date,
) -> tuple[list[tuple[date, int, str]], bool]:
    """Add up a month's payments when one bill is settled in several.

    Steady amounts are left alone however often they arrive: a genuine weekly
    repayment of the same figure is more useful forecast as four hits than as one
    monthly lump. It is the uneven ones landing several times a month that are
    pieces of a single bill.

    The month in progress is left out. Half of this month's rent is not a month's
    rent, and because it is always the smallest figure in the set it dragged the
    projection below what the bill actually costs now.
    """
    by_month: dict[tuple[int, int], list[tuple[date, int, str]]] = {}
    for row in occurrences:
        by_month.setdefault((row[0].year, row[0].month), []).append(row)
    per_month = sorted(len(rows) for rows in by_month.values())
    if len(by_month) < 2 or statistics.median(per_month) < 2:
        return list(occurrences), False
    if _amount_spread([row[1] for row in occurrences]) <= STEADY_AMOUNT_SPREAD:
        return list(occurrences), False

    complete = dict(by_month)
    current = (as_of.year, as_of.month)
    last_day = calendar.monthrange(as_of.year, as_of.month)[1]
    if as_of.day < last_day and current in complete and len(complete) - 1 >= MIN_OCCURRENCES:
        complete.pop(current)

    consolidated: list[tuple[date, int, str]] = []
    for _month, rows in sorted(complete.items()):
        total = sum(row[1] for row in rows)
        # The last payment of the month is when the bill is actually settled.
        consolidated.append((max(row[0] for row in rows), total, rows[0][2]))
    return consolidated, True


def _frequency_from_gap(median_gap_days: float) -> str:
    """Name the cycle from how far apart the payments actually are.

    The bands sit close around the real cadences and leave deliberate gaps. A
    series arriving every 130 days is not quarterly and not annual; saying it is
    quarterly would put a date on the forecast that nothing supports, so it is
    called irregular and dropped instead.
    """
    if median_gap_days <= 10:
        return "weekly"
    if median_gap_days <= 20:
        return "biweekly"
    if 21 <= median_gap_days <= 45:
        return "monthly"
    if 75 <= median_gap_days <= 115:
        return "quarterly"
    if 300 <= median_gap_days <= 430:
        return "annual"
    return "irregular"


def _confidence(
    *, dates: Sequence[date], amounts: Sequence[int], gaps: Sequence[int]
) -> int:
    """How much of this the operator should believe, in basis points.

    Amount steadiness carries real weight. A series swinging between $75 and
    $30,000 was being called "Likely" purely because it arrived often, which is
    the opposite of what the label should mean.
    """
    occurrence_score = min(1.0, len(dates) / 6.0)
    spread = _amount_spread(amounts)
    amount_score = max(0.0, 1.0 - min(1.0, spread))
    median_gap = float(statistics.median(gaps)) or 1.0
    drift = statistics.median([abs(gap - median_gap) for gap in gaps]) / median_gap
    timing_score = max(0.0, 1.0 - min(1.0, drift))
    blended = 0.35 * occurrence_score + 0.35 * timing_score + 0.30 * amount_score
    # An unpredictable amount caps the whole thing, because the amount IS the
    # forecast. Arriving reliably on the 1st tells the operator nothing useful if
    # the charge swings between 75 and 30,000, and it was earning "Likely" on
    # timing alone.
    blended = min(blended, 0.30 + 0.70 * amount_score)
    return max(0, min(10_000, int(round(blended * 10_000))))


def _display_label(merchant: str, occurrences: Sequence[tuple[date, int, str]]) -> str:
    """The name to put on the page.

    The raw descriptor reads like plumbing: "Withdrawal ACH B TYPE: WEB PMTS CO:
    Boulder Ranch L." names nothing the operator would recognise. Grouping is
    deliberately done on two words so a vendor survives the bank rewording the
    same payment, but two words makes a poor label ("Canyon View" for Canyon View
    Management), so the label is the shortest fully cleaned name in the group.
    Shortest wins because the extra words are usually the noise that differs.
    """
    cleaned = set()
    for row in occurrences:
        if len(row) <= 3:
            continue
        name = _cleaned_name(str(row[3] or ""))
        if name:
            cleaned.add(name)
    if cleaned:
        return min(sorted(cleaned), key=len).title()[:120]
    return merchant.title()[:120]


def _cleaned_name(descriptor: str) -> str:
    """The descriptor with the bank wording stripped but the words kept."""
    from sales_support_agent.services.cashflow.bookkeeping import (
        _BANK_NOISE_RE,
        _normalise,
    )

    cleaned = _BANK_NOISE_RE.sub(" ", _normalise(descriptor))
    cleaned = _EXTRA_BILL_NOISE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", cleaned)
    words = [word for word in cleaned.split() if len(word) > 1 and not word.isdigit()]
    without_codes = [word for word in words if not any(ch.isdigit() for ch in word)]
    return " ".join((without_codes or words)[:6])


def _keys_already_on_a_schedule() -> set[str]:
    """Merchant keys a recurring template already covers.

    Matching on the same merchant key the grouping uses is exact, where the
    detector's fuzzy vendor comparison both missed real matches and joined
    unrelated vendors.
    """
    from sales_support_agent.models.database import get_engine

    try:
        with get_engine().connect() as connection:
            rows = connection.execute(text(
                "SELECT name, vendor_or_customer FROM recurring_templates "
                "WHERE event_type = 'outflow'"
            )).fetchall()
    except Exception:
        return set()
    keys: set[str] = set()
    for row in rows:
        values = dict(row._mapping)
        for field in ("vendor_or_customer", "name"):
            key = bill_merchant_key(str(values.get(field) or ""))
            if key:
                keys.add(key)
    return keys


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
    """Every future date this bill is expected inside the forecast window.

    Plus the very next one whatever its date, so a tracked bill five weeks out is
    still accounted for somewhere rather than vanishing.
    """
    frequency = str(pattern["frequency"])
    cursor = pattern["next_due"]
    typical_day = cursor.day
    dates: list[date] = []
    for _ in range(600):
        if cursor > horizon_end and dates:
            break
        if cursor > as_of:
            dates.append(cursor)
            if cursor > horizon_end:
                break
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
            return (
                f"{opening}, always within {_days(spread)} of the {_ordinal(typical_day)}"
            )
    else:
        weekdays = [day.weekday() for day in dates]
        common = Counter(weekdays).most_common(1)[0][0]
        if weekdays.count(common) * 10 >= len(weekdays) * 7:
            return f"{opening}, nearly always on a {calendar.day_name[common]}"
    return f"{opening}, about every {_days(int(round(median_gap_days)))}"


def _days(count: int) -> str:
    """Reads as a sentence rather than as a field: "1 day", not "1 days"."""
    return "1 day" if count == 1 else f"{count} days"


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
    if decision == "reset":
        return ""
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
    *, as_of: date, lookback_days: int, source: str | None = None,
) -> dict[str, list[tuple[date, int, str]]]:
    """Posted bank outflows grouped by the same vendor key the detector uses.

    The detector returns an average and a range but not the payments themselves,
    and a bill needs the payments: for the high quantile amount, the evidence
    list and the explanation.
    """
    from sales_support_agent.models.database import get_engine

    cutoff = (as_of - timedelta(days=lookback_days)).isoformat()
    selected_source = source if source in {"plaid", "csv"} else None
    with get_engine().connect() as connection:
        rows = connection.execute(
            text("""
                SELECT amount_cents, due_date, vendor_or_customer, name, description, category
                FROM cash_events
                WHERE cash_events.source = COALESCE(
                    :selected_source,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM cash_events
                        WHERE source = 'plaid'
                          AND status IN ('posted', 'matched')
                          AND event_type = 'outflow'
                          AND due_date >= :cutoff
                          AND amount_cents > 0
                    ) THEN 'plaid' ELSE 'csv' END
                )
                  AND status IN ('posted', 'matched')
                  AND event_type = 'outflow'
                  AND due_date >= :cutoff
                  AND amount_cents > 0
                ORDER BY due_date ASC
            """),
            {"cutoff": cutoff, "selected_source": selected_source},
        ).fetchall()

    from sales_support_agent.services.cashflow.transfers import is_internal_transfer

    from sales_support_agent.services.cashflow.vendor_aliases import (
        alias_map,
        canonical_name,
    )

    aliases = alias_map()
    grouped: dict[str, list[tuple[date, int, str, str]]] = {}
    for row in rows:
        values = dict(row._mapping)
        row_date = _to_date(values.get("due_date"))
        # A payment after the forecast date is not yet evidence.
        if row_date is None or row_date > as_of:
            continue
        # Moving money between the operator's own accounts is not a bill. Without
        # this, share transfers and home-banking moves were offered as bills to
        # track, which would have inflated the cash the forecast says is needed.
        if is_internal_transfer(values):
            continue
        readable = str(
            values.get("vendor_or_customer") or values.get("name") or ""
        ).strip()
        # The same reader the filing queue uses, so a merchant whose descriptor
        # varies between payments stays one bill instead of several.
        merchant = bill_merchant_key(
            readable or str(values.get("description") or "")
        )
        alias = aliases.get(merchant)
        if alias:
            merchant = alias["canonical_key"]
            readable = canonical_name(merchant, readable)
        if not merchant or _is_not_a_merchant(merchant):
            continue
        grouped.setdefault(merchant, []).append((
            row_date,
            int(values.get("amount_cents") or 0),
            str(values.get("category") or ""),
            readable,
        ))
    for occurrences in grouped.values():
        occurrences.sort(key=lambda entry: entry[0])
    return grouped


def _matching_legacy_decision(
    current: Mapping[str, Any], legacy_patterns: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    """Return a prior CSV decision backed by the same real payments."""
    current_evidence = {
        (str(item.get("due_date") or "")[:10], int(item.get("amount_cents") or 0))
        for item in current.get("evidence") or [] if isinstance(item, Mapping)
    }
    for legacy in legacy_patterns:
        if str(legacy.get("frequency") or "") != str(current.get("frequency") or ""):
            continue
        legacy_evidence = {
            (str(item.get("due_date") or "")[:10], int(item.get("amount_cents") or 0))
            for item in legacy.get("evidence") or [] if isinstance(item, Mapping)
        }
        if len(current_evidence & legacy_evidence) >= 2:
            return legacy
    return None


# Bank wording the filing queue leaves behind. Grouping the queue can live with
# "boulder ranch web" and "b web pmts boulder ranch" being two piles, because
# each is still one decision. A bill cannot: the same rent split in two is
# counted twice and each half looks like a smaller, more frequent bill.
_EXTRA_BILL_NOISE = (
    r"\bach\b", r"\bweb\b", r"\bpmts?\b", r"\bccd\b", r"\bppd\b", r"\btel\b",
    r"\bclass\b", r"\bcode\b", r"\btrace\b", r"\bnumber\b", r"\bdebits?\b",
    r"\bof\b", r"\bthe\b", r"\band\b", r"\bpos\b", r"\bmisc\b", r"\bcred\b",
)
_EXTRA_BILL_NOISE_RE = re.compile("|".join(_EXTRA_BILL_NOISE), re.IGNORECASE)
# Two words is enough to tell real vendors apart and it survives the trailing
# junk that differs between wordings of the same payment ("Stripe Cap David
# Narayan" against "Stripe Cap ... CCD").
_BILL_KEY_WORDS = 2


def bill_merchant_key(description: str) -> str:
    """The merchant behind a bank descriptor, grouped tightly enough for a bill."""
    from sales_support_agent.services.cashflow.bookkeeping import (
        _BANK_NOISE_RE,
        _normalise,
    )

    cleaned = _BANK_NOISE_RE.sub(" ", _normalise(description))
    cleaned = _EXTRA_BILL_NOISE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", cleaned)
    words = [word for word in cleaned.split() if len(word) > 1 and not word.isdigit()]
    without_codes = [word for word in words if not any(ch.isdigit() for ch in word)]
    words = without_codes or words
    # Words that name no payee must not take one of the two slots, or the same
    # vendor splits on whether the bank happened to prefix "Payment" or "Purch".
    named = [word for word in words if word not in _NOT_A_MERCHANT]
    return " ".join((named or words)[:_BILL_KEY_WORDS])[:255]


def _is_not_a_merchant(merchant: str) -> bool:
    """True when nothing identifying survived the bank boilerplate.

    A key of "check" or "draft draft" names no payee, so asking whether to track
    it as a bill is a question with no useful answer.
    """
    words = [word for word in merchant.split() if word]
    return not words or all(word in _NOT_A_MERCHANT for word in words)


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
