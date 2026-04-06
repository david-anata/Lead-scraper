"""Detect recurring cashflow patterns from historical posted bank transactions.

Mines the bank CSV history to surface:
  - Recurring AR (inflow) patterns not yet tracked in ClickUp or templates
  - Recurring AP (outflow) patterns that are missing from ClickUp
  - Patterns that exist in templates but show drift in amount or timing

Algorithm
---------
1. Load all `posted` bank CSV events within a lookback window.
2. Normalise vendor/description strings (strip dates, ref numbers, ACH codes).
3. Group by (normalised_vendor, event_type).
4. For groups with >= min_occurrences:
     - Compute amount mean + coefficient of variation (CV).
     - Sort by date, compute day-gaps, infer frequency from median gap.
     - Score confidence from occurrence count, amount CV, gap consistency.
5. Cross-reference existing recurring_templates — flag already-tracked items.
6. Return RecurringPattern list sorted by confidence desc.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any


# ---------------------------------------------------------------------------
# Data object
# ---------------------------------------------------------------------------

@dataclass
class RecurringPattern:
    """A detected recurring cashflow pattern inferred from bank history."""
    normalized_vendor: str           # cleaned vendor key
    raw_vendors: list[str]           # original descriptions seen in CSV
    event_type: str                  # "inflow" | "outflow"
    category: str
    occurrence_count: int            # # of bank rows in the lookback window
    avg_amount_cents: int            # arithmetic mean
    min_amount_cents: int
    max_amount_cents: int
    amount_cv: float                 # coefficient of variation (0 = stable)
    frequency: str                   # "weekly" | "biweekly" | "monthly" | "quarterly" | "irregular"
    median_gap_days: float
    last_seen: date
    next_expected: date
    confidence: float                # 0.0–1.0
    already_tracked: bool = False    # True if a matching recurring_template exists
    example_ids: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_recurring_patterns(
    *,
    min_occurrences: int = 2,
    lookback_days: int = 180,
    amount_cv_max: float = 0.30,
) -> list[RecurringPattern]:
    """Detect recurring patterns from posted bank CSV transactions.

    Args:
        min_occurrences:  Minimum appearances to be considered recurring (≥2).
        lookback_days:    How far back in history to mine (default 180 days).
        amount_cv_max:    Patterns with CV above this are still returned but
                          flagged with lower confidence (irregular amounts).

    Returns:
        RecurringPattern list sorted by confidence desc, with already_tracked
        patterns flagged but included so the UI can show full coverage.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    engine = get_engine()
    cutoff = (datetime.utcnow().date() - timedelta(days=lookback_days)).isoformat()

    with engine.connect() as conn:
        event_rows = conn.execute(text("""
            SELECT id, event_type, amount_cents, due_date,
                   vendor_or_customer, name, description, category
            FROM cash_events
            WHERE source = 'csv'
              AND status IN ('posted', 'matched')
              AND due_date >= :cutoff
              AND amount_cents > 0
            ORDER BY due_date ASC
        """), {"cutoff": cutoff}).fetchall()

        try:
            tmpl_rows = conn.execute(text("""
                SELECT vendor_or_customer, name, amount_cents, frequency, event_type
                FROM recurring_templates
                WHERE is_active = 1 OR is_active = true
            """)).fetchall()
            templates = [dict(r._mapping) for r in tmpl_rows]
        except Exception:
            templates = []

    events = [dict(r._mapping) for r in event_rows]

    # Group by (normalised_vendor, event_type)
    groups: dict[tuple[str, str], list[dict]] = {}
    for ev in events:
        raw = (
            ev.get("vendor_or_customer")
            or ev.get("name")
            or ev.get("description")
            or ""
        )
        norm = _normalize_vendor(raw)
        if not norm:
            continue
        key = (norm, str(ev.get("event_type", "outflow")))
        groups.setdefault(key, []).append(ev)

    patterns: list[RecurringPattern] = []

    for (norm_vendor, event_type), evs in groups.items():
        if len(evs) < min_occurrences:
            continue

        evs_sorted = sorted(evs, key=lambda e: str(e.get("due_date", "")))
        amounts = [int(e.get("amount_cents") or 0) for e in evs_sorted if e.get("amount_cents")]
        dates = [_to_date(e.get("due_date")) for e in evs_sorted]
        dates = [d for d in dates if d is not None]

        if len(dates) < min_occurrences or not amounts:
            continue

        avg_amt = int(statistics.mean(amounts))
        if avg_amt == 0:
            continue
        stdev_amt = statistics.pstdev(amounts) if len(amounts) > 1 else 0.0
        cv = stdev_amt / avg_amt

        gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
        if not gaps:
            continue
        median_gap = statistics.median(gaps)
        frequency, gap_tolerance = _infer_frequency(float(median_gap))

        consistent_gaps = sum(1 for g in gaps if abs(g - median_gap) <= gap_tolerance)
        gap_consistency = consistent_gaps / len(gaps)

        confidence = _calc_confidence(
            occurrence_count=len(evs_sorted),
            amount_cv=cv,
            gap_consistency=gap_consistency,
            frequency=frequency,
        )

        last_date = dates[-1]
        next_exp = last_date + timedelta(days=int(round(median_gap)))

        already_tracked = _is_already_tracked(
            norm_vendor, event_type, avg_amt, templates
        )

        category = evs_sorted[-1].get("category") or "other"
        raw_vendors = list(
            {(e.get("vendor_or_customer") or e.get("name") or "").strip()
             for e in evs_sorted
             if (e.get("vendor_or_customer") or e.get("name") or "").strip()}
        )

        patterns.append(RecurringPattern(
            normalized_vendor=norm_vendor,
            raw_vendors=raw_vendors,
            event_type=event_type,
            category=category,
            occurrence_count=len(evs_sorted),
            avg_amount_cents=avg_amt,
            min_amount_cents=min(amounts),
            max_amount_cents=max(amounts),
            amount_cv=cv,
            frequency=frequency,
            median_gap_days=float(median_gap),
            last_seen=last_date,
            next_expected=next_exp,
            confidence=round(confidence, 3),
            already_tracked=already_tracked,
            example_ids=[str(e.get("id", "")) for e in evs_sorted[-3:]],
        ))

    patterns.sort(key=lambda p: (-p.confidence, -p.occurrence_count))
    return patterns


def accept_pattern_as_template(pattern_dict: dict) -> dict:
    """Create a recurring_template row from an accepted pattern suggestion.

    Args:
        pattern_dict: Serialised RecurringPattern dict (from a POST form or JSON).

    Returns:
        The newly created template row dict.
    """
    from sales_support_agent.services.cashflow.obligations import create_recurring_template

    raw_date = pattern_dict.get("next_expected")
    if isinstance(raw_date, str):
        try:
            next_due = date.fromisoformat(raw_date[:10])
        except ValueError:
            next_due = datetime.utcnow().date()
    elif isinstance(raw_date, date):
        next_due = raw_date
    else:
        next_due = datetime.utcnow().date()

    return create_recurring_template(
        name=str(pattern_dict.get("normalized_vendor") or "Unknown")[:255],
        vendor_or_customer=str(pattern_dict.get("normalized_vendor") or "")[:255],
        event_type=str(pattern_dict.get("event_type") or "inflow")[:32],
        category=str(pattern_dict.get("category") or "other")[:100],
        amount_cents=int(pattern_dict.get("avg_amount_cents") or 0),
        frequency=str(pattern_dict.get("frequency") or "monthly")[:32],
        next_due_date=next_due,
    )


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

# Patterns to strip before keying vendor name
_STRIP_RE = re.compile(
    r"""
      \b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b   # MM/DD or MM/DD/YYYY
    | \b\d{4}-\d{2}-\d{2}\b               # ISO date
    | \b\d{6,}\b                           # long ref/trace numbers
    | \b(?:ACH|WEB|CCD|PPD|IAT|CTX|CO)\b  # ACH codes
    | \b(?:WITHDRAWAL|DEPOSIT|DEBIT|CREDIT|TRANSFER|ONLINE|MOBILE|RECURRING)\b
    | \b(?:TYPE|REF|CHK|CHECK|TRN|SEQ)\s*[\d\w]+
    | [*#|]+                               # noise separators
    """,
    re.VERBOSE | re.IGNORECASE,
)


def _normalize_vendor(raw: str) -> str:
    """Strip noise from a bank description and return a stable vendor key."""
    s = _STRIP_RE.sub(" ", raw)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split() if len(t) >= 3 and not t.isdigit()]
    if not tokens:
        return ""
    # Keep up to 4 meaningful tokens, title-cased
    return " ".join(tokens[:4]).title()


def _infer_frequency(median_gap: float) -> tuple[str, float]:
    """Map a median day-gap to a frequency label and matching tolerance."""
    if median_gap <= 10:
        return "weekly", 3.0
    elif median_gap <= 20:
        return "biweekly", 5.0
    elif median_gap <= 45:
        return "monthly", 10.0
    elif median_gap <= 100:
        return "quarterly", 20.0
    else:
        return "irregular", median_gap * 0.30


def _calc_confidence(
    *,
    occurrence_count: int,
    amount_cv: float,
    gap_consistency: float,
    frequency: str,
) -> float:
    """Score 0–1 representing how confident we are this is truly recurring."""
    score = 0.0
    # More occurrences → higher confidence (caps at 6)
    score += min(occurrence_count / 6.0, 1.0) * 0.35
    # Lower CV → more stable amount
    amount_stability = max(0.0, (0.30 - amount_cv) / 0.30)
    score += amount_stability * 0.30
    # Gap consistency (fraction of gaps within tolerance)
    score += gap_consistency * 0.25
    # Bonus for recognisable cadence
    if frequency in ("weekly", "monthly"):
        score += 0.10
    elif frequency == "biweekly":
        score += 0.05
    return min(score, 1.0)


def _is_already_tracked(
    norm_vendor: str,
    event_type: str,
    avg_amount_cents: int,
    templates: list[dict],
) -> bool:
    """Return True if a recurring_template already covers this pattern."""
    norm_v = norm_vendor.lower().split()
    for t in templates:
        if t.get("event_type") and t["event_type"] != event_type:
            continue
        t_vendor = _normalize_vendor(
            t.get("vendor_or_customer") or t.get("name") or ""
        )
        if not t_vendor:
            continue
        sim = _jaccard(norm_v, t_vendor.lower().split())
        if sim >= 0.45:
            t_amt = int(t.get("amount_cents") or 0)
            if t_amt == 0:
                return True
            ratio = abs(avg_amount_cents - t_amt) / t_amt if t_amt else 1.0
            if ratio <= 0.25:
                return True
    return False


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    return None
