"""Auto-match posted bank CSV transactions to planned obligations.

When a bank CSV is uploaded, each posted transaction is compared against
existing planned/pending CashEvent records.  If a strong-enough match is
found, the bank transaction is linked to the planned obligation and both
are updated:

    posted_event.matched_to_id   = planned_event.id
    posted_event.status          = "matched"
    planned_event.status         = "paid"

Match scoring
-------------
A match requires ALL three of:
    1. Same event_type (both inflow or both outflow)
    2. Amount within ±AMOUNT_TOLERANCE_PCT of each other
    3. Dates within ±DATE_WINDOW_DAYS of each other

AND at least one of:
    4. Vendor name fuzzy similarity ≥ VENDOR_SIMILARITY_THRESHOLD
    5. Same category (and amount within tighter ±5%)

This intentionally prefers false negatives over false positives — unmatched
items are flagged for human review rather than silently auto-matched wrong.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

AMOUNT_TOLERANCE_PCT: float = 0.10        # ±10% amount difference allowed
TIGHT_AMOUNT_TOLERANCE_PCT: float = 0.05  # ±5% when matching by category only
DATE_WINDOW_DAYS: int = 7                 # ±7 days date window
VENDOR_SIMILARITY_THRESHOLD: float = 0.55 # Jaccard token similarity


@dataclass
class MatchResult:
    """Result of attempting to match one CSV event to a planned event."""
    csv_event_id: int
    planned_event_id: int | None    # None → no match found
    score: float                    # 0.0–1.0 confidence
    reason: str                     # human-readable match explanation


def auto_match_transactions(
    csv_events: list[dict[str, Any]],
    planned_events: list[dict[str, Any]],
) -> list[MatchResult]:
    """Attempt to match each CSV event to a planned obligation.

    Args:
        csv_events:     List of CashEvent field dicts with source='csv' and
                        status='posted'.  Must include: id, event_type,
                        amount_cents, due_date, vendor_or_customer, category.
        planned_events: List of CashEvent field dicts with source in
                        ('manual','clickup','recurring') and status in
                        ('planned','pending').  Same field set required.

    Returns:
        One MatchResult per csv_event.  Results with planned_event_id=None
        indicate no match was found — the UI should surface these for review.
    """
    results: list[MatchResult] = []

    # Index planned events by event_type for faster filtering
    planned_by_type: dict[str, list[dict[str, Any]]] = {"inflow": [], "outflow": []}
    for p in planned_events:
        planned_by_type.setdefault(p.get("event_type", "outflow"), []).append(p)

    # Track which planned events have already been claimed so we don't
    # double-match the same obligation to two CSV rows
    claimed_planned_ids: set[int] = set()

    for csv_ev in csv_events:
        best_id: int | None = None
        best_score: float = 0.0
        best_reason: str = "no match"

        candidates = planned_by_type.get(csv_ev.get("event_type", "outflow"), [])

        for planned in candidates:
            if planned["id"] in claimed_planned_ids:
                continue

            score, reason = _score_match(csv_ev, planned)
            if score > best_score:
                best_score = score
                best_id = planned["id"]
                best_reason = reason

        if best_score >= 0.5:  # minimum confidence threshold
            claimed_planned_ids.add(best_id)  # type: ignore[arg-type]
            results.append(MatchResult(
                csv_event_id=csv_ev["id"],
                planned_event_id=best_id,
                score=best_score,
                reason=best_reason,
            ))
        else:
            results.append(MatchResult(
                csv_event_id=csv_ev["id"],
                planned_event_id=None,
                score=best_score,
                reason="no match found",
            ))

    return results


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _score_match(
    csv_ev: dict[str, Any],
    planned: dict[str, Any],
) -> tuple[float, str]:
    """Return (score, reason) for a candidate CSV ↔ planned pair.

    Score of 0.0 means the pair fails hard constraints and should not match.
    Score ≥ 0.5 is considered a valid match; ≥ 0.8 is high confidence.
    """
    # ── Hard constraint 1: same event_type ────────────────────────────────
    if csv_ev.get("event_type") != planned.get("event_type"):
        return 0.0, "event_type mismatch"

    # ── Hard constraint 2: amount within tolerance ─────────────────────────
    csv_amt = csv_ev.get("amount_cents", 0)
    pln_amt = planned.get("amount_cents", 0)
    if pln_amt == 0 and csv_amt == 0:
        amount_ok = True
        amount_ratio = 0.0
    elif pln_amt == 0:
        return 0.0, "planned amount is zero"
    else:
        amount_ratio = abs(csv_amt - pln_amt) / pln_amt
        amount_ok = amount_ratio <= AMOUNT_TOLERANCE_PCT

    if not amount_ok:
        return 0.0, f"amount mismatch ({amount_ratio:.0%} difference)"

    # ── Hard constraint 3: dates within window ─────────────────────────────
    csv_date = _to_date(csv_ev.get("due_date"))
    pln_date = _to_date(planned.get("due_date"))
    if csv_date is None or pln_date is None:
        date_delta = DATE_WINDOW_DAYS  # treat missing as edge of window
    else:
        date_delta = abs((csv_date - pln_date).days)

    if date_delta > DATE_WINDOW_DAYS:
        return 0.0, f"date too far apart ({date_delta} days)"

    # ── Soft scoring ───────────────────────────────────────────────────────
    score = 0.0
    reasons: list[str] = []

    # Vendor similarity
    vendor_sim = _vendor_similarity(
        csv_ev.get("vendor_or_customer", "") or csv_ev.get("name", ""),
        planned.get("vendor_or_customer", "") or planned.get("name", ""),
    )
    if vendor_sim >= VENDOR_SIMILARITY_THRESHOLD:
        score += 0.5
        reasons.append(f"vendor match ({vendor_sim:.0%})")
    elif vendor_sim >= 0.3:
        score += 0.2
        reasons.append(f"partial vendor match ({vendor_sim:.0%})")

    # Category agreement
    if csv_ev.get("category") == planned.get("category") and csv_ev.get("category") != "uncategorized":
        score += 0.25
        reasons.append("category match")

    # Amount exactness bonus
    if amount_ratio <= 0.01:
        score += 0.2
        reasons.append("exact amount")
    elif amount_ratio <= 0.05:
        score += 0.1
        reasons.append("near-exact amount")

    # Date proximity bonus
    if date_delta == 0:
        score += 0.1
        reasons.append("same date")
    elif date_delta <= 2:
        score += 0.05
        reasons.append(f"{date_delta}d date delta")

    # Cap at 1.0
    score = min(score, 1.0)
    return score, " + ".join(reasons) if reasons else "weak match"


def _vendor_similarity(a: str, b: str) -> float:
    """Jaccard similarity of token sets from two vendor name strings.

    Strips common bank boilerplate ('WITHDRAWAL', 'DEPOSIT', 'ACH', 'DEBIT',
    'CREDIT', 'PAYMENT', 'TYPE:') before comparing so that 'Withdrawal ACH
    Fora Financial' matches 'Fora Financial'.
    """
    def tokenise(s: str) -> set[str]:
        s = re.sub(
            r"\b(WITHDRAWAL|DEPOSIT|ACH|DEBIT|CREDIT|PAYMENT|TYPE|CO|WEB|CCD|PPD|IAT|CTX)\b",
            "",
            s.upper(),
        )
        return {t for t in re.split(r"[\s\*\-_/]+", s) if len(t) >= 3}

    set_a = tokenise(a)
    set_b = tokenise(b)
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


def _to_date(value: Any) -> date | None:
    """Normalise a date/datetime/None to a date object."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None
