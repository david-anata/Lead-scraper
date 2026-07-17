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
    2. Amount within ±AMOUNT_TOLERANCE_PCT of each other, or an explicitly
       high-confidence partial payment against a larger obligation
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
AUTO_MATCH_MIN_BPS: int = 8_000
AUTO_MATCH_LEAD_BPS: int = 1_500


@dataclass
class MatchResult:
    """Result of attempting to match one CSV event to a planned event."""
    csv_event_id: str
    planned_event_id: str | None    # None → no match found
    score: float                    # 0.0–1.0 confidence
    reason: str                     # human-readable match explanation
    score_bps: int = 0
    match_status: str = "unmatched"
    candidate_ids: list[str] | None = None


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
    claimed_planned_ids: set[str] = set()

    for csv_ev in csv_events:
        candidates = planned_by_type.get(csv_ev.get("event_type", "outflow"), [])
        scored: list[tuple[int, str, str]] = []
        for planned in candidates:
            planned_id = str(planned["id"])
            if planned_id in claimed_planned_ids:
                continue
            score_bps, reason = _score_match_bps(csv_ev, planned)
            if score_bps:
                scored.append((score_bps, planned_id, reason))

        scored.sort(key=lambda item: (-item[0], item[1]))
        best_score_bps, best_id, best_reason = scored[0] if scored else (0, None, "no match")
        runner_up_bps = scored[1][0] if len(scored) > 1 else 0
        qualifying_ids = [item[1] for item in scored if item[0] >= AUTO_MATCH_MIN_BPS]

        if (
            best_id is not None
            and best_score_bps >= AUTO_MATCH_MIN_BPS
            and (not runner_up_bps or best_score_bps - runner_up_bps >= AUTO_MATCH_LEAD_BPS)
        ):
            claimed_planned_ids.add(best_id)
            results.append(MatchResult(
                csv_event_id=str(csv_ev["id"]),
                planned_event_id=best_id,
                score=best_score_bps / 10_000,
                reason=best_reason,
                score_bps=best_score_bps,
                match_status="matched",
                candidate_ids=qualifying_ids,
            ))
        else:
            ambiguous = len(qualifying_ids) > 1 and best_score_bps - runner_up_bps < AUTO_MATCH_LEAD_BPS
            results.append(MatchResult(
                csv_event_id=str(csv_ev["id"]),
                planned_event_id=None,
                score=best_score_bps / 10_000,
                reason=(
                    f"ambiguous match: top candidates separated by {best_score_bps - runner_up_bps} bps"
                    if ambiguous else "no match found"
                ),
                score_bps=best_score_bps,
                match_status="ambiguous" if ambiguous else "unmatched",
                candidate_ids=qualifying_ids,
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
    score_bps, reason = _score_match_bps(csv_ev, planned)
    return score_bps / 10_000, reason


def _score_match_bps(
    csv_ev: dict[str, Any],
    planned: dict[str, Any],
) -> tuple[int, str]:
    """Return deterministic integer basis-point match evidence."""
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

    # A smaller posted transaction can be a real chunk payment.  Do not treat
    # it as an automatic match until the vendor and date evidence later earn a
    # materially higher score than a normal whole-bill match.
    planned_notes = " ".join(str(planned.get(field) or "") for field in ("notes", "description", "flexibility")).lower()
    partial_allowed = any(token in planned_notes for token in ("chunk", "partial", "installment"))
    partial_payment = bool(
        pln_amt > 0
        and 0 < csv_amt < pln_amt
        and not amount_ok
        and partial_allowed
    )
    if partial_payment:
        amount_ok = True

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
    score_bps = 0
    reasons: list[str] = []

    # Vendor similarity
    vendor_sim = _vendor_similarity(
        csv_ev.get("vendor_or_customer", "") or csv_ev.get("name", ""),
        planned.get("vendor_or_customer", "") or planned.get("name", ""),
    )
    if vendor_sim >= 0.80:
        score_bps += 3_500
        reasons.append(f"vendor match ({vendor_sim:.0%})")
    elif vendor_sim >= VENDOR_SIMILARITY_THRESHOLD:
        score_bps += 3_000
        reasons.append(f"vendor match ({vendor_sim:.0%})")
    elif vendor_sim >= 0.3:
        score_bps += 1_500
        reasons.append(f"partial vendor match ({vendor_sim:.0%})")

    # Category agreement
    if csv_ev.get("category") == planned.get("category") and csv_ev.get("category") != "uncategorized":
        score_bps += 1_500
        reasons.append("category match")

    # Amount exactness bonus. A partial is intentionally eligible only with an
    # exact/strong vendor match plus close date and category evidence.
    if partial_payment:
        if vendor_sim >= 0.80 and csv_ev.get("category") == planned.get("category") and date_delta <= 2:
            score_bps += 3_000
            reasons.append("partial payment with strong vendor/date evidence")
        else:
            return 0, "partial payment lacks strong vendor/date evidence"
    elif amount_ratio <= 0.01:
        score_bps += 3_000
        reasons.append("exact amount")
    elif amount_ratio <= 0.05:
        score_bps += 2_200
        reasons.append("near-exact amount")
    else:
        score_bps += 1_200
        reasons.append("amount within tolerance")

    # Date proximity bonus
    if date_delta == 0:
        score_bps += 2_000
        reasons.append("same date")
    elif date_delta <= 2:
        score_bps += 1_500
        reasons.append(f"{date_delta}d date delta")
    else:
        score_bps += 500
        reasons.append(f"{date_delta}d date delta")

    # Cap at 1.0
    return min(score_bps, 10_000), " + ".join(reasons) if reasons else "weak match"


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
