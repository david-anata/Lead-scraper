"""Bill audit: find where money is actually leaking, and say why.

The first version reported 421 findings against real data and almost none were
real. Two same-day Amazon orders is normal shopping, not a double charge, and
comparing an all-time minimum to an all-time maximum calls every varying charge
a price rise. A list nobody can trust is worse than no list, so every rule here
now has to clear a bar and state the evidence that got it there.

Three detectors:

* the same transaction imported from more than one bank source, reported once as
  a data problem rather than hundreds of fake double charges;
* a genuine double charge, which needs an unusual merchant or real money;
* a price rise measured recent-window against prior-window, not min against max.

Internal transfers are excluded from all of it. Read-only throughout.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine
from sales_support_agent.services.cashflow.transfers import is_internal_transfer

_PAID_STATUSES = ("posted", "matched", "completed")

# A merchant billed this often is ordinary trading, so two same-day charges from
# it prove nothing. Amazon, UPS, Costco and Walmart all sit far above this.
FREQUENT_MERCHANT_THRESHOLD = 12
# Two same-day charges only count when the money is worth a look.
DUPLICATE_PAIR_MIN_CENTS = 500_00
# Three or more identical same-day charges is unusual for anyone.
DUPLICATE_TRIPLE_COUNT = 3

PRICE_WINDOW_DAYS = 90
PRICE_MIN_CHARGES_PER_WINDOW = 3
PRICE_MIN_RISE_BPS = 2_000      # 20%
PRICE_MIN_RISE_CENTS = 25_00

SPIKE_MIN_RATIO_BPS = 13_000
SPIKE_MIN_BASELINE_CENTS = 100_00

# Bank wording that names no merchant, so a "price" for it is meaningless.
_GENERIC_DESCRIPTORS = (
    "withdrawal", "deposit", "draft withdrawal", "check #", "check#",
    "analysis fee", "to share", "from share", "round up", "overflow",
    "home banking", "transfer", "fee withdrawal", "ach withdrawal", "ach deposit",
)


def _fingerprint(kind: str, key: str) -> str:
    return hashlib.sha256(f"{kind}|{key}".encode()).hexdigest()[:32]


def _norm(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


def _is_generic(name: str) -> bool:
    lowered = _norm(name)
    if not lowered:
        return True
    return any(token in lowered for token in _GENERIC_DESCRIPTORS)


def _as_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def _load_outflows() -> list[dict[str, Any]]:
    """Posted spending, with internal transfers removed."""
    placeholders = ",".join(f"'{status}'" for status in _PAID_STATUSES)
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT name, vendor_or_customer, description, category, source,
                   amount_cents, COALESCE(effective_date, due_date) AS paid_on
            FROM cash_events
            WHERE event_type='outflow'
              AND record_kind='transaction'
              AND LOWER(COALESCE(status,'')) IN ({placeholders})
              AND COALESCE(amount_cents,0) > 0
        """)).fetchall()  # noqa: S608 - statuses are a fixed internal allowlist
    return [dict(row._mapping) for row in rows if not is_internal_transfer(dict(row._mapping))]


def _dismissed_fingerprints() -> set[str]:
    with get_engine().connect() as connection:
        rows = connection.execute(text("SELECT fingerprint FROM finance_audit_dismissals")).fetchall()
    return {str(row._mapping["fingerprint"]) for row in rows}


def _merchant_totals(outflows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in outflows:
        counts[_norm(row.get("name") or row.get("vendor_or_customer") or "")] += 1
    return counts


def _detect_source_overlap(outflows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One transaction imported from two banks feeds is a data problem, once.

    Reporting each pair separately buried the real findings under hundreds of
    rows, and dismissing them one at a time would never end.
    """
    groups: dict[tuple[str, int, str], set[str]] = defaultdict(set)
    for row in outflows:
        name = _norm(row.get("name") or row.get("vendor_or_customer") or "")
        paid = _as_date(row.get("paid_on"))
        if not name or not paid:
            continue
        groups[(name, int(row.get("amount_cents") or 0), paid.isoformat())].add(
            str(row.get("source") or "")
        )

    overlapping = {key: sources for key, sources in groups.items() if len(sources) > 1}
    if not overlapping:
        return []
    affected = sum(1 for _ in overlapping)
    sources = sorted({source for values in overlapping.values() for source in values})
    return [{
        "kind": "source_overlap",
        "severity": "high",
        "title": "Same transactions arriving from more than one bank source",
        "detail": (
            f"{affected} transaction(s) appear in {' and '.join(sources)}. "
            "Each one is being counted twice, which inflates spending, vendor totals "
            "and this audit. Worth deciding which source is the record."
        ),
        "fingerprint": _fingerprint("source_overlap", "|".join(sources)),
    }]


def _detect_duplicates(outflows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """A genuine double charge, not ordinary repeat business."""
    frequency = _merchant_totals(outflows)
    groups: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in outflows:
        name = _norm(row.get("name") or row.get("vendor_or_customer") or "")
        paid = _as_date(row.get("paid_on"))
        if not name or not paid:
            continue
        groups[(name, int(row.get("amount_cents") or 0), paid.isoformat())].append(row)

    findings = []
    for (name, amount, day), rows in groups.items():
        if len({str(row.get("source") or "") for row in rows}) > 1:
            continue  # same transaction from two feeds, reported once above
        count = len(rows)
        if count < 2:
            continue
        if frequency.get(name, 0) >= FREQUENT_MERCHANT_THRESHOLD and count < DUPLICATE_TRIPLE_COUNT:
            continue  # ordinary repeat business from a frequent merchant
        if count < DUPLICATE_TRIPLE_COUNT and amount < DUPLICATE_PAIR_MIN_CENTS:
            continue  # small pair, not worth the noise
        label = str(rows[0].get("name") or rows[0].get("vendor_or_customer") or name)
        why = (
            f"{count} identical charges in one day"
            if count >= DUPLICATE_TRIPLE_COUNT
            else f"not a frequent merchant, and {_dollars(amount)} is worth checking"
        )
        findings.append({
            "kind": "duplicate",
            "severity": "high",
            "title": "Possible double charge",
            "detail": f"{label} {_dollars(amount)} x{count} on {day}. Flagged because {why}.",
            "fingerprint": _fingerprint("duplicate", f"{name}|{amount}|{day}|{count}"),
        })
    return findings


def _detect_price_creep(outflows: list[dict[str, Any]], as_of: date) -> list[dict[str, Any]]:
    """A real rise: what this merchant charges now versus the period before."""
    recent_start = as_of - timedelta(days=PRICE_WINDOW_DAYS)
    prior_start = recent_start - timedelta(days=PRICE_WINDOW_DAYS)

    recent: dict[str, list[int]] = defaultdict(list)
    prior: dict[str, list[int]] = defaultdict(list)
    labels: dict[str, str] = {}
    for row in outflows:
        name = _norm(row.get("name") or row.get("vendor_or_customer") or "")
        if not name or _is_generic(name):
            continue
        paid = _as_date(row.get("paid_on"))
        amount = int(row.get("amount_cents") or 0)
        if not paid or amount <= 0:
            continue
        labels.setdefault(name, str(row.get("name") or row.get("vendor_or_customer") or name))
        if recent_start < paid <= as_of:
            recent[name].append(amount)
        elif prior_start < paid <= recent_start:
            prior[name].append(amount)

    findings = []
    for name, amounts in recent.items():
        before = prior.get(name, [])
        if len(amounts) < PRICE_MIN_CHARGES_PER_WINDOW or len(before) < PRICE_MIN_CHARGES_PER_WINDOW:
            continue
        now_typical = sorted(amounts)[len(amounts) // 2]
        was_typical = sorted(before)[len(before) // 2]
        if was_typical <= 0:
            continue
        rise = now_typical - was_typical
        if rise < PRICE_MIN_RISE_CENTS:
            continue
        if rise * 10_000 < was_typical * PRICE_MIN_RISE_BPS:
            continue
        percent = round(rise * 100 / was_typical)
        findings.append({
            "kind": "price_creep",
            "severity": "medium",
            "title": "Price crept up",
            "detail": (
                f"{labels[name]}: typically {_dollars(was_typical)} before, "
                f"{_dollars(now_typical)} now (+{percent}%). "
                f"{len(before)} charges then, {len(amounts)} since."
            ),
            "fingerprint": _fingerprint("price_creep", f"{name}|{was_typical}|{now_typical}"),
        })
    return findings


def _detect_category_spikes(outflows: list[dict[str, Any]], as_of: date) -> list[dict[str, Any]]:
    recent_start = as_of - timedelta(days=30)
    baseline_start = as_of - timedelta(days=120)
    recent: dict[str, int] = defaultdict(int)
    baseline: dict[str, int] = defaultdict(int)
    for row in outflows:
        category = str(row.get("category") or "other").lower()
        paid = _as_date(row.get("paid_on"))
        amount = int(row.get("amount_cents") or 0)
        if not paid or amount <= 0:
            continue
        if recent_start < paid <= as_of:
            recent[category] += amount
        elif baseline_start < paid <= recent_start:
            baseline[category] += amount

    findings = []
    for category, recent_cents in recent.items():
        baseline_30 = round(baseline.get(category, 0) / 3)
        if baseline_30 < SPIKE_MIN_BASELINE_CENTS:
            continue
        if recent_cents * 10_000 < baseline_30 * SPIKE_MIN_RATIO_BPS:
            continue
        percent = round((recent_cents / baseline_30 - 1) * 100)
        findings.append({
            "kind": "category_spike",
            "severity": "medium",
            "title": "Spending spike",
            "detail": (
                f"'{category}' is up {percent}% this month "
                f"({_dollars(recent_cents)} versus {_dollars(baseline_30)} typical)."
            ),
            "fingerprint": _fingerprint("category_spike", f"{category}|{as_of.isoformat()}"),
        })
    return findings


def run_bill_audit(*, as_of: Optional[date] = None) -> list[dict[str, Any]]:
    """Current, non-dismissed findings, most severe first."""
    as_of = as_of or date.today()
    outflows = _load_outflows()
    findings = (
        _detect_source_overlap(outflows)
        + _detect_duplicates(outflows)
        + _detect_price_creep(outflows, as_of)
        + _detect_category_spikes(outflows, as_of)
    )
    dismissed = _dismissed_fingerprints()
    findings = [item for item in findings if item["fingerprint"] not in dismissed]
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    findings.sort(key=lambda item: severity_rank.get(item["severity"], 1))
    return findings


def dismiss_finding(fingerprint: str) -> None:
    fingerprint = str(fingerprint or "").strip()
    if not fingerprint:
        return
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        exists = connection.execute(
            text("SELECT 1 FROM finance_audit_dismissals WHERE fingerprint=:fp"),
            {"fp": fingerprint},
        ).fetchone()
        if exists:
            return
        connection.execute(text("""
            INSERT INTO finance_audit_dismissals (id, scope_key, fingerprint, created_at)
            VALUES (:id, 'default', :fp, :now)
        """), {"id": str(uuid4()), "fp": fingerprint, "now": now})


def clear_dismissals() -> int:
    """Forget dismissals made against the old rules. Returns how many went."""
    with get_engine().begin() as connection:
        result = connection.execute(text("DELETE FROM finance_audit_dismissals"))
    return int(result.rowcount or 0)
