"""Bill audit: read real outflows and surface where money is leaking.

Three conservative detectors run over posted outflows: likely duplicate
charges, prices creeping up on a recurring payee, and category spend spiking
against its own recent baseline. Findings the operator dismisses are
fingerprinted and stay quiet on later runs. Read-only; it never changes a bill.
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

_PAID_STATUSES = ("posted", "matched", "completed")
_CREEP_MIN_CHARGES = 3
_CREEP_MIN_RATIO_BPS = 11500      # latest >= 1.15x earliest
_SPIKE_MIN_RATIO_BPS = 13000      # recent 30d >= 1.3x baseline
_SPIKE_MIN_BASELINE_CENTS = 100_00


def _fingerprint(kind: str, key: str) -> str:
    return hashlib.sha256(f"{kind}|{key}".encode()).hexdigest()[:32]


def _norm(name: str) -> str:
    return re.sub(r"\s+", " ", str(name or "").strip().lower())


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


def _load_outflows() -> list[dict[str, Any]]:
    placeholders = ",".join(f"'{status}'" for status in _PAID_STATUSES)
    with get_engine().connect() as connection:
        rows = connection.execute(text(f"""
            SELECT name, vendor_or_customer, category, amount_cents,
                   COALESCE(effective_date, due_date) AS paid_on
            FROM cash_events
            WHERE event_type='outflow'
              AND LOWER(COALESCE(status,'')) IN ({placeholders})
              AND COALESCE(amount_cents,0) > 0
        """)).fetchall()  # noqa: S608 - statuses are a fixed internal allowlist
    return [dict(row._mapping) for row in rows]


def _dismissed_fingerprints() -> set[str]:
    with get_engine().connect() as connection:
        rows = connection.execute(text("SELECT fingerprint FROM finance_audit_dismissals")).fetchall()
    return {str(row._mapping["fingerprint"]) for row in rows}


def _detect_duplicates(outflows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, str], int] = defaultdict(int)
    labels: dict[tuple[str, int, str], str] = {}
    for row in outflows:
        name = _norm(row.get("name") or row.get("vendor_or_customer") or "")
        amount = int(row.get("amount_cents") or 0)
        paid = _as_date(row.get("paid_on"))
        if not name or not paid:
            continue
        key = (name, amount, paid.isoformat())
        groups[key] += 1
        labels[key] = str(row.get("name") or row.get("vendor_or_customer") or name)
    findings = []
    for (name, amount, day), count in groups.items():
        if count < 2:
            continue
        findings.append({
            "kind": "duplicate",
            "severity": "high",
            "title": "Possible double charge",
            "detail": f"{labels[(name, amount, day)]} {_dollars(amount)} appears {count} times on {day}.",
            "fingerprint": _fingerprint("duplicate", f"{name}|{amount}|{day}|{count}"),
        })
    return findings


def _detect_price_creep(outflows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_vendor: dict[str, list[tuple[date, int]]] = defaultdict(list)
    labels: dict[str, str] = {}
    for row in outflows:
        name = _norm(row.get("name") or row.get("vendor_or_customer") or "")
        paid = _as_date(row.get("paid_on"))
        amount = int(row.get("amount_cents") or 0)
        if not name or not paid or amount <= 0:
            continue
        by_vendor[name].append((paid, amount))
        labels.setdefault(name, str(row.get("name") or row.get("vendor_or_customer") or name))
    findings = []
    for name, charges in by_vendor.items():
        if len(charges) < _CREEP_MIN_CHARGES:
            continue
        charges.sort(key=lambda item: item[0])
        earliest = charges[0][1]
        latest = charges[-1][1]
        if earliest <= 0 or latest * 10000 < earliest * _CREEP_MIN_RATIO_BPS:
            continue
        findings.append({
            "kind": "price_creep",
            "severity": "medium",
            "title": "Price crept up",
            "detail": f"{labels[name]} rose from {_dollars(earliest)} to {_dollars(latest)} over {len(charges)} charges.",
            "fingerprint": _fingerprint("price_creep", f"{name}|{earliest}|{latest}"),
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
        # Prior 90 days averaged into a comparable 30-day window.
        baseline_30 = round(baseline.get(category, 0) / 3)
        if baseline_30 < _SPIKE_MIN_BASELINE_CENTS:
            continue
        if recent_cents * 10000 < baseline_30 * _SPIKE_MIN_RATIO_BPS:
            continue
        pct = round((recent_cents / baseline_30 - 1) * 100)
        findings.append({
            "kind": "category_spike",
            "severity": "medium",
            "title": "Spending spike",
            "detail": f"'{category}' is up {pct}% this month versus its recent average.",
            "fingerprint": _fingerprint("category_spike", f"{category}|{as_of.isoformat()}"),
        })
    return findings


def _dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def run_bill_audit(*, as_of: Optional[date] = None) -> list[dict[str, Any]]:
    """Return current, non-dismissed audit findings, most severe first."""
    as_of = as_of or date.today()
    outflows = _load_outflows()
    findings = (
        _detect_duplicates(outflows)
        + _detect_price_creep(outflows)
        + _detect_category_spikes(outflows, as_of)
    )
    dismissed = _dismissed_fingerprints()
    findings = [f for f in findings if f["fingerprint"] not in dismissed]
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    findings.sort(key=lambda f: severity_rank.get(f["severity"], 1))
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
