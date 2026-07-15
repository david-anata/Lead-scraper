"""Non-destructive source reconciliation for Finance Control.

The Finance page must not reclassify historical provider records during a read.
This module therefore produces a conservative *shadow* view first.  It tells an
operator which recurring ClickUp occurrences look historical, while leaving the
current cash calculation untouched until the backfill is reviewed.
"""

from __future__ import annotations

import re
import hashlib
import json
import uuid
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Mapping, Sequence


_RECURRING_RULES = {"weekly", "biweekly", "monthly", "quarterly", "annual"}
_TERMINAL_STATUSES = {"completed", "paid", "matched", "cancelled", "canceled", "void"}


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _normalise_party(value: Any) -> str:
    text = str(value or "").lower()
    # Recurrence names often include a period marker. It is not identity.
    text = re.sub(r"\b(first|second|reserve|half|week|monthly|biweekly|weekly)\b", " ", text)
    text = re.sub(r"\b\d{1,2}(st|nd|rd|th)?\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split()) or "unknown"


def _series_key(row: Mapping[str, Any]) -> str | None:
    """Return a stable, deliberately narrow recurrence key for ClickUp rows."""
    rule = str(row.get("recurring_rule") or "").lower()
    if rule not in _RECURRING_RULES:
        return None
    party = _normalise_party(row.get("vendor_or_customer") or row.get("name"))
    return ":".join(
        (
            str(row.get("event_type") or "outflow").lower(),
            str(row.get("category") or "other").lower(),
            party,
            str(max(0, int(row.get("amount_cents") or 0))),
            rule,
        )
    )


def build_reconciliation_shadow(
    rows: Sequence[Mapping[str, Any]], *, as_of: date
) -> dict[str, Any]:
    """Report potentially stale ClickUp recurrence without changing Finance state.

    A prior recurring occurrence is a *candidate* only when a newer occurrence
    in the exact same series exists. It remains an exception instead of being
    released automatically because a missed payroll/rent payment can look the
    same as a completed occurrence in provider history.
    """
    series: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, source_row in enumerate(rows):
        row = dict(source_row)
        if str(row.get("source") or "").lower() != "clickup":
            continue
        if str(row.get("record_kind") or "obligation").lower() == "transaction":
            continue
        key = _series_key(row)
        if not key:
            continue
        due = _as_date(row.get("due_date"))
        if due is None:
            continue
        row["id"] = str(row.get("id") or index)
        row["_shadow_due_date"] = due
        row["_shadow_series_key"] = key
        series[key].append(row)

    candidates: list[dict[str, Any]] = []
    series_summaries: list[dict[str, Any]] = []
    for key, occurrences in series.items():
        ordered = sorted(occurrences, key=lambda item: (item["_shadow_due_date"], item["id"]))
        active = [
            item for item in ordered
            if str(item.get("status") or "planned").lower() not in _TERMINAL_STATUSES
        ]
        latest_active = active[-1] if active else None
        series_summaries.append(
            {
                "series_key": key,
                "occurrence_count": len(ordered),
                "active_occurrence_id": latest_active["id"] if latest_active else None,
                "latest_due_date": ordered[-1]["_shadow_due_date"].isoformat(),
            }
        )
        if latest_active is None:
            continue
        for occurrence in ordered:
            if occurrence["id"] == latest_active["id"]:
                continue
            status = str(occurrence.get("status") or "planned").lower()
            if status in _TERMINAL_STATUSES:
                continue
            if occurrence["_shadow_due_date"] >= latest_active["_shadow_due_date"]:
                continue
            candidates.append(
                {
                    "id": occurrence["id"],
                    "series_key": key,
                    "name": str(occurrence.get("name") or occurrence.get("vendor_or_customer") or "Unnamed occurrence"),
                    "due_date": occurrence["_shadow_due_date"].isoformat(),
                    "amount_cents": max(0, int(occurrence.get("amount_cents") or 0)),
                    "candidate_state": "candidate_superseded",
                    "reason": "A later open occurrence exists in the same ClickUp recurring series.",
                    "later_occurrence_id": latest_active["id"],
                    "later_due_date": latest_active["_shadow_due_date"].isoformat(),
                }
            )

    candidate_cents = sum(item["amount_cents"] for item in candidates)
    input_rows = [
        {
            "id": str(row.get("id") or ""),
            "source": str(row.get("source") or ""),
            "source_id": str(row.get("source_id") or ""),
            "event_type": str(row.get("event_type") or ""),
            "category": str(row.get("category") or ""),
            "name": str(row.get("name") or row.get("vendor_or_customer") or ""),
            "amount_cents": int(row.get("amount_cents") or 0),
            "due_date": str(row.get("due_date") or "")[:10],
            "status": str(row.get("status") or ""),
            "recurring_rule": str(row.get("recurring_rule") or ""),
            "source_updated_at": str(row.get("source_updated_at") or "")[:32],
        }
        for row in rows
        if str(row.get("source") or "").lower() == "clickup"
    ]
    input_hash = hashlib.sha256(
        json.dumps(sorted(input_rows, key=lambda item: item["id"]), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return {
        "mode": "shadow",
        "as_of_date": as_of.isoformat(),
        "recurring_series_count": len(series_summaries),
        "candidate_superseded_count": len(candidates),
        "candidate_superseded_cents": candidate_cents,
        "requires_operator_review": bool(candidates),
        "summary": (
            f"{len(candidates)} recurring occurrence(s) may be historical; "
            "cash calculations are unchanged."
            if candidates
            else "No recurring ClickUp occurrences need supersession review."
        ),
        "input_hash": input_hash,
        "series": sorted(series_summaries, key=lambda item: item["series_key"]),
        "candidates": sorted(candidates, key=lambda item: (item["due_date"], item["id"])),
    }


def persist_reconciliation_shadow(
    engine: Any,
    report: Mapping[str, Any],
    *,
    scope_key: str = "default",
) -> str:
    """Persist one idempotent shadow report without mutating cash events."""
    from sqlalchemy import text
    from sales_support_agent.models.database import ensure_finance_trust_schema

    ensure_finance_trust_schema(engine)
    as_of_date = str(report.get("as_of_date") or "")[:10]
    input_hash = str(report.get("input_hash") or "")
    if not as_of_date or not input_hash:
        raise ValueError("Shadow reconciliation report requires as_of_date and input_hash")
    summary = {
        key: report.get(key)
        for key in (
            "mode", "as_of_date", "recurring_series_count", "candidate_superseded_count",
            "candidate_superseded_cents", "requires_operator_review", "summary", "input_hash",
        )
    }
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT id FROM finance_reconciliation_reports
            WHERE scope_key=:scope_key AND as_of_date=:as_of_date AND input_hash=:input_hash
        """), {
            "scope_key": scope_key,
            "as_of_date": as_of_date,
            "input_hash": input_hash,
        }).scalar_one_or_none()
        if existing:
            return str(existing)
        report_id = str(uuid.uuid4())
        conn.execute(text("""
            INSERT INTO finance_reconciliation_reports
                (id, scope_key, as_of_date, input_hash, status, summary_json, report_json, created_at)
            VALUES
                (:id, :scope_key, :as_of_date, :input_hash, 'complete', :summary_json, :report_json, :created_at)
        """), {
            "id": report_id,
            "scope_key": scope_key,
            "as_of_date": as_of_date,
            "input_hash": input_hash,
            "summary_json": json.dumps(summary),
            "report_json": json.dumps(dict(report)),
            "created_at": datetime.utcnow().isoformat(),
        })
    return report_id


__all__ = ["build_reconciliation_shadow", "persist_reconciliation_shadow"]
