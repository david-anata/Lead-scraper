"""Durable operator review workflow for deterministic savings opportunities.

This module never changes cash events or forecasts.  It stores only the
operator's disposition and links a confirmed follow-up task when configured.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Mapping
from uuid import uuid4

import requests
from sqlalchemy import text

DEFAULT_SCOPE = "default"
VALID_ACTIONS = frozenset({"keep", "dismiss", "follow_up", "confirm_realized"})
_KEY_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_key(value: str) -> str:
    if not isinstance(value, str) or not _KEY_RE.fullmatch(value):
        raise ValueError("Savings opportunity is invalid; refresh Finance and try again")
    return value


def _actor(value: str) -> str:
    value = str(value or "").strip()
    if not value or len(value) > 255:
        raise ValueError("A valid operator identity is required")
    return value


def _json(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("Savings evidence is missing; refresh Finance and try again")
    try:
        encoded = json.dumps(dict(value), allow_nan=False, separators=(",", ":"), sort_keys=True)
        if len(encoded.encode()) > 32_768:
            raise ValueError("Savings evidence is too large")
        return json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError("Savings evidence is invalid") from exc


def _review_id(scope: str, key: str) -> str:
    return sha256(f"finance-savings-review-v1|{scope}|{key}".encode()).hexdigest()


def load_savings_reviews(*, scope: str = DEFAULT_SCOPE, engine=None) -> dict[str, dict[str, Any]]:
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    with db_engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT opportunity_key, evidence_hash, state, suppress_until, clickup_task_id,
                   clickup_task_url, potential_monthly_cents, baseline_amount_cents,
                   display_name, normalized_merchant, cadence, reason, evidence_json, updated_at
            FROM finance_savings_reviews WHERE scope_key=:scope
        """), {"scope": scope}).fetchall()
    return {str(row.opportunity_key): dict(row._mapping) for row in rows}


def _state_for(action: str) -> str:
    return {
        "keep": "kept",
        "dismiss": "dismissed",
        "follow_up": "monitoring",
        "confirm_realized": "realized",
    }[action]


def record_savings_review(
    opportunity: Mapping[str, Any],
    action: str,
    actor: str,
    *,
    reason: str = "",
    scope: str = DEFAULT_SCOPE,
    request_id: str | None = None,
    clickup_task: Mapping[str, str] | None = None,
    engine=None,
) -> dict[str, Any]:
    """Record a reviewed savings candidate without mutating finance facts."""
    if action not in VALID_ACTIONS:
        raise ValueError("Unsupported savings action")
    actor = _actor(actor)
    key = _validate_key(str(opportunity.get("opportunity_key") or opportunity.get("key") or ""))
    evidence_hash = str(opportunity.get("evidence_hash") or "")
    if not _KEY_RE.fullmatch(evidence_hash):
        raise ValueError("Savings evidence is stale; refresh Finance and try again")
    evidence = _json(opportunity)
    if action == "confirm_realized" and not bool(opportunity.get("realization_ready")):
        raise ValueError("Posted bank evidence has not verified this saving yet")
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = engine or get_engine()
    ensure_finance_trust_schema(db_engine)
    review_id = _review_id(scope, key)
    now = datetime.now(timezone.utc)
    next_state = _state_for(action)
    request_identity = request_id or sha256(
        f"{scope}|{key}|{evidence_hash}|{action}|{actor}|{reason}".encode()
    ).hexdigest()
    event_id = sha256(f"finance-savings-event-v1|{review_id}|{request_identity}".encode()).hexdigest()
    suppress_until = now + timedelta(days=90) if action in {"keep", "dismiss"} else None
    task = dict(clickup_task or {})
    with db_engine.begin() as connection:
        existing = connection.execute(text("""
            SELECT state FROM finance_savings_reviews WHERE id=:id
        """), {"id": review_id}).fetchone()
        prior_state = str(existing.state) if existing else ""
        inserted = connection.execute(text("""
            INSERT INTO finance_savings_review_events (
                id, review_id, scope_key, event_type, prior_state, next_state,
                actor, idempotency_key, payload_json, created_at
            ) VALUES (:id, :review_id, :scope, :event_type, :prior_state, :next_state,
                      :actor, :idempotency_key, :payload_json, :created_at)
            ON CONFLICT(idempotency_key) DO NOTHING
        """), {
            "id": event_id, "review_id": review_id, "scope": scope,
            "event_type": f"savings_{action}", "prior_state": prior_state,
            "next_state": next_state, "actor": actor, "idempotency_key": request_identity,
            "payload_json": json.dumps({"reason": reason.strip(), "evidence_hash": evidence_hash, "opportunity": evidence, "clickup_task": task}, separators=(",", ":"), sort_keys=True),
            "created_at": now,
        })
        if inserted.rowcount:
            connection.execute(text("""
                INSERT INTO finance_savings_reviews (
                    id, scope_key, opportunity_key, evidence_hash, state, display_name,
                    normalized_merchant, cadence, potential_monthly_cents, baseline_amount_cents,
                    suppress_until, clickup_task_id, clickup_task_url, reason, evidence_json,
                    created_by, created_at, updated_at
                ) VALUES (
                    :id, :scope, :key, :evidence_hash, :state, :display_name,
                    :merchant, :cadence, :monthly, :baseline, :suppress_until,
                    :task_id, :task_url, :reason, :evidence_json, :actor, :now, :now
                ) ON CONFLICT(scope_key, opportunity_key) DO UPDATE SET
                    evidence_hash=excluded.evidence_hash, state=excluded.state,
                    display_name=excluded.display_name, normalized_merchant=excluded.normalized_merchant,
                    cadence=excluded.cadence, potential_monthly_cents=excluded.potential_monthly_cents,
                    baseline_amount_cents=excluded.baseline_amount_cents,
                    suppress_until=excluded.suppress_until,
                    clickup_task_id=CASE WHEN excluded.clickup_task_id <> '' THEN excluded.clickup_task_id ELSE finance_savings_reviews.clickup_task_id END,
                    clickup_task_url=CASE WHEN excluded.clickup_task_url <> '' THEN excluded.clickup_task_url ELSE finance_savings_reviews.clickup_task_url END,
                    reason=excluded.reason, evidence_json=excluded.evidence_json, updated_at=excluded.updated_at
            """), {
                "id": review_id, "scope": scope, "key": key, "evidence_hash": evidence_hash,
                "state": next_state, "display_name": str(evidence.get("display_name") or ""),
                "merchant": str(evidence.get("normalized_merchant") or ""), "cadence": str(evidence.get("cadence") or ""),
                "monthly": evidence.get("monthly_potential_cents"), "baseline": evidence.get("baseline_amount_cents"),
                "suppress_until": suppress_until, "task_id": str(task.get("id") or ""),
                "task_url": str(task.get("url") or ""), "reason": reason.strip(),
                "evidence_json": json.dumps(evidence, separators=(",", ":"), sort_keys=True), "actor": actor, "now": now,
            })
    return {"review_id": review_id, "state": next_state, "created": bool(inserted.rowcount), "clickup_task": task}


def create_clickup_savings_review_task(opportunity: Mapping[str, Any]) -> dict[str, str]:
    """Create a review task only in the dedicated Finance review list."""
    token = os.getenv("CLICKUP_API_TOKEN", "").strip() or os.getenv("CLICKUP_API_KEY", "").strip()
    list_id = os.getenv("CLICKUP_FINANCE_REVIEW_LIST_ID", "").strip()
    if not token or not list_id:
        raise ValueError("Set CLICKUP_FINANCE_REVIEW_LIST_ID before creating savings review tasks")
    display = str(opportunity.get("display_name") or "Savings opportunity")
    potential = int(opportunity.get("monthly_potential_cents") or 0)
    amount = f"${potential / 100:,.2f}/month" if potential else "potential under review"
    description = "\n".join((
        "Finance savings review. This task does not change cash or cancel a service.",
        f"Opportunity: {display}",
        f"Potential: {amount}",
        f"Why: {opportunity.get('reason') or 'Review posted bank evidence.'}",
        f"Evidence: {', '.join(map(str, opportunity.get('evidence_dates') or [])) or 'See Finance Control'}",
        f"Limitations: {opportunity.get('limitations') or 'Verify terms, usage, and replacement cost.'}",
        "Next: verify the source, negotiate/cancel/consolidate outside Finance, then upload posted bank evidence.",
    ))
    response = requests.post(
        f"https://api.clickup.com/api/v2/list/{list_id}/task",
        headers={"Authorization": token, "Content-Type": "application/json"},
        json={"name": f"Review savings: {display} ({amount})", "description": description},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return {"id": str(payload.get("id") or ""), "url": str(payload.get("url") or "")}


def _posted_reduction_ready(review: Mapping[str, Any], events: list[Mapping[str, Any]], as_of: date) -> bool:
    """Require a later posted CSV charge materially below the recorded baseline.

    This deliberately does not infer a cancellation from an absence of charges:
    the expected cadence can change and absence is not bank settlement evidence.
    """
    if str(review.get("state") or "") != "monitoring":
        return False
    baseline = review.get("baseline_amount_cents")
    merchant = str(review.get("normalized_merchant") or "")
    created_at = review.get("updated_at") or review.get("created_at")
    if not baseline or not merchant or not created_at:
        return False
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return False
    created_day = created_at.date() if hasattr(created_at, "date") else None
    if created_day is None or created_day >= as_of:
        return False
    from sales_support_agent.services.cashflow.savings import _merchant

    for raw in events:
        row = dict(raw)
        if str(row.get("source") or "").lower() != "csv" or str(row.get("status") or "").lower() not in {"posted", "matched"}:
            continue
        if str(row.get("event_type") or "").lower() != "outflow" or _merchant(row) != merchant:
            continue
        when = row.get("effective_date") or row.get("due_date") or row.get("updated_at")
        try:
            event_day = date.fromisoformat(str(when)[:10])
        except (TypeError, ValueError):
            continue
        if event_day <= created_day:
            continue
        if int(row.get("amount_cents") or 0) <= int(baseline) * 80 // 100:
            return True
    return False


def merge_savings_reviews(
    view: Mapping[str, Any],
    reviews: Mapping[str, Mapping[str, Any]],
    *,
    events: list[Mapping[str, Any]] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Annotate candidates with review state; kept/dismissed matching evidence stays hidden."""
    result = dict(view)
    visible: list[dict[str, Any]] = []
    for raw in list(view.get("opportunities") or []):
        item = dict(raw)
        review = reviews.get(str(item.get("opportunity_key") or ""))
        if review:
            same_evidence = str(review.get("evidence_hash") or "") == str(item.get("evidence_hash") or "")
            state = str(review.get("state") or "")
            if same_evidence and state in {"kept", "dismissed"}:
                continue
            item["review_state"] = state
            item["clickup_task_url"] = str(review.get("clickup_task_url") or "")
            item["realization_ready"] = _posted_reduction_ready(
                review, list(events or []), as_of or date.today()
            )
        visible.append(item)
    result["opportunities"] = visible
    result["total_count"] = len(visible)
    included = [item for item in visible if item.get("included_in_headline", True)]
    headline = dict(result.get("headline") or {})
    headline.update({
        "opportunity_count": len(visible),
        "headline_opportunity_count": len(included),
        "recurring_monthly_potential_cents": sum(
            int(item.get("monthly_potential_cents") or 0) for item in included
        ),
        "recurring_annual_gross_potential_cents": sum(
            int(item.get("annual_gross_potential_cents") or 0)
            for item in included
            if item.get("opportunity_type") != "avoidable_fee"
        ),
        "fee_90d_potential_cents": sum(
            int(item.get("observed_90d_potential_cents") or 0) for item in included
        ),
    })
    result["headline"] = headline
    return result
