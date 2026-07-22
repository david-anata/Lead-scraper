"""Native Anata commitment workflow for Finance.

The workflow is deliberately separate from settlement truth: changing a task
state never fabricates posted cash.  Every confirmed transition is audited.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine


COMMITMENT_TYPES = {
    "payable", "receivable", "payroll", "tax", "debt", "recurring",
    "reserve", "manual_exception", "general",
}
WORKFLOW_STATUSES = {
    "draft", "needs_review", "approved", "scheduled", "partially_paid",
    "received", "bank_verified", "cancelled", "written_off",
}
APPROVAL_STATUSES = {"not_required", "pending", "approved", "rejected"}
PROTECTED_TYPES = {"payroll", "tax", "debt"}

_TRANSITIONS = {
    "draft": {"needs_review", "cancelled"},
    "needs_review": {"approved", "draft", "cancelled", "written_off"},
    "approved": {"scheduled", "partially_paid", "received", "cancelled"},
    "scheduled": {"partially_paid", "received", "bank_verified", "cancelled"},
    "partially_paid": {"bank_verified", "cancelled"},
    "received": {"bank_verified"},
    "bank_verified": set(),
    "cancelled": set(),
    "written_off": set(),
}


def _clean(value: Any, *, max_length: int = 255) -> str:
    return str(value or "").strip()[:max_length]


def validate_commitment_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Validate fields supplied by forms, imports, or an LLM preview."""
    cleaned = dict(fields)
    commitment_type = _clean(cleaned.get("commitment_type") or "general", max_length=32)
    workflow_status = _clean(cleaned.get("workflow_status") or "draft", max_length=32)
    approval_status = _clean(cleaned.get("approval_status") or "not_required", max_length=32)
    if commitment_type not in COMMITMENT_TYPES:
        raise ValueError("Unsupported commitment type")
    if workflow_status not in WORKFLOW_STATUSES:
        raise ValueError("Unsupported workflow status")
    if approval_status not in APPROVAL_STATUSES:
        raise ValueError("Unsupported approval status")
    if int(cleaned.get("amount_cents") or 0) < 0:
        raise ValueError("Commitment amount cannot be negative")
    if commitment_type in PROTECTED_TYPES and approval_status == "not_required":
        approval_status = "pending"
    cleaned.update(
        commitment_type=commitment_type,
        workflow_status=workflow_status,
        approval_status=approval_status,
        owner=_clean(cleaned.get("owner")),
        created_by=_clean(cleaned.get("created_by") or "system"),
    )
    return cleaned


def preview_transition(commitment: Mapping[str, Any], target_status: str) -> dict[str, Any]:
    """Return a deterministic transition preview without writing anything."""
    current = _clean(commitment.get("workflow_status") or "draft", max_length=32)
    target = _clean(target_status, max_length=32)
    if target not in WORKFLOW_STATUSES:
        raise ValueError("Unsupported workflow status")
    if target not in _TRANSITIONS.get(current, set()):
        raise ValueError(f"Cannot move a commitment from {current} to {target}")
    commitment_type = _clean(commitment.get("commitment_type") or "general", max_length=32)
    approval = _clean(commitment.get("approval_status") or "not_required", max_length=32)
    if target in {"approved", "scheduled"} and commitment_type in PROTECTED_TYPES and approval != "approved":
        raise ValueError("Protected commitments require explicit approval")
    if target == "bank_verified":
        evidence = bool(commitment.get("settlement_evidence_available"))
        if not evidence:
            raise ValueError("Bank verification requires posted settlement evidence")
    return {
        "commitment_id": str(commitment.get("id") or ""),
        "from_status": current,
        "to_status": target,
        "changes_cash": False,
        "requires_confirmation": True,
        "warning": "This changes workflow only; it does not move money or create bank evidence.",
    }


def confirm_transition(
    commitment_id: str,
    target_status: str,
    *,
    actor: str,
    idempotency_key: str,
) -> dict[str, Any]:
    """Confirm one previewed transition and append an audit event."""
    if not _clean(idempotency_key, max_length=128):
        raise ValueError("An idempotency key is required")
    now = datetime.now(timezone.utc)
    with get_engine().begin() as connection:
        prior_audit = connection.execute(
            text("SELECT evidence_json FROM finance_action_audit WHERE idempotency_key=:key LIMIT 1"),
            {"key": idempotency_key},
        ).fetchone()
        if prior_audit:
            row = connection.execute(text("SELECT * FROM cash_events WHERE id=:id"), {"id": commitment_id}).fetchone()
            return dict(row._mapping) if row else {}
        row = connection.execute(text("SELECT * FROM cash_events WHERE id=:id"), {"id": commitment_id}).fetchone()
        if row is None or row._mapping.get("record_kind") != "obligation":
            raise ValueError("Commitment not found")
        commitment = dict(row._mapping)
        if target_status == "bank_verified":
            evidence = connection.execute(
                text("SELECT COUNT(*) FROM settlement_allocations WHERE obligation_event_id=:id AND reversed_allocation_id IS NULL"),
                {"id": commitment_id},
            ).scalar_one()
            commitment["settlement_evidence_available"] = bool(evidence)
        preview = preview_transition(commitment, target_status)
        archived_at = now if target_status in {"cancelled", "written_off", "bank_verified"} else None
        connection.execute(
            text("UPDATE cash_events SET workflow_status=:status, archived_at=:archived_at, updated_at=:now WHERE id=:id"),
            {"status": target_status, "archived_at": archived_at, "now": now, "id": commitment_id},
        )
        import json
        evidence_json = json.dumps({**preview, "idempotency_key": idempotency_key}, sort_keys=True)
        connection.execute(
            text("INSERT INTO finance_action_audit (id, scope_key, action_type, entity_type, entity_id, actor, idempotency_key, evidence_json, created_at) VALUES (:audit_id, 'default', 'commitment_transition', 'cash_event', :id, :actor, :key, :evidence, :now)"),
            {"audit_id": str(uuid4()), "id": commitment_id, "actor": _clean(actor) or "system", "key": idempotency_key, "evidence": evidence_json, "now": now},
        )
        updated = connection.execute(text("SELECT * FROM cash_events WHERE id=:id"), {"id": commitment_id}).fetchone()
    return dict(updated._mapping)


def list_active_commitments(*, limit: int = 500) -> list[dict[str, Any]]:
    """Return current native work; archived history is intentionally excluded."""
    safe_limit = max(1, min(int(limit), 5000))
    with get_engine().connect() as connection:
        rows = connection.execute(
            text("""
                SELECT * FROM cash_events
                WHERE record_kind='obligation'
                  AND archived_at IS NULL
                  AND workflow_status NOT IN ('cancelled', 'written_off', 'bank_verified')
                ORDER BY CASE WHEN due_date IS NULL THEN 1 ELSE 0 END, due_date, created_at
                LIMIT :limit
            """),
            {"limit": safe_limit},
        ).fetchall()
    return [dict(row._mapping) for row in rows]
