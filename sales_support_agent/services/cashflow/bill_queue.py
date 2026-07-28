"""Atomic queue actions, combine previews, and session undo for predicted bills."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any, Iterable, Mapping
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine
from sales_support_agent.services.cashflow.bill_patterns import (
    BILL_PATTERN_ACTION, BILL_PATTERN_ENTITY, _PATTERN_CACHE, _build_pattern,
    _decision_records, _load_bill_history, _monthly_cost_cents, list_bill_patterns,
)
from sales_support_agent.services.cashflow.vendor_aliases import combine_vendor_keys

BATCH_ACTION = "bill_queue_batch_recorded"
BATCH_ENTITY = "bill_queue_batch"
VALID_ACTIONS = frozenset({"track", "not_a_bill", "snooze", "combine"})


def _forecast_effect(pattern: Mapping[str, Any], days: int) -> int:
    due = pattern.get("next_due")
    if not isinstance(due, date):
        due = date.fromisoformat(str(due)[:10])
    return int(pattern.get("amount_cents") or 0) if due <= date.today() + timedelta(days=days) else 0


def _selected_patterns(keys: Iterable[str]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    cleaned = list(dict.fromkeys(str(k or "").strip() for k in keys if str(k or "").strip()))
    listing = list_bill_patterns()
    available = {
        str(row["pattern_key"]): row
        for row in [*listing.get("patterns", []), *listing.get("tracked", [])]
    }
    if not cleaned:
        raise ValueError("choose at least one bill")
    if any(key not in available for key in cleaned):
        raise ValueError("one or more selected bills are no longer in the list")
    return cleaned, available


def preview_combine(
    pattern_keys: Iterable[str], *, canonical_key: str = "", canonical_name: str = ""
) -> dict[str, Any]:
    keys, available = _selected_patterns(pattern_keys)
    if len(keys) < 2:
        raise ValueError("choose at least two bills to combine")
    rows = [available[key] for key in keys]
    merchant_keys = [str(row["merchant_key"]) for row in rows]
    keep = canonical_key or merchant_keys[0]
    if keep not in merchant_keys:
        raise ValueError("choose one selected vendor to keep")
    name = (canonical_name or next(
        str(row["vendor"]) for row in rows if str(row["merchant_key"]) == keep
    )).strip()
    history = _load_bill_history(as_of=datetime.utcnow().date(), lookback_days=180)
    combined = []
    for merchant in merchant_keys:
        combined.extend(history.get(merchant, []))
    after = _build_pattern(
        keep, occurrences=combined, as_of=datetime.utcnow().date(),
        decisions={}, already_tracked=False,
    )
    if after is None:
        raise ValueError("these histories do not form one reliable bill")
    return {
        "before": [{
            "pattern_key": row["pattern_key"], "merchant_key": row["merchant_key"],
            "vendor": row["vendor"], "amount_cents": row["amount_cents"],
            "frequency": row["frequency"], "next_due": str(row["next_due"]),
        } for row in rows],
        "after": {
            "vendor": name, "merchant_key": keep,
            "amount_cents": after["amount_cents"], "frequency": after["frequency"],
            "next_due": str(after["next_due"]), "occurrences": after["occurrences"],
        },
        "explanation": (
            "The new estimate is recalculated from the combined payment history. "
            "The two existing estimates are not added together."
        ),
    }


def preview_track(pattern_keys: Iterable[str], *, payment_date: str = "") -> dict[str, Any]:
    """Explain the forecast change before a detected bill is tracked."""
    keys, available = _selected_patterns(pattern_keys)
    override = date.fromisoformat(payment_date) if payment_date else None
    rows = []
    for key in keys:
        pattern = dict(available[key])
        if override:
            pattern["next_due"] = override
        rows.append({
            "pattern_key": key,
            "vendor": pattern["vendor"],
            "amount_cents": int(pattern["amount_cents"]),
            "monthly_cost_cents": _monthly_cost_cents(pattern),
            "next_due": str(pattern["next_due"]),
            "effect_14_cents": _forecast_effect(pattern, 14),
            "effect_30_cents": _forecast_effect(pattern, 30),
            "possible_duplicate": bool(pattern.get("already_tracked")),
        })
    if any(row["possible_duplicate"] for row in rows):
        raise ValueError("A matching schedule already exists. Review that schedule before tracking this bill.")
    return {"rows": rows, "total_monthly_cost_cents": sum(row["monthly_cost_cents"] for row in rows)}


def list_queue_activity(*, limit: int = 20) -> list[dict[str, Any]]:
    """Recent authoritative bill-review audit records for the operator."""
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, action_type, entity_id, actor, evidence_json, created_at
            FROM finance_action_audit
            WHERE entity_type IN (:pattern_entity, :batch_entity)
            ORDER BY created_at DESC LIMIT :limit
        """), {
            "pattern_entity": BILL_PATTERN_ENTITY, "batch_entity": BATCH_ENTITY,
            "limit": max(1, min(100, int(limit))),
        }).fetchall()
    activity = []
    for row in rows:
        values = dict(row._mapping)
        payload = json.loads(values.get("evidence_json") or "{}")
        activity.append({
            "id": str(values["id"]), "action_type": str(values["action_type"]),
            "pattern_key": str(values["entity_id"]), "actor": str(values.get("actor") or ""),
            "created_at": str(values.get("created_at") or ""), "payload": payload,
        })
    return activity


def _write_decision(
    connection: Any, *, pattern_key: str, decision: str, actor: str,
    evidence: Mapping[str, Any], request_id: str,
) -> None:
    payload = {
        "decision": decision, "evidence": dict(evidence),
        "pattern_key": pattern_key, "request_id": request_id,
    }
    audit_id = sha256(f"bill-queue-v1\x1f{pattern_key}\x1f{request_id}".encode()).hexdigest()
    connection.execute(text("""
        INSERT INTO finance_action_audit (
            id, scope_key, action_type, entity_type, entity_id,
            actor, evidence_json, created_at
        ) VALUES (:id, 'default', :action, :entity, :key, :actor, :payload, :now)
        ON CONFLICT(id) DO NOTHING
    """), {
        "id": audit_id, "action": BILL_PATTERN_ACTION, "entity": BILL_PATTERN_ENTITY,
        "key": pattern_key, "actor": actor, "payload": json.dumps(payload),
        "now": datetime.now(timezone.utc),
    })


def apply_queue_action(
    pattern_keys: Iterable[str], action: str, *, actor: str, category: str = "",
    paid_in_pieces: bool | None = None, payment_date: str = "",
    canonical_key: str = "", canonical_name: str = "", request_id: str = "",
) -> dict[str, Any]:
    action = str(action or "").strip()
    if action not in VALID_ACTIONS:
        raise ValueError("choose a valid action")
    keys, available = _selected_patterns(pattern_keys)
    if action == "combine" and len(keys) < 2:
        raise ValueError("choose at least two bills to combine")
    if action == "track" and payment_date:
        date.fromisoformat(payment_date)
    request_id = request_id or uuid4().hex
    batch_id = uuid4().hex
    prior_records = _decision_records()
    previous = {
        key: {
            "decision": str((prior_records.get(key) or {}).get("decision") or "reset"),
            "evidence": dict((prior_records.get(key) or {}).get("evidence") or {}),
        } for key in keys
    }
    metadata: dict[str, Any] = {}
    with get_engine().begin() as connection:
        if action == "combine":
            preview = preview_combine(keys, canonical_key=canonical_key, canonical_name=canonical_name)
            combine_vendor_keys(
                [str(available[key]["merchant_key"]) for key in keys],
                canonical_key=preview["after"]["merchant_key"],
                canonical_name_value=preview["after"]["vendor"],
                actor=actor, connection=connection,
            )
            metadata["preview"] = preview
        else:
            evidence: dict[str, Any] = {"batch_id": batch_id}
            if action == "track" and category:
                evidence["category"] = category.strip().lower()
            if action == "track" and paid_in_pieces is not None:
                evidence["paid_in_pieces"] = bool(paid_in_pieces)
            if action == "track" and payment_date:
                evidence["payment_date"] = payment_date
            if action == "not_a_bill" and category:
                evidence["reason"] = category.strip()[:500]
            if action == "snooze":
                evidence["snoozed_on"] = date.today().isoformat()
                evidence["return_on"] = (date.today() + timedelta(days=7)).isoformat()
            for key in keys:
                _write_decision(
                    connection, pattern_key=key, decision=action, actor=actor,
                    evidence=evidence, request_id=f"{request_id}:{key}",
                )
        payload = {
            "keys": keys, "action": action, "previous": previous,
            "vendors": [{
                "pattern_key": key,
                "vendor": str(available[key].get("vendor") or ""),
                "before": {
                    "decision": str(available[key].get("decision") or "unreviewed"),
                    "amount_cents": int(available[key].get("amount_cents") or 0),
                    "next_due": str(available[key].get("next_due") or ""),
                },
                "after": {"decision": action},
            } for key in keys],
            "metadata": metadata, "request_id": request_id,
        }
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (:id, 'default', :action, :entity, :id, :actor, :payload, :now)
        """), {
            "id": batch_id, "action": BATCH_ACTION, "entity": BATCH_ENTITY,
            "actor": actor, "payload": json.dumps(payload), "now": datetime.now(timezone.utc),
        })
    _PATTERN_CACHE.clear()
    refreshed = list_bill_patterns()
    return {
        "batch_id": batch_id, "applied": len(keys), "action": action,
        "remaining": int(refreshed["counts"]["unreviewed"]), **metadata,
    }


def undo_queue_batch(batch_id: str, *, actor: str) -> dict[str, Any]:
    with get_engine().begin() as connection:
        row = connection.execute(text("""
            SELECT evidence_json FROM finance_action_audit
            WHERE id=:id AND action_type=:action AND entity_type=:entity
        """), {"id": batch_id, "action": BATCH_ACTION, "entity": BATCH_ENTITY}).fetchone()
        if row is None:
            raise ValueError("that undo is no longer available")
        payload = json.loads(row._mapping["evidence_json"] or "{}")
        if payload.get("action") == "combine":
            raise ValueError("combined vendors can be separated from the vendor alias history")
        for key, prior in dict(payload.get("previous") or {}).items():
            _write_decision(
                connection, pattern_key=key, decision=str(prior.get("decision") or "reset"),
                actor=actor, evidence={
                    **dict(prior.get("evidence") or {}), "undoes_batch": batch_id,
                },
                request_id=f"undo:{batch_id}:{key}",
            )
    _PATTERN_CACHE.clear()
    refreshed = list_bill_patterns()
    return {"undone": len(payload.get("keys") or []), "remaining": int(refreshed["counts"]["unreviewed"])}


__all__ = [
    "apply_queue_action", "list_queue_activity", "preview_combine",
    "preview_track", "undo_queue_batch",
]
