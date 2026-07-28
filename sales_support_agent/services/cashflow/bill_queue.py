"""Atomic queue actions, combine previews, and session undo for predicted bills."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any, Iterable, Mapping
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import get_engine
from sales_support_agent.services.cashflow.bill_patterns import (
    BILL_PATTERN_ACTION, BILL_PATTERN_ENTITY, _PATTERN_CACHE, _build_pattern,
    _decision_records, _load_bill_history, list_bill_patterns,
)
from sales_support_agent.services.cashflow.vendor_aliases import combine_vendor_keys

BATCH_ACTION = "bill_queue_batch_recorded"
BATCH_ENTITY = "bill_queue_batch"
VALID_ACTIONS = frozenset({"track", "not_a_bill", "snooze", "combine"})


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
    if payment_date:
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
            if category:
                evidence["category"] = category.strip().lower()
            if paid_in_pieces is not None:
                evidence["paid_in_pieces"] = bool(paid_in_pieces)
            if payment_date:
                evidence["payment_date"] = payment_date
            for key in keys:
                _write_decision(
                    connection, pattern_key=key, decision=action, actor=actor,
                    evidence=evidence, request_id=f"{request_id}:{key}",
                )
        payload = {
            "keys": keys, "action": action, "previous": previous,
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


__all__ = ["apply_queue_action", "preview_combine", "undo_queue_batch"]
