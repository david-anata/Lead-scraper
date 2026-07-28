"""Atomic queue actions, combine previews, and session undo for predicted bills."""

from __future__ import annotations

import json
import hmac
import re
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
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
_VENDOR_STOP_WORDS = frozenset({
    "ach", "autopay", "bill", "card", "company", "debit", "online",
    "payment", "payments", "pmt", "recurring", "services", "the", "web",
})


def _audit_payload(value: Any) -> dict[str, Any]:
    """Read JSON consistently from SQLite text and Postgres JSON columns."""
    if isinstance(value, Mapping):
        return dict(value)
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _forecast_effect(pattern: Mapping[str, Any], days: int) -> int:
    due = pattern.get("next_due")
    if not isinstance(due, date):
        due = date.fromisoformat(str(due)[:10])
    return int(pattern.get("amount_cents") or 0) if due <= date.today() + timedelta(days=days) else 0


def _vendor_tokens(value: str) -> set[str]:
    return {
        word for word in re.findall(r"[a-z0-9]+", str(value or "").lower())
        if len(word) > 1 and word not in _VENDOR_STOP_WORDS and not word.isdigit()
    }


def _possible_schedule_match(pattern: Mapping[str, Any]) -> dict[str, Any] | None:
    """Find a strong deterministic match without changing either record."""
    candidate_name = str(pattern.get("vendor") or "")
    candidate_tokens = _vendor_tokens(candidate_name)
    if not candidate_tokens:
        return None
    try:
        with get_engine().connect() as connection:
            schedules = connection.execute(text("""
                SELECT id, name, vendor_or_customer, amount_cents, next_due_date
                FROM recurring_templates
                WHERE event_type='outflow' AND is_active=true
            """)).fetchall()
    except Exception:
        return None
    matches: list[tuple[float, dict[str, Any]]] = []
    for schedule in schedules:
        values = dict(schedule._mapping)
        schedule_name = str(
            values.get("vendor_or_customer") or values.get("name") or ""
        )
        schedule_tokens = _vendor_tokens(schedule_name)
        if not schedule_tokens:
            continue
        overlap = len(candidate_tokens & schedule_tokens) / len(
            candidate_tokens | schedule_tokens
        )
        wording = SequenceMatcher(
            None, candidate_name.lower(), schedule_name.lower()
        ).ratio()
        score = max(overlap, wording)
        if score < 0.60:
            continue
        matches.append((score, {
            "schedule_id": str(values.get("id") or ""),
            "vendor": schedule_name,
            "amount_cents": int(values.get("amount_cents") or 0),
            "next_due": str(values.get("next_due_date") or ""),
            "match_score": round(score, 3),
        }))
    return max(matches, key=lambda match: match[0])[1] if matches else None


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
    result = {
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
    result["preview_token"] = sha256(json.dumps({
        "keys": keys,
        "canonical_key": keep,
        "canonical_name": name,
        "after": result["after"],
    }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return result


def preview_track(pattern_keys: Iterable[str], *, payment_date: str = "") -> dict[str, Any]:
    """Explain the forecast change before a detected bill is tracked."""
    keys, available = _selected_patterns(pattern_keys)
    override = date.fromisoformat(payment_date) if payment_date else None
    rows = []
    for key in keys:
        pattern = dict(available[key])
        if override:
            pattern["next_due"] = override
        duplicate = _possible_schedule_match(pattern)
        rows.append({
            "pattern_key": key,
            "vendor": pattern["vendor"],
            "amount_cents": int(pattern["amount_cents"]),
            "monthly_cost_cents": _monthly_cost_cents(pattern),
            "next_due": str(pattern["next_due"]),
            "effect_14_cents": _forecast_effect(pattern, 14),
            "effect_30_cents": _forecast_effect(pattern, 30),
            "possible_duplicate": duplicate,
        })
    return {
        "rows": rows,
        "blocked": any(bool(row["possible_duplicate"]) for row in rows),
        "total_monthly_cost_cents": sum(row["monthly_cost_cents"] for row in rows),
    }


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
        payload = _audit_payload(values.get("evidence_json"))
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
    canonical_key: str = "", canonical_name: str = "", preview_token: str = "",
    request_id: str = "",
) -> dict[str, Any]:
    action = str(action or "").strip()
    if action not in VALID_ACTIONS:
        raise ValueError("choose a valid action")
    requested_keys = list(dict.fromkeys(
        str(key or "").strip() for key in pattern_keys if str(key or "").strip()
    ))
    request_id = request_id or uuid4().hex
    batch_id = sha256(f"bill-queue-batch:{request_id}".encode()).hexdigest()[:32]
    with get_engine().connect() as connection:
        existing_row = connection.execute(text("""
            SELECT evidence_json FROM finance_action_audit
            WHERE id=:id AND action_type=:action_type
        """), {"id": batch_id, "action_type": BATCH_ACTION}).first()
    if existing_row:
        existing = _audit_payload(existing_row._mapping["evidence_json"])
        existing_keys = [
            str(item.get("pattern_key") or "")
            for item in existing.get("vendors") or []
        ]
        if existing.get("action") != action or set(existing_keys) != set(requested_keys):
            raise ValueError("This save request was already used for a different answer.")
        return {
            "batch_id": batch_id,
            "applied": len(existing_keys),
            "action": action,
            "idempotent_replay": True,
        }
    keys, available = _selected_patterns(requested_keys)
    if action == "combine" and len(keys) < 2:
        raise ValueError("choose at least two bills to combine")
    if action == "track":
        track_preview = preview_track(keys, payment_date=payment_date)
        if track_preview["blocked"]:
            matches = ", ".join(
                str(row["possible_duplicate"]["vendor"])
                for row in track_preview["rows"] if row["possible_duplicate"]
            )
            raise ValueError(
                f"Review the matching schedule before tracking this bill: {matches}."
            )
    if action == "track" and payment_date:
        date.fromisoformat(payment_date)
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
            if not preview_token or not hmac.compare_digest(
                preview["preview_token"], str(preview_token)
            ):
                raise ValueError("Preview the current combination before confirming it.")
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
            ON CONFLICT(id) DO NOTHING
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
