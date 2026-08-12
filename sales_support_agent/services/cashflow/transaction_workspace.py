"""Shared, auditable draft/preview/apply contract for Finance Control.

This service owns mutation eligibility. Pages may stage requested changes, but
they cannot decide whether protected financial records are safe to change.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import uuid4

from sqlalchemy import bindparam, text

from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine
from sales_support_agent.services.token_seal import seal_token, unseal_token

SCOPE = "default"
DRAFT_TTL = timedelta(days=30)
PREVIEW_TTL = timedelta(minutes=15)
OBJECT_TYPES = frozenset({"cash_event", "pattern", "savings_opportunity"})
SAVINGS_STATES = frozenset({"needed", "waste", "unknown", "investigate"})
LOW_RISK_ACTIONS = frozenset({
    "set_savings_state", "set_category", "set_note", "mark_internal_transfer",
    "mark_duplicate",
})
PROTECTED_TERMS = frozenset({"payroll", "salary", "wages", "tax", "irs", "debt", "loan", "mortgage"})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _is_expired(value: datetime, now: datetime | None = None) -> bool:
    """Compare SQLite-naive and Postgres-aware timestamps consistently."""
    current = now or _now()
    candidate = value
    if isinstance(candidate, str):
        candidate = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    return candidate < current


def _actor(value: str) -> str:
    actor = str(value or "").strip().lower()
    if not actor or len(actor) > 255:
        raise ValueError("A valid operator identity is required.")
    return actor


def _secret() -> str:
    value = (
        os.getenv("FINANCE_WORKSPACE_TOKEN_SECRET", "").strip()
        or os.getenv("PLAID_TOKEN_SECRET", "").strip()
        or os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
    )
    if not value:
        raise RuntimeError("Finance draft encryption is not configured.")
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _payload_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode()).hexdigest()


def _public(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _public(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_public(item) for item in value]
    return value


def _normalize_change(raw: Mapping[str, Any]) -> dict[str, Any]:
    object_type = str(raw.get("object_type") or "").strip()
    object_id = str(raw.get("object_id") or "").strip()
    action = str(raw.get("action") or "").strip()
    if object_type not in OBJECT_TYPES or not object_id or len(object_id) > 255:
        raise ValueError("A Finance item is invalid; refresh and try again.")
    if action not in LOW_RISK_ACTIONS:
        raise ValueError("That Finance action is not supported.")
    value = raw.get("value")
    if action == "set_savings_state" and str(value) not in SAVINGS_STATES:
        raise ValueError("Savings state must be needed, waste, unknown, or investigate.")
    if action in {"set_category", "set_note"}:
        value = str(value or "").strip()
        limit = 64 if action == "set_category" else 2000
        if not value or len(value) > limit:
            raise ValueError("The staged value is missing or too long.")
    if action in {"mark_internal_transfer", "mark_duplicate"}:
        value = bool(value)
    return {
        "object_type": object_type,
        "object_id": object_id,
        "action": action,
        "value": value,
        "expected_revision": int(raw.get("expected_revision") or 0),
    }


def _load_object(connection, object_type: str, object_id: str) -> dict[str, Any] | None:
    if object_type == "cash_event":
        row = connection.execute(text("""
            SELECT id, record_kind, event_type, name, vendor_or_customer, description,
                   amount_cents, category, status, confidence, workflow_status,
                   source, source_id, due_date, effective_date, notes, match_status, updated_at
            FROM cash_events WHERE id=:id
        """), {"id": object_id}).fetchone()
        return dict(row._mapping) if row else None
    if object_type == "savings_opportunity":
        row = connection.execute(text("""
            SELECT opportunity_key AS id, display_name AS name,
                   normalized_merchant AS vendor_or_customer,
                   potential_monthly_cents AS amount_cents, state, reason AS notes,
                   cadence, updated_at
            FROM finance_savings_reviews
            WHERE scope_key=:scope AND opportunity_key=:id
        """), {"scope": SCOPE, "id": object_id}).fetchone()
        if row:
            item = dict(row._mapping)
            evidence = connection.execute(text("""
                SELECT evidence_json FROM finance_savings_reviews
                WHERE scope_key=:scope AND opportunity_key=:id
            """), {"scope": SCOPE, "id": object_id}).scalar()
            if evidence:
                item["_evidence"] = evidence if isinstance(evidence, dict) else json.loads(evidence)
            return item
        # Unreviewed opportunities are deterministic read models. Reload the
        # current calculation so a stale browser snapshot never becomes truth.
        from sales_support_agent.services.cashflow.budgeting import load_budget_view
        view = load_budget_view()
        opportunity = next((
            dict(candidate) for candidate in view.get("trim_items") or []
            if str(candidate.get("opportunity_key") or "") == object_id
        ), None)
        if opportunity:
            return {
                "id": object_id,
                "name": opportunity.get("display_name"),
                "vendor_or_customer": opportunity.get("normalized_merchant"),
                "amount_cents": opportunity.get("monthly_potential_cents"),
                "state": opportunity.get("review_state") or "unknown",
                "notes": opportunity.get("review_note") or "",
                "cadence": opportunity.get("cadence"),
                "updated_at": opportunity.get("evidence_hash"),
                "_evidence": opportunity,
            }
        return None
    row = connection.execute(text("""
        SELECT id, name, terms_type, frequency AS cadence, payment_amount_cents AS amount_cents,
               updated_at FROM finance_vendors WHERE scope_key=:scope AND id=:id
    """), {"scope": SCOPE, "id": object_id}).fetchone()
    return dict(row._mapping) if row else None


def _protected(item: Mapping[str, Any]) -> bool:
    haystack = " ".join(str(item.get(key) or "") for key in (
        "name", "vendor_or_customer", "description", "category", "commitment_type"
    )).lower()
    return any(term in haystack for term in PROTECTED_TERMS)


def _current_decision(connection, object_type: str, object_id: str) -> dict[str, Any]:
    row = connection.execute(text("""
        SELECT revision, decision_json FROM finance_object_decisions
        WHERE scope_key=:scope AND object_type=:type AND object_id=:id
    """), {"scope": SCOPE, "type": object_type, "id": object_id}).fetchone()
    if not row:
        return {"revision": 0, "decision": {}}
    decision = row.decision_json if isinstance(row.decision_json, dict) else json.loads(row.decision_json or "{}")
    return {"revision": int(row.revision or 0), "decision": decision}


def _cash_evidence(connection, item: Mapping[str, Any]) -> dict[str, Any]:
    event_id = str(item.get("id") or "")
    kind = str(item.get("record_kind") or "")
    allocation_column = "transaction_event_id" if kind == "transaction" else "obligation_event_id"
    allocations = [dict(row._mapping) for row in connection.execute(text(f"""
        SELECT id, obligation_event_id, transaction_event_id, amount_cents,
               allocation_date, source, confidence, notes
        FROM settlement_allocations
        WHERE {allocation_column}=:id AND reversed_allocation_id IS NULL
        ORDER BY allocation_date DESC LIMIT 25
    """), {"id": event_id}).fetchall()]
    related_ids = [
        str(row.get("obligation_event_id") if kind == "transaction" else row.get("transaction_event_id") or "")
        for row in allocations
    ]
    related = []
    if related_ids:
        related = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, record_kind, name, vendor_or_customer, amount_cents, status,
                   confidence, due_date, effective_date
            FROM cash_events WHERE id IN :ids
        """).bindparams(bindparam("ids", expanding=True)), {"ids": related_ids}).fetchall()]
    vendor = str(item.get("vendor_or_customer") or item.get("name") or "").strip()
    similar = []
    if vendor:
        similar = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, amount_cents, status, confidence, due_date, effective_date, source
            FROM cash_events
            WHERE id<>:id AND (lower(vendor_or_customer)=lower(:vendor) OR lower(name)=lower(:vendor))
            ORDER BY COALESCE(effective_date, due_date, updated_at) DESC LIMIT 8
        """), {"id": event_id, "vendor": vendor}).fetchall()]
    source_rows = [dict(row._mapping) for row in connection.execute(text("""
        SELECT source_system, entity_type, external_id, updated_at
        FROM finance_source_records WHERE cash_event_id=:id ORDER BY updated_at DESC
    """), {"id": event_id}).fetchall()]
    for source in source_rows:
        external = str(source.pop("external_id", "") or "")
        source["masked_external_id"] = f"…{external[-6:]}" if external else "Unavailable"
    activity = [dict(row._mapping) for row in connection.execute(text("""
        SELECT action_type, actor, evidence_json, created_at
        FROM finance_action_audit WHERE entity_id=:id ORDER BY created_at DESC LIMIT 20
    """), {"id": event_id}).fetchall()]
    return {
        "payment_evidence": {
            "allocated_cents": sum(int(row.get("amount_cents") or 0) for row in allocations),
            "allocations": allocations,
        },
        "related_items": related,
        "similar_transactions": similar,
        "source_identifiers": source_rows,
        "activity": activity,
    }


def action_catalog() -> list[dict[str, Any]]:
    return [
        {"id": "set_savings_state", "label": "Classify spending", "bulk": True, "consequential": False},
        {"id": "set_category", "label": "Change category", "bulk": True, "consequential": False},
        {"id": "set_note", "label": "Add review note", "bulk": True, "consequential": False},
        {"id": "mark_internal_transfer", "label": "Mark internal transfer", "bulk": True, "consequential": False},
        {"id": "mark_duplicate", "label": "Mark duplicate", "bulk": True, "consequential": False},
    ]


def get_finance_object(object_type: str, object_id: str, *, engine=None) -> dict[str, Any]:
    if object_type not in OBJECT_TYPES:
        raise ValueError("Unsupported Finance object type.")
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.connect() as connection:
        item = _load_object(connection, object_type, object_id)
        if not item:
            raise LookupError("Finance item was not found.")
        current = _current_decision(connection, object_type, object_id)
        evidence = _cash_evidence(connection, item) if object_type == "cash_event" else {}
    item.pop("_evidence", None)
    item.update({"object_type": object_type, "protected": _protected(item), **current, **evidence})
    return _public(item)


def save_draft(changes: list[Mapping[str, Any]], *, actor: str, dataset_revision: str = "", engine=None) -> dict[str, Any]:
    operator = _actor(actor)
    normalized = [_normalize_change(change) for change in changes]
    if len(normalized) > 500:
        raise ValueError("A Finance draft can contain at most 500 changes.")
    encoded = _canonical_json({"changes": normalized})
    if len(encoded.encode()) > 512_000:
        raise ValueError("The Finance draft is too large.")
    sealed = seal_token(_secret(), encoded)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    now = _now()
    draft_id = str(uuid4())
    with db.begin() as connection:
        existing = connection.execute(text("""
            SELECT id, draft_revision FROM finance_workspace_drafts
            WHERE scope_key=:scope AND actor=:actor
        """), {"scope": SCOPE, "actor": operator}).fetchone()
        revision = int(existing.draft_revision or 0) + 1 if existing else 1
        if existing:
            draft_id = str(existing.id)
            connection.execute(text("""
                UPDATE finance_workspace_drafts SET dataset_revision=:dataset,
                    draft_revision=:revision, sealed_payload=:payload,
                    updated_at=:now, expires_at=:expires WHERE id=:id
            """), {"dataset": dataset_revision, "revision": revision, "payload": sealed,
                    "now": now, "expires": now + DRAFT_TTL, "id": draft_id})
        else:
            connection.execute(text("""
                INSERT INTO finance_workspace_drafts
                    (id, scope_key, actor, dataset_revision, draft_revision, sealed_payload,
                     created_at, updated_at, expires_at)
                VALUES (:id, :scope, :actor, :dataset, :revision, :payload, :now, :now, :expires)
            """), {"id": draft_id, "scope": SCOPE, "actor": operator,
                    "dataset": dataset_revision, "revision": revision, "payload": sealed,
                    "now": now, "expires": now + DRAFT_TTL})
    return {"id": draft_id, "draft_revision": revision, "change_count": len(normalized), "state": "Draft"}


def load_draft(*, actor: str, engine=None) -> dict[str, Any] | None:
    operator = _actor(actor)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.connect() as connection:
        row = connection.execute(text("""
            SELECT id, dataset_revision, draft_revision, sealed_payload, updated_at, expires_at
            FROM finance_workspace_drafts WHERE scope_key=:scope AND actor=:actor
        """), {"scope": SCOPE, "actor": operator}).fetchone()
    if not row or _is_expired(row.expires_at):
        return None
    payload = json.loads(unseal_token(_secret(), str(row.sealed_payload)))
    return {"id": row.id, "dataset_revision": row.dataset_revision,
            "draft_revision": row.draft_revision, "updated_at": _public(row.updated_at),
            "changes": payload.get("changes", []), "state": "Draft"}


def discard_draft(*, actor: str, engine=None) -> bool:
    operator = _actor(actor)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.begin() as connection:
        result = connection.execute(text("""
            DELETE FROM finance_workspace_drafts WHERE scope_key=:scope AND actor=:actor
        """), {"scope": SCOPE, "actor": operator})
    return bool(result.rowcount)


def preview_changes(changes: list[Mapping[str, Any]], *, actor: str, draft_revision: int = 0, engine=None) -> dict[str, Any]:
    operator = _actor(actor)
    normalized = [_normalize_change(change) for change in changes]
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    results: list[dict[str, Any]] = []
    with db.begin() as connection:
        for change in normalized:
            item = _load_object(connection, change["object_type"], change["object_id"])
            current = _current_decision(connection, change["object_type"], change["object_id"])
            status = "eligible"
            reason = ""
            if not item:
                status, reason = "invalid", "Item no longer exists."
            elif change["expected_revision"] and change["expected_revision"] != current["revision"]:
                status, reason = "conflict", "Item changed after this draft was created."
            elif _protected(item) and change["action"] in {"set_savings_state", "set_category", "mark_internal_transfer", "mark_duplicate"}:
                status, reason = "protected", "Payroll, tax, and debt items require individual review."
            comparable = {key: value for key, value in (item or {}).items() if key != "_evidence"}
            results.append({**change, "status": status, "reason": reason,
                            "current_revision": current["revision"],
                            "object_hash": _payload_hash(_public(comparable)) if item else "",
                            "amount_cents": int((item or {}).get("amount_cents") or 0)})
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        preview_id = str(uuid4())
        preview_payload = {"items": results}
        connection.execute(text("""
            INSERT INTO finance_batch_previews
                (id, scope_key, actor, token_hash, draft_revision, payload_hash,
                 preview_json, created_at, expires_at)
            VALUES (:id, :scope, :actor, :token, :revision, :hash, :preview, :now, :expires)
        """), {"id": preview_id, "scope": SCOPE, "actor": operator, "token": token_hash,
                "revision": int(draft_revision or 0), "hash": _payload_hash(normalized),
                "preview": _canonical_json(preview_payload), "now": _now(), "expires": _now() + PREVIEW_TTL})
    return {"preview_token": token, "preview_id": preview_id, "items": results,
            "eligible_count": sum(item["status"] == "eligible" for item in results),
            "protected_count": sum(item["status"] == "protected" for item in results),
            "conflict_count": sum(item["status"] == "conflict" for item in results),
            "invalid_count": sum(item["status"] == "invalid" for item in results),
            "state": "Ready"}


def _decision_after(prior: dict[str, Any], change: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(prior)
    key = {
        "set_savings_state": "savings_state",
        "set_category": "category",
        "set_note": "note",
        "mark_internal_transfer": "internal_transfer",
        "mark_duplicate": "duplicate",
    }[str(change["action"])]
    result[key] = change["value"]
    return result


def apply_preview(preview_token: str, *, actor: str, idempotency_key: str, reason: str = "", source_page: str = "", engine=None) -> dict[str, Any]:
    operator = _actor(actor)
    key = str(idempotency_key or "").strip()
    if not key or len(key) > 128:
        raise ValueError("A valid idempotency key is required.")
    token_hash = hashlib.sha256(str(preview_token or "").encode()).hexdigest()
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    now = _now()
    with db.begin() as connection:
        prior_batch = connection.execute(text("""
            SELECT id, status, item_count FROM finance_action_batches WHERE idempotency_key=:key
        """), {"key": key}).fetchone()
        if prior_batch:
            return {"batch_id": prior_batch.id, "status": prior_batch.status,
                    "applied": prior_batch.item_count, "idempotent_replay": True}
        row = connection.execute(text("""
            SELECT id, preview_json, expires_at, consumed_at FROM finance_batch_previews
            WHERE scope_key=:scope AND actor=:actor AND token_hash=:token
        """), {"scope": SCOPE, "actor": operator, "token": token_hash}).fetchone()
        if not row or row.consumed_at or _is_expired(row.expires_at, now):
            raise ValueError("This preview expired or was already used. Preview the changes again.")
        preview = row.preview_json if isinstance(row.preview_json, dict) else json.loads(row.preview_json)
        all_items = list(preview.get("items", []))
        eligible = [item for item in all_items if item.get("status") == "eligible"]
        if not eligible:
            raise ValueError("There are no eligible changes to apply.")
        batch_id = str(uuid4())
        total = sum(abs(int(item.get("amount_cents") or 0)) for item in eligible)
        batch_status = "partially_applied" if len(eligible) != len(all_items) else "applied"
        connection.execute(text("""
            INSERT INTO finance_action_batches
                (id, scope_key, actor, idempotency_key, reason, status, source_page,
                 item_count, amount_cents, created_at, committed_at)
            VALUES (:id, :scope, :actor, :key, :reason, :status, :page,
                    :count, :amount, :now, :now)
        """), {"id": batch_id, "scope": SCOPE, "actor": operator, "key": key,
                "reason": str(reason or "")[:2000], "status": batch_status,
                "page": str(source_page or "")[:64], "count": len(eligible),
                "amount": total, "now": now})
        batch_revisions: dict[tuple[str, str], int] = {}
        batch_objects: dict[tuple[str, str], dict[str, Any]] = {}
        for position, item in enumerate(all_items):
            if item.get("status") != "eligible":
                connection.execute(text("""
                    INSERT INTO finance_action_batch_items
                        (id, batch_id, position, object_type, object_id, action, prior_state_json,
                         new_state_json, eligibility_result, skip_reason, external_sync_status, created_at)
                    VALUES (:id, :batch, :position, :type, :object_id, :action, :prior, :new,
                            :eligibility, :reason, 'not_required', :now)
                """), {"id": str(uuid4()), "batch": batch_id, "position": position, "type": item["object_type"],
                        "object_id": item["object_id"], "action": item["action"],
                        "prior": _canonical_json({}), "new": _canonical_json({}),
                        "eligibility": str(item.get("status") or "invalid"),
                        "reason": str(item.get("reason") or ""), "now": now})
                continue
            object_key = (item["object_type"], item["object_id"])
            authoritative = batch_objects.get(object_key)
            if authoritative is None:
                authoritative = _load_object(connection, *object_key)
                comparable = {key: value for key, value in (authoritative or {}).items() if key != "_evidence"}
                if not authoritative or item.get("object_hash") != _payload_hash(_public(comparable)):
                    raise ValueError("A Finance item changed after preview. Preview the batch again.")
                batch_objects[object_key] = authoritative
            current = _current_decision(connection, *object_key)
            if object_key not in batch_revisions:
                if int(item.get("current_revision") or 0) != current["revision"]:
                    raise ValueError("A Finance item changed after preview. Preview the batch again.")
                batch_revisions[object_key] = current["revision"]
            next_decision = _decision_after(current["decision"], item)
            revision = current["revision"] + 1
            prior_for_undo = dict(current["decision"])
            if item["object_type"] == "cash_event" and item["action"] in {
                "set_category", "mark_internal_transfer", "mark_duplicate",
            }:
                if str(authoritative.get("record_kind") or "") != "transaction":
                    raise ValueError("Only posted transactions can be categorized.")
                prior_for_undo["__cash_event"] = {
                    "category": str(authoritative.get("category") or "uncategorized"),
                    "match_status": str(authoritative.get("match_status") or ""),
                }
            if item["object_type"] == "savings_opportunity" and item["action"] == "set_savings_state":
                prior_review = connection.execute(text("""
                    SELECT state, reason FROM finance_savings_reviews
                    WHERE scope_key=:scope AND opportunity_key=:key
                """), {"scope": SCOPE, "key": item["object_id"]}).fetchone()
                prior_for_undo["__savings_review"] = (
                    {"state": str(prior_review.state or "unknown"), "reason": str(prior_review.reason or "")}
                    if prior_review else None
                )
            decision_id = hashlib.sha256(f"{SCOPE}|{item['object_type']}|{item['object_id']}".encode()).hexdigest()
            connection.execute(text("""
                INSERT INTO finance_object_decisions
                    (id, scope_key, object_type, object_id, revision, decision_json,
                     updated_by, created_at, updated_at)
                VALUES (:id, :scope, :type, :object_id, :revision, :decision, :actor, :now, :now)
                ON CONFLICT(scope_key, object_type, object_id) DO UPDATE SET
                    revision=:revision, decision_json=:decision, updated_by=:actor, updated_at=:now
            """), {"id": decision_id, "scope": SCOPE, "type": item["object_type"],
                    "object_id": item["object_id"], "revision": revision,
                    "decision": _canonical_json(next_decision), "actor": operator, "now": now})
            connection.execute(text("""
                INSERT INTO finance_action_batch_items
                    (id, batch_id, position, object_type, object_id, action, prior_state_json,
                     new_state_json, eligibility_result, skip_reason, external_sync_status, created_at)
                VALUES (:id, :batch, :position, :type, :object_id, :action, :prior, :new,
                        'eligible', '', 'not_required', :now)
            """), {"id": str(uuid4()), "batch": batch_id, "position": position, "type": item["object_type"],
                    "object_id": item["object_id"], "action": item["action"],
                    "prior": _canonical_json(prior_for_undo),
                    "new": _canonical_json(next_decision), "now": now})
            if item["object_type"] == "cash_event" and item["action"] in {
                "set_category", "mark_internal_transfer", "mark_duplicate",
            }:
                category = str(authoritative.get("category") or "uncategorized")
                match_status = str(authoritative.get("match_status") or "")
                if item["action"] == "set_category":
                    category = str(item["value"])
                elif item["action"] == "mark_internal_transfer":
                    category = "transfer"
                elif item["action"] == "mark_duplicate":
                    match_status = "duplicate" if item["value"] else ""
                connection.execute(text("""
                    UPDATE cash_events SET category=:category, match_status=:match_status, updated_at=:now
                    WHERE id=:id AND record_kind='transaction'
                """), {"category": category, "match_status": match_status, "now": now, "id": item["object_id"]})
            if item["object_type"] == "savings_opportunity" and item["action"] == "set_savings_state":
                from sales_support_agent.services.cashflow.savings_reviews import _prepare_review, _record_prepared_review
                evidence = dict(authoritative.get("_evidence") or {})
                prepared = _prepare_review(
                    evidence, str(item["value"]), operator,
                    reason=str(next_decision.get("note") or ""), scope=SCOPE,
                    request_id=f"workspace:{batch_id}:{item['object_id']}:{revision}",
                    clickup_task=None, owner="", action_type="",
                    effective_date="", proof_note="",
                )
                _record_prepared_review(connection, prepared)
        connection.execute(text("UPDATE finance_batch_previews SET consumed_at=:now WHERE id=:id"), {"now": now, "id": row.id})
        connection.execute(text("DELETE FROM finance_workspace_drafts WHERE scope_key=:scope AND actor=:actor"), {"scope": SCOPE, "actor": operator})
        connection.execute(text("""
            INSERT INTO finance_action_audit
                (id, scope_key, action_type, entity_type, entity_id, actor,
                 idempotency_key, evidence_json, created_at)
            VALUES (:id, :scope, 'workspace_batch_applied', 'finance_action_batch',
                    :batch, :actor, :audit_key, :evidence, :now)
        """), {"id": str(uuid4()), "scope": SCOPE, "batch": batch_id, "actor": operator,
                "audit_key": f"workspace-audit:{key}",
                "evidence": _canonical_json({"item_count": len(eligible), "skipped_count": len(all_items) - len(eligible), "amount_cents": total}), "now": now})
    save_state = "Partially synced" if len(eligible) != len(all_items) else "Saved"
    return {"batch_id": batch_id, "status": save_state, "applied": len(eligible),
            "skipped": len(all_items) - len(eligible),
            "amount_cents": total, "idempotent_replay": False}


def undo_batch(batch_id: str, *, actor: str, engine=None) -> dict[str, Any]:
    operator = _actor(actor)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    now = _now()
    with db.begin() as connection:
        batch = connection.execute(text("""
            SELECT id, status, undone_at FROM finance_action_batches
            WHERE id=:id AND scope_key=:scope
        """), {"id": batch_id, "scope": SCOPE}).fetchone()
        if not batch or batch.undone_at:
            raise ValueError("This batch is unavailable or has already been undone.")
        items = connection.execute(text("""
            SELECT object_type, object_id, prior_state_json, new_state_json, eligibility_result
            FROM finance_action_batch_items WHERE batch_id=:id ORDER BY position DESC
        """), {"id": batch_id}).fetchall()
        for item in items:
            if item.eligibility_result != "eligible":
                continue
            current = _current_decision(connection, item.object_type, item.object_id)
            expected = item.new_state_json if isinstance(item.new_state_json, dict) else json.loads(item.new_state_json)
            if current["decision"] != expected:
                raise ValueError("A newer decision replaced this batch; undo was safely stopped.")
            prior = item.prior_state_json if isinstance(item.prior_state_json, dict) else json.loads(item.prior_state_json)
            prior_review = prior.pop("__savings_review", "not_applicable")
            prior_cash_event = prior.pop("__cash_event", None)
            connection.execute(text("""
                UPDATE finance_object_decisions SET revision=revision+1,
                    decision_json=:prior, updated_by=:actor, updated_at=:now
                WHERE scope_key=:scope AND object_type=:type AND object_id=:object_id
            """), {"prior": _canonical_json(prior), "actor": operator, "now": now,
                    "scope": SCOPE, "type": item.object_type, "object_id": item.object_id})
            if item.object_type == "savings_opportunity" and prior_review != "not_applicable":
                restored_state = str((prior_review or {}).get("state") or "unknown")
                restored_reason = str((prior_review or {}).get("reason") or "")
                connection.execute(text("""
                    UPDATE finance_savings_reviews SET state=:state, reason=:reason, updated_at=:now
                    WHERE scope_key=:scope AND opportunity_key=:key
                """), {"state": restored_state, "reason": restored_reason, "now": now,
                        "scope": SCOPE, "key": item.object_id})
            if item.object_type == "cash_event" and prior_cash_event is not None:
                connection.execute(text("""
                    UPDATE cash_events SET category=:category, match_status=:match_status, updated_at=:now
                    WHERE id=:id AND record_kind='transaction'
                """), {
                    "category": str(prior_cash_event.get("category") or "uncategorized"),
                    "match_status": str(prior_cash_event.get("match_status") or ""),
                    "now": now,
                    "id": item.object_id,
                })
        connection.execute(text("""
            UPDATE finance_action_batches SET status='undone', undone_at=:now WHERE id=:id
        """), {"now": now, "id": batch_id})
        connection.execute(text("""
            INSERT INTO finance_action_audit
                (id, scope_key, action_type, entity_type, entity_id, actor, evidence_json, created_at)
            VALUES (:id, :scope, 'workspace_batch_undone', 'finance_action_batch',
                    :batch, :actor, :evidence, :now)
        """), {"id": str(uuid4()), "scope": SCOPE, "batch": batch_id,
                "actor": operator, "evidence": _canonical_json({"restored": len(items)}), "now": now})
    return {"batch_id": batch_id, "status": "Undone",
            "restored": sum(item.eligibility_result == "eligible" for item in items)}


def get_batch_receipt(batch_id: str, *, engine=None) -> dict[str, Any]:
    """Return a plain audit receipt without exposing encrypted drafts."""
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.connect() as connection:
        batch = connection.execute(text("""
            SELECT id, actor, reason, status, source_page, item_count, amount_cents,
                   created_at, committed_at, undone_at
            FROM finance_action_batches WHERE id=:id AND scope_key=:scope
        """), {"id": batch_id, "scope": SCOPE}).fetchone()
        if not batch:
            raise LookupError("Finance receipt was not found.")
        rows = connection.execute(text("""
            SELECT object_type, object_id, action, prior_state_json, new_state_json,
                   eligibility_result, skip_reason, external_sync_status, created_at
            FROM finance_action_batch_items WHERE batch_id=:id ORDER BY position
        """), {"id": batch_id}).fetchall()
    items = []
    for row in rows:
        item = dict(row._mapping)
        for field in ("prior_state_json", "new_state_json"):
            if isinstance(item.get(field), str):
                item[field] = json.loads(item[field] or "{}")
        items.append(item)
    return _public({**dict(batch._mapping), "items": items,
            "applied_count": sum(item["eligibility_result"] == "eligible" for item in items),
            "skipped_count": sum(item["eligibility_result"] != "eligible" for item in items)})


def list_activity(*, limit: int = 50, engine=None) -> list[dict[str, Any]]:
    """Return recent shared-workspace audit activity newest first."""
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    safe_limit = max(1, min(int(limit or 50), 200))
    with db.connect() as connection:
        rows = connection.execute(text("""
            SELECT id, action_type, entity_type, entity_id, actor, evidence_json, created_at
            FROM finance_action_audit WHERE scope_key=:scope
            ORDER BY created_at DESC LIMIT :limit
        """), {"scope": SCOPE, "limit": safe_limit}).fetchall()
    return [_public(dict(row._mapping)) for row in rows]


def search_finance(query: str, *, limit: int = 30, engine=None) -> list[dict[str, Any]]:
    """Search canonical facts and saved savings reviews without source mutation."""
    term = str(query or "").strip()
    if len(term) < 2:
        return []
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    safe_limit = max(1, min(int(limit or 30), 50))
    like = f"%{term.lower()}%"
    with db.connect() as connection:
        events = [dict(row._mapping) for row in connection.execute(text("""
            SELECT id, record_kind AS object_type, name, vendor_or_customer,
                   description, amount_cents, status, confidence,
                   COALESCE(effective_date, due_date) AS event_date
            FROM cash_events
            WHERE archived_at IS NULL AND (
                lower(name) LIKE :term OR lower(vendor_or_customer) LIKE :term
                OR lower(description) LIKE :term OR lower(bank_reference) LIKE :term
            )
            ORDER BY COALESCE(effective_date, due_date, updated_at) DESC LIMIT :limit
        """), {"term": like, "limit": safe_limit}).fetchall()]
        remaining = max(0, safe_limit - len(events))
        savings = []
        if remaining:
            savings = [dict(row._mapping) for row in connection.execute(text("""
                SELECT opportunity_key AS id, 'savings_opportunity' AS object_type,
                       display_name AS name, normalized_merchant AS vendor_or_customer,
                       reason AS description, potential_monthly_cents AS amount_cents,
                       state AS status, cadence AS confidence, updated_at AS event_date
                FROM finance_savings_reviews
                WHERE scope_key=:scope AND (
                    lower(display_name) LIKE :term OR lower(normalized_merchant) LIKE :term
                    OR lower(reason) LIKE :term
                ) ORDER BY updated_at DESC LIMIT :limit
            """), {"scope": SCOPE, "term": like, "limit": remaining}).fetchall()]
    for event in events:
        event["object_type"] = "cash_event"
    return _public([*events, *savings])


def save_view(name: str, definition: Mapping[str, Any], *, actor: str, engine=None) -> dict[str, Any]:
    operator = _actor(actor)
    label = " ".join(str(name or "").split())
    if not label or len(label) > 96:
        raise ValueError("Saved view name must be between 1 and 96 characters.")
    allowed = {"query", "object_type", "payment_status", "planning_status", "savings_state", "category", "account", "recurring_confidence", "sort"}
    prepared = {key: definition[key] for key in allowed if key in definition and definition[key] not in (None, "", [])}
    encoded = _canonical_json(prepared)
    if len(encoded) > 8_000:
        raise ValueError("Saved view is too large.")
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    now = _now()
    view_id = hashlib.sha256(f"{SCOPE}|{operator}|{label.lower()}".encode()).hexdigest()
    with db.begin() as connection:
        connection.execute(text("""
            INSERT INTO finance_saved_views
                (id, scope_key, actor, name, definition_json, created_at, updated_at)
            VALUES (:id, :scope, :actor, :name, :definition, :now, :now)
            ON CONFLICT(scope_key, actor, name) DO UPDATE SET
                definition_json=:definition, updated_at=:now
        """), {"id": view_id, "scope": SCOPE, "actor": operator, "name": label,
                "definition": encoded, "now": now})
    return {"id": view_id, "name": label, "definition": prepared, "updated_at": now.isoformat()}


def list_saved_views(*, actor: str, engine=None) -> list[dict[str, Any]]:
    operator = _actor(actor)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.connect() as connection:
        rows = connection.execute(text("""
            SELECT id, name, definition_json, updated_at FROM finance_saved_views
            WHERE scope_key=:scope AND actor=:actor ORDER BY name
        """), {"scope": SCOPE, "actor": operator}).fetchall()
    out = []
    for row in rows:
        definition = row.definition_json if isinstance(row.definition_json, dict) else json.loads(row.definition_json or "{}")
        out.append({"id": row.id, "name": row.name, "definition": definition, "updated_at": _public(row.updated_at)})
    return out


def delete_saved_view(view_id: str, *, actor: str, engine=None) -> bool:
    operator = _actor(actor)
    db = engine or get_engine()
    ensure_finance_trust_schema(db)
    with db.begin() as connection:
        result = connection.execute(text("""
            DELETE FROM finance_saved_views WHERE id=:id AND scope_key=:scope AND actor=:actor
        """), {"id": str(view_id or ""), "scope": SCOPE, "actor": operator})
    return bool(result.rowcount)
