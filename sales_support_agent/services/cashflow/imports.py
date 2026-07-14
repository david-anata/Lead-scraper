"""Atomic staging and posting for Finance source imports."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from sqlalchemy import text

from sales_support_agent.services.cashflow.identity import (
    assign_bank_identities,
    assign_qbo_invoice_identities,
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items() if key != "_identity"}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))


@dataclass
class ImportPostResult:
    batch_id: str
    status: str = "staged"
    inserted: int = 0
    duplicates: int = 0
    review: int = 0
    invalid: int = 0
    new_events: list[dict[str, Any]] = field(default_factory=list)


def _legacy_csv_identity_map(conn, *, scope_key: str) -> dict[str, str]:
    """Map deterministic identities to legacy CSV events without mutating them."""
    rows = conn.execute(text("""
        SELECT id, source_id, event_type, amount_cents, due_date, effective_date,
               description, name, vendor_or_customer, bank_reference,
               account_balance_cents, created_at
        FROM cash_events
        WHERE source='csv'
        ORDER BY due_date ASC, created_at ASC, id ASC
    """)).fetchall()
    explicit: dict[str, str] = {}
    blanks: list[dict[str, Any]] = []
    for raw in rows:
        item = dict(raw._mapping)
        source_id = str(item.get("source_id") or "").strip()
        if source_id:
            explicit[source_id] = str(item["id"])
        else:
            blanks.append(item)
    mapping = dict(explicit)
    for item in assign_bank_identities(blanks, scope_key=scope_key):
        mapping[str(item["source_id"])] = str(item["id"])
    return mapping


def stage_and_post_bank_import(
    engine,
    *,
    file_hash: str,
    rows: list[dict[str, Any]],
    scope_key: str = "default",
) -> ImportPostResult:
    """Classify all rows and atomically post accepted events/source records.

    Each input item contains ``raw``, plus either ``normalized`` or ``error``.
    Changed payloads for an existing provider identity are quarantined for
    review; they never overwrite a posted transaction.
    """
    from sales_support_agent.models.database import ensure_finance_trust_schema

    ensure_finance_trust_schema(engine)
    batch_id = str(uuid.uuid4())
    result = ImportPostResult(batch_id=batch_id)
    normalized_inputs = [item["normalized"] for item in rows if item.get("normalized") is not None]
    identified = iter(assign_bank_identities(normalized_inputs, scope_key=scope_key))
    prepared: list[dict[str, Any]] = []
    for row_number, item in enumerate(rows, 1):
        normalized = next(identified) if item.get("normalized") is not None else None
        prepared.append({
            "row_number": row_number,
            "raw": item.get("raw") or {},
            "normalized": normalized,
            "error": str(item.get("error") or ""),
        })

    now = datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO finance_import_batches (
                id, source_type, file_hash, status, ready_count,
                duplicate_count, review_count, invalid_count, created_at, posted_at
            ) VALUES (
                :id, 'csv', :file_hash, 'staged', 0, 0, 0, 0, :now, NULL
            )
        """), {"id": batch_id, "file_hash": file_hash, "now": now})

        existing = {
            (str(row.source_system), str(row.scope_key), str(row.entity_type), str(row.external_id)):
                {"cash_event_id": str(row.cash_event_id), "payload_hash": str(row.payload_hash or "")}
            for row in conn.execute(text("""
                SELECT source_system, scope_key, entity_type, external_id,
                       cash_event_id, payload_hash
                FROM finance_source_records
                WHERE source_system='csv' AND scope_key=:scope_key
            """), {"scope_key": scope_key})
        }
        legacy = _legacy_csv_identity_map(conn, scope_key=scope_key)
        seen: dict[tuple[str, str, str, str], str] = {}
        classified: list[dict[str, Any]] = []

        # Classify the complete file before any canonical cash event is written.
        for item in prepared:
            normalized = item["normalized"]
            classification = "invalid"
            reason = item["error"] or "row could not be normalized"
            legacy_event_id: str | None = None
            if normalized is not None:
                identity = normalized["_identity"]
                key = (
                    str(identity["source_system"]), str(identity["scope_key"]),
                    str(identity["entity_type"]), str(identity["external_id"]),
                )
                prior = existing.get(key)
                if prior:
                    if prior["payload_hash"] == identity["payload_hash"]:
                        classification, reason = "duplicate", "same source identity and payload"
                    else:
                        classification, reason = "review", "source identity payload changed"
                elif key in seen:
                    if seen[key] == identity["payload_hash"]:
                        classification, reason = "duplicate", "duplicate identity within upload"
                    else:
                        classification, reason = "review", "conflicting identity within upload"
                elif str(identity["external_id"]) in legacy:
                    classification, reason = "duplicate", "matches legacy posted transaction"
                    legacy_event_id = legacy[str(identity["external_id"])]
                else:
                    classification, reason = "ready", "new source identity"
                    seen[key] = str(identity["payload_hash"])
            classified.append({
                **item,
                "classification": classification,
                "reason": reason,
                "legacy_event_id": legacy_event_id,
            })

        ready_count = sum(item["classification"] == "ready" for item in classified)
        result.duplicates = sum(item["classification"] == "duplicate" for item in classified)
        result.review = sum(item["classification"] == "review" for item in classified)
        result.invalid = sum(item["classification"] == "invalid" for item in classified)
        blocked = bool(result.review or result.invalid)

        for item in classified:
            row_id = str(uuid.uuid4())
            normalized = item["normalized"]
            conn.execute(text("""
                INSERT INTO finance_import_rows (
                    id, import_batch_id, row_number, raw_payload,
                    normalized_payload, classification, reason
                ) VALUES (
                    :id, :batch_id, :row_number, :raw, :normalized,
                    :classification, :reason
                )
            """), {
                "id": row_id, "batch_id": batch_id, "row_number": item["row_number"],
                "raw": _json(item["raw"]), "normalized": _json(normalized or {}),
                "classification": item["classification"], "reason": item["reason"],
            })

        if not blocked:
            for item in classified:
                normalized = item["normalized"]
                if normalized is None:
                    continue
                identity = normalized["_identity"]
                if item["classification"] == "duplicate" and item["legacy_event_id"]:
                    conn.execute(text("""
                        INSERT INTO finance_source_records (
                            id, cash_event_id, source_system, scope_key, entity_type,
                            external_id, payload_hash, soft_fingerprint, created_at, updated_at
                        ) VALUES (
                            :id, :cash_event_id, 'csv', :scope_key, 'bank_transaction',
                            :external_id, :payload_hash, :fingerprint, :now, :now
                        )
                        ON CONFLICT(source_system, scope_key, entity_type, external_id) DO NOTHING
                    """), {
                        "id": str(uuid.uuid4()), "cash_event_id": item["legacy_event_id"],
                        "scope_key": scope_key, "external_id": identity["external_id"],
                        "payload_hash": identity["payload_hash"],
                        "fingerprint": identity["soft_fingerprint"], "now": now,
                    })
                    continue
                if item["classification"] != "ready":
                    continue
                event_id = str(uuid.uuid4())
                due = normalized.get("due_date")
                effective = normalized.get("effective_date")
                vendor = str(normalized.get("vendor_or_customer") or "")
                conn.execute(text("""
                    INSERT INTO cash_events (
                        id, source, source_id, record_kind, event_type, category,
                        subcategory, description, name, vendor_or_customer,
                        amount_cents, due_date, effective_date, status, confidence,
                        source_status, source_updated_at, account_balance_cents,
                        bank_transaction_type, bank_reference, match_status,
                        match_candidates_json, notes, recurring_rule,
                        clickup_task_id, friendly_name, created_at, updated_at
                    ) VALUES (
                        :id, 'csv', :source_id, 'transaction', :event_type, :category,
                        :subcategory, :description, :name, :vendor,
                        :amount_cents, :due_date, :effective_date, 'posted', 'confirmed',
                        'posted', :now, :balance, :bank_type, :bank_reference, '',
                        :match_candidates, '', '', '', :friendly_name, :now, :now
                    )
                """), {
                    "id": event_id, "source_id": identity["external_id"],
                    "event_type": normalized.get("event_type", "outflow"),
                    "category": normalized.get("category", "other"),
                    "subcategory": normalized.get("subcategory", ""),
                    "description": normalized.get("description", "") or "",
                    "name": normalized.get("name", "") or "", "vendor": vendor,
                    "amount_cents": int(normalized.get("amount_cents") or 0),
                    "due_date": due.isoformat() if hasattr(due, "isoformat") else due,
                    "effective_date": effective.isoformat() if hasattr(effective, "isoformat") else effective,
                    "balance": normalized.get("account_balance_cents"),
                    "bank_type": normalized.get("bank_transaction_type", "") or "",
                    "bank_reference": normalized.get("bank_reference", "") or "",
                    "match_candidates": "[]", "friendly_name": vendor[:255] or None,
                    "now": now,
                })
                conn.execute(text("""
                    INSERT INTO finance_source_records (
                        id, cash_event_id, source_system, scope_key, entity_type,
                        external_id, payload_hash, soft_fingerprint, created_at, updated_at
                    ) VALUES (
                        :id, :cash_event_id, 'csv', :scope_key, 'bank_transaction',
                        :external_id, :payload_hash, :fingerprint, :now, :now
                    )
                """), {
                    "id": str(uuid.uuid4()), "cash_event_id": event_id,
                    "scope_key": scope_key, "external_id": identity["external_id"],
                    "payload_hash": identity["payload_hash"],
                    "fingerprint": identity["soft_fingerprint"], "now": now,
                })
                result.new_events.append({"id": event_id, **normalized})

        result.inserted = ready_count if not blocked else 0
        result.status = "posted" if not blocked else ("failed" if result.invalid else "staged")

        conn.execute(text("""
            UPDATE finance_import_batches SET
                status=:status, ready_count=:ready, duplicate_count=:duplicates,
                review_count=:review, invalid_count=:invalid, posted_at=:now
            WHERE id=:id
        """), {
            "id": batch_id, "status": result.status, "ready": ready_count,
            "duplicates": result.duplicates,
            "review": result.review, "invalid": result.invalid, "now": now,
        })
        conn.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (
                :id, :scope_key, 'bank_csv_import', 'finance_import_batch',
                :batch_id, 'operator', :evidence, :now
            )
        """), {
            "id": str(uuid.uuid4()), "scope_key": scope_key, "batch_id": batch_id,
            "evidence": _json({
                "file_hash": file_hash, "status": result.status,
                "inserted": result.inserted, "ready": ready_count,
                "duplicates": result.duplicates, "review": result.review,
                "invalid": result.invalid,
            }), "now": now,
        })
    return result


def _legacy_qbo_invoice_map(conn) -> dict[str, dict[str, Any]]:
    """Return legacy QBO CSV rows that predate source-record tracking."""
    return {
        str(row.source_id): {
            "cash_event_id": str(row.id),
            "amount_cents": int(row.amount_cents or 0),
            "due_date": _jsonable(row.due_date)[:10] if row.due_date else None,
            "vendor_or_customer": str(row.vendor_or_customer or ""),
        }
        for row in conn.execute(text("""
            SELECT id, source_id, amount_cents, due_date, vendor_or_customer
            FROM cash_events
            WHERE source='qbo-csv'
        """))
        if row.source_id
    }


def stage_and_post_qbo_import(
    engine,
    *,
    file_hash: str,
    rows: list[dict[str, Any]],
    scope_key: str = "default",
) -> ImportPostResult:
    """Stage and atomically post a QBO Open Invoices CSV import.

    Existing canonical obligations are append-only. An identical report is
    idempotent; a changed payload for the same invoice is quarantined for
    operator review and blocks every new row in that file from posting.
    """
    from sales_support_agent.models.database import (
        ensure_finance_trust_schema,
        upsert_cash_event,
    )

    ensure_finance_trust_schema(engine)
    batch_id = str(uuid.uuid4())
    result = ImportPostResult(batch_id=batch_id)
    normalized_inputs = [
        item["normalized"] for item in rows if item.get("normalized") is not None
    ]
    identified = iter(assign_qbo_invoice_identities(normalized_inputs, scope_key=scope_key))
    prepared: list[dict[str, Any]] = []
    for row_number, item in enumerate(rows, 1):
        normalized = next(identified) if item.get("normalized") is not None else None
        prepared.append({
            "row_number": row_number,
            "raw": item.get("raw") or {},
            "normalized": normalized,
            "error": str(item.get("error") or ""),
        })

    now = datetime.utcnow()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO finance_import_batches (
                id, source_type, file_hash, status, ready_count,
                duplicate_count, review_count, invalid_count, created_at, posted_at
            ) VALUES (
                :id, 'qbo_csv', :file_hash, 'staged', 0, 0, 0, 0, :now, NULL
            )
        """), {"id": batch_id, "file_hash": file_hash, "now": now})

        existing = {
            (str(row.source_system), str(row.scope_key), str(row.entity_type), str(row.external_id)):
                {"cash_event_id": str(row.cash_event_id), "payload_hash": str(row.payload_hash or "")}
            for row in conn.execute(text("""
                SELECT source_system, scope_key, entity_type, external_id,
                       cash_event_id, payload_hash
                FROM finance_source_records
                WHERE source_system='qbo_csv' AND scope_key=:scope_key
            """), {"scope_key": scope_key})
        }
        legacy = _legacy_qbo_invoice_map(conn)
        seen: dict[tuple[str, str, str, str], str] = {}
        classified: list[dict[str, Any]] = []

        for item in prepared:
            normalized = item["normalized"]
            classification = "invalid"
            reason = item["error"] or "row could not be normalized"
            legacy_event_id: str | None = None
            if normalized is not None:
                identity = normalized["_identity"]
                key = (
                    str(identity["source_system"]), str(identity["scope_key"]),
                    str(identity["entity_type"]), str(identity["external_id"]),
                )
                prior = existing.get(key)
                legacy_row = legacy.get(str(identity["external_id"]))
                if prior:
                    if prior["payload_hash"] == identity["payload_hash"]:
                        classification, reason = "duplicate", "same source identity and payload"
                    else:
                        classification, reason = "review", "source identity payload changed"
                elif key in seen:
                    if seen[key] == identity["payload_hash"]:
                        classification, reason = "duplicate", "duplicate identity within upload"
                    else:
                        classification, reason = "review", "conflicting identity within upload"
                elif legacy_row:
                    due = normalized.get("due_date")
                    due_text = due.isoformat()[:10] if hasattr(due, "isoformat") else str(due or "")[:10]
                    unchanged = (
                        legacy_row["amount_cents"] == int(normalized.get("amount_cents") or 0)
                        and legacy_row["due_date"] == (due_text or None)
                        and legacy_row["vendor_or_customer"]
                        == str(normalized.get("vendor_or_customer") or "")
                    )
                    if unchanged:
                        classification, reason = "duplicate", "matches legacy QBO invoice"
                        legacy_event_id = legacy_row["cash_event_id"]
                    else:
                        classification, reason = "review", "legacy QBO invoice payload changed"
                else:
                    classification, reason = "ready", "new source identity"
                    seen[key] = str(identity["payload_hash"])
            classified.append({
                **item,
                "classification": classification,
                "reason": reason,
                "legacy_event_id": legacy_event_id,
            })

        ready_count = sum(item["classification"] == "ready" for item in classified)
        result.duplicates = sum(item["classification"] == "duplicate" for item in classified)
        result.review = sum(item["classification"] == "review" for item in classified)
        result.invalid = sum(item["classification"] == "invalid" for item in classified)
        blocked = bool(result.review or result.invalid)

        for item in classified:
            conn.execute(text("""
                INSERT INTO finance_import_rows (
                    id, import_batch_id, row_number, raw_payload,
                    normalized_payload, classification, reason
                ) VALUES (
                    :id, :batch_id, :row_number, :raw, :normalized,
                    :classification, :reason
                )
            """), {
                "id": str(uuid.uuid4()), "batch_id": batch_id,
                "row_number": item["row_number"], "raw": _json(item["raw"]),
                "normalized": _json(item["normalized"] or {}),
                "classification": item["classification"], "reason": item["reason"],
            })

        if not blocked:
            for item in classified:
                normalized = item["normalized"]
                if normalized is None:
                    continue
                identity = normalized["_identity"]
                if item["classification"] == "duplicate" and item["legacy_event_id"]:
                    event_id = item["legacy_event_id"]
                elif item["classification"] == "ready":
                    event_id = str(uuid.uuid4())
                    upsert_cash_event(conn, {
                        **normalized,
                        "id": event_id,
                        "source": "qbo-csv",
                        "record_kind": "obligation",
                    })
                    result.new_events.append({"id": event_id, **normalized})
                else:
                    continue
                conn.execute(text("""
                    INSERT INTO finance_source_records (
                        id, cash_event_id, source_system, scope_key, entity_type,
                        external_id, payload_hash, soft_fingerprint, created_at, updated_at
                    ) VALUES (
                        :id, :cash_event_id, 'qbo_csv', :scope_key, 'open_invoice',
                        :external_id, :payload_hash, :fingerprint, :now, :now
                    )
                    ON CONFLICT(source_system, scope_key, entity_type, external_id) DO NOTHING
                """), {
                    "id": str(uuid.uuid4()), "cash_event_id": event_id,
                    "scope_key": scope_key, "external_id": identity["external_id"],
                    "payload_hash": identity["payload_hash"],
                    "fingerprint": identity["soft_fingerprint"], "now": now,
                })

        result.inserted = ready_count if not blocked else 0
        result.status = "posted" if not blocked else ("failed" if result.invalid else "staged")
        conn.execute(text("""
            UPDATE finance_import_batches SET
                status=:status, ready_count=:ready, duplicate_count=:duplicates,
                review_count=:review, invalid_count=:invalid, posted_at=:now
            WHERE id=:id
        """), {
            "id": batch_id, "status": result.status, "ready": ready_count,
            "duplicates": result.duplicates, "review": result.review,
            "invalid": result.invalid, "now": now,
        })
        conn.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, evidence_json, created_at
            ) VALUES (
                :id, :scope_key, 'qbo_csv_import', 'finance_import_batch',
                :batch_id, 'operator', :evidence, :now
            )
        """), {
            "id": str(uuid.uuid4()), "scope_key": scope_key, "batch_id": batch_id,
            "evidence": _json({
                "file_hash": file_hash, "status": result.status,
                "inserted": result.inserted, "ready": ready_count,
                "duplicates": result.duplicates, "review": result.review,
                "invalid": result.invalid,
            }), "now": now,
        })
    return result
