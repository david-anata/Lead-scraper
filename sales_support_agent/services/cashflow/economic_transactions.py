"""Group mirrored bank feeds without inventing or deleting transactions.

Only exact cross-source identities are auto-excluded. Same-source repeats stay
independent because two equal charges can both be real. Less certain cross-feed
pairs are sent to Review and never silently removed from spending.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
import re
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.models.database import (
    _ensure_finance_settlement_tables,
    get_engine,
)

SOURCE_PRIORITY = {"plaid": 0, "qbo_bank": 1, "csv": 2}


def _day(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]


def _description(row: dict[str, Any]) -> str:
    value = row.get("description") or row.get("name") or row.get("vendor_or_customer")
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _fingerprint(parts: tuple[Any, ...]) -> str:
    return hashlib.sha256("|".join(str(value) for value in parts).encode()).hexdigest()


def _load_rows() -> list[dict[str, Any]]:
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT id, source, event_type, amount_cents,
                   COALESCE(effective_date, due_date) AS paid_on,
                   name, vendor_or_customer, description,
                   match_status, source_status
            FROM cash_events
            WHERE record_kind='transaction'
              AND source IN ('plaid','qbo_bank','csv')
              AND status IN ('posted','matched')
              AND COALESCE(amount_cents,0) > 0
              AND archived_at IS NULL
        """)).fetchall()
    return [dict(row._mapping) for row in rows]


def plan_cross_feed_groups() -> dict[str, Any]:
    """Return deterministic exact groups and ambiguous candidates, read-only."""
    exact: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    loose: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in _load_rows():
        day = _day(row.get("paid_on"))
        if not day:
            continue
        common = (str(row.get("event_type") or ""), int(row.get("amount_cents") or 0), day)
        exact[(*common, _description(row))].append(row)
        loose[common].append(row)

    exact_groups = [
        values for values in exact.values()
        if len({str(item["source"]) for item in values}) > 1
    ]
    exact_ids = {str(row["id"]) for group in exact_groups for row in group}
    review_groups = [
        [row for row in values if str(row["id"]) not in exact_ids]
        for values in loose.values()
    ]
    review_groups = [
        values for values in review_groups
        if len(values) > 1 and len({str(item["source"]) for item in values}) > 1
    ]
    return {
        "exact": exact_groups,
        "review": review_groups,
        "exact_group_count": len(exact_groups),
        "review_group_count": len(review_groups),
    }


def reconcile_cross_feed_transactions(
    *, dry_run: bool = True, actor: str = "system",
) -> dict[str, Any]:
    """Apply exact groups or preview the full deterministic backfill."""
    engine = get_engine()
    _ensure_finance_settlement_tables(engine)
    plan = plan_cross_feed_groups()
    result = {
        "dry_run": dry_run,
        "exact_group_count": plan["exact_group_count"],
        "review_group_count": plan["review_group_count"],
        "duplicates_excluded": sum(len(group) - 1 for group in plan["exact"]),
        "groups_written": 0,
    }
    if dry_run:
        return result

    now = datetime.now(timezone.utc)
    for status, groups in (("exact", plan["exact"]), ("review", plan["review"])):
        for rows in groups:
            ordered = sorted(
                rows,
                key=lambda row: (SOURCE_PRIORITY.get(str(row["source"]), 99), str(row["id"])),
            )
            canonical_id = str(ordered[0]["id"]) if status == "exact" else ""
            signature = (
                status,
                str(ordered[0].get("event_type") or ""),
                int(ordered[0].get("amount_cents") or 0),
                _day(ordered[0].get("paid_on")),
                _description(ordered[0]) if status == "exact" else "",
                *sorted(str(row["id"]) for row in ordered),
            )
            fingerprint = _fingerprint(signature)
            group_id = f"economic-{fingerprint[:48]}"
            with engine.begin() as connection:
                exists = connection.execute(text("""
                    SELECT status FROM finance_economic_transaction_groups
                    WHERE fingerprint=:fingerprint
                """), {"fingerprint": fingerprint}).fetchone()
                if exists and str(exists[0]) != "undone":
                    continue
                connection.execute(text("""
                    INSERT INTO finance_economic_transaction_groups (
                        id, scope_key, fingerprint, status, canonical_event_id,
                        evidence_json, created_at, updated_at, undone_at
                    ) VALUES (
                        :id, 'default', :fingerprint, :status, :canonical,
                        :evidence, :now, :now, NULL
                    )
                    ON CONFLICT (fingerprint) DO UPDATE SET
                        status=:status, canonical_event_id=:canonical,
                        evidence_json=:evidence, updated_at=:now, undone_at=NULL
                """), {
                    "id": group_id, "fingerprint": fingerprint, "status": status,
                    "canonical": canonical_id,
                    "evidence": json.dumps({"event_ids": [str(row["id"]) for row in ordered]}),
                    "now": now,
                })
                for position, row in enumerate(ordered):
                    event_id = str(row["id"])
                    role = "canonical" if event_id == canonical_id else (
                        "duplicate" if status == "exact" else "candidate"
                    )
                    connection.execute(text("""
                        INSERT INTO finance_economic_transaction_members (
                            id, group_id, event_id, source, role,
                            prior_match_status, prior_source_status, created_at
                        ) VALUES (
                            :id, :group_id, :event_id, :source, :role,
                            :prior_match, :prior_source, :now
                        ) ON CONFLICT (group_id, event_id) DO NOTHING
                    """), {
                        "id": str(uuid4()), "group_id": group_id, "event_id": event_id,
                        "source": str(row["source"]), "role": role,
                        "prior_match": str(row.get("match_status") or ""),
                        "prior_source": str(row.get("source_status") or ""), "now": now,
                    })
                    if role == "duplicate":
                        connection.execute(text("""
                            UPDATE cash_events SET match_status='duplicate',
                                source_status='probable_duplicate', updated_at=:now
                            WHERE id=:id
                        """), {"id": event_id, "now": now})
                    elif role == "candidate":
                        connection.execute(text("""
                            UPDATE cash_events SET match_status='review',
                                match_candidates_json=:candidates, updated_at=:now
                            WHERE id=:id
                        """), {
                            "id": event_id, "now": now,
                            "candidates": json.dumps([str(item["id"]) for item in ordered]),
                        })
                connection.execute(text("""
                    INSERT INTO finance_action_audit (
                        id, scope_key, action_type, entity_type, entity_id,
                        actor, idempotency_key, evidence_json, created_at
                    ) VALUES (
                        :id, 'default', 'cross_feed_grouped', 'economic_transaction_group',
                        :group_id, :actor, :idempotency, :evidence, :now
                    ) ON CONFLICT (idempotency_key) DO NOTHING
                """), {
                    "id": str(uuid4()), "group_id": group_id, "actor": actor,
                    "idempotency": f"cross-feed:{fingerprint}",
                    "evidence": json.dumps({"status": status, "event_ids": [str(row["id"]) for row in ordered]}),
                    "now": now,
                })
            result["groups_written"] += 1
    return result


def undo_cross_feed_group(group_id: str, *, actor: str = "system") -> dict[str, int]:
    """Restore every member's exact prior classification; never delete it."""
    engine = get_engine()
    _ensure_finance_settlement_tables(engine)
    now = datetime.now(timezone.utc)
    restored = 0
    with engine.begin() as connection:
        group = connection.execute(text("""
            SELECT id, status FROM finance_economic_transaction_groups WHERE id=:id
        """), {"id": group_id}).fetchone()
        if not group or str(group.status) == "undone":
            return {"restored": 0}
        members = connection.execute(text("""
            SELECT event_id, prior_match_status, prior_source_status
            FROM finance_economic_transaction_members WHERE group_id=:id
        """), {"id": group_id}).fetchall()
        for member in members:
            connection.execute(text("""
                UPDATE cash_events SET match_status=:match_status,
                    source_status=:source_status, match_candidates_json='[]',
                    updated_at=:now WHERE id=:event_id
            """), {
                "event_id": member.event_id, "match_status": member.prior_match_status,
                "source_status": member.prior_source_status, "now": now,
            })
            restored += 1
        connection.execute(text("""
            UPDATE finance_economic_transaction_groups
            SET status='undone', undone_at=:now, updated_at=:now WHERE id=:id
        """), {"id": group_id, "now": now})
        connection.execute(text("""
            INSERT INTO finance_action_audit (
                id, scope_key, action_type, entity_type, entity_id,
                actor, idempotency_key, evidence_json, created_at
            ) VALUES (
                :audit_id, 'default', 'cross_feed_group_undone',
                'economic_transaction_group', :group_id, :actor,
                :idempotency, :evidence, :now
            ) ON CONFLICT (idempotency_key) DO NOTHING
        """), {
            "audit_id": str(uuid4()), "group_id": group_id, "actor": actor,
            "idempotency": f"cross-feed-undo:{group_id}",
            "evidence": json.dumps({"restored": restored}), "now": now,
        })
    return {"restored": restored}


__all__ = [
    "plan_cross_feed_groups", "reconcile_cross_feed_transactions",
    "undo_cross_feed_group",
]
