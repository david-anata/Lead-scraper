"""Persistence and connection state for Finance income-pattern decisions."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Mapping

from sqlalchemy import text


DEFAULT_SCOPE = "default"
INCOME_PATTERN_ACTION = "income_pattern_decision_recorded"
INCOME_PATTERN_ENTITY = "income_pattern"
VALID_INCOME_DECISIONS = frozenset({"track_expected", "one_time", "exclude"})
_PATTERN_KEY_RE = re.compile(r"^[0-9a-f]{16}$")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{16,128}$")
_MAX_EVIDENCE_BYTES = 16_384


def _validate_scope(scope: str) -> str:
    if not isinstance(scope, str) or not scope or scope != scope.strip() or len(scope) > 255:
        raise ValueError("scope must be a non-empty value of at most 255 characters")
    return scope


def _validate_pattern_key(pattern_key: str) -> str:
    if not isinstance(pattern_key, str) or not _PATTERN_KEY_RE.fullmatch(pattern_key):
        raise ValueError("pattern_key must be exactly 16 lowercase hexadecimal characters")
    return pattern_key


def _validate_decision(decision: str) -> str:
    if not isinstance(decision, str) or decision not in VALID_INCOME_DECISIONS:
        allowed = ", ".join(sorted(VALID_INCOME_DECISIONS))
        raise ValueError(f"decision must be one of: {allowed}")
    return decision


def _validate_actor(actor: str) -> str:
    if not isinstance(actor, str) or not actor.strip() or len(actor.strip()) > 255:
        raise ValueError("actor must be a non-empty value of at most 255 characters")
    return actor.strip()


def _request_identity(
    request_id: str | None,
    *,
    scope: str,
    pattern_key: str,
    decision: str,
    actor: str,
    canonical_evidence: str,
) -> str:
    """Return a stable idempotency key without adding mutable state."""
    if request_id is not None:
        if not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
            raise ValueError("request_id must be 16-128 URL-safe characters")
        return request_id
    return sha256(
        "\x1f".join((scope, pattern_key, decision, actor, canonical_evidence)).encode("utf-8")
    ).hexdigest()


def _normalize_evidence(evidence: Mapping[str, Any] | None) -> tuple[dict[str, Any], str]:
    if evidence is None:
        normalized: dict[str, Any] = {}
    elif isinstance(evidence, Mapping):
        try:
            encoded = json.dumps(
                dict(evidence),
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            normalized = json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("evidence must contain only JSON-compatible values") from exc
    else:
        raise ValueError("evidence must be a mapping")

    canonical = json.dumps(
        normalized,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    if len(canonical.encode("utf-8")) > _MAX_EVIDENCE_BYTES:
        raise ValueError("evidence exceeds the 16 KB audit limit")
    return normalized, canonical


def _decode_payload(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, Mapping):
        return dict(raw_payload)
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _serialize_created_at(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


def _decision_from_row(row: Any) -> dict[str, Any] | None:
    values = dict(row._mapping)
    pattern_key = str(values.get("entity_id") or "")
    payload = _decode_payload(values.get("evidence_json"))
    decision = payload.get("decision")
    if not _PATTERN_KEY_RE.fullmatch(pattern_key) or decision not in VALID_INCOME_DECISIONS:
        return None
    evidence = payload.get("evidence")
    return {
        "pattern_key": pattern_key,
        "decision": decision,
        "actor": str(values.get("actor") or ""),
        "evidence": dict(evidence) if isinstance(evidence, Mapping) else {},
        "scope": str(values.get("scope_key") or DEFAULT_SCOPE),
        "audit_id": str(values.get("id") or ""),
        "created_at": _serialize_created_at(values.get("created_at")),
    }


def load_income_pattern_decisions(scope: str = DEFAULT_SCOPE) -> dict[str, dict[str, Any]]:
    """Return the latest valid audit-backed decision for each income pattern."""
    scope = _validate_scope(scope)
    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = get_engine()
    ensure_finance_trust_schema(db_engine)
    with db_engine.connect() as connection:
        rows = connection.execute(
            text("""
                SELECT id, scope_key, entity_id, actor, evidence_json, created_at
                FROM finance_action_audit
                WHERE scope_key = :scope
                  AND action_type = :action_type
                  AND entity_type = :entity_type
                ORDER BY created_at DESC, id DESC
            """),
            {
                "scope": scope,
                "action_type": INCOME_PATTERN_ACTION,
                "entity_type": INCOME_PATTERN_ENTITY,
            },
        ).fetchall()

    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _decision_from_row(row)
        if item is not None and item["pattern_key"] not in latest:
            latest[item["pattern_key"]] = item
    return latest


def record_income_pattern_decision(
    pattern_key: str,
    decision: str,
    actor: str,
    evidence: Mapping[str, Any] | None = None,
    *,
    scope: str = DEFAULT_SCOPE,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Append a decision audit row, safely collapsing retries by request identity."""
    pattern_key = _validate_pattern_key(pattern_key)
    decision = _validate_decision(decision)
    actor = _validate_actor(actor)
    scope = _validate_scope(scope)
    normalized_evidence, canonical_evidence = _normalize_evidence(evidence)
    request_identity = _request_identity(
        request_id,
        scope=scope,
        pattern_key=pattern_key,
        decision=decision,
        actor=actor,
        canonical_evidence=canonical_evidence,
    )

    from sales_support_agent.models.database import ensure_finance_trust_schema, get_engine

    db_engine = get_engine()
    ensure_finance_trust_schema(db_engine)
    now = datetime.now(timezone.utc)
    audit_id = sha256(
        "\x1f".join(("income-pattern-decision-v1", scope, pattern_key, request_identity)).encode(
            "utf-8"
        )
    ).hexdigest()
    with db_engine.begin() as connection:
        payload = {
            "decision": decision,
            "evidence": normalized_evidence,
            "pattern_key": pattern_key,
            "request_id": request_identity,
        }
        result = connection.execute(
            text("""
                INSERT INTO finance_action_audit (
                    id, scope_key, action_type, entity_type, entity_id,
                    actor, evidence_json, created_at
                ) VALUES (
                    :id, :scope, :action_type, :entity_type, :pattern_key,
                    :actor, :evidence_json, :created_at
                )
                ON CONFLICT(id) DO NOTHING
            """),
            {
                "id": audit_id,
                "scope": scope,
                "action_type": INCOME_PATTERN_ACTION,
                "entity_type": INCOME_PATTERN_ENTITY,
                "pattern_key": pattern_key,
                "actor": actor,
                "evidence_json": json.dumps(payload, separators=(",", ":"), sort_keys=True),
                "created_at": now,
            },
        )
        if result.rowcount == 0:
            retry_row = connection.execute(
                text("""
                    SELECT id, scope_key, entity_id, actor, evidence_json, created_at
                    FROM finance_action_audit WHERE id = :id
                """),
                {"id": audit_id},
            ).one()
            retry_item = _decision_from_row(retry_row)
            retry_payload = _decode_payload(retry_row._mapping["evidence_json"])
            if (
                retry_item is None
                or retry_payload.get("request_id") != request_identity
                or retry_item["pattern_key"] != pattern_key
                or retry_item["decision"] != decision
                or retry_item["actor"] != actor
                or retry_item["evidence"] != normalized_evidence
            ):
                raise ValueError("request_id is already associated with a different decision")
            return {**retry_item, "created": False}

    return {
        "pattern_key": pattern_key,
        "decision": decision,
        "actor": actor,
        "evidence": normalized_evidence,
        "scope": scope,
        "audit_id": audit_id,
        "created_at": now.isoformat(),
        "created": True,
    }


def _configured_value(settings: Any, attribute: str, *env_names: str) -> tuple[str, str]:
    if settings is not None:
        value = str(getattr(settings, attribute, "") or "").strip()
        if value:
            return value, "settings"
    for env_name in env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return value, "environment"
    return "", ""


def _has_qbo_database_tokens() -> bool:
    """Check token presence without loading, decrypting, or returning secrets."""
    try:
        from sales_support_agent.models.database import get_engine

        with get_engine().connect() as connection:
            row = connection.execute(text("""
                SELECT 1
                FROM quickbooks_tokens
                WHERE id = 'singleton'
                  AND COALESCE(access_token, '') <> ''
                  AND COALESCE(realm_id, '') <> ''
                LIMIT 1
            """)).fetchone()
        return row is not None
    except Exception:
        return False


def load_finance_source_connections(settings: Any = None) -> dict[str, dict[str, Any]]:
    """Report local Finance source configuration without network calls or secrets."""
    clickup_token, clickup_token_source = _configured_value(
        settings, "clickup_api_token", "CLICKUP_API_TOKEN", "CLICKUP_API_KEY"
    )
    clickup_ap_list, ap_source = _configured_value(
        settings, "clickup_ap_list_id", "CLICKUP_AP_LIST_ID"
    )
    clickup_ar_list, ar_source = _configured_value(
        settings, "clickup_ar_list_id", "CLICKUP_AR_LIST_ID"
    )
    clickup_configured = bool(clickup_token and (clickup_ap_list or clickup_ar_list))
    clickup_sources = {source for source in (clickup_token_source, ap_source, ar_source) if source}

    qbo_database_connected = _has_qbo_database_tokens()
    qbo_values = [
        _configured_value(settings, "qbo_client_id", "QBO_CLIENT_ID")[0],
        _configured_value(settings, "qbo_client_secret", "QBO_CLIENT_SECRET")[0],
        _configured_value(settings, "qbo_refresh_token", "QBO_REFRESH_TOKEN")[0],
        _configured_value(settings, "qbo_realm_id", "QBO_REALM_ID")[0],
    ]
    qbo_env_connected = all(qbo_values)
    qbo_connected = qbo_database_connected or qbo_env_connected

    plaid_summary: dict[str, Any] = {}
    if settings is not None:
        try:
            from sales_support_agent.services.cashflow.plaid import connection_summary

            plaid_summary = connection_summary(settings=settings)
        except Exception:
            plaid_summary = {}
    plaid_connected = int(plaid_summary.get("connected_count") or 0) > 0

    return {
        "plaid": {
            "connected": plaid_connected,
            "status": "connected" if plaid_connected else "not_connected",
            "configured": bool(plaid_summary.get("configured")),
            "account_count": int(plaid_summary.get("account_count") or 0),
        },
        "clickup": {
            "configured": clickup_configured,
            "status": "configured" if clickup_configured else "not_configured",
            "configuration_source": "+".join(sorted(clickup_sources)) or None,
        },
        "qbo": {
            "connected": qbo_connected,
            "status": "connected" if qbo_connected else "not_connected",
            "connection_source": (
                "database" if qbo_database_connected else "environment" if qbo_env_connected else None
            ),
        },
    }


__all__ = [
    "load_finance_source_connections",
    "load_income_pattern_decisions",
    "record_income_pattern_decision",
]
