"""Deterministic, source-local identities for Finance imports."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable


def _text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(_jsonable(payload), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def bank_base_fingerprint(row: dict[str, Any], *, scope_key: str) -> str:
    direction = str(row.get("event_type") or "outflow").lower()
    signed_cents = int(row.get("amount_cents") or 0) * (-1 if direction == "outflow" else 1)
    canonical = "|".join((
        _text(scope_key),
        _date(row.get("due_date") or row.get("effective_date")),
        str(signed_cents),
        _text(row.get("description") or row.get("name") or row.get("vendor_or_customer")),
        _text(row.get("bank_reference")),
    ))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def assign_bank_identities(
    rows: Iterable[dict[str, Any]],
    *,
    scope_key: str = "default",
) -> list[dict[str, Any]]:
    """Attach stable external IDs while preserving identical-row multiplicity.

    For blank provider IDs, occurrence ordinals model the input as a multiset:
    reuploading N identical rows produces the same N identities, while an
    additional identical transaction receives ordinal N+1.
    """
    counts: dict[str, int] = defaultdict(int)
    identified: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        provider_id = str(item.get("source_id") or "").strip()
        base = bank_base_fingerprint(item, scope_key=scope_key)
        if provider_id:
            external_id = provider_id
            identity_kind = "provider"
            ordinal = None
        else:
            counts[base] += 1
            ordinal = counts[base]
            external_id = f"fp:{base}:{ordinal}"
            identity_kind = "fingerprint"
        item["source_id"] = external_id
        item["_identity"] = {
            "source_system": "csv",
            "scope_key": scope_key,
            "entity_type": "bank_transaction",
            "external_id": external_id,
            "soft_fingerprint": base,
            "payload_hash": payload_hash(item),
            "identity_kind": identity_kind,
            "occurrence_ordinal": ordinal,
        }
        identified.append(item)
    return identified


def assign_qbo_invoice_identities(
    rows: Iterable[dict[str, Any]],
    *,
    scope_key: str = "default",
) -> list[dict[str, Any]]:
    """Attach stable source identities to QBO Open Invoices CSV rows.

    QBO report normalization stamps the observation time at parse time. That
    timestamp is evidence metadata, not business payload, so it is excluded
    from the hash used to decide whether a re-upload is identical.
    """
    identified: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        external_id = str(item.get("source_id") or "").strip()
        if not external_id:
            raise ValueError("QBO invoice source ID is required")
        canonical_payload = {
            key: value
            for key, value in item.items()
            if key not in {"_identity", "source_updated_at"}
        }
        fingerprint = hashlib.sha256(
            "|".join((
                _text(scope_key),
                _text(external_id),
                _text(item.get("vendor_or_customer") or item.get("name")),
                _date(item.get("due_date")),
            )).encode("utf-8")
        ).hexdigest()
        item["_identity"] = {
            "source_system": "qbo_csv",
            "scope_key": scope_key,
            "entity_type": "open_invoice",
            "external_id": external_id,
            "soft_fingerprint": fingerprint,
            "payload_hash": payload_hash(canonical_payload),
            "identity_kind": "provider",
            "occurrence_ordinal": None,
        }
        identified.append(item)
    return identified


def source_record_key(identity: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(identity["source_system"]),
        str(identity["scope_key"]),
        str(identity["entity_type"]),
        str(identity["external_id"]),
    )
