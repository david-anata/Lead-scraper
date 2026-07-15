"""Evidence-bound Smart CFO analysis for Finance Control.

The model is an interpreter, not a ledger.  Every canonical event contributes
to the deterministic packet and the model may only recommend actions supported
by packet evidence.  It never writes cash events or changes a forecast.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.obligations import list_obligations


PROMPT_VERSION = "smart-cfo-v1"
_CACHE_KEY = "finance_smart_cfo_analysis"
_MAX_RECOMMENDATIONS = 5


def build_ledger_packet(rows: Iterable[Mapping[str, Any]], *, as_of: date | None = None) -> dict[str, Any]:
    """Build a stable, complete merchant rollup from every canonical event."""
    today = as_of or date.today()
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    totals = defaultdict(int)
    record_count = 0
    for raw in rows:
        row = dict(raw)
        record_count += 1
        event_type = str(row.get("event_type") or "unknown").lower()
        status = str(row.get("status") or "unknown").lower()
        source = str(row.get("source") or "unknown").lower()
        merchant = str(row.get("vendor_or_customer") or row.get("name") or row.get("description") or "Unassigned").strip()[:120]
        category = str(row.get("category") or "uncategorized").strip()[:64]
        due = _event_date(row)
        amount = int(row.get("amount_cents") or 0)
        key = (merchant.casefold(), category.casefold(), event_type, status)
        group = groups.setdefault(key, {
            "merchant": merchant, "category": category, "event_type": event_type,
            "status": status, "count": 0, "amount_cents": 0, "first_date": "", "last_date": "",
            "sources": set(), "record_ids": [],
        })
        group["count"] += 1
        group["amount_cents"] += amount
        group["sources"].add(source)
        if row.get("id"):
            group["record_ids"].append(str(row["id"])[:80])
        if due:
            iso = due.isoformat()
            group["first_date"] = min(group["first_date"] or iso, iso)
            group["last_date"] = max(group["last_date"] or iso, iso)
        totals[f"{event_type}:{status}"] += amount

    rollups = []
    for group in groups.values():
        group["sources"] = sorted(group["sources"])
        group["record_ids"] = sorted(set(group["record_ids"]))
        rollups.append(group)
    rollups.sort(key=lambda item: (item["event_type"], -abs(item["amount_cents"]), item["merchant"].casefold()))
    return {
        "packet_version": PROMPT_VERSION,
        "as_of": today.isoformat(),
        "record_count": record_count,
        "rollup_count": len(rollups),
        "totals_cents": dict(sorted(totals.items())),
        "merchant_rollups": rollups,
    }


def run_smart_cfo(settings: Any, *, force: bool = False) -> dict[str, Any]:
    """Run or reuse a structured Anthropic analysis for the full persisted ledger."""
    rows = list_obligations(limit=10_000)
    packet = build_ledger_packet(rows)
    packet_hash = _packet_hash(packet)
    cached = kv_get_json(_CACHE_KEY) or {}
    if not force and cached.get("packet_hash") == packet_hash and cached.get("prompt_version") == PROMPT_VERSION:
        return {**cached, "cached": True}
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {
            "status": "not_configured", "packet_hash": packet_hash, "prompt_version": PROMPT_VERSION,
            "record_count": packet["record_count"], "recommendations": [], "cached": False,
        }

    model = os.getenv("FINANCE_SMART_CFO_MODEL", "claude-sonnet-4-20250514").strip() or "claude-sonnet-4-20250514"
    result = _call_anthropic(api_key, model, packet)
    analysis = _validate_analysis(result, packet)
    stored = {
        "status": "ready", "packet_hash": packet_hash, "prompt_version": PROMPT_VERSION,
        "record_count": packet["record_count"], "created_at": datetime.utcnow().isoformat(),
        "recommendations": analysis["recommendations"], "summary": analysis["summary"], "cached": False,
    }
    kv_set_json(_CACHE_KEY, stored)
    return stored


def _call_anthropic(api_key: str, model: str, packet: Mapping[str, Any]) -> Mapping[str, Any]:
    """Use Anthropic's installed SDK; JSON parsing is validated before display."""
    import anthropic

    message = anthropic.Anthropic(api_key=api_key).messages.create(
        model=model,
        max_tokens=1600,
        system=_instructions() + " Return JSON only, matching this schema: " + json.dumps(_schema(), separators=(",", ":")),
        messages=[{"role": "user", "content": json.dumps(packet, separators=(",", ":"))}],
    )
    text = message.content[0].text if message.content else ""
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise ValueError("Smart CFO returned an invalid analysis")
    return value


def load_smart_cfo_analysis() -> dict[str, Any]:
    return kv_get_json(_CACHE_KEY) or {"status": "empty", "recommendations": []}


def _event_date(row: Mapping[str, Any]) -> date | None:
    value = row.get("effective_date") or row.get("due_date") or row.get("updated_at")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _packet_hash(packet: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(packet, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _instructions() -> str:
    return """You are the Smart CFO for an operator, not a bookkeeper. Analyze every supplied ledger rollup.
Return concise decisions for savings, collections, cash_risk, or data_quality. Do not invent a dollar amount,
merchant, date, status, or source record. Only use amounts and record_ids present in the packet. A recommendation
is advice only: do not say a payment was made, a bill is resolved, or cash changed. Prefer no recommendation over
weak evidence. Each item needs a practical next_action and a short operator_question when a human fact is required."""


def _schema() -> dict[str, Any]:
    item = {
        "type": "object", "additionalProperties": False,
        "properties": {
            "category": {"type": "string", "enum": ["savings", "collections", "cash_risk", "data_quality"]},
            "priority": {"type": "string", "enum": ["high", "medium", "low"]},
            "title": {"type": "string"}, "reason": {"type": "string"}, "next_action": {"type": "string"},
            "operator_question": {"type": "string"}, "record_ids": {"type": "array", "items": {"type": "string"}},
        }, "required": ["category", "priority", "title", "reason", "next_action", "operator_question", "record_ids"],
    }
    return {"type": "object", "additionalProperties": False, "properties": {"summary": {"type": "string"}, "recommendations": {"type": "array", "items": item}}, "required": ["summary", "recommendations"]}


def _validate_analysis(value: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    allowed_ids = {record_id for rollup in packet["merchant_rollups"] for record_id in rollup["record_ids"]}
    results = []
    for item in list(value.get("recommendations") or [])[:_MAX_RECOMMENDATIONS]:
        if not isinstance(item, Mapping):
            continue
        ids = [str(record_id) for record_id in item.get("record_ids") or []]
        if not ids or not set(ids).issubset(allowed_ids):
            continue
        fields = {name: str(item.get(name) or "").strip() for name in ("category", "priority", "title", "reason", "next_action", "operator_question")}
        if not all(fields.values()) or fields["category"] not in {"savings", "collections", "cash_risk", "data_quality"}:
            continue
        results.append({**fields, "record_ids": ids})
    return {"summary": str(value.get("summary") or "Review the evidence-backed actions below.").strip()[:500], "recommendations": results}
