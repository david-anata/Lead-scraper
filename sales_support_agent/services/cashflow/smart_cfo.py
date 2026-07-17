"""Evidence-bound Smart CFO analysis for Finance Control.

The model is an interpreter, not a ledger.  Every canonical event contributes
to the deterministic packet and the model may only recommend actions supported
by packet evidence.  It never writes cash events or changes a forecast.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.obligations import list_obligations


PROMPT_VERSION = "smart-cfo-v3"
_CACHE_KEY = "finance_smart_cfo_analysis"
_MAX_RECOMMENDATIONS = 5
_DEFAULT_MODEL = "claude-haiku-4-5-20251001"

logger = logging.getLogger(__name__)


class SmartCfoProviderError(RuntimeError):
    """Raised when Anthropic cannot complete an advisory-only request."""


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
    for index, group in enumerate(rollups, start=1):
        # Compact refs make evidence citation reliable without exposing a long ID list to the model.
        group["evidence_ref"] = f"r{index}"
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

    model = os.getenv("FINANCE_SMART_CFO_MODEL", _DEFAULT_MODEL).strip() or _DEFAULT_MODEL
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

    try:
        message = anthropic.Anthropic(api_key=api_key).messages.create(
            model=model,
            max_tokens=1600,
            system=_instructions(),
            messages=[{"role": "user", "content": json.dumps(_llm_packet(packet), separators=(",", ":"))}],
            tools=[{
                "name": "submit_finance_advice",
                "description": "Submit evidence-bound Smart CFO advice for the supplied finance ledger.",
                "input_schema": _schema(),
            }],
            tool_choice={"type": "tool", "name": "submit_finance_advice"},
        )
    except Exception as exc:
        logger.warning("Smart CFO Anthropic request failed for model %s: %s", model, type(exc).__name__)
        raise SmartCfoProviderError("Anthropic Smart CFO request failed") from exc
    value = _extract_response_value(message)
    if not isinstance(value, Mapping):
        raise ValueError("Smart CFO returned an invalid analysis")
    return value


def _extract_response_value(message: Any) -> Mapping[str, Any]:
    """Prefer Anthropic tool-use input; accept text as a provider compatibility fallback."""
    for block in list(getattr(message, "content", None) or []):
        if getattr(block, "type", "") == "tool_use" and getattr(block, "name", "") == "submit_finance_advice":
            value = getattr(block, "input", None)
            if isinstance(value, Mapping):
                return value
            raise ValueError("Smart CFO tool response had invalid input")
    text_blocks = [str(getattr(block, "text", "") or "") for block in list(getattr(message, "content", None) or [])]
    return _parse_response_json("\n".join(part for part in text_blocks if part))


def _parse_response_json(text: str) -> Mapping[str, Any]:
    """Accept the JSON-only contract even when a provider wraps it in fences."""
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            raise
        value = json.loads(text[start:end + 1])
    if not isinstance(value, Mapping):
        raise ValueError("Smart CFO returned a non-object response")
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
merchant, date, status, or source record. Only use amounts and evidence_refs present in the packet. A recommendation
is advice only: do not say a payment was made, a bill is resolved, or cash changed. Prefer no recommendation over
weak evidence. When the packet has records, return 1 to 5 recommendations: choose a data_quality action if the
evidence is too weak for a cash or savings decision. Each item needs a practical next_action and a short
operator_question when a human fact is required. Cite only the compact evidence_refs supplied with each rollup."""


def _schema() -> dict[str, Any]:
    item = {
        "type": "object", "additionalProperties": False,
        "properties": {
            "category": {"type": "string", "enum": ["savings", "collections", "cash_risk", "data_quality"]},
            "priority": {"type": "string", "enum": ["high", "medium", "low"]},
            "title": {"type": "string"}, "reason": {"type": "string"}, "next_action": {"type": "string"},
            "operator_question": {"type": "string"}, "evidence_refs": {"type": "array", "minItems": 1, "items": {"type": "string"}},
        }, "required": ["category", "priority", "title", "reason", "next_action", "operator_question", "evidence_refs"],
    }
    return {"type": "object", "additionalProperties": False, "properties": {"summary": {"type": "string"}, "recommendations": {"type": "array", "minItems": 1, "items": item}}, "required": ["summary", "recommendations"]}


def _validate_analysis(value: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, Any]:
    evidence = {str(rollup["evidence_ref"]): rollup["record_ids"] for rollup in packet["merchant_rollups"]}
    results = []
    for item in list(value.get("recommendations") or [])[:_MAX_RECOMMENDATIONS]:
        if not isinstance(item, Mapping):
            continue
        refs = [str(ref) for ref in item.get("evidence_refs") or []]
        if not refs or not set(refs).issubset(evidence):
            continue
        ids = sorted({record_id for ref in refs for record_id in evidence[ref]})
        fields = {name: str(item.get(name) or "").strip() for name in ("category", "priority", "title", "reason", "next_action", "operator_question")}
        if not all(fields.values()) or fields["category"] not in {"savings", "collections", "cash_risk", "data_quality"}:
            continue
        results.append({**fields, "record_ids": ids})
    return {"summary": str(value.get("summary") or "Review the evidence-backed actions below.").strip()[:500], "recommendations": results}


def _llm_packet(packet: Mapping[str, Any]) -> dict[str, Any]:
    """Send complete rollup facts, with compact evidence refs instead of opaque record IDs."""
    rollup_fields = ("evidence_ref", "merchant", "category", "event_type", "status", "count", "amount_cents", "first_date", "last_date", "sources")
    return {
        **{key: value for key, value in packet.items() if key != "merchant_rollups"},
        "merchant_rollups": [{key: rollup[key] for key in rollup_fields} for rollup in packet["merchant_rollups"]],
        "evidence_reference_rule": "Use only evidence_refs from merchant_rollups. Do not use record IDs.",
    }
