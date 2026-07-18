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
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.obligations import list_obligations


PROMPT_VERSION = "smart-cfo-v6"
_CACHE_KEY = "finance_smart_cfo_analysis"
_MAX_RECOMMENDATIONS = 5
_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_NON_CASH_TERMINAL_STATUSES = {"cancelled", "canceled", "void"}
_OPERATING_LOOKBACK_DAYS = 90

logger = logging.getLogger(__name__)


class SmartCfoProviderError(RuntimeError):
    """Raised when Anthropic cannot complete an advisory-only request."""


def build_ledger_packet(rows: Iterable[Mapping[str, Any]], *, as_of: date | None = None) -> dict[str, Any]:
    """Build a current operating-period rollup from finance events with cash meaning.

    Cancelled and void source rows remain in the system's audit trail, but are
    not cash events and must not become artificial CFO risk or savings advice.
    """
    today = as_of or date.today()
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    totals = defaultdict(int)
    source_record_count = 0
    record_count = 0
    excluded_terminal_count = 0
    excluded_out_of_scope_count = 0
    for raw in rows:
        row = dict(raw)
        source_record_count += 1
        event_type = str(row.get("event_type") or "unknown").lower()
        status = str(row.get("status") or "unknown").lower()
        if status in _NON_CASH_TERMINAL_STATUSES:
            excluded_terminal_count += 1
            continue
        if not _is_current_operating_record(row, as_of=today):
            excluded_out_of_scope_count += 1
            continue
        record_count += 1
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
        "operating_lookback_days": _OPERATING_LOOKBACK_DAYS,
        "source_record_count": source_record_count,
        "record_count": record_count,
        "rollup_count": len(rollups),
        "totals_cents": dict(sorted(totals.items())),
        "merchant_rollups": rollups,
        "excluded_terminal_count": excluded_terminal_count,
        "excluded_out_of_scope_count": excluded_out_of_scope_count,
    }


def _is_current_operating_record(row: Mapping[str, Any], *, as_of: date) -> bool:
    """Keep CFO advice tied to live cash decisions instead of lifetime history.

    Open obligations remain material regardless of age because they can still
    require collection or settlement. Posted bank activity is useful for
    operating analysis only inside the recent trend window.
    """
    status = str(row.get("status") or "").lower()
    if str(row.get("record_kind") or "").lower() == "transaction" or str(row.get("source") or "").lower() in {"csv", "qbo_bank"}:
        occurred = _event_date(row)
        return occurred is not None and occurred >= as_of - timedelta(days=_OPERATING_LOOKBACK_DAYS - 1)
    return status not in {"paid", "matched"}


def _load_settlement_annotations() -> list[dict[str, Any]]:
    """Read allocation evidence without making Smart CFO responsible for it."""
    try:
        from sqlalchemy import text
        from sales_support_agent.models.database import get_engine

        with get_engine().connect() as connection:
            return [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM settlement_allocations")
            ).fetchall()]
    except Exception:
        # Finance Control will mark the packet as verification-only when the
        # settlement ledger is unavailable. The model must not invent around it.
        return []


def _attach_payment_installments(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Give Smart CFO the same installment evidence used by the control page."""
    source_rows = [dict(row) for row in rows]
    try:
        from sqlalchemy import text
        from sales_support_agent.models.database import get_engine

        with get_engine().connect() as connection:
            installments = [dict(row._mapping) for row in connection.execute(
                text("SELECT * FROM payment_installments")
            ).fetchall()]
    except Exception:
        return source_rows
    by_obligation: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for installment in installments:
        by_obligation[str(installment.get("obligation_event_id") or "")].append(installment)
    return [
        {**row, "payment_installments": by_obligation.get(str(row.get("id") or ""), [])}
        for row in source_rows
    ]


def _aging_summary(
    rows: Iterable[Mapping[str, Any]], *, as_of: date, event_type: str
) -> dict[str, Any]:
    buckets = {"current": 0, "1_30": 0, "31_60": 0, "61_90": 0, "90_plus": 0}
    parties: dict[str, int] = defaultdict(int)
    total = 0
    for row in rows:
        if str(row.get("event_type") or "").lower() != event_type:
            continue
        if (
            str(row.get("record_kind") or "").lower() == "transaction"
            or str(row.get("source") or "").lower() in {"csv", "qbo_bank"}
            or str(row.get("status") or "").lower() in {"posted", "matched"}
        ):
            continue
        if row.get("historical_reconciliation_pending"):
            continue
        amount = max(0, int(row.get("open_amount_cents") or 0))
        if not amount:
            continue
        due = _event_date(row)
        days = max(0, (as_of - due).days) if due else 0
        bucket = "current" if days == 0 else "1_30" if days <= 30 else "31_60" if days <= 60 else "61_90" if days <= 90 else "90_plus"
        buckets[bucket] += amount
        party = str(row.get("vendor_or_customer") or row.get("name") or "Unassigned").strip()
        parties[party] += amount
        total += amount
    ranked = [
        {"party": party, "amount_cents": amount, "share_bps": amount * 10_000 // total if total else 0}
        for party, amount in sorted(parties.items(), key=lambda item: (-item[1], item[0].casefold()))[:5]
    ]
    return {"total_cents": total, "buckets_cents": buckets, "top_parties": ranked}


def build_finance_packet(
    rows: Iterable[Mapping[str, Any]],
    *,
    settlement_annotations: Iterable[Mapping[str, Any]] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Build the CFO packet from the same reconciled read model as the page."""
    from sales_support_agent.services.cashflow.control import (
        annotate_open_amounts,
        build_finance_control_state,
    )

    today = as_of or date.today()
    source_rows = [dict(row) for row in rows]
    allocations = list(settlement_annotations or [])
    state = build_finance_control_state(
        source_rows, allocations, as_of=today,
    )
    historical_ids = {
        str(item) for item in state["reconciliation_shadow"].get("forecast_excluded_ids") or []
    }
    canonical = annotate_open_amounts(source_rows, allocations)
    canonical = [
        {**row, "historical_reconciliation_pending": str(row.get("id") or "") in historical_ids}
        for row in canonical
    ]
    packet = build_ledger_packet(canonical, as_of=today)
    metrics = state["metrics"]
    forecast = state["forecast"]
    packet["analytical_summary"] = {
        "cash": {
            "cash_on_hand_cents": metrics.get("cash_on_hand_cents"),
            "safe_to_commit_cents": metrics.get("safe_to_commit_cents"),
            "funding_gap_cents": metrics.get("funding_gap_cents"),
            "floor_cents": metrics.get("floor_cents"),
        },
        "forecast": {
            "minimum_committed_cash_cents": forecast.get("minimum_committed_cash_cents"),
            "minimum_expected_cash_cents": forecast.get("minimum_expected_cash_cents"),
            "minimum_stress_cash_cents": forecast.get("minimum_stress_cash_cents"),
        },
        "receivables": _aging_summary(canonical, as_of=today, event_type="inflow"),
        "payables": _aging_summary(canonical, as_of=today, event_type="outflow"),
        "reconciliation": {
            "historical_excluded_count": len(historical_ids),
            "review_count": state["reconciliation_shadow"].get("supersession_review_count", 0),
        },
        "trust": {
            "ready": bool(state["trust_gate"].get("ready")),
            "issues": list(state["trust_gate"].get("issues") or []),
            "reasons": list(state["trust_gate"].get("reasons") or []),
            "next_action": state["trust_gate"].get("next_action"),
            "payable_issues": list(state["trust_gate"].get("payable_issues") or []),
        },
    }
    return packet


def run_smart_cfo(settings: Any, *, force: bool = False) -> dict[str, Any]:
    """Run or reuse a structured Anthropic analysis for the full persisted ledger."""
    rows = _attach_payment_installments(list_obligations(limit=10_000))
    packet = build_finance_packet(rows, settlement_annotations=_load_settlement_annotations())
    packet_hash = _packet_hash(packet)
    cached = kv_get_json(_CACHE_KEY) or {}
    if not force and cached.get("packet_hash") == packet_hash and cached.get("prompt_version") == PROMPT_VERSION:
        return {**cached, "cached": True}
    # Do not let the model narrate cash conclusions when the deterministic
    # control layer has already withheld trust. The operator needs exact
    # evidence resolution, not an AI interpretation of incomplete records.
    if not packet["analytical_summary"]["trust"]["ready"]:
        stored = _unready_trust_analysis(packet, packet_hash)
        kv_set_json(_CACHE_KEY, stored)
        return stored
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


def _unready_trust_analysis(packet: Mapping[str, Any], packet_hash: str) -> dict[str, Any]:
    """Return a deterministic hold state when Finance Control evidence is incomplete."""
    trust = packet["analytical_summary"]["trust"]
    reasons = [str(reason).strip() for reason in trust.get("reasons") or [] if str(reason).strip()]
    issues = [item for item in trust.get("payable_issues") or [] if isinstance(item, Mapping)]
    record_ids = sorted({str(item.get("id")) for item in issues if item.get("id")})
    reason = "; ".join(reasons) or "Finance source evidence is incomplete."
    return {
        "status": "ready",
        "packet_hash": packet_hash,
        "prompt_version": PROMPT_VERSION,
        "record_count": packet["record_count"],
        "created_at": datetime.utcnow().isoformat(),
        "summary": f"Cash recommendations are paused because {reason}. Resolve the listed source evidence before relying on Finance Control.",
        "recommendations": [{
            "category": "data_quality",
            "priority": "high",
            "title": "Resolve finance evidence before cash decisions",
            "reason": reason,
            "next_action": str(trust.get("next_action") or "Resolve the listed source evidence."),
            "operator_question": "Which listed obligations have posted bank or source evidence that can resolve the blocker?",
            "record_ids": record_ids,
        }],
        "cached": False,
    }


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
    return """You are the Smart CFO for an operator, not a bookkeeper. Analyze the supplied reconciled finance packet.
Use the analytical_summary for cash, aging, forecast, concentration, and trust context; use merchant_rollups only as
the evidence trail. Return concise decisions for savings, collections, cash_risk, or data_quality. Do not invent a
dollar amount, merchant, date, status, source record, payment, or saving. A recommendation is advice only: do not say
a bill was paid, cash changed, or an allocation is complete. When trust.ready is false, return only data_quality
recommendations. Prefer no recommendation over weak evidence. Each item needs a practical next_action and a short
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
    summary = str(value.get("summary") or "Review the evidence-backed actions below.").strip()[:500]
    if not results and packet["record_count"]:
        results = _fallback_recommendations(packet)
        summary = f"{summary} Model evidence references could not be verified, so Finance is showing a conservative ledger-backed review action instead."[:500]
    return {"summary": summary, "recommendations": results}


def _fallback_recommendations(packet: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Keep the operator moving when LLM prose cannot cite the ledger exactly."""
    rollups = list(packet["merchant_rollups"])
    cancelled = [item for item in rollups if str(item["status"]).lower() in {"cancelled", "canceled"}]
    overdue_outflows = [item for item in rollups if str(item["event_type"]).lower() == "outflow" and str(item["status"]).lower() in {"overdue", "open"}]
    candidates = cancelled or overdue_outflows or rollups
    candidates.sort(key=lambda item: (-abs(int(item["amount_cents"])), item["merchant"].casefold()))
    selected = candidates[:5]
    ids = sorted({record_id for item in selected for record_id in item["record_ids"]})
    if not ids:
        return []
    if cancelled:
        return [{
            "category": "data_quality", "priority": "high",
            "title": "Reconcile cancelled finance occurrences",
            "reason": f"{len(selected)} high-value cancelled rollup(s) still shape the ledger view and may be stale schedule history.",
            "next_action": "Review the source records and confirm which occurrences should remain excluded from future cash decisions.",
            "operator_question": "Are these cancelled occurrences historical only, or should the recurring plan be corrected?",
            "record_ids": ids,
        }]
    return [{
        "category": "cash_risk", "priority": "high",
        "title": "Verify the largest unresolved cash obligations",
        "reason": f"{len(selected)} high-value obligation rollup(s) need source evidence before committing cash.",
        "next_action": "Review the source record, posted bank evidence, and remaining unpaid balance for each occurrence.",
        "operator_question": "Which obligations are still open and require a payment plan this week?",
        "record_ids": ids,
    }]


def _llm_packet(packet: Mapping[str, Any]) -> dict[str, Any]:
    """Send complete rollup facts, with compact evidence refs instead of opaque record IDs."""
    rollup_fields = ("evidence_ref", "merchant", "category", "event_type", "status", "count", "amount_cents", "first_date", "last_date", "sources")
    return {
        **{key: value for key, value in packet.items() if key != "merchant_rollups"},
        "merchant_rollups": [{key: rollup[key] for key in rollup_fields} for rollup in packet["merchant_rollups"]],
        "evidence_reference_rule": "Use only evidence_refs from merchant_rollups. Do not use record IDs.",
    }
