"""Guarded LLM-assisted Finance intake.

The model can prepare a draft commitment only.  Saving remains a separate,
authenticated confirmation backed by a server-side preview.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping
from uuid import uuid4

import requests

from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.services.cashflow.commitments import validate_commitment_fields
from sales_support_agent.services.cashflow.obligations import create_obligation


class FinanceAssistantError(RuntimeError):
    pass


def _request_openai(*, settings: Any, prompt: str) -> dict[str, Any]:
    if not settings.openai_api_key:
        raise FinanceAssistantError("Finance assistant needs OPENAI_API_KEY")
    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {settings.openai_api_key}", "Content-Type": "application/json"},
        json={
            "model": settings.openai_model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": (
                    "Convert the user's request into one draft finance commitment. "
                    "Return JSON only with name, event_type (inflow or outflow), "
                    "commitment_type, category, vendor_or_customer, amount_cents, "
                    "due_date (YYYY-MM-DD or null), priority, owner, notes, and missing_fields. "
                    "Never claim money moved, never mark paid, and never invent an amount or date."
                )},
                {"role": "user", "content": prompt[:4000]},
            ],
        },
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise FinanceAssistantError("Finance assistant returned an unreadable response") from exc
    if response.status_code >= 400:
        raise FinanceAssistantError("Finance assistant request failed")
    try:
        return json.loads(payload["choices"][0]["message"]["content"])
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise FinanceAssistantError("Finance assistant returned an invalid draft") from exc


def create_preview(
    prompt: str,
    *,
    actor: str,
    settings: Any,
    requester: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    clean_prompt = str(prompt or "").strip()
    if not clean_prompt:
        raise ValueError("Tell Finance what you want to add")
    raw = dict((requester or _request_openai)(settings=settings, prompt=clean_prompt))
    missing = [str(item) for item in (raw.get("missing_fields") or []) if str(item).strip()]
    due_value = raw.get("due_date")
    due_date = None
    if due_value:
        try:
            due_date = date.fromisoformat(str(due_value)[:10])
        except ValueError:
            missing.append("due_date")
    try:
        amount_cents = int(raw.get("amount_cents")) if raw.get("amount_cents") is not None else 0
    except (TypeError, ValueError):
        amount_cents = 0
        missing.append("amount")
    if amount_cents <= 0 and "amount" not in missing:
        missing.append("amount")
    if due_date is None and "due_date" not in missing:
        missing.append("due_date")
    event_type = str(raw.get("event_type") or "").lower()
    if event_type not in {"inflow", "outflow"}:
        missing.append("direction")
        event_type = "outflow"
    fields = validate_commitment_fields({
        "commitment_type": raw.get("commitment_type") or "general",
        "workflow_status": "draft",
        "approval_status": "not_required",
        "owner": raw.get("owner") or "",
        "created_by": actor,
        "amount_cents": amount_cents,
    })
    fields.update({
        "name": str(raw.get("name") or "Draft commitment")[:255],
        "event_type": event_type,
        "category": str(raw.get("category") or "uncategorized")[:64],
        "vendor_or_customer": str(raw.get("vendor_or_customer") or "")[:255],
        "due_date": due_date.isoformat() if due_date else None,
        "pay_priority": str(raw.get("priority") or "review")[:16],
        "notes": str(raw.get("notes") or "")[:4000],
    })
    preview_id = str(uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)
    stored = {
        "preview_id": preview_id, "actor": actor, "fields": fields,
        "missing_fields": sorted(set(missing)), "expires_at": expires_at.isoformat(),
        "confirmed_commitment_id": "",
    }
    kv_set_json(f"finance_assistant_preview:{preview_id}", stored)
    return {
        **stored,
        "warning": "Review before saving. This creates a commitment only and never moves bank money.",
    }


def confirm_preview(preview_id: str, *, actor: str) -> dict[str, Any]:
    key = f"finance_assistant_preview:{preview_id}"
    stored = kv_get_json(key)
    if not stored:
        raise ValueError("Finance preview was not found")
    if str(stored.get("actor") or "") != actor:
        raise PermissionError("Finance preview belongs to another user")
    if stored.get("confirmed_commitment_id"):
        from sales_support_agent.services.cashflow.obligations import get_obligation
        return get_obligation(str(stored["confirmed_commitment_id"])) or {}
    expires_at = datetime.fromisoformat(str(stored["expires_at"]))
    if datetime.now(timezone.utc) >= expires_at:
        raise ValueError("Finance preview expired; create a fresh preview")
    if stored.get("missing_fields"):
        raise ValueError("Complete the missing fields before saving")
    fields = validate_commitment_fields(dict(stored["fields"]))
    due_date = date.fromisoformat(str(fields.pop("due_date"))[:10])
    pay_priority = fields.pop("pay_priority", "review")
    # create_obligation keeps financial amount/date validation and source audit
    # separate from the model response.
    commitment = create_obligation(due_date=due_date, **fields)
    if pay_priority:
        from sales_support_agent.services.cashflow.obligations import update_obligation
        commitment = update_obligation(commitment["id"], pay_priority=pay_priority) or commitment
    stored["confirmed_commitment_id"] = commitment["id"]
    kv_set_json(key, stored)
    return commitment
