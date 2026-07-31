"""Durable public lead intake for the marketing site's short note forms."""

from __future__ import annotations

import logging
import os
import re
from html import escape
from typing import Any, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.public_request_guard import durable_rate_limit_response


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/public/leads", tags=["leads-public"])

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9./:_-]{0,119}$", re.IGNORECASE)
_KINDS = {"contact", "partners", "careers"}


def _text(value: Any, limit: int) -> str:
    return str(value or "").strip()[:limit]


def _site_key(request: Request, provided: Optional[str]) -> Optional[JSONResponse]:
    configured = str(
        getattr(request.app.state.settings, "marketing_site_intake_key", "") or ""
    ).strip()
    if not configured:
        return JSONResponse(status_code=503, content={"detail": "Lead intake is not configured."})
    if str(provided or "").strip() != configured:
        return JSONResponse(status_code=401, content={"detail": "Invalid intake key."})
    return None


def _notify(settings, *, lead_id: int, payload: dict[str, str]) -> bool:
    client = ResendClient(settings)
    recipients = [
        value.strip().lower()
        for value in os.environ.get("MARKETING_LEAD_EMAIL_TO", "david@anatainc.com").split(",")
        if value.strip()
    ]
    if not recipients or not client.is_configured():
        return False
    client.send_message(
        to=recipients,
        subject=f"New {payload['kind']} note: {payload.get('company') or payload['name']}",
        text="\n".join(
            [
                "New Anata website note",
                "",
                f"Lead: {lead_id}",
                f"Form: {payload['kind']}",
                f"Name: {payload['name']}",
                f"Company: {payload.get('company') or 'Not provided'}",
                f"Email: {payload['email']}",
                f"Role: {payload.get('role') or 'Not provided'}",
                f"Source: {payload['source']}",
                "",
                payload["message"],
            ]
        ),
        reply_to=payload["email"],
        idempotency_key=f"website-note-{lead_id}",
    )
    return True


def _record_hubspot(settings, payload: dict[str, str]) -> bool:
    client = HubSpotClient(settings)
    if not client.is_configured:
        return False
    contact_id = ""
    properties = {"email": payload["email"], "firstname": payload["name"]}
    if payload.get("company"):
        properties["company"] = payload["company"]
    try:
        created = client.create_contact(properties)
        contact_id = str((created or {}).get("id", "") or "")
    except Exception as exc:  # duplicate contacts are expected
        match = re.search(r"Existing ID:\s*(\d+)", str(exc))
        if not match:
            logger.warning("[website_note] HubSpot contact failed: %s", exc)
            return False
        contact_id = match.group(1)
    if not contact_id:
        return False
    try:
        client.create_contact_note(
            contact_id=contact_id,
            body=(
                f"{escape(payload['kind'].title())} form note from anatainc.com."
                f"<br>Source: {escape(payload['source'])}"
                + (f"<br>Role: {escape(payload['role'])}" if payload.get("role") else "")
                + f"<br>Message: {escape(payload['message'])}"
            ),
        )
    except Exception as exc:
        logger.warning("[website_note] HubSpot note failed for %s: %s", contact_id, exc)
        return False
    return True


def deliver_website_note(
    settings,
    *,
    lead_id: int,
    payload: dict[str, str],
    previous: Optional[dict[str, Any]] = None,
) -> dict[str, bool]:
    """Retry only missing handoffs; Resend also deduplicates by lead id."""
    prior = previous or {}
    hubspot = prior.get("hubspot") is True
    notified = prior.get("notified") is True
    if not hubspot:
        try:
            hubspot = _record_hubspot(settings, payload)
        except Exception:
            logger.exception("[website_note] unexpected HubSpot failure for %s", lead_id)
    if not notified:
        try:
            notified = _notify(settings, lead_id=lead_id, payload=payload)
        except Exception:
            logger.exception("[website_note] notification failed for %s", lead_id)
    return {"hubspot": hubspot, "notified": notified}


@router.post("/contact")
async def contact_lead(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _site_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="lead:contact", limit=10)
    if limited is not None:
        return limited
    try:
        raw = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"detail": "Request body must be valid JSON."})
    if not isinstance(raw, dict):
        return JSONResponse(status_code=400, content={"detail": "Request body must be a JSON object."})

    payload = {
        "kind": _text(raw.get("kind"), 20),
        "name": _text(raw.get("name"), 160),
        "email": _text(raw.get("email"), 254).lower(),
        "company": _text(raw.get("company"), 160),
        "role": _text(raw.get("role"), 160),
        "message": _text(raw.get("message"), 2_000),
        "source": _text(raw.get("source"), 120),
    }
    if not _SOURCE_RE.fullmatch(payload["source"]):
        payload["source"] = "anatainc.com"
    if payload["kind"] not in _KINDS:
        return JSONResponse(status_code=400, content={"detail": "Invalid form kind."})
    if not payload["name"] or not _EMAIL_RE.fullmatch(payload["email"]) or not payload["message"]:
        return JSONResponse(status_code=400, content={"detail": "Name, valid email, and message are required."})

    with session_scope(request.app.state.session_factory) as session:
        run = AuditService(session).start_run(
            "website_contact_note",
            trigger="marketing_site",
            metadata=payload,
        )
        lead_id = int(run.id)

    delivery = deliver_website_note(
        request.app.state.settings,
        lead_id=lead_id,
        payload=payload,
    )

    with session_scope(request.app.state.session_factory) as session:
        from sales_support_agent.models.entities import AutomationRun

        run = session.get(AutomationRun, lead_id)
        if run is not None:
            AuditService(session).finish_run(
                run,
                status="success",
                summary={"accepted": True, **delivery},
            )
    return JSONResponse(status_code=201, content={"ok": True, "lead_id": lead_id})
