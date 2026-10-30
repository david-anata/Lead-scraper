"""Abby reply webhook and the job that registers it with Instantly.

``POST /api/webhooks/instantly/reply`` checks the derived header, accepts only
``reply_received`` and ``auto_reply_received``, and returns 200 before the
reply is handled. The five-minute poll remains the backup.

``/api/jobs/instantly/register-webhook`` is guarded like the other scheduler
jobs. It is also invoked from Abby's cron once the keys exist, so a deploy
registers the webhook without anyone copying a secret.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth
from sales_support_agent.services.instantly_abby_webhook import (
    WEBHOOK_HEADER,
    internal_api_key_from_env,
    process_reply,
    reply_state,
    webhook_authorized,
)

logger = logging.getLogger(__name__)

webhook_router = APIRouter(prefix="/api/webhooks/instantly", tags=["instantly-abby"])
job_router = APIRouter(prefix="/api/jobs/instantly", tags=["instantly-abby"])


def _api_key(request: Request) -> str:
    settings = getattr(request.app.state, "settings", None)
    configured = str(getattr(settings, "internal_api_key", "") or "").strip()
    return configured or internal_api_key_from_env()


def _settled(email_id: str) -> bool:
    try:
        from sales_support_agent.models.database import get_engine

        return reply_state(get_engine(), email_id) == "settled"
    except Exception:  # noqa: BLE001
        logger.exception("[abby] could not check abby_handled for %s", email_id)
        return False


def _run_reply(payload: dict[str, Any]) -> None:
    try:
        process_reply(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[abby] webhook reply failed")


@webhook_router.post("/reply")
async def instantly_reply_webhook(request: Request, background: BackgroundTasks) -> dict[str, Any]:
    """Accept an Instantly reply and handle it after the 200 is returned."""

    if not webhook_authorized(request.headers.get(WEBHOOK_HEADER, ""), _api_key(request)):
        raise HTTPException(status_code=401, detail="Invalid webhook secret.")
    try:
        payload = await request.json()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid JSON.") from exc
    if not isinstance(payload, dict):
        return {"status": "ignored", "reason": "payload"}
    event_type = str(payload.get("event_type") or "")
    from sales_support_agent.services.instantly_abby_webhook import ACCEPTED_EVENTS

    if event_type not in ACCEPTED_EVENTS:
        return {"status": "ignored", "reason": "event_type", "event_type": event_type}
    email_id = str(payload.get("email_id") or "").strip()
    if not email_id:
        return {"status": "ignored", "reason": "missing_email_id"}
    if _settled(email_id):
        return {"status": "duplicate", "email_id": email_id}
    background.add_task(_run_reply, payload)
    return {"status": "accepted", "email_id": email_id}


@job_router.api_route("/register-webhook", methods=["GET", "POST"])
def register_instantly_webhook(request: Request) -> dict[str, Any]:
    """Register Abby's reply webhooks. Idempotent: existing hooks are kept."""

    _require_scheduler_auth(request)
    from sales_support_agent.services.instantly_abby_webhook import maybe_register_abby_webhook

    result = maybe_register_abby_webhook()
    return {"status": "ok" if result.get("ok") or result.get("skipped") else "error", "registration": result}
