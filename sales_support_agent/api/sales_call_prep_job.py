"""Vercel Cron, every 15 minutes: prep booked sales calls.

Guarded the same way as Abby: GET or POST with `Authorization: Bearer $CRON_SECRET`
or `X-Internal-Api-Key`. A lease keeps two overlapping ticks from posting twice.
The work itself also dedupes on the calendar event id.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth
from sales_support_agent.models.database import session_scope
from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job
from sales_support_agent.services.sales.call_script.bookings import load_bookings
from sales_support_agent.services.sales.call_script.prep import gather_context, generate_notes, run_prep

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/sales-call-prep", tags=["sales-call-prep"])


def _gmail_clients(settings) -> list:
    from sales_support_agent.integrations.gmail import GmailClient

    clients = []
    primary = GmailClient(settings)
    if primary.is_configured():
        clients.append(primary)
    seen = {getattr(primary, "user_id", "") or "primary"}
    for account in getattr(settings, "gmail_mailbox_accounts", ()) or ():
        client = GmailClient(settings, account)
        key = getattr(client, "user_id", "") or getattr(client, "account_key", "")
        if not key or key in seen or not client.is_configured():
            continue
        seen.add(key)
        clients.append(client)
    return clients


def _clickup(settings):
    if not str(getattr(settings, "clickup_api_token", "") or "").strip():
        return None
    from sales_support_agent.integrations.clickup import ClickUpClient

    return ClickUpClient(settings)


def _slack(settings):
    from sales_support_agent.integrations.slack import SlackClient

    return SlackClient(settings)


def execute(request: Request, now: datetime) -> dict:
    """Read bookings and store prep. Does not send email."""

    settings = request.app.state.settings
    gmail_clients = _gmail_clients(settings)
    loaded = load_bookings(settings, now, gmail_clients=gmail_clients)
    with session_scope(request.app.state.session_factory) as session:
        return run_prep(
            session,
            now=now,
            bookings=loaded.bookings,
            source=loaded.source,
            gap=loaded.gap,
            attempts=loaded.attempts,
            context_for=lambda booking, tracker=None: gather_context(
                session, _clickup(settings), booking, gmail=gmail_clients[0] if gmail_clients else None, tracker=tracker,
            ),
            notes_for=generate_notes,
            slack=_slack(settings),
            gmail=gmail_clients[0] if gmail_clients else None,
        )


@router.api_route("/run", methods=["GET", "POST"])
def run_scheduled_sales_call_prep(request: Request) -> dict:
    """Called by Vercel Cron. Same auth check the other jobs use."""

    _require_scheduler_auth(request)
    now = datetime.now(timezone.utc)
    run_key = now.strftime("%Y-%m-%dT%H:") + f"{(now.minute // 15) * 15:02d}"
    engine = request.app.state.session_factory.kw.get("bind")
    lease = claim_scheduled_job(
        engine,
        job_key="sales_call_prep",
        run_key=run_key,
        lease_minutes=12,
    )
    if lease is None:
        return {
            "status": "skipped",
            "message": "This prep slot is already running or finished.",
        }
    try:
        result = execute(request, now)
    except Exception as exc:
        logger.exception("[call-prep] scheduled run failed")
        finish_scheduled_job(
            engine, lease, status="failed", details={"error": type(exc).__name__}
        )
        raise HTTPException(status_code=503, detail="Sales call prep did not complete.") from exc
    finish_scheduled_job(
        engine,
        lease,
        status="failed" if result.get("status") == "error" else "succeeded",
        details={
            "status": result.get("status"),
            "source": result.get("source"),
            "seen": result.get("seen"),
            "created": result.get("created"),
            "slack": result.get("slack"),
            "drafts": result.get("drafts"),
        },
    )
    return result
