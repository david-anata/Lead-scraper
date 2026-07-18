"""Public marketing-site intake routes (anatainc.com free analysis).

The marketing site posts server-side with a shared secret header
(``X-Internal-Api-Key`` = ``MARKETING_SITE_INTAKE_KEY``, a separate secret from
the sales agent internal key). The handler enforces one analysis per email per
day, kicks off the existing Digital Shelf deck generation in a background task,
and on completion emails the tokenized deck URL via Resend and records the lead
in HubSpot.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, time as dt_time
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.deck.service import DeckGenerationService


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public/marketing", tags=["marketing-public"])

# Run type for the intake audit row (separate from the deck_generation run the
# deck service records itself), so the daily limit and status lookups are cheap.
INTAKE_RUN_TYPE = "marketing_analysis_intake"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _enforce_marketing_intake_key(request: Request, provided: Optional[str]) -> Optional[JSONResponse]:
    """Shared-secret gate (same header convention as the internal API key
    routes, but keyed on MARKETING_SITE_INTAKE_KEY so the marketing site never
    holds the sales agent key)."""
    configured = str(getattr(request.app.state.settings, "marketing_site_intake_key", "") or "").strip()
    if not configured:
        return JSONResponse(status_code=503, content={"detail": "Marketing intake is not configured."})
    if str(provided or "").strip() != configured:
        return JSONResponse(status_code=401, content={"detail": "Invalid intake key."})
    return None


def _today_intakes_for_email(session, email: str) -> list[AutomationRun]:
    """Intake runs started today (UTC) for this email. Metadata is JSON, so we
    filter by email in Python; the date filter keeps the scan tiny."""
    midnight_utc = datetime.combine(datetime.utcnow().date(), dt_time.min)
    rows = session.execute(
        select(AutomationRun).where(
            AutomationRun.run_type == INTAKE_RUN_TYPE,
            AutomationRun.started_at >= midnight_utc,
        )
    ).scalars().all()
    normalized = email.strip().lower()
    return [run for run in rows if str((run.metadata_json or {}).get("email", "")).strip().lower() == normalized]


def _latest_intake(session, *, email: str, asin: str) -> Optional[AutomationRun]:
    rows = session.execute(
        select(AutomationRun)
        .where(AutomationRun.run_type == INTAKE_RUN_TYPE)
        .order_by(AutomationRun.id.desc())
        .limit(200)
    ).scalars().all()
    email_norm = email.strip().lower()
    asin_norm = asin.strip()
    for run in rows:
        meta = run.metadata_json or {}
        if (
            str(meta.get("email", "")).strip().lower() == email_norm
            and str(meta.get("asin", "")).strip() == asin_norm
        ):
            return run
    return None


def _send_result_email(settings, *, email: str, asin: str, view_url: str) -> None:
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] Resend not configured; skipping result email to %s", email)
        return
    booking_url = str(getattr(settings, "marketing_booking_url", "") or "").strip()
    lines = [
        "Hi,",
        "",
        f"Your product analysis for {asin} is ready. You can view it here:",
        view_url,
        "",
    ]
    if booking_url:
        lines += [
            "If you would like help putting the recommendations to work, you can schedule a call with us here:",
            booking_url,
            "",
        ]
    lines += ["Anata"]
    client.send_message(
        to=email,
        subject="Your Anata product analysis is ready",
        text="\n".join(lines),
    )


def _record_hubspot_lead(settings, *, email: str, asin: str, view_url: str, source: str) -> None:
    """Create the contact (standard email property only; custom properties are
    not confirmed to exist in the portal) and attach the run details as a note.
    On a duplicate-email 409, reuse the existing contact id HubSpot reports."""
    client = HubSpotClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] HubSpot not configured; skipping contact for %s", email)
        return
    contact_id = ""
    try:
        created = client.create_contact({"email": email})
        contact_id = str((created or {}).get("id", "") or "")
    except Exception as exc:  # noqa: BLE001 — duplicate email is expected for repeat visitors
        match = re.search(r"Existing ID:\s*(\d+)", str(exc))
        if match:
            contact_id = match.group(1)
        else:
            logger.warning("[marketing_intake] HubSpot create_contact failed for %s: %s", email, exc)
            return
    if not contact_id:
        return
    note_body = (
        "Free analysis requested from the marketing site."
        f"<br>ASIN: {asin}"
        f"<br>Deck: {view_url}"
        f"<br>Source: {source or 'anatainc.com'}"
    )
    try:
        client.create_contact_note(contact_id=contact_id, body=note_body)
    except Exception as exc:  # noqa: BLE001 — the contact itself is the critical write
        logger.warning("[marketing_intake] HubSpot note failed for contact %s: %s", contact_id, exc)


def _run_analysis_and_deliver(app, *, intake_run_id: int, asin: str, email: str, source: str) -> None:
    """Background task: run the existing Digital Shelf deck generation, then
    email the tokenized deck URL and record the HubSpot lead. Mirrors the
    internal digital-shelf route's call into DeckGenerationService."""
    settings = app.state.settings
    view_url = ""
    error_message = ""
    try:
        from sales_support_agent.services.deck.formatting import DEFAULT_SERVICE_TABS, _normalize_offers

        with session_scope(app.state.session_factory) as session:
            result = DeckGenerationService(settings, session).generate_deck(
                target_product_input=asin,
                rainforest_asin=asin,
                competitor_xray_csv_payloads=[],
                keyword_xray_csv_payloads=[],
                channels=list(DEFAULT_SERVICE_TABS),
                offers=_normalize_offers([]),
                include_recommended_plan=True,
                growth_plan_inputs=None,
                trigger="marketing_site",
            )
            view_url = result.view_url
    except Exception as exc:  # noqa: BLE001 — must never crash the server thread
        error_message = str(exc)
        logger.error("[marketing_intake] deck generation failed for %s: %s", asin, exc, exc_info=True)

    # Update the intake audit row so the status endpoint reflects reality.
    try:
        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, intake_run_id)
            if run is not None:
                AuditService(session).finish_run(
                    run,
                    status="success" if view_url else "failed",
                    summary={"view_url": view_url, "error": error_message},
                )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] failed to update intake run %s", intake_run_id)

    if not view_url:
        return

    try:
        _send_result_email(settings, email=email, asin=asin, view_url=view_url)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] result email failed for %s", email)
    try:
        _record_hubspot_lead(settings, email=email, asin=asin, view_url=view_url, source=source)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] HubSpot lead recording failed for %s", email)


@router.post("/analysis")
async def marketing_analysis_intake(
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied

    try:
        body: dict[str, Any] = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"detail": "Request body must be valid JSON."})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"detail": "Request body must be a JSON object."})

    asin = str(body.get("asin", "") or "").strip()
    email = str(body.get("email", "") or "").strip()
    source = str(body.get("source", "") or "").strip()
    if not asin or len(asin) > 2048:
        return JSONResponse(status_code=400, content={"detail": "asin is required (ASIN or Amazon URL)."})
    if not email or not _EMAIL_RE.match(email):
        return JSONResponse(status_code=400, content={"detail": "A valid email is required."})

    with session_scope(request.app.state.session_factory) as session:
        if _today_intakes_for_email(session, email):
            return JSONResponse(status_code=429, content={"reason": "daily_limit"})
        intake_run = AuditService(session).start_run(
            INTAKE_RUN_TYPE,
            trigger="marketing_site",
            metadata={"email": email.lower(), "asin": asin, "source": source},
        )
        intake_run_id = intake_run.id

    background_tasks.add_task(
        _run_analysis_and_deliver,
        request.app,
        intake_run_id=intake_run_id,
        asin=asin,
        email=email,
        source=source,
    )
    return JSONResponse(status_code=202, content={"status": "building"})


@router.get("/analysis/status")
def marketing_analysis_status(
    request: Request,
    asin: str = "",
    email: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    if not asin.strip() or not email.strip():
        return JSONResponse(status_code=400, content={"detail": "asin and email query params are required."})

    with session_scope(request.app.state.session_factory) as session:
        run = _latest_intake(session, email=email, asin=asin)
        if run is None:
            return JSONResponse(status_code=404, content={"status": "not_found"})
        if run.status == "success":
            return JSONResponse(content={"status": "ready"})
        if run.status == "failed":
            return JSONResponse(content={"status": "failed"})
        return JSONResponse(content={"status": "building"})
