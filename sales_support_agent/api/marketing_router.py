"""Public marketing-site intake routes (anatainc.com free analysis).

The marketing site posts server-side with a shared secret header
(``X-Internal-Api-Key`` = ``MARKETING_SITE_INTAKE_KEY``, a separate secret from
the sales agent internal key). The handler enforces one analysis per email per
day, kicks off the existing Digital Shelf deck generation in a background task,
and on completion emails the tokenized deck URL via Resend and records the lead
in HubSpot.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import logging
import json
import os
import re
import secrets
from html import escape
from datetime import datetime, time as dt_time, timezone
from statistics import median
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import AutomationAction, AutomationRun
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.deck.service import DeckGenerationService
from sales_support_agent.services.marketing_junk_guard import (
    junk_signals,
    normalize_email_identity,
)
from sales_support_agent.services.public_request_guard import (
    RATE_LIMIT_RUN_TYPE_PREFIX,
    durable_rate_limit_response,
    durable_rate_limited,
    read_public_json_object,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public/marketing", tags=["marketing-public"])

# Run type for the intake audit row (separate from the deck_generation run the
# deck service records itself), so the daily limit and status lookups are cheap.
INTAKE_RUN_TYPE = "marketing_analysis_intake"
ANALYSIS_LOOKUP_ACTION = "marketing_analysis_lookup"
ANALYSIS_LOOKUP_PREFIX = "marketing-analysis:"
DAILY_EMAIL_ACTION = "marketing_daily_email"
DAILY_EMAIL_PREFIX = "marketing-daily:"

# Run type for the two-step site intake (identifier → needs → email unlock).
SITE_INTAKE_RUN_TYPE = "marketing_intake"

# Indexed, key-specific run type for calendar confirmations that arrive without
# a Strategy Audit token. The 40-character digest suffix keeps the full value
# within AutomationRun.run_type's 64-character column.
BOOKING_RUN_TYPE_PREFIX = "marketing_booking_"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_ASIN_RE = re.compile(r"^B0[A-Z0-9]{8}$", re.IGNORECASE)
_AMAZON_ASIN_RE = re.compile(
    r"amazon\.[a-z.]+/(?:[^?#]*/)?(?:dp|gp/product|product)/(B0[A-Z0-9]{8})",
    re.IGNORECASE,
)
_BARE_AMAZON_ASIN_RE = re.compile(r"\b(B0[A-Z0-9]{8})\b", re.IGNORECASE)
_ATTRIBUTION_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9./:_-]{0,119}$", re.IGNORECASE)

# Needs chips the site can send; anything else is dropped silently.
_KNOWN_NEEDS = {"analytics", "advertising", "strategy", "catalog", "creative", "fulfillment"}
_SERVICES_NEEDS = {"advertising", "strategy", "catalog", "creative", "fulfillment"}
_QUALIFICATION_FIELDS = {
    "name": 160,
    "company": 160,
    "phone": 64,
    "storefront": 500,
    "revenue_range": 80,
    "challenge": 1000,
    "next_step": 120,
    "audit_run_id": 40,
}


def _sanitize_qualification(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    sanitized = {
        key: str(raw.get(key, "") or "").strip()[:limit]
        for key, limit in _QUALIFICATION_FIELDS.items()
        if str(raw.get(key, "") or "").strip()
    }
    audit_run_id = sanitized.get("audit_run_id", "")
    if audit_run_id and not re.fullmatch(r"\d{1,20}", audit_run_id):
        sanitized.pop("audit_run_id", None)
    return sanitized


def _sanitize_source(raw: Any, fallback: str) -> str:
    """Keep attribution route-like so visitor text and PII never enter CRM metadata."""
    candidate = str(raw or "").strip()
    return candidate if _ATTRIBUTION_SOURCE_RE.fullmatch(candidate) else fallback

# Hard ceiling on the cheap identity lookups so the intake endpoint stays fast.
_IDENTITY_TIMEOUT_SECONDS = 25

# PR-C2.1: prospect decks now always ship the 4-phase Growth Plan section.
# Passing an empty dict (rather than None) makes parse_growth_plan_inputs fill
# every field from its defaults, and the deck pipeline derives the product-
# specific inputs it already has: average order value falls back to the target
# listing price, and current sessions are reverse-engineered from the listing's
# BSR-based unit estimate. No dollar targets are fabricated: where a real figure
# is not derivable the phase model still renders (phases + what each does) using
# published industry benchmarks that the section labels as directional.
_PROSPECT_GROWTH_PLAN_INPUTS: dict[str, Any] = {}


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


def _daily_gate_enabled() -> bool:
    """One-per-mailbox-per-day gate.

    Was OFF by default for the 2026-07-19 testing phase and never switched back
    on, which let a single scripted mailbox submit without limit (David
    2026-07-31). Now ON by default; set MARKETING_DAILY_GATE=0 to suspend it
    for a testing window.
    """
    return os.getenv("MARKETING_DAILY_GATE", "").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _today_intakes_for_email(
    session, email: str, run_types: tuple[str, ...] = (INTAKE_RUN_TYPE, SITE_INTAKE_RUN_TYPE)
) -> list[AutomationRun]:
    """Bounded exact lookup for today's shared per-email intake gate."""
    midnight_utc = datetime.combine(datetime.utcnow().date(), dt_time.min)
    dedupe_key = _daily_email_key(email=email)
    indexed = session.execute(
        select(AutomationRun)
        .join(AutomationAction, AutomationAction.run_id == AutomationRun.id)
        .where(
            AutomationAction.dedupe_key == dedupe_key,
            AutomationAction.action_type == DAILY_EMAIL_ACTION,
            AutomationAction.success.is_(True),
            AutomationRun.run_type.in_(run_types),
        )
        .order_by(AutomationAction.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if indexed is not None:
        return [indexed]

    # Bounded compatibility for records created before daily lookup actions.
    rows = session.execute(
        select(AutomationRun)
        .where(
            AutomationRun.run_type.in_(run_types),
            AutomationRun.started_at >= midnight_utc,
        )
        .order_by(AutomationRun.id.desc())
        .limit(100)
    ).scalars().all()
    normalized = normalize_email_identity(email)
    matches = [
        run
        for run in rows
        if normalize_email_identity(str((run.metadata_json or {}).get("email", "")))
        == normalized
    ]
    if matches:
        _bind_daily_email(
            session,
            run_id=int(matches[0].id),
            email=normalized,
        )
    return matches


def _daily_email_key(*, email: str) -> str:
    # Keyed on the mailbox, not the spelling: dot-padded Gmail aliases are one
    # person and must share one daily allowance.
    material = (
        f"{datetime.now(timezone.utc).date().isoformat()}|"
        f"{normalize_email_identity(email)}"
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"{DAILY_EMAIL_PREFIX}{digest}"


def _bind_daily_email(session, *, run_id: int, email: str) -> None:
    session.add(
        AutomationAction(
            run_id=run_id,
            clickup_task_id="",
            system="marketing",
            action_type=DAILY_EMAIL_ACTION,
            dedupe_key=_daily_email_key(email=email),
            success=True,
            error_message="",
            before_json={},
            after_json={},
        )
    )


def _analysis_lookup_key(*, email: str, asin: str) -> str:
    material = f"{email.strip().lower()}|{asin.strip()}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"{ANALYSIS_LOOKUP_PREFIX}{digest}"


def _bind_analysis_lookup(
    session,
    *,
    run_id: int,
    email: str,
    asin: str,
) -> None:
    session.add(
        AutomationAction(
            run_id=run_id,
            clickup_task_id="",
            system="marketing",
            action_type=ANALYSIS_LOOKUP_ACTION,
            dedupe_key=_analysis_lookup_key(email=email, asin=asin),
            success=True,
            error_message="",
            before_json={},
            after_json={},
        )
    )


def _latest_intake(session, *, email: str, asin: str) -> Optional[AutomationRun]:
    lookup_key = _analysis_lookup_key(email=email, asin=asin)
    run = session.execute(
        select(AutomationRun)
        .join(AutomationAction, AutomationAction.run_id == AutomationRun.id)
        .where(
            AutomationAction.dedupe_key == lookup_key,
            AutomationAction.action_type == ANALYSIS_LOOKUP_ACTION,
            AutomationAction.success.is_(True),
            AutomationRun.run_type == INTAKE_RUN_TYPE,
        )
        .order_by(AutomationAction.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if run is not None:
        return run

    # Compatibility for the most recent pre-index records only. All new
    # analysis requests create an indexed action in the same transaction.
    rows = session.execute(
        select(AutomationRun)
        .where(AutomationRun.run_type == INTAKE_RUN_TYPE)
        .order_by(AutomationRun.id.desc())
        .limit(200)
    ).scalars().all()
    email_norm = email.strip().lower()
    asin_norm = asin.strip()
    for legacy_run in rows:
        meta = legacy_run.metadata_json or {}
        if (
            str(meta.get("email", "")).strip().lower() == email_norm
            and str(meta.get("asin", "")).strip() == asin_norm
        ):
            _bind_analysis_lookup(
                session,
                run_id=int(legacy_run.id),
                email=email_norm,
                asin=asin_norm,
            )
            return legacy_run
    return None


def _normalize_amazon_asin(raw: Any) -> str:
    value = str(raw or "").strip()
    upper = value.upper()
    if _ASIN_RE.fullmatch(upper):
        return upper
    match = _AMAZON_ASIN_RE.search(value)
    if match:
        return match.group(1).upper()
    if "amazon." in value.lower():
        match = _BARE_AMAZON_ASIN_RE.search(value)
        if match:
            return match.group(1).upper()
    return ""


def _send_result_email(
    settings,
    *,
    email: str,
    asin: str,
    view_url: str,
    intake_run_id: int = 0,
) -> bool:
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] Resend not configured; skipping result email to %s", email)
        return False
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
        idempotency_key=(
            f"marketing-intake-{intake_run_id}-result"
            if intake_run_id
            else ""
        ),
    )
    return True


def _send_unlock_ack_email(
    settings,
    *,
    email: str,
    display_name: str,
    kind: str,
    intake_run_id: int,
) -> bool:
    """Confirm receipt before the expensive analysis starts.

    This is intentionally separate from the final report email. A deck build
    can fail or outlive a web worker, but a valid request must never disappear
    without a prospect acknowledgement.
    """
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning(
            "[marketing_intake] Resend not configured; skipping acknowledgement to %s",
            email,
        )
        return False
    label = display_name or ("your product" if kind == "asin" else "your store")
    booking_url = str(getattr(settings, "marketing_booking_url", "") or "").strip()
    lines = [
        "Hi,",
        "",
        f"We received your Anata analysis request for {label}.",
        "We are building the complete report now and will email you again when it is ready.",
        "",
    ]
    if booking_url:
        lines += [
            "You can also choose a time to review it with our team:",
            booking_url,
            "",
        ]
    lines += ["Anata"]
    client.send_message(
        to=email,
        subject="We received your Anata analysis request",
        text="\n".join(lines),
        idempotency_key=f"marketing-intake-{intake_run_id}-ack",
    )
    return True


def _lead_notification_recipients() -> list[str]:
    configured = os.getenv("MARKETING_LEAD_EMAIL_TO", "").strip()
    values = configured.split(",") if configured else ["david@anatainc.com"]
    return [value.strip().lower() for value in values if value.strip()]


def _send_internal_lead_email(
    settings,
    *,
    email: str,
    kind: str,
    identifier: str,
    brand_name: str,
    source: str,
    needs: list[str],
    qualification: Optional[dict[str, str]],
    intake_run_id: int,
) -> bool:
    """Send the internal new-lead alert independently of report generation."""
    client = ResendClient(settings)
    recipients = _lead_notification_recipients()
    if not recipients or not client.is_configured():
        logger.warning(
            "[marketing_intake] internal lead email is not configured for intake %s",
            intake_run_id,
        )
        return False
    qualification = qualification or {}
    lines = [
        "New Anata website analysis request",
        "",
        f"Intake: {intake_run_id}",
        f"Name: {qualification.get('name', '') or 'Not provided'}",
        f"Company: {qualification.get('company', '') or brand_name or 'Not provided'}",
        f"Phone: {qualification.get('phone', '') or 'Not provided'}",
        f"Email: {email}",
        f"Type: {'Amazon ASIN' if kind == 'asin' else 'Store website'}",
        f"Submitted: {identifier or 'Not available'}",
        f"Needs: {', '.join(needs) if needs else 'Not selected'}",
        f"Source: {source or 'anatainc.com'}",
    ]
    client.send_message(
        to=recipients,
        subject=f"New website analysis lead: {qualification.get('company') or brand_name or email}",
        text="\n".join(lines),
        reply_to=email,
        idempotency_key=f"marketing-intake-{intake_run_id}-lead",
    )
    return True


def _write_intake_delivery_state(
    app,
    intake_run_id: int,
    **state: str,
) -> None:
    try:
        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, intake_run_id)
            if run is None:
                return
            run.summary_json = {**(run.summary_json or {}), **state}
            session.add(run)
    except Exception:  # noqa: BLE001
        logger.exception(
            "[marketing_intake] failed to write delivery state for intake %s",
            intake_run_id,
        )


def _record_hubspot_lead(
    settings, *, email: str, asin: str, view_url: str, source: str, needs: Optional[list[str]] = None,
    qualification: Optional[dict[str, str]] = None,
) -> bool:
    """Create the contact (standard email property only; custom properties are
    not confirmed to exist in the portal) and attach the run details as a note.
    On a duplicate-email 409, reuse the existing contact id HubSpot reports."""
    client = HubSpotClient(settings)
    if not client.is_configured:  # property, not a method (hubspot.py:98)
        logger.warning("[marketing_intake] HubSpot not configured; skipping contact for %s", email)
        return False
    contact_id = ""
    try:
        qualification = qualification or {}
        properties = {"email": email}
        if qualification.get("name"):
            properties["firstname"] = qualification["name"]
        for source_key, hubspot_key in (("company", "company"), ("phone", "phone"), ("storefront", "website")):
            if qualification.get(source_key):
                properties[hubspot_key] = qualification[source_key]
        created = client.create_contact(properties)
        contact_id = str((created or {}).get("id", "") or "")
    except Exception as exc:  # noqa: BLE001 — duplicate email is expected for repeat visitors
        match = re.search(r"Existing ID:\s*(\d+)", str(exc))
        if match:
            contact_id = match.group(1)
        else:
            logger.warning("[marketing_intake] HubSpot create_contact failed for %s: %s", email, exc)
            return False
    if not contact_id:
        return False
    contact_updates = {key: value for key, value in properties.items() if key != "email"}
    if contact_updates:
        try:
            client.update_contact(contact_id, contact_updates)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[marketing_intake] HubSpot contact update failed for %s: %s", contact_id, exc)
    request_label = (
        "Advertising Audit requested from the marketing site."
        if needs and "advertising" in needs
        else "Strategy Audit requested from the marketing site."
    )
    note_body = request_label + f"<br>ASIN: {asin}"
    if view_url:
        note_body += f"<br>Deck: {view_url}"
    note_body += f"<br>Source: {source or 'anatainc.com'}"
    if needs:
        note_body += f"<br>Needs: {', '.join(needs)}"
    for key in ("company", "phone", "storefront", "revenue_range", "challenge", "next_step", "audit_run_id"):
        if qualification and qualification.get(key):
            note_body += f"<br>{key.replace('_', ' ').title()}: {escape(qualification[key])}"
    try:
        client.create_contact_note(contact_id=contact_id, body=note_body)
    except Exception as exc:  # noqa: BLE001 — the contact itself is the critical write
        logger.warning("[marketing_intake] HubSpot note failed for contact %s: %s", contact_id, exc)
        return False
    return True


def _run_analysis_and_deliver(
    app,
    *,
    intake_run_id: int,
    asin: str,
    email: str,
    source: str,
    trigger: str = "marketing_site",
    needs: Optional[list[str]] = None,
    qualification: Optional[dict[str, str]] = None,
) -> None:
    """Background task: run the existing Digital Shelf deck generation, then
    email the tokenized deck URL and record the HubSpot lead. Mirrors the
    internal digital-shelf route's call into DeckGenerationService."""
    settings = app.state.settings
    view_url = ""
    error_message = ""
    deck_title = ""
    competitor_rows = 0
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
                growth_plan_inputs=dict(_PROSPECT_GROWTH_PLAN_INPUTS),
                trigger=trigger,
            )
            view_url = result.view_url
            deck_title = (result.design_title or "").strip()
            competitor_rows = int(result.competitor_row_count or 0)
            logger.info(
                "[marketing_intake] deck done for %s: competitors=%s title=%r",
                asin,
                competitor_rows,
                deck_title,
            )
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
                    # Merge so the site-intake summary (token, needs, brand
                    # identity) survives the deck-completion update.
                    summary={
                        **(run.summary_json or {}),
                        "view_url": view_url,
                        "error": error_message,
                        "competitor_row_count": competitor_rows,
                        # Backfill identity from the finished deck when the cheap lookup missed.
                        **(
                            {"product_title": deck_title}
                            if deck_title and not (run.summary_json or {}).get("product_title")
                            else {}
                        ),
                    },
                )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] failed to update intake run %s", intake_run_id)

    if not view_url:
        return

    email_delivered = False
    try:
        email_delivered = _send_result_email(
            settings,
            email=email,
            asin=asin,
            view_url=view_url,
            intake_run_id=intake_run_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] result email failed for %s", email)
    hubspot_recorded = False
    try:
        hubspot_recorded = _record_hubspot_lead(
            settings,
            email=email,
            asin=asin,
            view_url=view_url,
            source=source,
            needs=needs,
            qualification=qualification,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] HubSpot lead recording failed for %s", email)
    try:
        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, intake_run_id)
            if run is not None:
                run.summary_json = {
                    **(run.summary_json or {}),
                    "email_delivery": "delivered" if email_delivered else "failed",
                    "hubspot_handoff": (
                        "recorded"
                        if hubspot_recorded
                        else (run.summary_json or {}).get(
                            "hubspot_handoff",
                            "failed",
                        )
                    ),
                }
                session.add(run)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] failed to record delivery state for %s", intake_run_id)


@router.post("/analysis")
async def marketing_analysis_intake(
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="analysis:create", limit=30)
    if limited is not None:
        return limited

    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    asin = str(body.get("asin", "") or "").strip()
    email = str(body.get("email", "") or "").strip()
    source = _sanitize_source(body.get("source"), "anatainc.com/tools/strategy-audit")
    if not asin or len(asin) > 2048:
        return JSONResponse(status_code=400, content={"detail": "asin is required (ASIN or Amazon URL)."})
    if not email or not _EMAIL_RE.match(email):
        return JSONResponse(status_code=400, content={"detail": "A valid email is required."})

    with session_scope(request.app.state.session_factory) as session:
        if _daily_gate_enabled() and _today_intakes_for_email(session, email):
            return JSONResponse(status_code=429, content={"reason": "daily_limit"})
        intake_run = AuditService(session).start_run(
            INTAKE_RUN_TYPE,
            trigger="marketing_site",
            metadata={"email": email.lower(), "asin": asin, "source": source},
        )
        intake_run_id = intake_run.id
        _bind_analysis_lookup(
            session,
            run_id=int(intake_run_id),
            email=email,
            asin=asin,
        )
        _bind_daily_email(
            session,
            run_id=int(intake_run_id),
            email=email,
        )

    background_tasks.add_task(
        _run_analysis_and_deliver,
        request.app,
        intake_run_id=intake_run_id,
        asin=asin,
        email=email,
        source=source,
    )
    return JSONResponse(status_code=202, content={"status": "building"})


@router.post("/advertising-audit")
async def advertising_audit_intake(
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Start the Strategy Audit that opens the sales-assisted Advertising Audit.

    The deeper advertising audit remains report-assisted. This endpoint creates
    the secure run/correlation record, starts the Strategy Audit, and gives the
    website a token-protected status handle without exposing PII in the URL.
    """
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="advertising:create", limit=30)
    if limited is not None:
        return limited
    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    asin = _normalize_amazon_asin(body.get("product"))
    email = str(body.get("email", "") or "").strip().lower()
    company = str(body.get("company", "") or "").strip()[:160]
    source = _sanitize_source(body.get("source"), "anatainc.com/tools/advertising-audit")
    if not asin:
        return JSONResponse(
            status_code=400,
            content={"reason": "invalid_product", "detail": "Enter an Amazon ASIN or Amazon product URL."},
        )
    if not email or not _EMAIL_RE.match(email):
        return JSONResponse(status_code=400, content={"reason": "invalid_email", "detail": "A valid work email is required."})
    if not company:
        return JSONResponse(status_code=400, content={"reason": "company_required", "detail": "Company name is required."})

    with session_scope(request.app.state.session_factory) as session:
        if _daily_gate_enabled() and _today_intakes_for_email(session, email):
            return JSONResponse(status_code=429, content={"reason": "daily_limit"})
        status_token = secrets.token_urlsafe(24)
        intake_run = AuditService(session).start_run(
            INTAKE_RUN_TYPE,
            trigger="marketing_site_advertising_audit",
            metadata={
                "email": email,
                "asin": asin,
                "company": company,
                "source": source,
                "tool": "advertising_audit",
                "status_token": status_token,
                "qualification": {
                    "company": company,
                    "storefront": f"https://www.amazon.com/dp/{asin}",
                    "challenge": "Advertising Audit requested from anatainc.com.",
                    "next_step": "Call prospect and confirm the four-report handoff.",
                },
            },
        )
        intake_run.summary_json = {
            "strategy_audit": "building",
            "advertising_audit": "reports_required",
            "email_delivery": "pending",
            "hubspot_handoff": "pending",
        }
        session.add(intake_run)
        intake_run_id = intake_run.id
        _bind_daily_email(
            session,
            run_id=int(intake_run_id),
            email=email,
        )

    background_tasks.add_task(
        _run_analysis_and_deliver,
        request.app,
        intake_run_id=intake_run_id,
        asin=asin,
        email=email,
        source=source or "anatainc.com/tools/advertising-audit",
        trigger="marketing_site_advertising_audit",
        needs=["advertising"],
        qualification={
            "company": company,
            "storefront": f"https://www.amazon.com/dp/{asin}",
            "challenge": "Advertising Audit requested from anatainc.com.",
            "next_step": "Call prospect and confirm the four-report handoff.",
            "audit_run_id": str(intake_run_id),
        },
    )
    return JSONResponse(
        status_code=202,
        content={
            "run_id": str(intake_run_id),
            "token": status_token,
            "asin": asin,
            "status": "accepted",
        },
    )


@router.get("/advertising-audit/{run_id}")
def advertising_audit_status(
    request: Request,
    run_id: int,
    token: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(
        request,
        scope="advertising:status",
        limit=240,
    )
    if limited is not None:
        return limited
    with session_scope(request.app.state.session_factory) as session:
        run = session.get(AutomationRun, run_id)
        if run is None or run.run_type != INTAKE_RUN_TYPE:
            return JSONResponse(status_code=404, content={"status": "not_found"})
        metadata = run.metadata_json or {}
        if metadata.get("tool") != "advertising_audit":
            return JSONResponse(status_code=404, content={"status": "not_found"})
        expected = str(metadata.get("status_token", "") or "")
        if not expected or not secrets.compare_digest(str(token or ""), expected):
            return JSONResponse(status_code=403, content={"detail": "Invalid status token."})
        summary = run.summary_json or {}
        delivery = str(summary.get("email_delivery", "pending") or "pending")
        if run.status not in {"running", "success", "failed"} or delivery not in {
            "pending",
            "delivered",
            "failed",
        }:
            logger.error(
                "[marketing_advertising] invalid public lifecycle for run %s: run=%r email=%r",
                run_id,
                run.status,
                delivery,
            )
            return JSONResponse(status_code=500, content={"detail": "Invalid advertising audit lifecycle."})
        if run.status == "failed":
            public_status = "failed"
            strategy_status = "failed"
        elif run.status == "success":
            public_status = "delivered" if delivery == "delivered" else (
                "delivery_failed" if delivery == "failed" else "ready"
            )
            strategy_status = "ready"
        else:
            public_status = "building"
            strategy_status = "building"
        return JSONResponse(
            content={
                "status": public_status,
                "strategy_audit": strategy_status,
                "advertising_audit": "reports_required",
                "email_delivery": delivery,
            }
        )


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
    limited = durable_rate_limit_response(request, scope="analysis:status", limit=240)
    if limited is not None:
        return limited
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
        if run.status == "running":
            return JSONResponse(content={"status": "building"})
        logger.error(
            "[marketing_analysis] invalid public lifecycle for run %s: %r",
            run.id,
            run.status,
        )
        return JSONResponse(status_code=500, content={"detail": "Invalid analysis lifecycle."})


# ---------------------------------------------------------------------------
# Two-step site intake (identifier -> needs -> email unlock)
# ---------------------------------------------------------------------------


def _compose_brand_read(identity: dict[str, str], kind: str) -> str:
    """Write the 'someone actually looked' paragraph from REAL fetched fields only.
    Anata voice: second person, calm operator, no em dashes, no invented facts.
    Best-effort: empty string on any failure; never blocks the intake."""
    title = identity.get("product_title", "").strip()
    brand = identity.get("brand_name", "").strip()
    if not title and not brand:
        return ""
    facts = {
        "brand": brand,
        "product_title": title,
        "price": identity.get("price", "").strip(),
        "rating": identity.get("rating", "").strip(),
        "ratings_total": identity.get("ratings_total", "").strip(),
        "kind": kind,
        "store_domain": identity.get("domain", "").strip(),
    }
    try:
        import anthropic

        client = anthropic.Anthropic()
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=220,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "You write for Anata, an ecommerce operations partner. Using ONLY the facts "
                        "below, write 2 to 3 sentences addressed to this brand about their own "
                        "product and position, the way a sharp operator would open a strategy deck. "
                        "Rules: second person (you, your). Calm, specific, warm, zero hype. Never "
                        "use an em dash. Do not invent numbers, categories, or claims beyond what "
                        "the facts support; you may describe what the product plainly is from its "
                        "title. Do not mention Anata or sell anything. Return ONLY the sentences.\n\n"
                        f"FACTS: {json.dumps(facts)}"
                    ),
                }
            ],
        )
        text = (msg.content[0].text if msg.content else "").strip()
        if "\u2014" in text or "\u2013" in text.replace("-", ""):
            text = text.replace("\u2014", ",").replace("\u2013", ",")
        return text[:600]
    except Exception:
        logger.debug("[marketing_intake] brand read composition failed", exc_info=True)
        return ""


def _asin_identity(identifier: str) -> dict[str, str]:
    """Cheap identity lookup for an ASIN/Amazon URL: ONE Rainforest product
    fetch (title, image, brand) with a hard timeout, graceful empties on any
    failure. No competitor/keyword work happens here."""
    from sales_support_agent.services.rainforest import RainforestClient, _normalize_asin

    asin = _normalize_asin(identifier)
    identity = {"asin": asin, "brand_name": "", "product_title": "", "product_image": ""}
    if not asin:
        return identity

    def _fetch() -> dict[str, Any]:
        return RainforestClient().get_product(asin)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            data = pool.submit(_fetch).result(timeout=_IDENTITY_TIMEOUT_SECONDS)
        product = data.get("product", {}) or {}
        identity["brand_name"] = str(product.get("brand", "") or "").strip()
        identity["product_title"] = str(product.get("title", "") or "").strip()
        identity["product_image"] = str(((product.get("main_image") or {}).get("link", "")) or "").strip()
        # Real numbers from the same call, no extra cost: the page's proof-of-look.
        buybox = product.get("buybox_winner") or {}
        price = (buybox.get("price") or {}).get("raw", "") or ""
        identity["price"] = str(price).strip()
        identity["rating"] = str(product.get("rating", "") or "").strip()
        identity["ratings_total"] = str(product.get("ratings_total", "") or "").strip()
    except Exception as exc:  # noqa: BLE001 — identity is best-effort, never blocks intake
        logger.warning("[marketing_intake] Rainforest identity lookup failed for %s: %s", asin, exc)
    return identity


_OG_TAG_RE_TEMPLATE = (
    r'<meta[^>]+(?:property|name)=["\']og:{name}["\'][^>]+content=["\']([^"\']*)["\']'
    r'|<meta[^>]+content=["\']([^"\']*)["\'][^>]+(?:property|name)=["\']og:{name}["\']'
)


def _og_tag(html: str, name: str) -> str:
    match = re.search(_OG_TAG_RE_TEMPLATE.format(name=name), html, flags=re.IGNORECASE)
    if not match:
        return ""
    return (match.group(1) or match.group(2) or "").strip()


def _store_identity(identifier: str) -> dict[str, str]:
    """Cheap identity lookup for a store domain: fetch the homepage and regex
    out og:site_name / og:title / og:image. Graceful empties on any failure."""
    domain = re.sub(r"^https?://", "", identifier.strip(), flags=re.IGNORECASE).strip("/").split("/")[0]
    identity = {"domain": domain, "brand_name": "", "product_title": "", "product_image": ""}
    if not domain:
        return identity
    try:
        import requests

        resp = requests.get(
            f"https://{domain}",
            timeout=_IDENTITY_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AnataIntake/1.0)"},
        )
        resp.raise_for_status()
        html = resp.text[:500_000]
        identity["brand_name"] = _og_tag(html, "site_name") or _og_tag(html, "title")
        identity["product_title"] = _og_tag(html, "title")
        identity["product_image"] = _og_tag(html, "image")
    except Exception as exc:  # noqa: BLE001 — identity is best-effort, never blocks intake
        logger.warning("[marketing_intake] store identity lookup failed for %s: %s", domain, exc)
    return identity


def _load_site_intake(session, intake_id: int, token: str):
    """Fetch a site-intake run and validate its token. Returns (run, None) or
    (None, JSONResponse error)."""
    run = session.get(AutomationRun, intake_id)
    if run is None or run.run_type != SITE_INTAKE_RUN_TYPE:
        return None, JSONResponse(status_code=404, content={"detail": "Intake not found."})
    expected = str((run.summary_json or {}).get("token", "") or "")
    if not expected or not secrets.compare_digest(expected, str(token or "")):
        return None, JSONResponse(status_code=403, content={"detail": "Invalid intake token."})
    return run, None


def _public_intake_correlation_id(run: AutomationRun) -> str:
    """Return a stable, opaque identifier without exposing the database id."""
    summary = dict(run.summary_json or {})
    existing = str(summary.get("correlation_id", "") or "").strip()
    if existing:
        return existing
    token = str(summary.get("token", "") or "")
    digest = hashlib.sha256(f"{run.id}:{token}".encode("utf-8")).hexdigest()[:24]
    return f"mkt_{digest}"


def _public_outcome(
    value: Any,
    *,
    complete_values: set[str],
    unlocked: bool,
) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in complete_values:
        return "complete"
    if normalized in {"failed", "error"}:
        return "failed"
    if normalized in {"pending", "building", "retrying"}:
        return "pending" if normalized != "retrying" else "retrying"
    return "pending" if unlocked else "not_required"


def _public_intake_delivery_status(run: AutomationRun) -> dict[str, Any]:
    """Serialize only the delivery state the tokenized website experience needs."""
    summary = dict(run.summary_json or {})
    metadata = dict(run.metadata_json or {})
    unlocked = bool(str(metadata.get("email", "") or "").strip())
    acknowledgement = _public_outcome(
        summary.get("acknowledgement_email"),
        complete_values={"delivered", "sent", "complete"},
        unlocked=unlocked,
    )
    internal_notification = _public_outcome(
        summary.get("internal_lead_email"),
        complete_values={"delivered", "sent", "complete"},
        unlocked=unlocked,
    )
    sales_handoff = _public_outcome(
        summary.get("hubspot_handoff"),
        complete_values={"recorded", "complete"},
        unlocked=unlocked,
    )
    final_email = _public_outcome(
        summary.get("email_delivery"),
        complete_values={"delivered", "sent", "complete"},
        unlocked=unlocked,
    )
    booking_handoff = _public_outcome(
        summary.get("booking_handoff"),
        complete_values={"recorded", "complete"},
        unlocked=False,
    )

    view_url = str(summary.get("view_url", "") or "").strip()
    if view_url:
        report_status = "complete"
    elif run.status == "failed" and unlocked:
        report_status = "failed"
    else:
        report_status = "pending" if unlocked else "not_required"

    captured = internal_notification == "complete" or sales_handoff == "complete"
    failed_outcomes = {
        acknowledgement,
        internal_notification,
        sales_handoff,
        final_email,
        report_status,
    }
    if not unlocked:
        request_status = "draft"
    elif not captured:
        request_status = "failed"
    elif run.status == "failed":
        request_status = "partial_failure"
    elif report_status == "complete" and final_email == "complete":
        request_status = "completed"
    elif report_status == "complete":
        request_status = "ready"
    elif "failed" in failed_outcomes:
        request_status = "partial_failure"
    else:
        request_status = "building"

    updated_at = run.completed_at or run.started_at
    request_state = {
        "status": request_status,
        "retryable": request_status in {"failed", "partial_failure"}
        or "failed" in failed_outcomes,
        "updated_at": (
            updated_at.replace(tzinfo=timezone.utc).isoformat()
            if updated_at and updated_at.tzinfo is None
            else updated_at.isoformat()
            if updated_at
            else ""
        ),
    }
    return {
        "correlation_id": _public_intake_correlation_id(run),
        "request": request_state,
        "acknowledgement": {"status": acknowledgement},
        "internal_notification": {"status": internal_notification},
        "sales_handoff": {"status": sales_handoff},
        "report": {
            "status": report_status,
            **({"result_url": view_url} if view_url else {}),
        },
        "final_email": {"status": final_email},
        "booking_handoff": {"status": booking_handoff},
    }


def _send_store_ack_email(
    settings,
    *,
    email: str,
    brand_name: str,
    domain: str,
    intake_run_id: int = 0,
) -> bool:
    """Store-only unlock: no deck, acknowledge the page and point to booking."""
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] Resend not configured; skipping store ack email to %s", email)
        return False
    booking_url = str(getattr(settings, "marketing_booking_url", "") or "").strip()
    display = brand_name or domain
    lines = [
        "Hi,",
        "",
        f"Thanks for sharing {display} with us. We are putting together your page now.",
        "",
    ]
    if booking_url:
        lines += [
            "If you would like to talk through it with us directly, you can schedule a call here:",
            booking_url,
            "",
        ]
    lines += ["Anata"]
    client.send_message(
        to=email,
        subject="Your Anata brand page is on its way",
        text="\n".join(lines),
        idempotency_key=(
            f"marketing-intake-{intake_run_id}-ack"
            if intake_run_id
            else ""
        ),
    )
    return True


def _record_store_hubspot_lead(
    settings,
    *,
    email: str,
    domain: str,
    needs: list[str],
    source: str,
    qualification: Optional[dict[str, str]] = None,
    view_url: str = "",
) -> bool:
    client = HubSpotClient(settings)
    if not client.is_configured:
        logger.warning("[marketing_intake] HubSpot not configured; skipping contact for %s", email)
        return False
    contact_id = ""
    try:
        qualification = qualification or {}
        properties = {"email": email}
        if qualification.get("name"):
            properties["firstname"] = qualification["name"]
        for source_key, hubspot_key in (("company", "company"), ("phone", "phone"), ("storefront", "website")):
            if qualification.get(source_key):
                properties[hubspot_key] = qualification[source_key]
        created = client.create_contact(properties)
        contact_id = str((created or {}).get("id", "") or "")
    except Exception as exc:  # noqa: BLE001 — duplicate email is expected for repeat visitors
        match = re.search(r"Existing ID:\s*(\d+)", str(exc))
        if match:
            contact_id = match.group(1)
        else:
            logger.warning("[marketing_intake] HubSpot create_contact failed for %s: %s", email, exc)
            return False
    if not contact_id:
        return False
    contact_updates = {key: value for key, value in properties.items() if key != "email"}
    if contact_updates:
        try:
            client.update_contact(contact_id, contact_updates)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[marketing_intake] HubSpot contact update failed for %s: %s", contact_id, exc)
    note_body = (
        "Site intake from the marketing site (store, no ASIN)."
        f"<br>Store: {domain}"
        f"<br>Source: {source or 'anatainc.com'}"
    )
    if needs:
        note_body += f"<br>Needs: {', '.join(needs)}"
    if view_url:
        note_body += f"<br>Deck: {view_url}"
    for key in ("company", "phone", "storefront", "revenue_range", "challenge", "next_step"):
        if qualification and qualification.get(key):
            note_body += f"<br>{key.replace('_', ' ').title()}: {escape(qualification[key])}"
    try:
        client.create_contact_note(contact_id=contact_id, body=note_body)
    except Exception as exc:  # noqa: BLE001 — the contact itself is the critical write
        logger.warning("[marketing_intake] HubSpot note failed for contact %s: %s", contact_id, exc)
        return False
    return True


def _record_hubspot_booking(
    settings,
    *,
    email: str,
    brand_name: str,
    source: str,
    tool: str = "strategy",
    booking_reference: str = "",
    qualification: Optional[dict[str, str]] = None,
) -> tuple[bool, str]:
    """Advance or create the strategy deal when the embedded calendar confirms.

    The browser only reports a trusted HubSpot `meetingBookSucceeded` event.
    Contact identity comes from the already-tokenized intake, never from the
    cross-origin iframe message.
    """
    client = HubSpotClient(settings)
    if not client.is_configured:
        logger.warning("[marketing_intake] HubSpot not configured; booking was not recorded")
        return False, ""
    contact = client.find_contact_by_email(email)
    contact_id = str((contact or {}).get("id", "") or "")
    if not contact_id:
        logger.warning("[marketing_intake] no HubSpot contact found for booked email %s", email)
        return False, ""

    qualification = qualification or {}
    contact_properties = dict((contact or {}).get("properties") or {})
    company = (
        qualification.get("company")
        or brand_name
        or str(contact_properties.get("company", "") or "").strip()
        or email.split("@", 1)[0]
    )
    contact_updates = {
        key: value
        for key, value in {
            "firstname": qualification.get("name", ""),
            "company": qualification.get("company", ""),
            "phone": qualification.get("phone", ""),
        }.items()
        if value
    }
    if contact_updates:
        client.update_contact(contact_id, contact_updates)

    tool_labels = {
        "strategy": "Strategy Audit",
        "rate": "Fulfillment Rate Review",
        "ads": "Advertising Audit",
        "profit": "Profit Review",
    }
    tool_label = tool_labels.get(tool, "Strategy Audit")
    expected_deal_name = f"{company} - {tool_label}"
    stage = (
        os.getenv("HUBSPOT_BOOKED_DEAL_STAGE", "").strip()
        or os.getenv("HUBSPOT_DEFAULT_DEAL_STAGE", "").strip()
        or "appointmentscheduled"
    )
    deal_ids = client.list_associations("contacts", contact_id, "deals")[:100]
    deal_id = ""
    if deal_ids:
        associated_deals = client.batch_read(
            "deals",
            deal_ids,
            properties=["dealname", "pipeline", "dealstage"],
        )
        expected_folded = expected_deal_name.casefold()
        tool_suffix = f" - {tool_label}".casefold()
        for row in associated_deals:
            properties = dict(row.get("properties") or {})
            deal_name = str(properties.get("dealname", "") or "").strip()
            if deal_name.casefold() == expected_folded or deal_name.casefold().endswith(tool_suffix):
                deal_id = str(row.get("id", "") or "")
                break
    if deal_id:
        client.update_deal(deal_id, {"dealstage": stage})
    else:
        pipeline = (
            str(getattr(settings, "hubspot_sales_pipeline_id", "") or "").strip()
            or os.getenv("HUBSPOT_DEFAULT_DEAL_PIPELINE", "").strip()
            or "default"
        )
        created = client.create_deal(
            {
                "dealname": expected_deal_name,
                "pipeline": pipeline,
                "dealstage": stage,
            },
            associations=[
                {
                    "to": {"id": contact_id},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 3,
                        }
                    ],
                }
            ],
        )
        deal_id = str((created or {}).get("id", "") or "")
    if not deal_id:
        return False, ""

    note = (
        f"{escape(tool_label)} call booked from the embedded Anata calendar."
        f"<br>Source: {escape(source or 'diagnostic-report-unlocked')}"
        f"<br>Contact: {escape(email)}"
    )
    if booking_reference:
        note += f"<br>Booking reference: {escape(booking_reference)}"
    client.create_note(deal_id=deal_id, body=note)
    client.create_contact_note(contact_id=contact_id, body=note)
    return True, deal_id


def _booking_idempotency_key(
    *,
    email: str,
    tool: str,
    source: str,
    booking_reference: str,
) -> str:
    stable_reference = booking_reference or datetime.now(timezone.utc).date().isoformat()
    material = "|".join(
        [
            email.strip().lower(),
            tool.strip().lower(),
            source.strip().lower(),
            stable_reference.strip().lower(),
        ]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _find_booking_run(session, idempotency_key: str) -> Optional[AutomationRun]:
    run_type = f"{BOOKING_RUN_TYPE_PREFIX}{idempotency_key[:40]}"
    return session.execute(
        select(AutomationRun)
        .where(AutomationRun.run_type == run_type)
        .order_by(AutomationRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _send_internal_booking_email(
    settings,
    *,
    email: str,
    tool: str,
    source: str,
    qualification: dict[str, str],
    deal_id: str,
    idempotency_key: str,
) -> bool:
    client = ResendClient(settings)
    recipients = _lead_notification_recipients()
    if not recipients or not client.is_configured():
        logger.warning("[marketing_booking] internal booking email is not configured")
        return False
    label = {
        "strategy": "Strategy Audit",
        "rate": "Fulfillment Rate Review",
        "ads": "Advertising Audit",
        "profit": "Profit Review",
    }.get(tool, "Strategy Audit")
    lines = [
        "New Anata website booking",
        "",
        f"Type: {label}",
        f"Name: {qualification.get('name', '') or 'Provided in HubSpot'}",
        f"Company: {qualification.get('company', '') or 'Provided in HubSpot'}",
        f"Phone: {qualification.get('phone', '') or 'Provided in HubSpot'}",
        f"Email: {email}",
        f"Source: {source or 'anatainc.com/book'}",
        f"HubSpot deal: {deal_id}",
    ]
    client.send_message(
        to=recipients,
        subject=f"New website booking: {qualification.get('company') or email}",
        text="\n".join(lines),
        reply_to=email,
        idempotency_key=f"marketing-booking-{idempotency_key[:32]}",
    )
    return True


def _send_store_deck_email(
    settings,
    *,
    email: str,
    brand_name: str,
    domain: str,
    view_url: str,
    intake_run_id: int = 0,
) -> bool:
    """Store deck ready: email the tokenized deck URL plus the booking line."""
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] Resend not configured; skipping store deck email to %s", email)
        return False
    booking_url = str(getattr(settings, "marketing_booking_url", "") or "").strip()
    display = brand_name or domain
    lines = [
        "Hi,",
        "",
        f"Your Strategy Audit for {display} is ready. You can view it here:",
        view_url,
        "",
    ]
    if booking_url:
        lines += [
            "If you would like to walk through it with us on a free advisement call, you can book a time here:",
            booking_url,
            "",
        ]
    lines += ["Anata"]
    client.send_message(
        to=email,
        subject="Your Anata Strategy Audit is ready",
        text="\n".join(lines),
        idempotency_key=(
            f"marketing-intake-{intake_run_id}-result"
            if intake_run_id
            else ""
        ),
    )
    return True


def _deliver_store_unlock(app, *, intake_run_id: int, email: str, domain: str, brand_name: str, needs: list[str], source: str, qualification: Optional[dict[str, str]] = None) -> None:
    """Background task for kind=store unlock (Phase 3D).

    Builds a real store / DTC strategy deck from the store URL, reusing the
    existing deck pipeline in its website (DTC) mode. That mode populates the
    sections store data can honestly fill (identity, market read,
    recommendations, growth plan) and omits the Amazon-only competitor pulls,
    since no ASIN/Xray exists for a store URL. On any failure it falls back to
    the original acknowledgement email so the lead is never dropped.
    """
    settings = app.state.settings
    view_url = ""
    store_url = f"https://{domain}" if domain and not domain.startswith("http") else (domain or "")
    if store_url:
        try:
            from sales_support_agent.services.deck.formatting import DEFAULT_SERVICE_TABS, _normalize_offers

            with session_scope(app.state.session_factory) as session:
                result = DeckGenerationService(settings, session).generate_deck(
                    target_product_input=store_url,
                    competitor_xray_csv_payloads=[],
                    keyword_xray_csv_payloads=[],
                    channels=list(DEFAULT_SERVICE_TABS),
                    offers=_normalize_offers([]),
                    include_recommended_plan=True,
                    growth_plan_inputs=dict(_PROSPECT_GROWTH_PLAN_INPUTS),
                    trigger="marketing_site_store",
                )
                view_url = result.view_url
        except Exception as exc:  # noqa: BLE001 - fall back to the ack email
            logger.error("[marketing_intake] store deck generation failed for %s: %s", domain, exc, exc_info=True)

    email_delivered = False
    try:
        if view_url:
            email_delivered = _send_store_deck_email(
                settings,
                email=email,
                brand_name=brand_name,
                domain=domain,
                view_url=view_url,
                intake_run_id=intake_run_id,
            )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] store email failed for %s", email)
    hubspot_recorded = False
    try:
        if view_url:
            hubspot_recorded = _record_store_hubspot_lead(
                settings,
                email=email,
                domain=domain,
                needs=needs,
                source=source,
                qualification=qualification,
                view_url=view_url,
            )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] store HubSpot lead recording failed for %s", email)
    try:
        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, intake_run_id)
            if run is not None:
                AuditService(session).finish_run(
                    run,
                    status="success",
                    summary={
                        **(run.summary_json or {}),
                        "delivered": "store_deck" if view_url else "store_ack_only",
                        "view_url": view_url,
                        "email_delivery": (
                            "delivered"
                            if email_delivered
                            else (run.summary_json or {}).get("email_delivery", "failed")
                        ),
                        "hubspot_handoff": (
                            "recorded"
                            if hubspot_recorded
                            else (run.summary_json or {}).get("hubspot_handoff", "failed")
                        ),
                    },
                )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] failed to update intake run %s", intake_run_id)


@router.post("/intake")
async def marketing_site_intake_create(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="intake:create", limit=30)
    if limited is not None:
        return limited

    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    identifier = str(body.get("identifier", "") or "").strip()
    kind = str(body.get("kind", "") or "").strip()
    source = _sanitize_source(body.get("source"), "site")
    if not identifier or len(identifier) > 2048:
        return JSONResponse(status_code=400, content={"detail": "identifier is required."})
    if kind not in {"asin", "store"}:
        return JSONResponse(status_code=400, content={"detail": "kind must be 'asin' or 'store'."})

    identity = _asin_identity(identifier) if kind == "asin" else _store_identity(identifier)
    token = secrets.token_urlsafe(16)

    with session_scope(request.app.state.session_factory) as session:
        run = AuditService(session).start_run(
            SITE_INTAKE_RUN_TYPE,
            trigger="marketing_site",
            metadata={"kind": kind, "identifier": identifier, "source": source},
        )
        brand_read = _compose_brand_read(identity, kind)
        run.summary_json = {
            "token": token,
            "kind": kind,
            "brand_read": brand_read,
            "brand_name": identity.get("brand_name", ""),
            "product_title": identity.get("product_title", ""),
            "product_image": identity.get("product_image", ""),
            "price": identity.get("price", ""),
            "rating": identity.get("rating", ""),
            "ratings_total": identity.get("ratings_total", ""),
            "asin": identity.get("asin", ""),
            "domain": identity.get("domain", ""),
            "needs": [],
        }
        session.add(run)
        intake_id = run.id

    payload: dict[str, Any] = {
        "intake_id": intake_id,
        "token": token,
        "kind": kind,
        "brand_name": identity.get("brand_name", ""),
        "product_title": identity.get("product_title", ""),
        "product_image": identity.get("product_image", ""),
        "price": identity.get("price", ""),
        "rating": identity.get("rating", ""),
        "ratings_total": identity.get("ratings_total", ""),
        "brand_read": brand_read,
    }
    # dtc_domain: for kind=asin the deck pipeline has no brand-website field to
    # reuse and we do not scrape search engines, so it is only present for
    # kind=store (the domain the visitor gave us).
    if kind == "store" and identity.get("domain"):
        payload["dtc_domain"] = identity["domain"]
    return JSONResponse(status_code=201, content=payload)


# Digital shelf: cap competitor product pulls and the overall build time so a
# slow Rainforest day cannot pin a worker (the shelf simply stays "pending"
# until the next status poll after completion, or lands "empty" on failure).
# Pull beyond the visible five because category/search results are often crowded
# by several variants from the same brand. The public payload still exposes only
# the best five distinct outside brands.
_SHELF_COMPETITOR_LIMIT = 24
_SHELF_MAX_ITEMS = 5
_SHELF_REQUIRED_ITEMS = 5
_SHELF_TIMEOUT_SECONDS = 180


def _write_shelf(app, intake_run_id: int, shelf: dict[str, Any]) -> None:
    """Merge-write summary_json.shelf without clobbering token/needs/identity."""
    with session_scope(app.state.session_factory) as session:
        run = session.get(AutomationRun, intake_run_id)
        if run is None:
            return
        run.summary_json = {**(run.summary_json or {}), "shelf": shelf}
        session.add(run)


def _shelf_product_payload(product: Any) -> dict[str, Any]:
    """Return a public, calculation-ready product record for the website.

    ``units_label`` carries a trailing ``+`` only when Amazon exposed its
    "bought in past month" floor. All other unit values are BSR estimates.
    """
    units_label = str(product.units_label or "")
    units_source = (
        "recent_sales"
        if units_label.endswith("+") and product.units_sold is not None
        else "bsr"
        if product.units_sold is not None
        else "unavailable"
    )
    revenue_source = units_source if product.revenue is not None else "unavailable"
    return {
        "asin": str(product.asin or ""),
        "title": str(product.title or ""),
        "brand": str(product.brand or ""),
        "image": str(product.image_url or ""),
        # Preserve the original formatted value while adding a numeric field
        # for calculations in the report.
        "price": str(product.price_label or ""),
        "price_value": float(product.price) if product.price is not None else None,
        "rating": float(product.rating) if product.rating is not None else None,
        "ratings_total": int(product.review_count) if product.review_count is not None else None,
        "category": str(product.category or ""),
        "bsr": int(product.bsr) if product.bsr is not None else None,
        "recent_sales": (
            int(product.units_sold)
            if units_source == "recent_sales" and product.units_sold is not None
            else None
        ),
        "estimated_units": int(product.units_sold) if product.units_sold is not None else None,
        "units_source": units_source,
        "estimated_revenue": float(product.revenue) if product.revenue is not None else None,
        "revenue_source": revenue_source,
        "fulfillment": str(product.fulfillment or ""),
        "dimensions": str(product.dimensions or ""),
        "weight": str(product.weight or ""),
    }


def _assemble_shelf_payload(
    target_product: Any | None,
    products: list[Any],
    warnings: list[str],
) -> dict[str, Any]:
    """Build the bounded public comparison payload used by the website."""
    target_brand = re.sub(
        r"[^a-z0-9]+", "", str(getattr(target_product, "brand", "") or "").lower()
    )
    visible_products = []
    seen: set[str] = set()
    for product in products:
        brand = re.sub(r"[^a-z0-9]+", "", str(getattr(product, "brand", "") or "").lower())
        key = brand or str(getattr(product, "asin", "") or "").strip().upper()
        if not key or key in seen or (target_brand and brand == target_brand):
            continue
        seen.add(key)
        visible_products.append(product)
        if len(visible_products) >= _SHELF_MAX_ITEMS:
            break
    competitors = [_shelf_product_payload(product) for product in visible_products]
    revenues = [
        float(product.revenue)
        for product in visible_products
        if product.revenue is not None
    ]
    prices = [float(product.price) for product in visible_products if product.price is not None]
    ratings = [float(product.rating) for product in visible_products if product.rating is not None]
    real_revenue_count = sum(
        1
        for product in visible_products
        if str(product.units_label or "").endswith("+") and product.revenue is not None
    )
    if visible_products and real_revenue_count == len(visible_products):
        revenue_warning = (
            "Unit/revenue figures use Amazon's real \"bought in past month\" "
            f"data for all {len(visible_products)} visible comparison listings (a reported floor)."
        )
    elif real_revenue_count:
        revenue_warning = (
            "Unit/revenue figures use Amazon's real \"bought in past month\" data "
            f"where available ({real_revenue_count} of {len(visible_products)} visible listings); "
            "the rest are BSR-based estimates."
        )
    elif visible_products:
        revenue_warning = (
            "Unit/revenue figures are BSR-based estimates because Amazon did not expose "
            "a recent-sales floor for the visible comparison listings."
        )
    else:
        revenue_warning = str(warnings[0]) if warnings else ""

    return {
        "status": (
            "ready"
            if len(competitors) >= _SHELF_REQUIRED_ITEMS
            else "incomplete"
            if competitors
            else "empty"
        ),
        "target": _shelf_product_payload(target_product) if target_product is not None else None,
        "competitors": competitors,
        # Existing fields remain for backwards compatibility.
        "count": len(visible_products),
        "avg_price": f"${sum(prices) / len(prices):.2f}" if prices else "",
        "avg_rating": f"{sum(ratings) / len(ratings):.1f}" if ratings else "",
        # New evidence contract.
        "comparison_count": len(competitors),
        "required_comparison_count": _SHELF_REQUIRED_ITEMS,
        "revenue_product_count": len(revenues),
        "visible_revenue": round(sum(revenues), 2) if revenues else None,
        "median_revenue": round(float(median(revenues)), 2) if revenues else None,
        "revenue_warning": revenue_warning,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def _shelf_has_complete_comparison(shelf: Any) -> bool:
    """Validate stored shelf evidence, including legacy payloads marked ready."""
    if not isinstance(shelf, dict) or shelf.get("status") != "ready":
        return False
    target = shelf.get("target") if isinstance(shelf.get("target"), dict) else {}
    target_brand = re.sub(r"[^a-z0-9]+", "", str(target.get("brand", "") or "").lower())
    keys: set[str] = set()
    for competitor in shelf.get("competitors") or []:
        if not isinstance(competitor, dict):
            continue
        brand = re.sub(r"[^a-z0-9]+", "", str(competitor.get("brand", "") or "").lower())
        if target_brand and brand == target_brand:
            continue
        key = brand or str(competitor.get("asin", "") or "").strip().upper()
        if key:
            keys.add(key)
    return len(keys) >= _SHELF_REQUIRED_ITEMS


def _build_shelf(app, intake_run_id: int, asin: str) -> None:
    """Background digital-shelf builder for ASIN intakes.

    Reuses the deck pipeline's competitor collection
    (RainforestClient.build_xray_report: bestsellers by category with keyword
    fallback, parallel product pulls). Only real Rainforest rows are stored;
    an empty result or any failure lands status "empty", never invented data.
    """
    from sales_support_agent.services.rainforest import RainforestClient

    try:
        _write_shelf(app, intake_run_id, {"status": "pending"})
        client = RainforestClient()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                client.build_xray_report,
                asin,
                competitor_limit=_SHELF_COMPETITOR_LIMIT,
                minimum_distinct_brands=_SHELF_REQUIRED_ITEMS,
            )
            xray_report, target_raw = future.result(timeout=_SHELF_TIMEOUT_SECONDS)

        target = str(asin).strip().upper()
        products = [p for p in xray_report.products if (p.asin or "").upper() != target]
        target_product = client._product_to_xray(target_raw, display_order=0)
        shelf = _assemble_shelf_payload(target_product, products, list(xray_report.warnings or []))
        _write_shelf(app, intake_run_id, shelf)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] shelf build failed for run %s", intake_run_id)
        try:
            _write_shelf(app, intake_run_id, {"status": "empty"})
        except Exception:  # noqa: BLE001
            logger.exception("[marketing_intake] shelf failure write failed for run %s", intake_run_id)


@router.post("/intake/{intake_id}/needs")
async def marketing_site_intake_needs(
    intake_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="intake:needs", limit=60)
    if limited is not None:
        return limited

    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    raw_needs = body.get("needs", [])
    if not isinstance(raw_needs, list):
        return JSONResponse(status_code=400, content={"detail": "needs must be a list."})
    needs = [str(n).strip().lower() for n in raw_needs if str(n).strip().lower() in _KNOWN_NEEDS]

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, str(body.get("token", "") or ""))
        if error is not None:
            return error
        summary = {**(run.summary_json or {}), "needs": needs}
        kind = str(summary.get("kind", "") or "")
        asin = str(summary.get("asin", "") or "")
        if kind == "asin" and asin and not _shelf_has_complete_comparison(summary.get("shelf")):
            summary["shelf"] = {"status": "pending"}
            background_tasks.add_task(_build_shelf, request.app, run.id, asin)
        run.summary_json = summary
        session.add(run)
    return JSONResponse(content={"status": "ok"})


@router.post("/intake/{intake_id}/unlock")
async def marketing_site_intake_unlock(
    intake_id: int,
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="intake:unlock", limit=30)
    if limited is not None:
        return limited

    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    email = str(body.get("email", "") or "").strip()
    if not email or not _EMAIL_RE.match(email):
        return JSONResponse(status_code=400, content={"detail": "A valid email is required."})

    qualification = _sanitize_qualification(body.get("qualification"))

    # Scripted submissions get the same answer a real one gets, but nothing
    # downstream runs: no acknowledgement, no alert, no HubSpot record, no
    # paid analysis. Telling a bot it failed only teaches it what to change.
    spam_signals = junk_signals(email=email, qualification=qualification)
    if len(spam_signals) >= 2:
        logger.warning(
            "[marketing_intake] suppressed automated submission for intake %s (%s)",
            intake_id,
            ", ".join(spam_signals),
        )
        return JSONResponse(
            status_code=202,
            content={
                "status": "building",
                "delivery_status": {},
                "closers": {"software": False, "services": False},
            },
        )

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, str(body.get("token", "") or ""))
        if error is not None:
            return error
        if _daily_gate_enabled() and _today_intakes_for_email(session, email):
            return JSONResponse(status_code=429, content={"reason": "daily_limit"})
        summary = run.summary_json or {}
        kind = str(summary.get("kind", "") or "")
        asin = str(summary.get("asin", "") or "")
        domain = str(summary.get("domain", "") or "")
        brand_name = str(summary.get("brand_name", "") or "")
        needs = [str(n) for n in (summary.get("needs") or [])]
        source = str((run.metadata_json or {}).get("source", "") or "")
        # Record the email on the run so the shared daily gate sees it.
        consent_version = str(body.get("consent_version", "") or "").strip()[:64]
        run.metadata_json = {
            **(run.metadata_json or {}),
            "email": email.lower(),
            **({"consent_version": consent_version} if consent_version else {}),
            **({"qualification": qualification} if qualification else {}),
        }
        session.add(run)
        run_id = run.id
        _bind_daily_email(
            session,
            run_id=int(run_id),
            email=email,
        )

    acknowledgement_sent = False
    internal_lead_sent = False
    hubspot_recorded = False
    display_name = brand_name or asin or domain
    try:
        acknowledgement_sent = _send_unlock_ack_email(
            request.app.state.settings,
            email=email,
            display_name=display_name,
            kind=kind,
            intake_run_id=run_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "[marketing_intake] acknowledgement email failed for intake %s",
            run_id,
        )
    try:
        internal_lead_sent = _send_internal_lead_email(
            request.app.state.settings,
            email=email,
            kind=kind,
            identifier=asin or domain,
            brand_name=brand_name,
            source=source,
            needs=needs,
            qualification=qualification,
            intake_run_id=run_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "[marketing_intake] internal lead email failed for intake %s",
            run_id,
        )
    try:
        if kind == "asin" and asin:
            hubspot_recorded = _record_hubspot_lead(
                request.app.state.settings,
                email=email,
                asin=asin,
                view_url="",
                source=source,
                needs=needs,
                qualification=qualification,
            )
        else:
            hubspot_recorded = _record_store_hubspot_lead(
                request.app.state.settings,
                email=email,
                domain=domain,
                needs=needs,
                source=source,
                qualification=qualification,
            )
    except Exception:  # noqa: BLE001
        logger.exception(
            "[marketing_intake] immediate HubSpot handoff failed for intake %s",
            run_id,
        )

    _write_intake_delivery_state(
        request.app,
        run_id,
        acknowledgement_email=(
            "delivered" if acknowledgement_sent else "failed"
        ),
        internal_lead_email=("delivered" if internal_lead_sent else "failed"),
        hubspot_handoff=("recorded" if hubspot_recorded else "failed"),
    )

    if kind == "asin" and asin:
        background_tasks.add_task(
            _run_analysis_and_deliver,
            request.app,
            intake_run_id=run_id,
            asin=asin,
            email=email,
            source=source,
            trigger="marketing_site_intake",
            needs=needs,
            qualification=qualification,
        )
    else:
        background_tasks.add_task(
            _deliver_store_unlock,
            request.app,
            intake_run_id=run_id,
            email=email,
            domain=domain,
            brand_name=brand_name,
            needs=needs,
            source=source,
            qualification=qualification,
        )

    captured = internal_lead_sent or hubspot_recorded
    with session_scope(request.app.state.session_factory) as session:
        persisted_run = session.get(AutomationRun, run_id)
        delivery_status = (
            _public_intake_delivery_status(persisted_run)
            if persisted_run is not None
            else {}
        )
    response_content = {
        "status": "building" if captured else "delivery_unavailable",
        "delivery_status": delivery_status,
        "delivery": {
            "acknowledgement": acknowledgement_sent,
            "internal_notification": internal_lead_sent,
            "hubspot": hubspot_recorded,
        },
        "closers": {
            "software": "analytics" in needs,
            "services": bool(set(needs) & _SERVICES_NEEDS),
        },
    }
    if not captured:
        response_content["detail"] = (
            "We could not confirm the internal handoff. Please try again."
        )
    return JSONResponse(
        status_code=202 if captured else 503,
        content=response_content,
    )


@router.post("/booking")
async def marketing_site_direct_booking(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Attach a direct website calendar booking to the matching HubSpot lead.

    HubSpot creates the contact and meeting first. This endpoint only accepts a
    booking-success handoff from the server-side website proxy, finds that
    existing contact, and advances or creates the matching tool deal.
    """
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="booking:create", limit=30)
    if limited is not None:
        return limited
    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    email = str(body.get("email", "") or "").strip().lower()[:254]
    tool = str(body.get("tool", "") or "").strip().lower()[:24]
    source = _sanitize_source(body.get("source"), "booking-page")
    booking_reference = str(body.get("booking_reference", "") or "").strip()[:160]
    if not email or not _EMAIL_RE.fullmatch(email):
        return JSONResponse(
            status_code=400,
            content={"detail": "A valid booked contact email is required."},
        )
    if tool not in {"strategy", "rate", "ads", "profit"}:
        return JSONResponse(
            status_code=400,
            content={"detail": "A recognized booking tool is required."},
        )
    qualification = _sanitize_qualification(body.get("qualification"))
    idempotency_key = _booking_idempotency_key(
        email=email,
        tool=tool,
        source=source,
        booking_reference=booking_reference,
    )

    with session_scope(request.app.state.session_factory) as session:
        existing = _find_booking_run(session, idempotency_key)
        if existing and existing.status == "success":
            return JSONResponse(
                content={
                    "status": "recorded",
                    "deal_id": str((existing.summary_json or {}).get("deal_id", "") or ""),
                    "notification": str(
                        (existing.summary_json or {}).get("notification", "pending") or "pending"
                    ),
                    "duplicate": True,
                }
            )
        booking_run = AuditService(session).start_run(
            f"{BOOKING_RUN_TYPE_PREFIX}{idempotency_key[:40]}",
            trigger="marketing_site_booking",
            metadata={
                "email": email,
                "tool": tool,
                "source": source,
                "booking_reference": booking_reference,
                "idempotency_key": idempotency_key,
            },
        )
        booking_run_id = booking_run.id

    try:
        recorded, deal_id = _record_hubspot_booking(
            request.app.state.settings,
            email=email,
            brand_name=qualification.get("company", ""),
            source=source,
            tool=tool,
            booking_reference=booking_reference,
            qualification=qualification,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_booking] direct booking handoff failed for %s", email)
        recorded, deal_id = False, ""

    notification_sent = False
    if recorded:
        try:
            notification_sent = _send_internal_booking_email(
                request.app.state.settings,
                email=email,
                tool=tool,
                source=source,
                qualification=qualification,
                deal_id=deal_id,
                idempotency_key=idempotency_key,
            )
        except Exception:  # noqa: BLE001
            logger.exception("[marketing_booking] internal booking email failed for %s", email)

    with session_scope(request.app.state.session_factory) as session:
        run = session.get(AutomationRun, booking_run_id)
        if run is not None:
            AuditService(session).finish_run(
                run,
                status="success" if recorded else "failed",
                summary={
                    "deal_id": deal_id,
                    "notification": "delivered" if notification_sent else "failed",
                },
            )

    if not recorded:
        return JSONResponse(
            status_code=502,
            content={"detail": "The HubSpot booking handoff did not complete."},
        )
    return JSONResponse(
        content={
            "status": "recorded",
            "deal_id": deal_id,
            "notification": "delivered" if notification_sent else "failed",
            "duplicate": False,
        }
    )


@router.post("/intake/{intake_id}/booked")
async def marketing_site_intake_booked(
    intake_id: int,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Record a confirmed HubSpot Meetings booking against the captured lead."""
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="booking:tokenized", limit=30)
    if limited is not None:
        return limited
    body, bad = await read_public_json_object(request)
    if bad is not None:
        return bad
    assert body is not None

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(
            session,
            intake_id,
            str(body.get("token", "") or ""),
        )
        if error is not None:
            return error
        summary = dict(run.summary_json or {})
        metadata = dict(run.metadata_json or {})
        if summary.get("booking_handoff") == "recorded":
            return JSONResponse(
                content={
                    "status": "recorded",
                    "deal_id": str(summary.get("booking_deal_id", "") or ""),
                    "duplicate": True,
                }
            )
        email = str(metadata.get("email", "") or "").strip().lower()
        brand_name = str(summary.get("brand_name", "") or "")
        source = _sanitize_source(
            body.get("source") or metadata.get("source"),
            "booking-page",
        )
        qualification = _sanitize_qualification(metadata.get("qualification"))

    if not email:
        return JSONResponse(
            status_code=409,
            content={"detail": "The intake has no captured contact email."},
        )

    try:
        recorded, deal_id = _record_hubspot_booking(
            request.app.state.settings,
            email=email,
            brand_name=brand_name,
            source=source,
            qualification=qualification,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "[marketing_intake] booking handoff failed for intake %s",
            intake_id,
        )
        recorded, deal_id = False, ""

    if not recorded:
        _write_intake_delivery_state(
            request.app,
            intake_id,
            booking_handoff="failed",
        )
        return JSONResponse(
            status_code=502,
            content={"detail": "The HubSpot booking handoff did not complete."},
        )

    _write_intake_delivery_state(
        request.app,
        intake_id,
        booking_handoff="recorded",
        booking_deal_id=deal_id,
        booking_recorded_at=datetime.now(timezone.utc).isoformat(),
    )
    return JSONResponse(
        content={"status": "recorded", "deal_id": deal_id, "duplicate": False}
    )


@router.get("/intake/{intake_id}")
def marketing_site_intake_status(
    intake_id: int,
    request: Request,
    token: str = "",
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_marketing_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    limited = durable_rate_limit_response(request, scope="intake:read", limit=240)
    if limited is not None:
        return limited

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, token)
        if error is not None:
            return error
        summary = run.summary_json or {}
        return JSONResponse(
            content={
                "status": run.status,
                "kind": str(summary.get("kind", "") or ""),
                "dtc_domain": (
                    str(summary.get("domain", "") or "")
                    if str(summary.get("kind", "") or "") == "store"
                    else ""
                ),
                "brand_name": str(summary.get("brand_name", "") or ""),
                "product_title": str(summary.get("product_title", "") or ""),
                "product_image": str(summary.get("product_image", "") or ""),
                "price": str(summary.get("price", "") or ""),
                "rating": str(summary.get("rating", "") or ""),
                "ratings_total": str(summary.get("ratings_total", "") or ""),
                "brand_read": str(summary.get("brand_read", "") or ""),
                "needs": [str(n) for n in (summary.get("needs") or [])],
                "shelf": summary.get("shelf") or None,
                "delivery_status": _public_intake_delivery_status(run),
            }
        )
