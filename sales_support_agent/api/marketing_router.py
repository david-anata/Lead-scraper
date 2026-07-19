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
import logging
import re
import secrets
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

# Run type for the two-step site intake (identifier → needs → email unlock).
SITE_INTAKE_RUN_TYPE = "marketing_intake"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Needs chips the site can send; anything else is dropped silently.
_KNOWN_NEEDS = {"analytics", "advertising", "strategy", "catalog", "creative", "fulfillment"}
_SERVICES_NEEDS = {"advertising", "strategy", "catalog", "creative", "fulfillment"}

# Hard ceiling on the cheap identity lookups so the intake endpoint stays fast.
_IDENTITY_TIMEOUT_SECONDS = 10


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


def _today_intakes_for_email(
    session, email: str, run_types: tuple[str, ...] = (INTAKE_RUN_TYPE, SITE_INTAKE_RUN_TYPE)
) -> list[AutomationRun]:
    """Intake runs started today (UTC) for this email, across both the
    one-shot analysis flow and the two-step site intake so the one-per-email-
    per-UTC-day gate is shared. Metadata is JSON, so we filter by email in
    Python; the date filter keeps the scan tiny."""
    midnight_utc = datetime.combine(datetime.utcnow().date(), dt_time.min)
    rows = session.execute(
        select(AutomationRun).where(
            AutomationRun.run_type.in_(run_types),
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


def _record_hubspot_lead(
    settings, *, email: str, asin: str, view_url: str, source: str, needs: Optional[list[str]] = None
) -> None:
    """Create the contact (standard email property only; custom properties are
    not confirmed to exist in the portal) and attach the run details as a note.
    On a duplicate-email 409, reuse the existing contact id HubSpot reports."""
    client = HubSpotClient(settings)
    if not client.is_configured:  # property, not a method (hubspot.py:98)
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
    if needs:
        note_body += f"<br>Needs: {', '.join(needs)}"
    try:
        client.create_contact_note(contact_id=contact_id, body=note_body)
    except Exception as exc:  # noqa: BLE001 — the contact itself is the critical write
        logger.warning("[marketing_intake] HubSpot note failed for contact %s: %s", contact_id, exc)


def _run_analysis_and_deliver(
    app,
    *,
    intake_run_id: int,
    asin: str,
    email: str,
    source: str,
    trigger: str = "marketing_site",
    needs: Optional[list[str]] = None,
) -> None:
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
                trigger=trigger,
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
                    # Merge so the site-intake summary (token, needs, brand
                    # identity) survives the deck-completion update.
                    summary={**(run.summary_json or {}), "view_url": view_url, "error": error_message},
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
        _record_hubspot_lead(settings, email=email, asin=asin, view_url=view_url, source=source, needs=needs)
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


# ---------------------------------------------------------------------------
# Two-step site intake (identifier -> needs -> email unlock)
# ---------------------------------------------------------------------------


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


def _send_store_ack_email(settings, *, email: str, brand_name: str, domain: str) -> None:
    """Store-only unlock: no deck, acknowledge the page and point to booking."""
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[marketing_intake] Resend not configured; skipping store ack email to %s", email)
        return
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
    )


def _record_store_hubspot_lead(settings, *, email: str, domain: str, needs: list[str], source: str) -> None:
    client = HubSpotClient(settings)
    if not client.is_configured:
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
        "Site intake from the marketing site (store, no ASIN)."
        f"<br>Store: {domain}"
        f"<br>Source: {source or 'anatainc.com'}"
    )
    if needs:
        note_body += f"<br>Needs: {', '.join(needs)}"
    try:
        client.create_contact_note(contact_id=contact_id, body=note_body)
    except Exception as exc:  # noqa: BLE001 — the contact itself is the critical write
        logger.warning("[marketing_intake] HubSpot note failed for contact %s: %s", contact_id, exc)


def _deliver_store_unlock(app, *, intake_run_id: int, email: str, domain: str, brand_name: str, needs: list[str], source: str) -> None:
    """Background task for kind=store unlock: no deck, just ack email + HubSpot."""
    settings = app.state.settings
    try:
        _send_store_ack_email(settings, email=email, brand_name=brand_name, domain=domain)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] store ack email failed for %s", email)
    try:
        _record_store_hubspot_lead(settings, email=email, domain=domain, needs=needs, source=source)
    except Exception:  # noqa: BLE001
        logger.exception("[marketing_intake] store HubSpot lead recording failed for %s", email)
    try:
        with session_scope(app.state.session_factory) as session:
            run = session.get(AutomationRun, intake_run_id)
            if run is not None:
                AuditService(session).finish_run(
                    run,
                    status="success",
                    summary={**(run.summary_json or {}), "delivered": "store_ack"},
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

    try:
        body: dict[str, Any] = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"detail": "Request body must be valid JSON."})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"detail": "Request body must be a JSON object."})

    identifier = str(body.get("identifier", "") or "").strip()
    kind = str(body.get("kind", "") or "").strip()
    source = str(body.get("source", "") or "").strip()
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
        run.summary_json = {
            "token": token,
            "kind": kind,
            "brand_name": identity.get("brand_name", ""),
            "product_title": identity.get("product_title", ""),
            "product_image": identity.get("product_image", ""),
            "asin": identity.get("asin", ""),
            "domain": identity.get("domain", ""),
            "needs": [],
        }
        session.add(run)
        intake_id = run.id

    payload: dict[str, Any] = {
        "intake_id": intake_id,
        "token": token,
        "brand_name": identity.get("brand_name", ""),
        "product_title": identity.get("product_title", ""),
        "product_image": identity.get("product_image", ""),
    }
    # dtc_domain: for kind=asin the deck pipeline has no brand-website field to
    # reuse and we do not scrape search engines, so it is only present for
    # kind=store (the domain the visitor gave us).
    if kind == "store" and identity.get("domain"):
        payload["dtc_domain"] = identity["domain"]
    return JSONResponse(status_code=201, content=payload)


@router.post("/intake/{intake_id}/needs")
async def marketing_site_intake_needs(
    intake_id: int,
    request: Request,
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

    raw_needs = body.get("needs", [])
    if not isinstance(raw_needs, list):
        return JSONResponse(status_code=400, content={"detail": "needs must be a list."})
    needs = [str(n).strip().lower() for n in raw_needs if str(n).strip().lower() in _KNOWN_NEEDS]

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, str(body.get("token", "") or ""))
        if error is not None:
            return error
        run.summary_json = {**(run.summary_json or {}), "needs": needs}
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

    try:
        body: dict[str, Any] = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"detail": "Request body must be valid JSON."})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"detail": "Request body must be a JSON object."})

    email = str(body.get("email", "") or "").strip()
    if not email or not _EMAIL_RE.match(email):
        return JSONResponse(status_code=400, content={"detail": "A valid email is required."})

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, str(body.get("token", "") or ""))
        if error is not None:
            return error
        if _today_intakes_for_email(session, email):
            return JSONResponse(status_code=429, content={"reason": "daily_limit"})
        summary = run.summary_json or {}
        kind = str(summary.get("kind", "") or "")
        asin = str(summary.get("asin", "") or "")
        domain = str(summary.get("domain", "") or "")
        brand_name = str(summary.get("brand_name", "") or "")
        needs = [str(n) for n in (summary.get("needs") or [])]
        source = str((run.metadata_json or {}).get("source", "") or "")
        # Record the email on the run so the shared daily gate sees it.
        run.metadata_json = {**(run.metadata_json or {}), "email": email.lower()}
        session.add(run)
        run_id = run.id

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
        )

    return JSONResponse(
        status_code=202,
        content={
            "status": "building",
            "closers": {
                "software": "analytics" in needs,
                "services": bool(set(needs) & _SERVICES_NEEDS),
            },
        },
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

    with session_scope(request.app.state.session_factory) as session:
        run, error = _load_site_intake(session, intake_id, token)
        if error is not None:
            return error
        summary = run.summary_json or {}
        return JSONResponse(
            content={
                "status": run.status,
                "brand_name": str(summary.get("brand_name", "") or ""),
                "product_title": str(summary.get("product_title", "") or ""),
                "product_image": str(summary.get("product_image", "") or ""),
                "needs": [str(n) for n in (summary.get("needs") or [])],
            }
        )
