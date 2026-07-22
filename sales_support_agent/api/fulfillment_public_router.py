"""Public self-serve funnel for the Fulfillment Rate Sheet (anatainc.com).

The marketing site posts server-side with the shared secret header
(``X-Internal-Api-Key`` == ``MARKETING_SITE_INTAKE_KEY`` — the same convention
as ``marketing_router``, and a separate secret from the sales agent key). No
CORS: browsers never call these routes directly.

Two steps, wrapped around the SAME rate-sheet engine the rep-driven admin
generator uses (``services.fulfillment_deck.service``):

  POST /api/public/fulfillment/rate-sheet/taste
      {url, segment:"dfy"|"diy", origin_zip?, source?} -> extraction + a TRIMMED rate pass
      (a few products, for speed), saves a DRAFT run, and returns TEASER fields
      only (blended rate, transit, product/brand). No full sheet, no email.

  POST /api/public/fulfillment/rate-sheet/unlock
      {run_id, token, email, monthly_orders?, origin_zip?} -> applies the
      optional refinements, completes + publishes the sheet, emails the
      tokenized public view URL, and fires the existing HubSpot company+deal
      sync. Returns 202 {status:"building"}.

The rep-driven flow (admin_router) is untouched; this is a public funnel around
the same engine.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.config import load_settings
from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.schema import RATE_SOURCE_WMS, clean_segment, clean_zip
from sales_support_agent.services.fulfillment_deck.service import (
    apply_profile_edits,
    generate_rate_sheet,
    rerender_rate_sheet,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/public/fulfillment", tags=["fulfillment-public"])

# Cap the teaser rate pass so the synchronous taste call stays fast; the full
# product catalog is still persisted for the published sheet.
TASTE_MAX_PRODUCTS = 3

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _enforce_intake_key(request: Request, provided: Optional[str]) -> Optional[JSONResponse]:
    """Shared-secret gate (mirrors marketing_router._enforce_marketing_intake_key)."""
    configured = str(getattr(request.app.state.settings, "marketing_site_intake_key", "") or "").strip()
    if not configured:
        return JSONResponse(status_code=503, content={"detail": "Fulfillment intake is not configured."})
    if str(provided or "").strip() != configured:
        return JSONResponse(status_code=401, content={"detail": "Invalid intake key."})
    return None


async def _json_body(request: Request) -> tuple[Optional[dict], Optional[JSONResponse]]:
    try:
        body: Any = await request.json()
    except Exception:  # noqa: BLE001
        return None, JSONResponse(status_code=400, content={"detail": "Request body must be valid JSON."})
    if not isinstance(body, dict):
        return None, JSONResponse(status_code=400, content={"detail": "Request body must be a JSON object."})
    return body, None


def _is_bot(body: dict) -> bool:
    """Honeypot: a filled hidden field means a bot (mirrors the marketing form
    convention). Real visitors never populate it."""
    for field in ("hp", "website_hp", "company_website"):
        if str(body.get(field) or "").strip():
            return True
    return False


# ---------------------------------------------------------------------------
# Step 1 — taste (teaser)
# ---------------------------------------------------------------------------


@router.post("/rate-sheet/taste")
async def rate_sheet_taste(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    body, bad = await _json_body(request)
    if bad is not None:
        return bad

    if _is_bot(body):
        # Look like success without doing any work.
        return JSONResponse(status_code=202, content={"status": "building"})

    url = str(body.get("url", "") or "").strip()
    if not url or len(url) > 2048:
        return JSONResponse(status_code=400, content={"detail": "url is required (your store or product page)."})
    segment = clean_segment(body.get("segment"))
    origin_zip = str(body.get("origin_zip", "") or "").strip()
    if segment == "diy" and clean_zip(origin_zip) is None:
        return JSONResponse(
            status_code=400,
            content={"detail": "A valid ship-from ZIP is required when you ship from your own dock."},
        )
    source = str(body.get("source", "") or "").strip()[:120]

    try:
        result = generate_rate_sheet(
            settings=request.app.state.settings if getattr(request.app.state, "settings", None) else load_settings(),
            notes="",
            files=[],
            website_url=url,
            origin_zip=origin_zip if segment == "diy" else "",
            segment=segment,
            max_products=TASTE_MAX_PRODUCTS,
            trigger="public_funnel",
            suppress_fulfillment_pricing=True,
        )
    except Exception as exc:  # noqa: BLE001 — never leak internals to the public
        logger.exception("[fulfillment_public] taste generation failed for %s", url[:120])
        return JSONResponse(status_code=502, content={"detail": "We could not build a teaser from that link. Please try another URL."})

    # Record the funnel source (audit only; the segment already lives in summary).
    if source:
        try:
            storage.update_summary(result["run_id"], {"public_source": source})
        except Exception:  # noqa: BLE001
            logger.debug("[fulfillment_public] source persist failed", exc_info=True)

    profile = dict(result.get("prospect_profile") or {})
    product_count = len(profile.get("products") or [])
    brand_name = str(result.get("prospect") or "").strip()

    rates_source = str(result.get("rates_source") or "")
    live_rates = rates_source == "wms"

    # TEASER FIELDS ONLY — no deck_html, no full profile, no email. Mock/sample
    # rates are useful for internal rendering tests but must never be presented
    # to a public visitor as a real quote.
    return JSONResponse(
        status_code=202,
        content={
            "run_id": result["run_id"],
            "token": str(result.get("export_token") or ""),
            "carrier_rate": result.get("blended_rate") if live_rates else None,
            "avg_transit_days": result.get("avg_transit_days") if live_rates else None,
            "rates_source": "live" if live_rates else "unavailable",
            "excludes_3pl_fees": True,
            "product_count": product_count,
            "brand_name": brand_name,
        },
    )


# ---------------------------------------------------------------------------
# Step 2 — unlock (publish + deliver)
# ---------------------------------------------------------------------------


def _absolute_view_url(settings, view_path: str) -> str:
    base = str(getattr(settings, "deck_public_base_url", "") or "").strip().rstrip("/")
    path = str(view_path or "").strip()
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}" if base else path


def _send_unlock_email(settings, *, email: str, brand: str, view_url: str) -> None:
    client = ResendClient(settings)
    if not client.is_configured():
        logger.warning("[fulfillment_public] Resend not configured; skipping email to %s", email)
        return
    label = brand or "your brand"
    booking_url = str(getattr(settings, "marketing_booking_url", "") or "").strip()
    lines = [
        "Hi,",
        "",
        f"Your Anata rate sheet for {label} is ready. You can view it here:",
        view_url,
        "",
    ]
    if booking_url:
        lines += [
            "If you want help putting these rates to work, you can grab time with us here:",
            booking_url,
            "",
        ]
    lines += ["Anata"]
    client.send_message(
        to=email,
        subject="Your Anata rate sheet is ready",
        text="\n".join(lines),
    )


def _finish_unlock(
    app,
    *,
    run_id: int,
    email: str,
    monthly_orders: Optional[int],
    origin_zip: str,
) -> None:
    """Background task: apply refinements, complete + publish, email the
    tokenized view URL, and fire the HubSpot company+deal sync."""
    settings = getattr(app.state, "settings", None) or load_settings()
    try:
        edits: dict = {}
        if monthly_orders is not None:
            edits["monthly_order_volume"] = monthly_orders
        cleaned_origin = clean_zip(origin_zip) if origin_zip else None
        if cleaned_origin:
            edits["origin_zip"] = cleaned_origin
        # Rebuild the full sheet (the taste render may have been trimmed).
        if edits:
            apply_profile_edits(run_id, edits, settings=settings)
        else:
            rerender_rate_sheet(run_id, settings=settings)
    except Exception:  # noqa: BLE001
        logger.exception("[fulfillment_public] refinement/rerender failed for run %d", run_id)

    refreshed = storage.get_run(run_id)
    refreshed_summary = dict(refreshed.summary_json or {}) if refreshed is not None else {}
    if str(refreshed_summary.get("rates_source") or "") != RATE_SOURCE_WMS:
        logger.warning(
            "[fulfillment_public] live rates unavailable after rerender for run %d; skipping publish and delivery",
            run_id,
        )
        return

    published = False
    try:
        published = storage.publish_run(run_id)
    except Exception:  # noqa: BLE001
        logger.exception("[fulfillment_public] publish failed for run %d", run_id)
    if not published:
        logger.warning("[fulfillment_public] run %d not published; skipping delivery", run_id)
        return

    run = storage.get_run(run_id)
    summary = dict(run.summary_json or {}) if run is not None else {}
    profile = dict(summary.get("prospect_profile") or {})
    brand = str(summary.get("prospect") or "")
    view_url = _absolute_view_url(settings, str(summary.get("view_path") or ""))

    # Store the requester email for first-view notifications / audit.
    try:
        storage.update_summary(run_id, {"public_unlock_email": email})
    except Exception:  # noqa: BLE001
        logger.debug("[fulfillment_public] email persist failed", exc_info=True)

    if view_url:
        try:
            _send_unlock_email(settings, email=email, brand=brand, view_url=view_url)
        except Exception:  # noqa: BLE001
            logger.exception("[fulfillment_public] unlock email failed for %s", email)

    # HubSpot company+deal sync (segment rides in summary -> deal brief).
    try:
        from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_new_prospect
        sync_new_prospect(run_id, summary, profile)
    except Exception:  # noqa: BLE001
        logger.exception("[fulfillment_public] hubspot sync_new_prospect failed for run %d", run_id)


@router.post("/rate-sheet/unlock")
async def rate_sheet_unlock(
    request: Request,
    background_tasks: BackgroundTasks,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    denied = _enforce_intake_key(request, x_internal_api_key)
    if denied is not None:
        return denied
    body, bad = await _json_body(request)
    if bad is not None:
        return bad

    if _is_bot(body):
        return JSONResponse(status_code=202, content={"status": "building"})

    try:
        run_id = int(body.get("run_id"))
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"detail": "run_id is required."})
    token = str(body.get("token", "") or "").strip()
    email = str(body.get("email", "") or "").strip()
    if not token:
        return JSONResponse(status_code=400, content={"detail": "token is required."})
    if not email or not _EMAIL_RE.match(email) or len(email) > 200:
        return JSONResponse(status_code=400, content={"detail": "A valid email is required."})

    run = storage.get_run(run_id)
    if run is None:
        return JSONResponse(status_code=404, content={"detail": "Rate sheet not found."})
    summary = dict(run.summary_json or {})
    if token != str(summary.get("export_token") or ""):
        return JSONResponse(status_code=404, content={"detail": "Rate sheet not found."})
    if str(summary.get("rates_source") or "") != RATE_SOURCE_WMS:
        return JSONResponse(
            status_code=503,
            content={"detail": "Live carrier rates are temporarily unavailable. Please build a new preview and try again."},
        )
    consent_version = str(body.get("consent_version", "") or "").strip()[:64]
    if consent_version:
        try:
            storage.update_summary(run_id, {"consent_version": consent_version})
        except Exception:  # noqa: BLE001
            logger.debug("[fulfillment_public] consent persist failed", exc_info=True)

    monthly_orders: Optional[int] = None
    raw_orders = body.get("monthly_orders")
    if raw_orders is not None and str(raw_orders).strip() != "":
        try:
            monthly_orders = int(raw_orders)
        except (TypeError, ValueError):
            monthly_orders = None
        if monthly_orders is not None and monthly_orders <= 0:
            monthly_orders = None
    origin_zip = str(body.get("origin_zip", "") or "").strip()

    background_tasks.add_task(
        _finish_unlock,
        request.app,
        run_id=run_id,
        email=email,
        monthly_orders=monthly_orders,
        origin_zip=origin_zip,
    )
    return JSONResponse(status_code=202, content={"status": "building"})
