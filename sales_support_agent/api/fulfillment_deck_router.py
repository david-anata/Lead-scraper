"""Fulfillment > Sales Deck (rate sheets) controller.

Two routers, both mounted in-process by the frontend (same pattern as
brand_analysis_router):
  * admin_router  — /admin/fulfillment/sales (generator form, history, delete),
                    tool-gated by `fulfillment.rate_sheets`.
  * public_router — /rate-sheets/{slug}/{run_id}/{token} hosted view +
                    /heartbeat engagement endpoint, token-gated only.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import load_settings
from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import DeckSectionView, DeckVisitSession
from sales_support_agent.services.auth_deps import (
    get_current_user,
    require_tool,
)
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.schema import (
    ProductSpec,
    RateMatrix,
    clean_zip,
)
from sales_support_agent.services.fulfillment_deck.us_map import map_payload
from sales_support_agent.services.fulfillment_deck.admin_page import (
    render_fulfillment_sales_page,
    render_rate_sheet_review_page,
)
from sales_support_agent.services.fulfillment_deck.service import (
    apply_profile_edits,
    apply_viewer_requote,
    generate_rate_sheet,
)
from sales_support_agent.services.visitor_meta import (
    MAX_SESSION_SECONDS,
    categorize_referrer,
    extract_visitor_geo,
    parse_user_agent,
)

logger = logging.getLogger(__name__)

_BASE = "/admin/fulfillment/sales"

admin_router = APIRouter(
    prefix=_BASE,
    tags=["fulfillment-rate-sheets"],
    dependencies=[Depends(require_tool("fulfillment.rate_sheets"))],
)

public_router = APIRouter(tags=["fulfillment-rate-sheets-public"])


# ---------------------------------------------------------------------------
# Admin pages
# ---------------------------------------------------------------------------


@admin_router.get("", response_class=HTMLResponse)
def landing(request: Request, msg: str = "", kind: str = "") -> HTMLResponse:
    runs = storage.list_runs()
    engagement = storage.engagement_for([r["id"] for r in runs])
    return HTMLResponse(
        render_fulfillment_sales_page(
            runs,
            engagement,
            user=get_current_user(request),
            flash=msg,
            flash_kind=kind,
        )
    )


@admin_router.post("/generate")
async def generate(
    notes: str = Form(default=""),
    files: list[UploadFile] = File(default=[]),
    website_url: str = Form(default=""),
    brand: str = Form(default=""),
    origin_zip: str = Form(default=""),
) -> RedirectResponse:
    batch: list[tuple[str, bytes]] = []
    for f in files or []:
        if f is not None and f.filename:
            data = await f.read()
            if data:
                batch.append((f.filename, data))

    if not (notes or "").strip() and not batch and not (website_url or "").strip():
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus("Add some notes, a file, or a website URL first — the rate sheet is built from whatever you provide."),
            status_code=303,
        )

    try:
        result = generate_rate_sheet(
            settings=load_settings(),
            notes=notes or "",
            files=batch,
            website_url=(website_url or "").strip(),
            origin_zip=(origin_zip or "").strip(),
            brand_override=(brand or "").strip(),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[fulfillment_deck] generate failed")
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus(f"Generation failed: {str(exc)[:140]}"),
            status_code=303,
        )

    # Land on the review page — the sheet stays a draft until published there.
    review_path = result.get("review_path") or f"{_BASE}/runs/{result['run_id']}/review"
    try:
        from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_new_prospect as _hs_new
        _run = storage.get_run(result["run_id"])
        if _run is not None:
            _summary = dict(_run.summary_json or {})
            _hs_new(result["run_id"], _summary, dict(_summary.get("prospect_profile") or {}))
    except Exception:
        logger.exception("[fulfillment_deck] hubspot sync_new failed")
    return RedirectResponse(review_path, status_code=303)


# ---------------------------------------------------------------------------
# Review / edit / publish (draft lifecycle)
# ---------------------------------------------------------------------------


def _load_reviewable_run(run_id: int):
    """Run + summary for the review page — drafts and published runs only."""
    run = storage.get_run(run_id)
    if run is None or run.status not in ("draft", "completed"):
        return None, None
    summary = dict(run.summary_json or {})
    if not summary.get("deck_html"):
        return None, None
    return run, summary


@admin_router.get("/runs/{run_id}/review", response_class=HTMLResponse)
def review_run(request: Request, run_id: int, msg: str = ""):
    run, summary = _load_reviewable_run(run_id)
    if run is None:
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus("Rate sheet not found or not reviewable."),
            status_code=303,
        )
    return HTMLResponse(
        render_rate_sheet_review_page(
            {"id": run_id, "status": run.status},
            summary,
            user=get_current_user(request),
            flash=msg,
        )
    )


@admin_router.get("/runs/{run_id}/preview", response_class=HTMLResponse)
def preview_run(run_id: int) -> HTMLResponse:
    """Admin-gated preview of the rendered sheet, any status — feeds the
    review page's iframe so drafts never need a live public link."""
    run, summary = _load_reviewable_run(run_id)
    if run is None:
        return HTMLResponse("Rate sheet not found.", status_code=404)
    return HTMLResponse(str(summary.get("deck_html") or ""))


def _opt_int(value: str):
    value = (value or "").replace(",", "").strip()
    if not value:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _opt_float(value: str):
    value = (value or "").replace("$", "").replace(",", "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


@admin_router.post("/runs/{run_id}/update")
def update_run(
    run_id: int,
    brand: str = Form(default=""),
    origin_zip: str = Form(default=""),
    monthly_order_volume: str = Form(default=""),
    current_cost_per_parcel_usd: str = Form(default=""),
    quote_margin_override: str = Form(default=""),
    destinations_note: str = Form(default=""),
    current_costs_note: str = Form(default=""),
    product_name: list[str] = Form(default=[]),
    product_length: list[str] = Form(default=[]),
    product_width: list[str] = Form(default=[]),
    product_height: list[str] = Form(default=[]),
    product_weight: list[str] = Form(default=[]),
    product_units: list[str] = Form(default=[]),
    product_estimated: list[str] = Form(default=[]),
    product_remove: list[str] = Form(default=[]),
    rate_receiving: str = Form(default=""),
    rate_storage: str = Form(default=""),
    rate_pick_pack: str = Form(default=""),
    rate_additional_item: str = Form(default=""),
    rate_kitting: str = Form(default=""),
    rate_labeling: str = Form(default=""),
    rate_wholesale: str = Form(default=""),
    rate_returns: str = Form(default=""),
    rate_tech_fee: str = Form(default=""),
    rate_minimum: str = Form(default=""),
    rate_card_note: str = Form(default=""),
) -> RedirectResponse:
    removed = {str(idx).strip() for idx in product_remove or []}

    def _cell(values: list[str], index: int) -> str:
        return values[index] if index < len(values) else ""

    products: list[dict] = []
    for i in range(len(product_name or [])):
        if str(i) in removed:
            continue
        name = (_cell(product_name, i) or "").strip()
        dims = [
            _opt_float(_cell(product_length, i)),
            _opt_float(_cell(product_width, i)),
            _opt_float(_cell(product_height, i)),
            _opt_float(_cell(product_weight, i)),
        ]
        if not name and all(d is None for d in dims):
            continue  # untouched template row
        products.append(
            {
                "name": name,
                "length_in": dims[0],
                "width_in": dims[1],
                "height_in": dims[2],
                "weight_lb": dims[3],
                "monthly_units": _opt_int(_cell(product_units, i)),
                "dims_estimated": (_cell(product_estimated, i) or "").strip() == "1",
            }
        )

    _rate_fields = {
        "receiving_per_pallet": rate_receiving,
        "storage_short_per_pallet_mo": rate_storage,
        "dtc_base_per_order": rate_pick_pack,
        "dtc_additional_item": rate_additional_item,
        "kitting_per_unit": rate_kitting,
        "labeling_per_unit": rate_labeling,
        "wholesale_per_unit": rate_wholesale,
        "returns_per_unit": rate_returns,
        "monthly_tech_fee": rate_tech_fee,
        "monthly_minimum": rate_minimum,
    }
    rate_overrides = {k: v for k, raw in _rate_fields.items() if (v := _opt_float(raw)) is not None}

    edits = {
        "brand": (brand or "").strip(),
        "origin_zip": (origin_zip or "").strip(),
        "monthly_order_volume": _opt_int(monthly_order_volume),
        "current_cost_per_parcel_usd": _opt_float(current_cost_per_parcel_usd),
        # Blank = automatic category-based quote margins; None clears.
        "quote_margin_override": _opt_float(quote_margin_override.replace("%", "")),
        "destinations_note": (destinations_note or "").strip(),
        "current_costs_note": (current_costs_note or "").strip(),
        "products": products,
        "rate_overrides": rate_overrides,
        "rate_card_note": (rate_card_note or "").strip(),
    }
    try:
        apply_profile_edits(run_id, edits, settings=load_settings())
    except ValueError:
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus("Rate sheet not found."), status_code=303
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[fulfillment_deck] update failed")
        return RedirectResponse(
            f"{_BASE}/runs/{run_id}/review?msg=" + quote_plus(f"Update failed: {str(exc)[:140]}"),
            status_code=303,
        )
    return RedirectResponse(
        f"{_BASE}/runs/{run_id}/review?msg=" + quote_plus("Updated"), status_code=303
    )


@admin_router.post("/runs/{run_id}/publish")
def publish_run(run_id: int, request: Request) -> RedirectResponse:
    if not storage.publish_run(run_id):
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus("Rate sheet not found or not publishable."),
            status_code=303,
        )
    run = storage.get_run(run_id)
    summary = dict(run.summary_json or {}) if run is not None else {}
    view_path = str(summary.get("view_path") or "")
    prospect = str(summary.get("prospect") or "")
    _owner_email = str((get_current_user(request) or {}).get("email") or "")
    try:
        from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_quote as _hs_quote
        _hs_quote(run_id, owner_email=_owner_email)
    except Exception:
        logger.exception("[fulfillment_deck] hubspot sync_quote failed")
    # Store the publishing rep's email for first-view notifications.
    if _owner_email:
        try:
            storage.update_summary(run_id, {"owner_email": _owner_email})
        except Exception:
            logger.exception("[fulfillment_deck] owner_email store failed")
    # Auto-advance pipeline stage to "published" (unless already won/lost).
    stage_now = str(summary.get("pipeline_stage") or "intake")
    if stage_now not in ("won", "lost", "published"):
        try:
            storage.update_stage(run_id, "published")
        except Exception:
            logger.exception("[fulfillment_deck] auto stage advance failed")
    if view_path:
        try:
            from sales_support_agent.services.sales.asset_linker import try_link_rate_sheet
            with Session(get_engine()) as _s:
                try_link_rate_sheet(_s, brand_name=prospect, run_id=run_id, url=view_path)
                _s.commit()
        except Exception:
            logger.exception("[fulfillment_deck] auto deal asset link failed")
    return RedirectResponse(
        f"{_BASE}/runs/{run_id}/review?msg="
        + quote_plus("Published — rate sheet is live. Use the link above to copy or share."),
        status_code=303,
    )


@admin_router.post("/runs/{run_id}/quote")
def create_quote(run_id: int, request: Request) -> RedirectResponse:
    """Trigger HubSpot quote creation (or re-creation) for an already-published run."""
    run = storage.get_run(run_id)
    if run is None or run.status != "completed":
        return RedirectResponse(
            f"{_BASE}?kind=warn&msg=" + quote_plus("Rate sheet not found or not yet published."),
            status_code=303,
        )
    _owner_email = str((get_current_user(request) or {}).get("email") or "")
    try:
        from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_quote as _hs_quote
        _hs_quote(run_id, owner_email=_owner_email, force=True)
        msg = "Creating HubSpot quote — refresh in a few seconds to see the Quote button."
    except Exception:
        logger.exception("[fulfillment_deck] hubspot create_quote failed")
        msg = "Quote creation failed — check that HUBSPOT_API_TOKEN is set in Render."
    return RedirectResponse(
        f"{_BASE}/runs/{run_id}/review?msg=" + quote_plus(msg),
        status_code=303,
    )


@admin_router.post("/runs/{run_id}/delete")
def delete_run(run_id: int) -> RedirectResponse:
    deleted = storage.delete_run(run_id)
    msg = "Rate sheet deleted." if deleted else "Rate sheet not found."
    return RedirectResponse(f"{_BASE}?msg=" + quote_plus(msg), status_code=303)


# ---------------------------------------------------------------------------
# Pipeline: stage / costs / notes PATCH endpoints (JSON, no page reload)
# ---------------------------------------------------------------------------

_VALID_STAGES = {"intake", "pending_fulfillment", "costs_received", "published", "won", "lost"}


@admin_router.patch("/runs/{run_id}/stage")
async def patch_stage(run_id: int, request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"error": "invalid JSON"})
    stage = str(body.get("stage") or "").strip()
    if stage not in _VALID_STAGES:
        return JSONResponse(status_code=400, content={"error": f"unknown stage: {stage}"})
    if not storage.update_stage(run_id, stage):
        return JSONResponse(status_code=404, content={"error": "not found"})
    from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_stage as _hs_stage
    _hs_stage(run_id, stage)
    return JSONResponse({"ok": True})


@admin_router.patch("/runs/{run_id}/costs")
async def patch_costs(run_id: int, request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"error": "invalid JSON"})

    def _f(key: str):
        v = body.get(key)
        if v in (None, ""):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    costs = {
        "pick_pack_per_order": _f("pick_pack_per_order"),
        "storage_per_pallet_mo": _f("storage_per_pallet_mo"),
        "receiving_per_pallet": _f("receiving_per_pallet"),
        "monthly_tech_fee": _f("monthly_tech_fee"),
    }
    if not storage.update_costs(run_id, costs):
        return JSONResponse(status_code=404, content={"error": "not found"})

    # Return computed margin so the UI can update without a reload.
    run = storage.get_run(run_id)
    if run is not None:
        from sales_support_agent.services.fulfillment_deck.quote import compute_margin
        from sales_support_agent.services.fulfillment_deck.schema import ProspectProfile
        summary = dict(run.summary_json or {})
        profile_dict = summary.get("prospect_profile") or {}
        pitched = float((summary.get("fulfillment_quote") or {}).get("monthly_total") or 0)
        if profile_dict and pitched and any(v for v in costs.values() if v):
            profile = ProspectProfile.from_dict(profile_dict)
            margin = compute_margin(pitched, costs, profile)
            from sales_support_agent.services.fulfillment_deck.hubspot_sync import sync_margin as _hs_margin
            _hs_margin(run_id, margin, pitched)
            return JSONResponse({"ok": True, "margin": margin, "pitched": pitched, "actual_monthly": margin.get("actual_monthly")})
    return JSONResponse({"ok": True})


@admin_router.patch("/runs/{run_id}/notes")
async def patch_notes(run_id: int, request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"error": "invalid JSON"})
    notes = str(body.get("notes") or "")[:2000]
    if not storage.update_notes(run_id, notes):
        return JSONResponse(status_code=404, content={"error": "not found"})
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Pipeline CSV export
# ---------------------------------------------------------------------------


@admin_router.get("/export.csv")
def export_pipeline_csv() -> HTMLResponse:
    """Download all pipeline runs as a CSV file for Excel/Sheets."""
    import csv
    import io

    runs = storage.list_runs(limit=500)
    engagement = storage.engagement_for([r["id"] for r in runs])

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "ID", "Prospect", "Stage", "Status", "Created",
        "Volume/mo", "Pitched $/mo", "Pick&Pack $/order",
        "Storage $/pallet/mo", "Receiving $/pallet", "Tech Fee $/mo",
        "Views", "Last Viewed", "Notes",
    ])
    for r in runs:
        rid = r["id"]
        costs = r.get("fulfillment_actual_costs") or {}
        stats = engagement.get(rid) or {}
        writer.writerow([
            rid,
            r.get("prospect") or "",
            r.get("pipeline_stage") or "intake",
            r.get("status") or "",
            (r.get("started_at") or "")[:10],
            r.get("monthly_order_volume") or "",
            r.get("pitched_monthly") or "",
            costs.get("pick_pack_per_order") or "",
            costs.get("storage_per_pallet_mo") or "",
            costs.get("receiving_per_pallet") or "",
            costs.get("monthly_tech_fee") or "",
            int(stats.get("external_sessions") or 0),
            (stats.get("last_viewed_at") or "")[:10],
            r.get("pipeline_notes") or "",
        ])

    from fastapi.responses import Response
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="fulfillment-pipeline.csv"'},
    )


# ---------------------------------------------------------------------------
# First-view notification (background — non-blocking)
# ---------------------------------------------------------------------------


def _notify_first_view(run_id: int) -> None:
    """Send a Resend email to the rep when a prospect opens their rate sheet for the first time."""
    import threading
    threading.Thread(target=_do_notify_first_view, args=(run_id,), daemon=True).start()


def _do_notify_first_view(run_id: int) -> None:
    import os, requests as _req
    resend_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not resend_key:
        return
    run = storage.get_run(run_id)
    if run is None:
        return
    summary = dict(run.summary_json or {})
    owner_email = str(summary.get("owner_email") or "").strip()
    if not owner_email:
        return
    prospect = str(summary.get("prospect") or f"Run {run_id}")
    view_path = str(summary.get("view_path") or "")
    rate_sheet_url = f"https://agent.anatainc.com{view_path}" if view_path else "https://agent.anatainc.com/admin/fulfillment/sales"
    pipeline_url = f"https://agent.anatainc.com/admin/fulfillment/sales"
    try:
        _req.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
            json={
                "from": "Anata Agent <agent@anatainc.com>",
                "to": [owner_email],
                "subject": f"🔔 {prospect} just opened your rate sheet",
                "html": (
                    f"<p>Hi,</p>"
                    f"<p><strong>{prospect}</strong> just viewed their Anata fulfillment rate sheet for the first time.</p>"
                    f"<p><a href='{rate_sheet_url}?viewer=internal'>Open the rate sheet →</a></p>"
                    f"<p><a href='{pipeline_url}'>View pipeline →</a></p>"
                    f"<p style='color:#888;font-size:12px'>You're receiving this because you published this rate sheet. — Anata Agent</p>"
                ),
            },
            timeout=8,
        )
        logger.info("[fulfillment_deck] first-view notification sent to %s for run %d", owner_email, run_id)
    except Exception:
        logger.exception("[fulfillment_deck] first-view notification failed for run %d", run_id)


# ---------------------------------------------------------------------------
# Public hosted view + engagement heartbeat (token-gated, no session)
# ---------------------------------------------------------------------------


def _load_valid_run(run_id: int, token: str):
    run = storage.get_run(run_id)
    if run is None:
        return None
    # Drafts are never publicly visible — admins preview via the gated
    # /runs/{id}/preview route on the review page instead.
    if run.status != "completed":
        return None
    summary = dict(run.summary_json or {})
    if not token or summary.get("export_token") != token:
        return None
    return run


@public_router.get("/rate-sheets/{slug}/{run_id}/{token}", response_class=HTMLResponse)
def rate_sheet_view(slug: str, run_id: int, token: str) -> HTMLResponse:
    run = _load_valid_run(run_id, token)
    if run is None:
        return HTMLResponse("Rate sheet not found.", status_code=404)
    deck_html = str((run.summary_json or {}).get("deck_html") or "")
    if not deck_html:
        return HTMLResponse("Rate sheet not found.", status_code=404)
    return HTMLResponse(deck_html)


# Sections the requote response re-ships as swappable HTML fragments. The
# combined rates-explorer section is intentionally absent — its JS state AND
# its data-driven table live in the page and update client-side from the
# returned products payload (v6: map + table merged into one never-swapped
# section, so the carrier filter / toggle / optimizer survive a requote).
# (volume-economics + savings merged into monthly-math in v3; the estimated
# invoice "quote" section joined in v4 — its totals move with the dims.)
_FRAGMENT_KEYS = ("monthly-math", "quote")


@public_router.post("/rate-sheets/{slug}/{run_id}/{token}/requote")
async def rate_sheet_requote(request: Request, slug: str, run_id: int, token: str) -> JSONResponse:
    """Live re-quote for the interactive map's "Request rates" button.

    The viewer edits dims/weight on the rendered sheet; this rebuilds the
    rate matrix AND PERSISTS the updated report (profile, rates, savings,
    narrative, HTML), so the edit survives leaving and coming back. Returns
    the fresh map payload plus re-rendered section fragments the page swaps
    in. Token-gated; allowed for drafts too (the admin review preview embeds
    the same map). Concurrent requotes: last write wins."""
    run = storage.get_run(run_id)
    if run is None or (dict(run.summary_json or {}).get("export_token") != token) or not token:
        return JSONResponse(status_code=404, content={"detail": "Rate sheet not found."})

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    raw_products = payload.get("products")
    if not isinstance(raw_products, list) or not raw_products:
        return JSONResponse(status_code=400, content={"detail": "products required."})

    # Schema clamps dims/weight; cap the count so a public token can't make
    # the server quote an arbitrary catalog.
    products = [
        ProductSpec.from_dict(p) for p in raw_products[:6] if isinstance(p, dict)
    ]
    origin = clean_zip(payload.get("origin_zip")) or ""

    try:
        result = apply_viewer_requote(
            run_id, products, origin, settings=load_settings()
        )
    except ValueError:
        return JSONResponse(status_code=404, content={"detail": "Rate sheet not found."})

    matrix = RateMatrix.from_dict(result.get("rate_matrix") or {})
    map_data = map_payload(matrix)
    deck_html = str(result.get("deck_html") or "")
    fragments = {}
    for key in _FRAGMENT_KEYS:
        match = re.search(
            r'<section[^>]*data-key="' + key + r'".*?</section>', deck_html, re.S
        )
        fragments[key] = match.group(0) if match else ""
    return JSONResponse(status_code=200, content={
        "products": map_data["products"],
        "source": map_data["source"],
        "fragments": fragments,
    })


@public_router.post("/rate-sheets/{slug}/{run_id}/{token}/heartbeat")
async def rate_sheet_heartbeat(request: Request, slug: str, run_id: int, token: str) -> JSONResponse:
    run = _load_valid_run(run_id, token)
    if run is None:
        return JSONResponse(status_code=404, content={"detail": "Rate sheet not found."})

    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    visitor_token = str(payload.get("visitor_token") or "").strip()[:64]
    if not visitor_token:
        return JSONResponse(status_code=400, content={"detail": "visitor_token required."})

    is_internal = bool(payload.get("is_internal", False))
    client_total_seconds = int(payload.get("total_seconds", 0) or 0)
    client_total_seconds = max(0, min(client_total_seconds, MAX_SESSION_SECONDS))
    max_scroll = int(payload.get("max_scroll_pct", 0) or 0)
    max_scroll = max(0, min(max_scroll, 100))

    sections_payload: dict[str, int] = {}
    raw_sections = payload.get("sections") or {}
    if isinstance(raw_sections, dict):
        for sec_id, secs in raw_sections.items():
            sec_id_clean = str(sec_id)[:64]
            if not sec_id_clean:
                continue
            try:
                secs_int = int(secs)
            except (TypeError, ValueError):
                continue
            sections_payload[sec_id_clean] = max(0, min(secs_int, MAX_SESSION_SECONDS))

    now = datetime.now(timezone.utc)
    session = Session(get_engine(), expire_on_commit=False)
    try:
        existing = session.execute(
            select(DeckVisitSession).where(
                DeckVisitSession.run_id == run_id,
                DeckVisitSession.visitor_token == visitor_token,
            )
        ).scalar_one_or_none()

        if existing is None:
            ua_raw = (request.headers.get("user-agent") or "")[:512]
            ua_parts = parse_user_agent(ua_raw)
            geo = extract_visitor_geo(request)
            referrer_url = str(payload.get("referrer") or request.headers.get("referer") or "")
            ref_host, ref_cat = categorize_referrer(referrer_url)
            is_first_external = (
                not is_internal
                and session.execute(
                    select(DeckVisitSession).where(
                        DeckVisitSession.run_id == run_id,
                        DeckVisitSession.is_internal == False,  # noqa: E712
                    )
                ).first() is None
            )
            existing = DeckVisitSession(
                run_id=run_id,
                visitor_token=visitor_token,
                is_internal=is_internal,
                started_at=now,
                last_heartbeat_at=now,
                total_seconds=client_total_seconds,
                max_scroll_pct=max_scroll,
                ip_country=geo["country"],
                ip_region=geo["region"],
                ip_city=geo["city"],
                device=ua_parts["device"],
                os=ua_parts["os"],
                browser=ua_parts["browser"],
                user_agent_raw=ua_raw,
                referrer_host=ref_host[:128],
                referrer_category=ref_cat,
            )
            session.add(existing)
            session.flush()
            if is_first_external:
                _notify_first_view(run_id)
        else:
            existing.last_heartbeat_at = now
            if client_total_seconds > existing.total_seconds:
                existing.total_seconds = client_total_seconds
            if max_scroll > existing.max_scroll_pct:
                existing.max_scroll_pct = max_scroll
            session.add(existing)

        for sec_id, secs in sections_payload.items():
            sec_row = session.execute(
                select(DeckSectionView).where(
                    DeckSectionView.session_id == existing.id,
                    DeckSectionView.section_id == sec_id,
                )
            ).scalar_one_or_none()
            if sec_row is None:
                session.add(
                    DeckSectionView(
                        session_id=existing.id,
                        section_id=sec_id,
                        first_seen_at=now,
                        last_seen_at=now,
                        total_seconds=secs,
                    )
                )
            else:
                sec_row.last_seen_at = now
                if secs > sec_row.total_seconds:
                    sec_row.total_seconds = secs
                session.add(sec_row)

        session.commit()
        return JSONResponse(status_code=200, content={"status": "ok", "session_id": existing.id})
    except Exception:  # noqa: BLE001
        session.rollback()
        logger.exception("[fulfillment_deck] heartbeat failed")
        return JSONResponse(status_code=200, content={"status": "dropped"})
    finally:
        session.close()
