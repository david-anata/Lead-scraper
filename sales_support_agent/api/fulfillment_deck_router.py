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
from sales_support_agent.services.fulfillment_deck.admin_page import (
    render_fulfillment_sales_page,
)
from sales_support_agent.services.fulfillment_deck.service import generate_rate_sheet
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

    warning_count = len(result.get("warnings") or [])
    note = f" ({warning_count} warning{'s' if warning_count != 1 else ''} — see run details)" if warning_count else ""
    return RedirectResponse(
        f"{_BASE}?msg=" + quote_plus(f"Rate sheet ready for {result.get('prospect') or 'prospect'}{note}. Use Open or Copy link in History."),
        status_code=303,
    )


@admin_router.post("/runs/{run_id}/delete")
def delete_run(run_id: int) -> RedirectResponse:
    deleted = storage.delete_run(run_id)
    msg = "Rate sheet deleted." if deleted else "Rate sheet not found."
    return RedirectResponse(f"{_BASE}?msg=" + quote_plus(msg), status_code=303)


# ---------------------------------------------------------------------------
# Public hosted view + engagement heartbeat (token-gated, no session)
# ---------------------------------------------------------------------------


def _load_valid_run(run_id: int, token: str):
    run = storage.get_run(run_id)
    if run is None:
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
