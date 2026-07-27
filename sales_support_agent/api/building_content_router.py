"""Reviewed Building content records and public-safe projections."""

from __future__ import annotations

import html
from datetime import date, datetime, timezone
from typing import Any, Literal, Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingLifestyleMedia,
    BuildingTenantLogo,
    BuildingTestimonial,
)
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)
from sales_support_agent.services.auth_deps import require_any_tool
from sales_support_agent.services.building_content import (
    CONTENT_STATUSES,
    public_content_projection,
    utc_now,
    validate_content_for_approval,
)
from sales_support_agent.services.building_security import (
    csrf_token,
    require_building_form_security,
)

public_router = APIRouter(prefix="/api/public/building", tags=["building-public-content"])
internal_router = APIRouter(prefix="/api/internal/building/content", tags=["building-content"])
admin_router = APIRouter(prefix="/admin/building/content", tags=["building-content-admin"])

CONTENT_MODELS = {
    "lifestyle_media": BuildingLifestyleMedia,
    "tenant_logo": BuildingTenantLogo,
    "testimonial": BuildingTestimonial,
}
REVIEW_TRANSITIONS = {
    "draft": {"needs_review", "retired"},
    "needs_review": {"approved", "rejected", "draft"},
    "approved": {"needs_review", "retired"},
    "rejected": {"draft", "retired"},
    "retired": set(),
}
FORM_DEPS = [Depends(require_building_form_security)]
CONTENT_ACCESS = require_any_tool("building.content.manage", "building.manage")


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _model(kind: str):
    model = CONTENT_MODELS.get(kind)
    if model is None:
        raise HTTPException(status_code=404, detail="Unknown Building content type.")
    return model


def _record_payload(row: Any, kind: str) -> dict[str, Any]:
    common = {
        "id": row.id,
        "kind": kind,
        "source_reference": row.source_reference,
        "consent_reference": row.consent_reference,
        "review_expires_on": row.review_expires_on.isoformat(),
        "status": row.status,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at.isoformat() if row.approved_at else None,
        "internal_notes": row.internal_notes,
        "created_by": row.created_by,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }
    if kind == "lifestyle_media":
        common.update(
            title=row.title,
            media_url=row.media_url,
            media_kind=row.media_kind,
            alt_text=row.alt_text,
            caption=row.caption,
            placement=row.placement,
        )
    elif kind == "tenant_logo":
        common.update(
            tenant_name=row.tenant_name,
            asset_url=row.asset_url,
            alt_text=row.alt_text,
            destination_url=row.destination_url,
        )
    else:
        common.update(
            quote=row.quote,
            attribution_name=row.attribution_name,
            attribution_title=row.attribution_title,
            attribution_company=row.attribution_company,
            rating=row.rating,
        )
    return common


class ContentInput(BaseModel):
    id: str = Field(default="", max_length=64)
    actor: str = Field(min_length=1, max_length=255)
    source_reference: str = Field(min_length=1, max_length=1024)
    consent_reference: str = Field(min_length=1, max_length=1024)
    review_expires_on: date
    internal_notes: str = Field(default="", max_length=5000)
    title: str = Field(default="", max_length=255)
    media_url: str = Field(default="", max_length=1024)
    media_kind: Literal["image", "video"] = "image"
    alt_text: str = Field(default="", max_length=512)
    caption: str = Field(default="", max_length=2000)
    placement: str = Field(default="gallery", max_length=64)
    tenant_name: str = Field(default="", max_length=255)
    asset_url: str = Field(default="", max_length=1024)
    destination_url: str = Field(default="", max_length=1024)
    quote: str = Field(default="", max_length=4000)
    attribution_name: str = Field(default="", max_length=255)
    attribution_title: str = Field(default="", max_length=255)
    attribution_company: str = Field(default="", max_length=255)
    rating: int | None = Field(default=None, ge=1, le=5)


class ReviewInput(BaseModel):
    status: Literal["draft", "needs_review", "approved", "rejected", "retired"]
    actor: str = Field(min_length=1, max_length=255)
    reason: str = Field(min_length=3, max_length=1000)


def _values(payload: ContentInput, model: Any) -> dict[str, Any]:
    """Return only fields mapped by the selected first-class content model."""

    supplied = payload.model_dump(exclude={"id", "actor"})
    mapped = {column.key for column in model.__table__.columns}
    return {key: value for key, value in supplied.items() if key in mapped}


def _save_record(session, kind: str, record_id: str, payload: ContentInput) -> Any:
    model = _model(kind)
    row = session.get(model, record_id)
    before = _record_payload(row, kind) if row else {}
    if row is not None and row.status == "approved":
        raise HTTPException(
            status_code=409,
            detail="Approved content is locked. Return it to needs review before editing.",
        )
    values = _values(payload, model)
    errors = validate_content_for_approval(kind, values)
    # Drafts may be incomplete, but malformed URLs/private-benefit language are
    # never accepted into fields intended for public projection.
    hard_errors = [
        item for item in errors
        if "URL" in item or "private-benefit" in item or "Boom" in item
    ]
    if hard_errors:
        raise HTTPException(status_code=422, detail=hard_errors)
    if row is None:
        required = {
            "lifestyle_media": {
                "title": payload.title,
                "media_url": payload.media_url,
                "alt_text": payload.alt_text,
            },
            "tenant_logo": {
                "tenant_name": payload.tenant_name,
                "asset_url": payload.asset_url,
                "alt_text": payload.alt_text,
            },
            "testimonial": {
                "quote": payload.quote,
                "attribution_name": payload.attribution_name,
            },
        }[kind]
        missing = [name for name, value in required.items() if not str(value or "").strip()]
        if missing:
            raise HTTPException(status_code=422, detail=f"Missing required fields: {', '.join(missing)}.")
        row = model(id=record_id, created_by=payload.actor, **values)
    else:
        for key, value in values.items():
            if hasattr(row, key):
                setattr(row, key, value)
        row.updated_at = utc_now()
    session.add(row)
    session.flush()
    after = _record_payload(row, kind)
    session.add(
        BuildingAuditEvent(
            entity_type=kind,
            entity_id=record_id,
            action="created" if not before else "updated",
            actor=payload.actor,
            before_json=before,
            after_json=after,
        )
    )
    return row


def _transition_record(session, kind: str, record_id: str, payload: ReviewInput) -> Any:
    model = _model(kind)
    row = session.get(model, record_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Content record not found.")
    if payload.status not in REVIEW_TRANSITIONS.get(row.status, set()):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot move {row.status} content to {payload.status}.",
        )
    before = _record_payload(row, kind)
    if payload.status == "approved":
        errors = validate_content_for_approval(kind, before)
        if errors:
            raise HTTPException(status_code=422, detail=errors)
        row.approved_by = payload.actor
        row.approved_at = utc_now()
    elif row.status == "approved":
        row.approved_by = ""
        row.approved_at = None
    row.status = payload.status
    row.updated_at = utc_now()
    session.add(row)
    session.add(
        BuildingAuditEvent(
            entity_type=kind,
            entity_id=record_id,
            action=f"status_{payload.status}",
            actor=payload.actor,
            before_json=before,
            after_json={**_record_payload(row, kind), "reason": payload.reason},
        )
    )
    return row


@public_router.get("/content")
def get_public_building_content(request: Request) -> dict[str, Any]:
    """Return only approved, unexpired, allow-listed Building content."""

    with session_scope(request.app.state.session_factory) as session:
        return public_content_projection(session)


@internal_router.get("")
def list_content(
    request: Request,
    kind: str | None = None,
    status: str | None = None,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    kinds = [kind] if kind else list(CONTENT_MODELS)
    records: list[dict[str, Any]] = []
    with session_scope(request.app.state.session_factory) as session:
        for selected_kind in kinds:
            model = _model(selected_kind)
            query = select(model).order_by(model.updated_at.desc())
            if status:
                query = query.where(model.status == status)
            records.extend(
                _record_payload(row, selected_kind)
                for row in session.execute(query).scalars()
            )
    return {"records": records}


@internal_router.put("/{kind}/{record_id}")
def upsert_content(
    kind: str,
    record_id: str,
    payload: ContentInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    if payload.id and payload.id != record_id:
        raise HTTPException(status_code=422, detail="Content ID does not match route.")
    with session_scope(request.app.state.session_factory) as session:
        row = _save_record(session, kind, record_id, payload)
        return {"ok": True, "record": _record_payload(row, kind)}


@internal_router.post("/{kind}/{record_id}/review")
def review_content(
    kind: str,
    record_id: str,
    payload: ReviewInput,
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> dict[str, Any]:
    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        row = _transition_record(session, kind, record_id, payload)
        return {"ok": True, "record": _record_payload(row, kind)}


def _redirect(*, notice: str = "", error: str = "") -> RedirectResponse:
    query = urlencode({"notice": notice} if notice else {"error": error})
    return RedirectResponse(f"/admin/building/content?{query}", status_code=303)


@admin_router.post("", dependencies=FORM_DEPS)
def save_content_from_admin(
    request: Request,
    kind: str = Form(...),
    record_id: str = Form(""),
    title: str = Form(""),
    media_url: str = Form(""),
    media_kind: str = Form("image"),
    alt_text: str = Form(""),
    caption: str = Form(""),
    placement: str = Form("gallery"),
    tenant_name: str = Form(""),
    asset_url: str = Form(""),
    destination_url: str = Form(""),
    quote: str = Form(""),
    attribution_name: str = Form(""),
    attribution_title: str = Form(""),
    attribution_company: str = Form(""),
    rating: str = Form(""),
    source_reference: str = Form(...),
    consent_reference: str = Form(...),
    review_expires_on: date = Form(...),
    internal_notes: str = Form(""),
    user: dict = Depends(CONTENT_ACCESS),
) -> RedirectResponse:
    actor = str(user.get("email") or "building-content-operator")
    try:
        payload = ContentInput(
            id=record_id,
            actor=actor,
            source_reference=source_reference,
            consent_reference=consent_reference,
            review_expires_on=review_expires_on,
            internal_notes=internal_notes,
            title=title,
            media_url=media_url,
            media_kind=media_kind,
            alt_text=alt_text,
            caption=caption,
            placement=placement,
            tenant_name=tenant_name,
            asset_url=asset_url,
            destination_url=destination_url,
            quote=quote,
            attribution_name=attribution_name,
            attribution_title=attribution_title,
            attribution_company=attribution_company,
            rating=int(rating) if rating else None,
        )
        with session_scope(request.app.state.session_factory) as session:
            _save_record(session, kind, record_id or str(uuid4()), payload)
    except (HTTPException, ValidationError, ValueError) as exc:
        if isinstance(exc, HTTPException):
            message = exc.detail
        elif isinstance(exc, ValidationError):
            message = exc.errors()[0].get("msg", "Review the content fields.")
        else:
            message = str(exc)
        return _redirect(error=str(message))
    return _redirect(notice="Content draft saved.")


@admin_router.post("/{kind}/{record_id}/review", dependencies=FORM_DEPS)
def review_content_from_admin(
    kind: str,
    record_id: str,
    request: Request,
    status: str = Form(...),
    reason: str = Form(...),
    user: dict = Depends(CONTENT_ACCESS),
) -> RedirectResponse:
    actor = str(user.get("email") or "building-content-operator")
    try:
        payload = ReviewInput(status=status, actor=actor, reason=reason)
        with session_scope(request.app.state.session_factory) as session:
            _transition_record(session, kind, record_id, payload)
    except HTTPException as exc:
        return _redirect(error=str(exc.detail))
    return _redirect(notice=f"Content moved to {status.replace('_', ' ')}.")


def _esc(value: Any) -> str:
    return html.escape(str(value or ""))


@admin_router.get("", response_class=HTMLResponse)
def content_admin_page(
    request: Request,
    notice: str = "",
    error: str = "",
    user: dict = Depends(CONTENT_ACCESS),
) -> HTMLResponse:
    with session_scope(request.app.state.session_factory) as session:
        records = []
        for kind, model in CONTENT_MODELS.items():
            records.extend(
                _record_payload(row, kind)
                for row in session.execute(select(model).order_by(model.updated_at.desc())).scalars()
            )
    rows = "".join(
        f"""<tr><td><strong>{_esc(item.get('kind').replace('_', ' ').title())}</strong><span>{_esc(item.get('title') or item.get('tenant_name') or item.get('attribution_name'))}</span></td>
        <td>{_esc(item.get('status').replace('_', ' ').title())}</td><td>{_esc(item.get('review_expires_on'))}</td>
        <td><form method="post" action="/admin/building/content/{_esc(item['kind'])}/{_esc(item['id'])}/review">
        <input type="hidden" name="_csrf_token" value="{_esc(csrf_token(user))}">
        <label>Next state<select name="status"><option value="needs_review">Needs review</option><option value="approved">Approved</option><option value="rejected">Rejected</option><option value="draft">Draft</option><option value="retired">Retired</option></select></label>
        <label>Reason<input name="reason" required minlength="3"></label><button type="submit">Apply</button></form></td></tr>"""
        for item in records
    ) or '<tr><td colspan="4">No content records yet. Save a draft to begin review.</td></tr>'
    flash = (
        f'<div class="flash ok" role="status">{_esc(notice)}</div>' if notice else
        (f'<div class="flash error" role="alert">{_esc(error)}</div>' if error else "")
    )
    nav = render_agent_nav("building", user=user)
    return HTMLResponse(
        f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Building content · Anata Agent</title>{render_agent_favicon_links()}
        <link rel="stylesheet" href="/static/admin.css"><style>{render_agent_nav_styles()}
        .shell{{max-width:1320px;margin:auto;padding:28px 24px 56px}}.head{{display:flex;justify-content:space-between;gap:20px;align-items:end}}.head p,td span{{display:block;color:var(--muted,#68727d)}}.metrics{{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--border,#d9dfe3);margin:22px 0}}.metric{{padding:18px;border-right:1px solid var(--border,#d9dfe3)}}.panel{{border:1px solid var(--border,#d9dfe3);background:#fff;margin-top:20px;padding:20px}}.form-grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}label{{display:grid;gap:5px;font-weight:700;font-size:13px}}input,select,textarea{{min-height:42px;padding:9px;border:1px solid var(--border,#cbd3d8);border-radius:8px;font:inherit}}textarea{{min-height:90px}}button{{min-height:40px;padding:0 15px;background:var(--ink,#23313d);color:#fff;border:0;border-radius:8px;font-weight:700}}table{{width:100%;border-collapse:collapse}}th,td{{padding:12px;text-align:left;border-top:1px solid var(--border,#d9dfe3);vertical-align:top}}td form{{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:end}}.wide{{grid-column:1/-1}}.flash{{padding:13px;margin-bottom:16px;border:1px solid}}.ok{{background:#e4f4f1}}.error{{background:#fff0ed}}@media(max-width:760px){{.form-grid,.metrics{{grid-template-columns:1fr}}.wide{{grid-column:auto}}.table-wrap{{overflow:auto}}}}</style></head>
        <body>{nav}<main class="shell">{flash}<header class="head"><div><p>Building operations</p><h1>Public content review</h1><p>Manage consent-backed lifestyle media, tenant logos, and testimonials. Only approved, current records reach the public projection.</p></div><a href="/admin/building">Back to control room</a></header>
        <section class="metrics" aria-label="Content summary"><div class="metric"><strong>{len(records)}</strong><span>Total records</span></div><div class="metric"><strong>{sum(1 for x in records if x['status']=='needs_review')}</strong><span>Need review</span></div><div class="metric"><strong>{sum(1 for x in records if x['status']=='approved' and x['review_expires_on'] >= date.today().isoformat())}</strong><span>Current approved</span></div></section>
        <section class="panel"><h2>Save a content draft</h2><p>Choose a type, complete its relevant public fields, and preserve source and consent evidence.</p>
        <form class="form-grid" method="post" action="/admin/building/content"><input type="hidden" name="_csrf_token" value="{_esc(csrf_token(user))}">
        <label>Content type<select name="kind" required><option value="lifestyle_media">Lifestyle media</option><option value="tenant_logo">Tenant logo</option><option value="testimonial">Testimonial / review</option></select></label>
        <label>Existing stable ID (leave blank to create)<input name="record_id"></label>
        <label>Title / media URL<input name="title"><input name="media_url" placeholder="https://… or /media/…"></label>
        <label>Media details<select name="media_kind"><option value="image">Image</option><option value="video">Video</option></select><input name="placement" value="gallery"></label>
        <label>Tenant name / logo URL<input name="tenant_name"><input name="asset_url" placeholder="https://… or /media/…"></label>
        <label>Alt text / destination URL<input name="alt_text"><input name="destination_url" placeholder="Optional HTTPS link"></label>
        <label class="wide">Testimonial text<textarea name="quote"></textarea></label>
        <label>Attribution<input name="attribution_name" placeholder="Public name"><input name="attribution_title" placeholder="Title"></label>
        <label>Company / rating<input name="attribution_company"><input name="rating" type="number" min="1" max="5"></label>
        <label class="wide">Caption<textarea name="caption"></textarea></label>
        <label>Source evidence<input name="source_reference" required></label><label>Consent evidence<input name="consent_reference" required></label>
        <label>Review expires<input type="date" name="review_expires_on" required></label><label>Internal notes<textarea name="internal_notes"></textarea></label>
        <div class="wide"><button type="submit">Save draft</button></div></form></section>
        <section class="panel"><h2>Review queue</h2><div class="table-wrap"><table><thead><tr><th>Record</th><th>Status</th><th>Review expires</th><th>Review action</th></tr></thead><tbody>{rows}</tbody></table></div></section>
        </main></body></html>"""
    )
