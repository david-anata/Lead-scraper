"""Admin and bot access to the Anata Building tenant directory."""

from __future__ import annotations

import hmac
import os
from typing import Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.building_security import (
    csrf_token,
    require_building_form_security,
)
from sales_support_agent.services.building_tenant_directory import (
    MAX_CSV_BYTES,
    apply_import,
    contact_input_from_form,
    decode_csv_bytes,
    directory_metrics,
    directory_table_ready,
    find_contact,
    import_preview_token,
    list_contacts,
    notice_recipients,
    preview_import,
    recipient_document,
    save_contact,
    set_active,
    snapshot,
    import_summary,
)
from sales_support_agent.services.building_tenant_directory_page import (
    render_tenant_directory_page,
)

admin_router = APIRouter(prefix="/admin/building/tenants", tags=["building-tenant-directory"])
internal_router = APIRouter(
    prefix="/api/internal/building/tenant-directory",
    tags=["building-tenant-directory"],
)
FORM_DEPS = [Depends(require_building_form_security)]
ACCESS = require_tool("building.manage")
STATUSES = {"active", "inactive", "all", "notices"}
_NO_STORE = {"Cache-Control": "no-store"}


def _require_internal_key(request: Request, provided: Optional[str]) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    if not configured:
        raise HTTPException(status_code=503, detail="Internal API is not configured.")
    if str(provided or "").strip() != configured:
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _actor(user: dict) -> str:
    return str(user.get("email") or user.get("name") or "building-operator")


def _redirect(**params: str) -> RedirectResponse:
    query = urlencode({key: value for key, value in params.items() if value})
    target = "/admin/building/tenants"
    if query:
        target = f"{target}?{query}"
    return RedirectResponse(target, status_code=303)


def _confirmation_secret(request: Request) -> str:
    secret = (
        os.getenv("BUILDING_CAMPAIGN_TOKEN_SECRET", "").strip()
        or os.getenv("ADMIN_DASHBOARD_SESSION_SECRET", "").strip()
        or str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    )
    if not secret:
        raise HTTPException(status_code=503, detail="Import confirmation is not configured.")
    return secret


def _page(
    request: Request,
    user: dict,
    *,
    notice: str = "",
    error: str = "",
    query: str = "",
    status: str = "active",
    edit_id: str = "",
    preview: Optional[dict] = None,
) -> HTMLResponse:
    status = status if status in STATUSES else "active"
    with session_scope(request.app.state.session_factory) as session:
        ready = directory_table_ready(session)
        if ready:
            rows, matched = list_contacts(session, query=query, status=status)
            metrics = directory_metrics(session)
            found = find_contact(session, edit_id) if edit_id else None
            editing = snapshot(found) if found else None
            if edit_id and editing is None:
                error = error or "That directory contact was not found."
            emails = [row.email for row in notice_recipients(session)]
            contact_rows = [snapshot(row) for row in rows]
        else:
            contact_rows, matched, metrics, editing, emails = [], 0, {
                "active": 0, "notices": 0, "inactive": 0, "companies": 0,
            }, None, []
    document = render_tenant_directory_page(
        user=user,
        contacts=contact_rows,
        matched=matched,
        metrics=metrics,
        query=query,
        status=status,
        edit_contact=editing,
        notice=notice,
        error=error,
        preview=preview,
        table_ready=ready,
        csrf_token=csrf_token(user),
        recipient_emails=emails,
    )
    return HTMLResponse(document, headers=_NO_STORE)


@admin_router.get("", response_class=HTMLResponse)
def tenant_directory_page(
    request: Request,
    q: str = "",
    status: str = "active",
    edit: str = "",
    notice: str = "",
    error: str = "",
    user: dict = Depends(ACCESS),
) -> HTMLResponse:
    """List, search, and open the add or edit form."""

    return _page(
        request,
        user,
        notice=notice,
        error=error,
        query=q,
        status=status,
        edit_id=edit,
    )


@admin_router.get("/notice-recipients")
def copy_notice_recipients(
    request: Request,
    user: dict = Depends(ACCESS),
) -> JSONResponse:
    """Return emails for active contacts who receive building notices."""

    del user
    with session_scope(request.app.state.session_factory) as session:
        if not directory_table_ready(session):
            raise HTTPException(status_code=503, detail="Tenant directory is not installed.")
        payload = recipient_document(notice_recipients(session))
    return JSONResponse(payload, headers=_NO_STORE)


@admin_router.post("", dependencies=FORM_DEPS)
def save_tenant_contact(
    request: Request,
    contact_id: str = Form(""),
    full_name: str = Form(""),
    company_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    office_suite: str = Form(""),
    august_lock_name: str = Form(""),
    has_august_lock: str = Form(""),
    receives_notices: str = Form(""),
    is_active: str = Form(""),
    notes: str = Form(""),
    user: dict = Depends(ACCESS),
) -> RedirectResponse:
    record_id = contact_id.strip() or uuid4().hex
    try:
        fields = contact_input_from_form(
            {
                "full_name": full_name,
                "company_name": company_name,
                "email": email,
                "phone": phone,
                "office_suite": office_suite,
                "august_lock_name": august_lock_name,
                "has_august_lock": has_august_lock,
                "receives_notices": receives_notices,
                "is_active": is_active,
                "notes": notes,
            }
        )
        with session_scope(request.app.state.session_factory) as session:
            if not directory_table_ready(session):
                raise ValueError("The tenant directory table is not installed yet.")
            save_contact(session, contact_id=record_id, fields=fields, actor=_actor(user))
    except ValueError as exc:
        return _redirect(error=str(exc), edit=contact_id.strip())
    return _redirect(notice="Tenant contact saved.")


@admin_router.post("/{contact_id}/deactivate", dependencies=FORM_DEPS)
def deactivate_tenant_contact(
    contact_id: str,
    request: Request,
    user: dict = Depends(ACCESS),
) -> RedirectResponse:
    return _set_active(request, contact_id, active=False, user=user)


@admin_router.post("/{contact_id}/reactivate", dependencies=FORM_DEPS)
def reactivate_tenant_contact(
    contact_id: str,
    request: Request,
    user: dict = Depends(ACCESS),
) -> RedirectResponse:
    return _set_active(request, contact_id, active=True, user=user)


def _set_active(request: Request, contact_id: str, *, active: bool, user: dict) -> RedirectResponse:
    try:
        with session_scope(request.app.state.session_factory) as session:
            if not directory_table_ready(session):
                raise ValueError("The tenant directory table is not installed yet.")
            set_active(session, contact_id, active=active, actor=_actor(user))
    except ValueError as exc:
        return _redirect(error=str(exc))
    notice = "Tenant contact reactivated." if active else "Tenant contact deactivated."
    return _redirect(notice=notice, status="" if active else "inactive")


@admin_router.post("/import/preview", dependencies=FORM_DEPS)
async def preview_tenant_import(
    request: Request,
    csv_file: UploadFile = File(...),
    user: dict = Depends(ACCESS),
) -> HTMLResponse:
    payload = await csv_file.read(MAX_CSV_BYTES + 1)
    if len(payload) > MAX_CSV_BYTES:
        return _page(request, user, error="That CSV is larger than 1 MB.")
    csv_text = decode_csv_bytes(payload)
    with session_scope(request.app.state.session_factory) as session:
        if not directory_table_ready(session):
            return _page(request, user, error="The tenant directory table is not installed yet.")
        rows, error = preview_import(session, csv_text)
    if error:
        return _page(request, user, error=error)
    secret = _confirmation_secret(request)
    preview = {
        "rows": rows,
        "summary": import_summary(rows),
        "csv_text": csv_text,
        "token": import_preview_token(secret, csv_text),
    }
    return _page(request, user, preview=preview)


@admin_router.post("/import/apply", dependencies=FORM_DEPS)
def apply_tenant_import(
    request: Request,
    csv_text: str = Form(""),
    preview_token: str = Form(""),
    user: dict = Depends(ACCESS),
) -> RedirectResponse:
    secret = _confirmation_secret(request)
    expected = import_preview_token(secret, csv_text)
    if not preview_token or not hmac.compare_digest(expected, preview_token.strip()):
        return _redirect(error="That import preview expired. Preview the file again.")
    try:
        with session_scope(request.app.state.session_factory) as session:
            if not directory_table_ready(session):
                raise ValueError("The tenant directory table is not installed yet.")
            rows, error = preview_import(session, csv_text)
            if error:
                raise ValueError(error)
            result = apply_import(session, rows, actor=_actor(user))
    except ValueError as exc:
        return _redirect(error=str(exc))
    return _redirect(
        notice=(
            f"Imported {result['created']} tenant contact"
            f"{'' if result['created'] == 1 else 's'}. "
            f"Skipped {result['skipped']}."
        )
    )


@internal_router.get("/notice-recipients")
def bot_notice_recipients(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None),
) -> JSONResponse:
    """Authenticated read of active building-notice recipients for the manager bot."""

    _require_internal_key(request, x_internal_api_key)
    with session_scope(request.app.state.session_factory) as session:
        if not directory_table_ready(session):
            raise HTTPException(status_code=503, detail="Tenant directory is not installed.")
        payload = recipient_document(notice_recipients(session))
    return JSONResponse(payload, headers=_NO_STORE)
