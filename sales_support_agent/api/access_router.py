"""Access admin UI — /admin/access.

Requires `access.manage` for all admin routes.
Public route (no auth guard, in middleware bypass list):
  GET  /admin/access/invite/{token}   → validate token, set cookie, bounce to Google login

Admin routes:
  GET  /admin/access                  → users list
  POST /admin/access/users/{id}/role  → assign/change role
  POST /admin/access/users/{id}/status → suspend / activate

  GET  /admin/access/roles            → roles list
  GET  /admin/access/roles/new        → create-role form
  POST /admin/access/roles/new        → create role
  GET  /admin/access/roles/{id}/edit  → edit-role form
  POST /admin/access/roles/{id}/edit  → save role edits
  POST /admin/access/roles/{id}/delete → delete role (blocked if assigned)

  GET  /admin/access/invites          → pending invites list + send form
  POST /admin/access/invites/new      → create invite → show link page
  POST /admin/access/invites/{id}/revoke → revoke invite

  GET  /admin/access/requests         → pending access requests
  POST /admin/access/requests/{id}/approve → approve + assign role
  POST /admin/access/requests/{id}/deny   → deny
"""
from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_support_agent.services.access import store
from sales_support_agent.services.access.pages import (
    render_invite_created_page,
    render_invite_invalid_page,
    render_invites_page,
    render_requests_page,
    render_role_form_page,
    render_roles_page,
    render_users_page,
)
from sales_support_agent.services.access.notify import send_approval_email, send_invite_email
from sales_support_agent.services.auth_deps import require_tool
from sales_support_agent.services.settings_page import render_settings_page

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/access")

_guard = require_tool("access.manage")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _user_counts_for_roles(roles: list) -> dict:
    """Map role_id -> number of active users with that role."""
    users = store.list_users()
    counts: dict = {}
    for u in users:
        rid = u.get("role_id")
        if rid:
            counts[rid] = counts.get(rid, 0) + 1
    return counts


def _redirect(path: str, flash: Optional[str] = None) -> RedirectResponse:
    url = path if not flash else f"{path}?ok={flash}"
    return RedirectResponse(url, status_code=303)


def _err_redirect(path: str, code: str) -> RedirectResponse:
    return RedirectResponse(f"{path}?err={code}", status_code=303)


def _email_settings(request: Request):
    return (getattr(request.app.state, "agent_settings", None)
            or getattr(request.app.state, "settings", None))


def _flash(request: Request) -> Optional[str]:
    ok = request.query_params.get("ok")
    err = request.query_params.get("err")
    return ok or err


# ---------------------------------------------------------------------------
# Users list
# ---------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
async def users_page(request: Request, current_user: dict = Depends(_guard)):
    users = store.list_users()
    roles = store.list_roles()
    invites = store.list_pending_invites()
    requests_list = store.list_access_requests(status="pending")
    history = sorted(
        store.list_access_requests(status="approved") + store.list_access_requests(status="denied"),
        key=lambda r: r.get("decided_at") or "", reverse=True)[:50]
    return HTMLResponse(render_users_page(users, roles, current_user=current_user,
                                          flash=_flash(request), invites=invites,
                                          requests_list=requests_list, history=history))


@router.post("/users/{user_id}/role")
async def set_role(user_id: str, role_id: str = Form(""),
                   current_user: dict = Depends(_guard)):
    store.set_user_role(user_id, role_id or None)
    return _redirect("/admin/access", "role")


@router.post("/users/{user_id}/status")
async def set_status(user_id: str,
                     action: str = Form(""),
                     current_user: dict = Depends(_guard)):
    new_status = "suspended" if action == "suspend" else "active"
    # Guard: cannot suspend oneself via the form (belt-and-suspenders; UI hides the btn).
    # Resolve the user being targeted.
    all_users = store.list_users()
    target = next((u for u in all_users if u["id"] == user_id), None)
    if target and target.get("email") == current_user.get("email"):
        return _redirect("/admin/access", "status")
    store.set_user_status(user_id, new_status)
    return _redirect("/admin/access", "status")


# ---------------------------------------------------------------------------
# Roles list
# ---------------------------------------------------------------------------


@router.get("/roles", response_class=HTMLResponse)
async def roles_page(request: Request, current_user: dict = Depends(_guard)):
    roles = store.list_roles()
    ucounts = _user_counts_for_roles(roles)
    return HTMLResponse(render_roles_page(roles, ucounts, current_user=current_user,
                                          flash=_flash(request)))


# ---------------------------------------------------------------------------
# Create role
# ---------------------------------------------------------------------------


@router.get("/roles/new", response_class=HTMLResponse)
async def new_role_form(current_user: dict = Depends(_guard)):
    return HTMLResponse(render_role_form_page(None, current_user=current_user, new=True))


@router.post("/roles/new")
async def create_role(
    name: str = Form(""),
    description: str = Form(""),
    permissions: List[str] = Form(default=[]),
    current_user: dict = Depends(_guard),
):
    name = name.strip()
    if not name:
        return HTMLResponse(
            render_role_form_page(
                {"name": name, "description": description, "permissions": permissions},
                current_user=current_user, new=True, error="Role name is required."),
            status_code=422)
    if store.get_role_by_name(name):
        return HTMLResponse(
            render_role_form_page(
                {"name": name, "description": description, "permissions": permissions},
                current_user=current_user, new=True,
                error=f"A role named «{name}» already exists."),
            status_code=422)
    store.create_role(name, permissions, description=description)
    return _redirect("/admin/access/roles", "created")


# ---------------------------------------------------------------------------
# Edit role
# ---------------------------------------------------------------------------


@router.get("/roles/{role_id}/edit", response_class=HTMLResponse)
async def edit_role_form(role_id: str, current_user: dict = Depends(_guard)):
    role = store.get_role(role_id)
    if not role:
        return RedirectResponse("/admin/access/roles", status_code=303)
    return HTMLResponse(render_role_form_page(role, current_user=current_user, new=False))


@router.post("/roles/{role_id}/edit")
async def update_role(
    role_id: str,
    name: str = Form(""),
    description: str = Form(""),
    permissions: List[str] = Form(default=[]),
    current_user: dict = Depends(_guard),
):
    name = name.strip()
    role = store.get_role(role_id)
    if not role:
        return RedirectResponse("/admin/access/roles", status_code=303)
    if not name:
        return HTMLResponse(
            render_role_form_page(
                {**role, "name": name, "description": description, "permissions": permissions},
                current_user=current_user, new=False, error="Role name is required."),
            status_code=422)
    # Name uniqueness: allow same name (no change) or a new unique name.
    existing = store.get_role_by_name(name)
    if existing and existing["id"] != role_id:
        return HTMLResponse(
            render_role_form_page(
                {**role, "name": name, "description": description, "permissions": permissions},
                current_user=current_user, new=False,
                error=f"A role named «{name}» already exists."),
            status_code=422)
    store.update_role(role_id, name=name, description=description, permissions=permissions)
    return _redirect("/admin/access/roles", "updated")


# ---------------------------------------------------------------------------
# Delete role
# ---------------------------------------------------------------------------


@router.post("/roles/{role_id}/delete")
async def delete_role(role_id: str, current_user: dict = Depends(_guard)):
    ok = store.delete_role(role_id)
    if ok:
        return _redirect("/admin/access/roles", "deleted")
    return _err_redirect("/admin/access/roles", "blocked")


# ---------------------------------------------------------------------------
# Invite landing (PUBLIC — no auth, middleware bypass covers /admin/access/invite)
# ---------------------------------------------------------------------------


@router.get("/invite/{token}", response_class=HTMLResponse)
async def invite_landing(token: str, request: Request):
    try:
        invite = store.get_pending_invite_by_token(token)
    except Exception:  # noqa: BLE001 — a public invite link must never 500
        logger.exception("Invite landing failed for token lookup")
        invite = None
    if not invite:
        return HTMLResponse(render_invite_invalid_page(), status_code=410)
    # Store the raw token in a short-lived cookie, then bounce to Google login.
    secure = "localhost" not in str(request.base_url)
    response = RedirectResponse("/admin/auth/google", status_code=302)
    response.set_cookie("pending_invite", token, httponly=True, samesite="lax",
                        path="/", secure=secure, max_age=600)
    return response


# ---------------------------------------------------------------------------
# Invites admin
# ---------------------------------------------------------------------------


@router.get("/invites", response_class=HTMLResponse)
async def invites_page(request: Request, current_user: dict = Depends(_guard)):
    invites = store.list_pending_invites()
    roles = store.list_roles()
    return HTMLResponse(render_invites_page(invites, roles, current_user=current_user,
                                            flash=_flash(request)))


@router.post("/invites/new", response_class=HTMLResponse)
async def create_invite(
    request: Request,
    email: str = Form(""),
    role_id: str = Form(""),
    current_user: dict = Depends(_guard),
):
    email = email.strip().lower()
    if not email:
        return RedirectResponse("/admin/access?err=noemail", status_code=303)
    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(days=7)
    store.create_invite(email, role_id or None, token=token,
                        invited_by=current_user.get("email", ""),
                        expires_at=expires)
    base = str(request.base_url).rstrip("/")
    if "localhost" not in base and "127.0.0.1" not in base:
        base = base.replace("http://", "https://")
    invite_link = f"{base}/admin/access/invite/{token}"
    role_name = ""
    if role_id:
        role = store.get_role(role_id)
        role_name = (role or {}).get("name") or ""
    email_sent = send_invite_email(_email_settings(request), to_email=email,
                                   invite_link=invite_link,
                                   invited_by=current_user.get("email", ""),
                                   role_name=role_name)
    return HTMLResponse(render_invite_created_page(invite_link, email,
                                                   current_user=current_user,
                                                   email_sent=email_sent))


@router.post("/invites/{invite_id}/revoke")
async def revoke_invite(invite_id: str, current_user: dict = Depends(_guard)):
    store.revoke_invite(invite_id)
    return _redirect("/admin/access", "revoked")


# ---------------------------------------------------------------------------
# Access requests admin
# ---------------------------------------------------------------------------


@router.get("/requests", response_class=HTMLResponse)
async def requests_page(request: Request, current_user: dict = Depends(_guard)):
    reqs = store.list_access_requests(status="pending")
    roles = store.list_roles()
    history = sorted(
        store.list_access_requests(status="approved") + store.list_access_requests(status="denied"),
        key=lambda r: r.get("decided_at") or "", reverse=True)[:50]
    return HTMLResponse(render_requests_page(reqs, roles, current_user=current_user,
                                             flash=_flash(request), history=history))


@router.post("/requests/{request_id}/approve")
async def approve_request(
    request: Request,
    request_id: str,
    role_id: str = Form(""),
    current_user: dict = Depends(_guard),
):
    approved_email = store.decide_access_request(request_id, approve=True,
                                                 role_id=role_id or None,
                                                 decided_by=current_user.get("email", ""))
    if approved_email:
        base = str(request.base_url).rstrip("/")
        if "localhost" not in base and "127.0.0.1" not in base:
            base = base.replace("http://", "https://")
        send_approval_email(_email_settings(request), to_email=approved_email,
                            base_url=base, decided_by=current_user.get("email", ""))
    return _redirect("/admin/access", "approved")


@router.post("/requests/{request_id}/deny")
async def deny_request(request_id: str, current_user: dict = Depends(_guard)):
    store.decide_access_request(request_id, approve=False,
                                decided_by=current_user.get("email", ""))
    return _redirect("/admin/access", "denied")


# ---------------------------------------------------------------------------
# Settings page  (/admin/settings — mounted outside the /admin/access prefix
# via the sibling router below)
# ---------------------------------------------------------------------------

_settings_router = APIRouter()


@_settings_router.get("/admin/settings", response_class=HTMLResponse)
async def settings_page(request: Request, current_user: dict = Depends(_guard)):
    all_users = store.list_users()
    active_users = sum(1 for u in all_users if u.get("status") == "active")
    pending_invites = len(store.list_pending_invites())
    pending_requests = len(store.list_access_requests("pending"))
    team_counts = {
        "total_users": len(all_users),
        "active_users": active_users,
        "pending_invites": pending_invites,
        "pending_requests": pending_requests,
    }
    agent_settings = getattr(request.app.state, "agent_settings", None)
    return HTMLResponse(render_settings_page(
        current_user,
        team_counts=team_counts,
        agent_settings=agent_settings,
    ))
