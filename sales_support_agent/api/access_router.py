"""Access admin UI — /admin/access.

Requires `access.manage` for every route (super-admin only by default).
Provides:
  GET  /admin/access                  → users list
  POST /admin/access/users/{id}/role  → assign/change role
  POST /admin/access/users/{id}/status → suspend / activate

  GET  /admin/access/roles            → roles list
  GET  /admin/access/roles/new        → create-role form
  POST /admin/access/roles/new        → create role
  GET  /admin/access/roles/{id}/edit  → edit-role form
  POST /admin/access/roles/{id}/edit  → save role edits
  POST /admin/access/roles/{id}/delete → delete role (blocked if assigned)
"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_support_agent.services.access import store
from sales_support_agent.services.access.pages import (
    render_role_form_page,
    render_roles_page,
    render_users_page,
)
from sales_support_agent.services.auth_deps import require_tool

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
    return HTMLResponse(render_users_page(users, roles, current_user=current_user,
                                          flash=_flash(request)))


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
