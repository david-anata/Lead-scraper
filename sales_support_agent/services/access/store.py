"""Persistence + permission resolution for RBAC.

Short-lived ORM Sessions on the shared global engine (mirrors
advertising/storage.py). Roles store their permitted tool keys as a JSON list;
a user's effective permissions = their role's keys (super-admins get all).
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.entities import (
    AppAccessRequest,
    AppInvite,
    AppRole,
    AppUser,
)
from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS, valid_keys

logger = logging.getLogger(__name__)


def _new_id() -> str:
    return str(uuid.uuid4())


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


def hash_token(token: str) -> str:
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------


def create_role(name: str, permissions: list, *, description: str = "") -> str:
    rid = _new_id()
    with _session() as s:
        s.add(AppRole(id=rid, name=name.strip(), description=description,
                      permissions_json=valid_keys(permissions)))
    return rid


def update_role(role_id: str, *, name: Optional[str] = None, permissions: Optional[list] = None,
                description: Optional[str] = None) -> bool:
    with _session() as s:
        role = s.get(AppRole, role_id)
        if not role:
            return False
        if name is not None:
            role.name = name.strip()
        if description is not None:
            role.description = description
        if permissions is not None:
            role.permissions_json = valid_keys(permissions)
        role.updated_at = datetime.utcnow()
        return True


def delete_role(role_id: str) -> bool:
    with _session() as s:
        # Block delete while assigned, so we never orphan a user's access.
        if s.query(AppUser).filter(AppUser.role_id == role_id).count() > 0:
            return False
        role = s.get(AppRole, role_id)
        if not role:
            return False
        s.delete(role)
        return True


def get_role(role_id: str) -> Optional[dict]:
    with _session() as s:
        role = s.get(AppRole, role_id)
        return _role_dict(role) if role else None


def get_role_by_name(name: str) -> Optional[dict]:
    with _session() as s:
        role = s.query(AppRole).filter(AppRole.name == name.strip()).first()
        return _role_dict(role) if role else None


def list_roles() -> list:
    with _session() as s:
        return [_role_dict(r) for r in s.query(AppRole).order_by(AppRole.name.asc()).all()]


def _role_dict(role: AppRole) -> dict:
    return {
        "id": role.id,
        "name": role.name,
        "description": role.description or "",
        "permissions": valid_keys(role.permissions_json or []),
    }


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------


def get_user_by_email(email: str) -> Optional[dict]:
    email = _norm_email(email)
    if not email:
        return None
    with _session() as s:
        user = s.query(AppUser).filter(AppUser.email == email).first()
        if not user:
            return None
        return _user_dict(s, user)


def upsert_user(email: str, name: str = "", *, role_id: Optional[str] = None,
                is_superadmin: bool = False, status: str = "active",
                picture_url: str = "") -> str:
    """Create a user if absent; return the user id. Does not downgrade an
    existing super-admin or overwrite an existing role unless explicitly given."""
    email = _norm_email(email)
    with _session() as s:
        user = s.query(AppUser).filter(AppUser.email == email).first()
        if user:
            if name and not user.name:
                user.name = name
            if picture_url and picture_url != user.picture_url:
                user.picture_url = picture_url
            if role_id is not None:
                user.role_id = role_id
            if is_superadmin:
                user.is_superadmin = True
            return user.id
        uid = _new_id()
        s.add(AppUser(id=uid, email=email, name=name, role_id=role_id,
                      is_superadmin=is_superadmin, status=status,
                      picture_url=picture_url))
        return uid


def set_user_role(user_id: str, role_id: Optional[str]) -> bool:
    with _session() as s:
        user = s.get(AppUser, user_id)
        if not user:
            return False
        user.role_id = role_id
        return True


def set_user_status(user_id: str, status: str) -> bool:
    with _session() as s:
        user = s.get(AppUser, user_id)
        if not user or status not in ("active", "suspended"):
            return False
        user.status = status
        return True


def record_login(email: str) -> None:
    email = _norm_email(email)
    with _session() as s:
        user = s.query(AppUser).filter(AppUser.email == email).first()
        if user:
            user.last_login_at = datetime.utcnow()


def list_users() -> list:
    with _session() as s:
        users = s.query(AppUser).order_by(AppUser.email.asc()).all()
        roles = {r.id: r for r in s.query(AppRole).all()}
        out = []
        for u in users:
            d = _user_dict(s, u, roles=roles)
            out.append(d)
        return out


def _user_dict(s: Session, user: AppUser, *, roles: Optional[dict] = None) -> dict:
    # Permissions are granted per-user (user.permissions_json). Super-admins get
    # everything. (The legacy role system is gone; role_id is retained only so
    # old rows don't break and is no longer consulted for access.)
    own = valid_keys(user.permissions_json or [])
    perms = set(ALL_TOOL_KEYS) if user.is_superadmin else set(own)
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name or user.email,
        "picture": user.picture_url or "",
        "role_id": user.role_id,
        "role_name": ("Super-admin" if user.is_superadmin else ""),
        "status": user.status,
        "is_superadmin": bool(user.is_superadmin),
        "permissions": perms,
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
    }


def set_user_permissions(user_id: str, keys) -> bool:
    """Replace a user's granted tool keys (validated against the catalog)."""
    with _session() as s:
        user = s.get(AppUser, user_id)
        if not user:
            return False
        user.permissions_json = valid_keys(keys)
        return True


# ---------------------------------------------------------------------------
# Permission resolution (used by auth_deps on every request)
# ---------------------------------------------------------------------------


def resolve_access(email: str) -> Optional[dict]:
    """Return the enriched access dict for an email, or None if the user is not
    provisioned. A suspended user resolves with empty permissions (status told)."""
    user = get_user_by_email(email)
    if not user:
        return None
    if user["status"] != "active":
        user = {**user, "permissions": set()}
    return user


# ---------------------------------------------------------------------------
# Invites (token issued raw to the link; stored hashed)
# ---------------------------------------------------------------------------


def create_invite(email: str, role_id: Optional[str], *, token: str, invited_by: str = "",
                  expires_at: Optional[datetime] = None) -> str:
    iid = _new_id()
    with _session() as s:
        s.add(AppInvite(id=iid, email=_norm_email(email), role_id=role_id,
                        token_hash=hash_token(token), invited_by=invited_by,
                        status="pending", expires_at=expires_at))
    return iid


def _as_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Coerce a datetime to timezone-aware UTC. Postgres TIMESTAMPTZ columns come
    back tz-aware while datetime.utcnow() and SQLite values are naive — comparing
    the two raises TypeError. Normalize both sides through here before comparing."""
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def get_pending_invite_by_token(token: str, *, now: Optional[datetime] = None) -> Optional[dict]:
    now = _as_aware_utc(now or datetime.now(timezone.utc))
    with _session() as s:
        inv = (s.query(AppInvite)
               .filter(AppInvite.token_hash == hash_token(token), AppInvite.status == "pending")
               .first())
        if not inv:
            return None
        expires_at = _as_aware_utc(inv.expires_at)
        if expires_at and now > expires_at:
            inv.status = "expired"
            return None
        return {"id": inv.id, "email": inv.email, "role_id": inv.role_id}


def accept_invite(invite_id: str) -> None:
    with _session() as s:
        inv = s.get(AppInvite, invite_id)
        if inv:
            inv.status = "accepted"
            inv.accepted_at = datetime.utcnow()


def list_pending_invites() -> list:
    with _session() as s:
        rows = (s.query(AppInvite).filter(AppInvite.status == "pending")
                .order_by(AppInvite.created_at.desc()).all())
        role_ids = {i.role_id for i in rows if i.role_id}
        role_names: dict = {}
        if role_ids:
            for r in s.query(AppRole).filter(AppRole.id.in_(role_ids)).all():
                role_names[r.id] = r.name
        return [{"id": i.id, "email": i.email, "role_id": i.role_id,
                 "role_name": role_names.get(i.role_id, ""),
                 "created_at": i.created_at.isoformat() if i.created_at else None} for i in rows]


def revoke_invite(invite_id: str) -> None:
    with _session() as s:
        inv = s.get(AppInvite, invite_id)
        if inv and inv.status == "pending":
            inv.status = "revoked"


# ---------------------------------------------------------------------------
# Access requests
# ---------------------------------------------------------------------------


def create_access_request(email: str, name: str = "") -> str:
    email = _norm_email(email)
    with _session() as s:
        existing = (s.query(AppAccessRequest)
                    .filter(AppAccessRequest.email == email, AppAccessRequest.status == "pending")
                    .first())
        if existing:
            return existing.id
        rid = _new_id()
        s.add(AppAccessRequest(id=rid, email=email, name=name, status="pending"))
        return rid


def list_access_requests(status: str = "pending") -> list:
    with _session() as s:
        rows = (s.query(AppAccessRequest).filter(AppAccessRequest.status == status)
                .order_by(AppAccessRequest.requested_at.desc()).all())
        return [{"id": r.id, "email": r.email, "name": r.name, "status": r.status,
                 "requested_at": r.requested_at.isoformat() if r.requested_at else None,
                 "decided_by": r.decided_by or "",
                 "decided_at": r.decided_at.isoformat() if r.decided_at else None} for r in rows]


def decide_access_request(request_id: str, *, approve: bool, role_id: Optional[str] = None,
                          decided_by: str = "") -> Optional[str]:
    """Approve (creating/activating the user with role_id) or deny a request.
    Returns the user email on approval, else None."""
    with _session() as s:
        req = s.get(AppAccessRequest, request_id)
        if not req or req.status != "pending":
            return None
        req.status = "approved" if approve else "denied"
        req.decided_by = decided_by
        req.decided_at = datetime.utcnow()
        req.assigned_role_id = role_id if approve else None
        email = req.email if approve else None
    if approve and email:
        upsert_user(email, role_id=role_id, status="active")
    return email


# ---------------------------------------------------------------------------
# Seeding — never get locked out
# ---------------------------------------------------------------------------


def seed_superadmins(emails) -> None:
    """Idempotently ensure each email exists as an active super-admin."""
    for email in emails or ():
        email = _norm_email(email)
        if not email:
            continue
        try:
            upsert_user(email, is_superadmin=True, status="active")
        except Exception:  # noqa: BLE001 — never let seeding crash startup
            logger.exception("[access] failed to seed super-admin %s", email)
