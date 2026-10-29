"""Our own suppression list for outbound email.

Instantly's blocklist is still the sender's last line of defense. This table
is the one the push checks, so an opt-out, unsubscribe, hard bounce, or manual
add is skipped even when Instantly's import would have accepted it.

Rows are emails or registrable domains. A domain row covers the apex and its
store hosts (shop.brand.com and brand.com are the same brand). Sync from
Instantly inserts missing blocklist rows and does not overwrite a reason we
already recorded.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Optional

import requests
from sqlalchemy import text

import outbound_contacts as contacts

logger = logging.getLogger(__name__)

BLOCKLIST_URL = "https://api.instantly.ai/api/v2/block-lists-entries"
_SUPPRESS = "outbound_suppressions"
_STATE = "outbound_sync_state"
_EVENTS = "outbound_delivery_events"

_CREATE_SUPPRESS = f"""
CREATE TABLE IF NOT EXISTS {_SUPPRESS} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (kind, value)
)
"""
_CREATE_SUPPRESS_PG = _CREATE_SUPPRESS.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")

_CREATE_STATE = f"""
CREATE TABLE IF NOT EXISTS {_STATE} (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

_CREATE_EVENTS = f"""
CREATE TABLE IF NOT EXISTS {_EVENTS} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    campaign_id TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    external_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_EVENTS_PG = _CREATE_EVENTS.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")


def _skip_ddl(engine) -> bool:
    return bool(os.getenv("VERCEL")) and getattr(getattr(engine, "dialect", None), "name", "") == "postgresql"


def ensure_tables(engine) -> None:
    """Create the suppression, sync-cursor, and delivery-event tables.

    Vercel's runtime role cannot run DDL. Production uses the SQL migration.
    """
    if engine is None or _skip_ddl(engine):
        return
    pg = getattr(getattr(engine, "dialect", None), "name", "") == "postgresql"
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SUPPRESS_PG if pg else _CREATE_SUPPRESS))
        conn.execute(text(_CREATE_STATE))
        conn.execute(text(_CREATE_EVENTS_PG if pg else _CREATE_EVENTS))


def _clean_email(value: str) -> str:
    return (value or "").strip().lower()


def _kind_and_value(kind: str, value: str) -> tuple[str, str]:
    raw = (value or "").strip()
    if "@" in raw:
        email = _clean_email(raw)
        return ("email", email) if "@" in email else ("", "")
    # A blocklist row with no @ is a domain even if the flag was omitted.
    domain = contacts.registrable_domain(raw)
    if not domain or "@" in domain:
        return "", ""
    return "domain", domain


def add(engine, *, kind: str, value: str, reason: str, source: str,
        detail: str = "", overwrite: bool = True) -> bool:
    """Record one suppression. Returns False when the value is empty."""
    ensure_tables(engine)
    kind, value = _kind_and_value(kind, value)
    if not value:
        return False
    reason = (reason or "manual").strip()[:64]
    source = (source or "manual").strip()[:64]
    detail = (detail or "")[:300]
    sql = (
        f"INSERT INTO {_SUPPRESS} (kind, value, reason, source, detail) "
        "VALUES (:k, :v, :r, :s, :d) "
    )
    if overwrite:
        sql += (
            "ON CONFLICT (kind, value) DO UPDATE SET reason = excluded.reason, "
            "source = excluded.source, detail = excluded.detail"
        )
    else:
        sql += "ON CONFLICT (kind, value) DO NOTHING"
    with engine.begin() as conn:
        conn.execute(text(sql), {"k": kind, "v": value, "r": reason, "s": source, "d": detail})
    return True


def lists(engine) -> tuple[set[str], set[str]]:
    """(emails, registrable domains) currently suppressed."""
    ensure_tables(engine)
    emails: set[str] = set()
    domains: set[str] = set()
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT kind, value FROM {_SUPPRESS}")).fetchall()
    for kind, value in rows:
        if kind == "email":
            emails.add(str(value).lower())
        elif value:
            domains.add(str(value).lower())
    return emails, domains


def matched_value(email: str, domain: str, emails: set[str], domains: set[str]) -> str:
    """The suppression value that blocks this contact, or ''."""
    address = _clean_email(email)
    if address and address in emails:
        return address
    hosts = []
    if "@" in address:
        hosts.append(address.split("@", 1)[1])
    if domain:
        hosts.append(domain)
    for host in hosts:
        raw = contacts.normalize_host(host)
        reg = contacts.registrable_domain(host)
        if raw in domains:
            return raw
        if reg in domains:
            return reg
        for blocked in domains:
            if raw.endswith("." + blocked) or (reg and reg == blocked):
                return blocked
    return ""


def partition(engine, people: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split contacts into (allowed, suppressed). Raises if the list cannot be read."""
    emails, domains = lists(engine)
    allowed, blocked = [], []
    for person in people:
        hit = matched_value(str(person.get("email") or ""), str(person.get("domain") or ""), emails, domains)
        if hit:
            blocked.append({**person, "suppressed_match": hit})
        else:
            allowed.append(person)
    return allowed, blocked


def get_cursor(engine, key: str) -> str:
    ensure_tables(engine)
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT value FROM {_STATE} WHERE key = :k"), {"k": key}).fetchone()
    return str(row[0] or "") if row else ""


def set_cursor(engine, key: str, value: str) -> None:
    ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {_STATE} (key, value) VALUES (:k, :v) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP"),
            {"k": key, "v": value or ""})


def _api_key() -> str:
    return (os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip()


def _redact(message: str, key: str) -> str:
    if key and len(key) > 6:
        return message.replace(key, "<instantly-key>")
    return message


def sync_blocklist(engine, *, key: str = "", get: Optional[Callable[..., Any]] = None,
                   max_pages: int = 3) -> dict[str, Any]:
    """Copy Instantly blocklist entries into the local table. Never raises."""
    key = (key or _api_key()).strip()
    if not key:
        return {"synced": 0, "pages": 0, "reason": "no key"}
    try:
        ensure_tables(engine)
    except Exception:  # noqa: BLE001
        logger.exception("[suppression] could not prepare tables")
        return {"synced": 0, "pages": 0, "reason": "tables unavailable"}
    get = get or requests.get
    cursor = ""
    try:
        cursor = get_cursor(engine, "instantly_blocklist")
    except Exception:  # noqa: BLE001
        logger.exception("[suppression] could not read the blocklist cursor")
        return {"synced": 0, "pages": 0, "reason": "cursor unreadable"}
    synced = 0
    pages = 0
    for _ in range(max(1, max_pages)):
        params: dict[str, Any] = {"limit": 100}
        if cursor:
            params["starting_after"] = cursor
        try:
            resp = get(BLOCKLIST_URL, headers={"Authorization": f"Bearer {key}"}, params=params, timeout=30)
            status = int(getattr(resp, "status_code", 0) or 0)
            data = resp.json() if status == 200 else {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("[suppression] blocklist sync failed: %s", _redact(str(exc), key))
            return {"synced": synced, "pages": pages, "reason": "request failed"}
        if status >= 300 or not isinstance(data, dict):
            logger.warning("[suppression] blocklist sync returned %s", status)
            return {"synced": synced, "pages": pages, "reason": f"http {status}"}
        items = data.get("items") or []
        for item in items:
            if not isinstance(item, dict):
                continue
            value = str(item.get("bl_value") or "")
            kind = "domain" if item.get("is_domain") else "email"
            if value:
                add(engine, kind=kind, value=value, reason="instantly_blocklist",
                    source="instantly", overwrite=False)
                synced += 1
        pages += 1
        nxt = str(data.get("next_starting_after") or "")
        if not nxt or not items:
            # Final page. Keep the last non-empty cursor. Never persist "".
            break
        cursor = nxt
    try:
        if cursor:
            set_cursor(engine, "instantly_blocklist", cursor)
    except Exception:  # noqa: BLE001
        logger.exception("[suppression] could not save the blocklist cursor")
    return {"synced": synced, "pages": pages, "reason": ""}
