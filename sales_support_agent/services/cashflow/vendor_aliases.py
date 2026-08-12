"""Audited vendor aliases shared by bill detection and bookkeeping.

Aliases join raw merchant histories before a bill is calculated.  They never
join two already-calculated projections, which is the accounting invariant that
prevents a combine action from doubling a bill.
"""

from __future__ import annotations

import re
import weakref
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy import inspect, text

from sales_support_agent.models.database import get_engine

_DISPLAY_NOISE = re.compile(
    r"\b(?:ach|web|ccd|ppd|pos|debit|withdrawal|payment|pmt|pmts|autopay|"
    r"recurring|trace|company|type|card)\b|(?:\*{2,}|\b\d{4,}\b)",
    re.IGNORECASE,
)


def clean_vendor_display_name(value: str) -> str:
    """Return a readable vendor label without changing the underlying evidence."""
    cleaned = _DISPLAY_NOISE.sub(" ", str(value or ""))
    cleaned = re.sub(r"[^A-Za-z0-9&' -]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -")
    return (cleaned or str(value or "Unknown vendor").strip()).title()[:120]


# Engines already known to have the table. A WeakSet and not a set of id()
# values: CPython hands a dead object's address to the next one allocated, so an
# id-keyed cache will eventually tell a brand new engine that its schema is
# already there and skip creating it. That failure is intermittent and looks
# like anything but a caching bug.
_SCHEMA_READY: "weakref.WeakSet[Any]" = weakref.WeakSet()


def ensure_vendor_alias_schema(engine: Any | None = None) -> None:
    db = engine or get_engine()
    # Listing every table in the database to answer "does one table exist" is a
    # round trip, and this is called from the merchant reader that runs once per
    # transaction. Remembering the answer per live engine keeps that off the hot
    # path without weakening the guard: the table cannot disappear under a live
    # process, and a new engine checks again.
    try:
        if db in _SCHEMA_READY:
            return
    except TypeError:
        pass  # not weak-referenceable, so just check the database
    if "finance_vendor_aliases" in set(inspect(db).get_table_names()):
        _remember_schema(db)
        return
    timestamp = "TIMESTAMPTZ" if db.dialect.name == "postgresql" else "DATETIME"
    with db.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS finance_vendor_aliases (
                id VARCHAR(36) PRIMARY KEY,
                scope_key VARCHAR(120) NOT NULL DEFAULT 'default',
                alias_key VARCHAR(255) NOT NULL,
                canonical_key VARCHAR(255) NOT NULL,
                canonical_name VARCHAR(255) NOT NULL,
                created_by VARCHAR(255) NOT NULL,
                created_at {timestamp} NOT NULL,
                revoked_by VARCHAR(255) NULL,
                revoked_at {timestamp} NULL,
                UNIQUE(scope_key, alias_key)
            )
        """))
        connection.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_finance_vendor_aliases_canonical "
            "ON finance_vendor_aliases(scope_key, canonical_key)"
        ))
    _remember_schema(db)


def _remember_schema(db: Any) -> None:
    """Only live engines are remembered, so a recycled address cannot lie."""
    try:
        _SCHEMA_READY.add(db)
    except TypeError:
        pass


def alias_map(*, scope: str = "default", connection: Any | None = None) -> dict[str, dict[str, str]]:
    ensure_vendor_alias_schema()
    owns = connection is None
    conn = connection or get_engine().connect()
    try:
        rows = conn.execute(text("""
            SELECT alias_key, canonical_key, canonical_name
            FROM finance_vendor_aliases
            WHERE scope_key=:scope AND revoked_at IS NULL
        """), {"scope": scope}).fetchall()
        return {
            str(row._mapping["alias_key"]): {
                "canonical_key": str(row._mapping["canonical_key"]),
                "canonical_name": str(row._mapping["canonical_name"]),
            }
            for row in rows
        }
    finally:
        if owns:
            conn.close()


def list_vendor_aliases(*, scope: str = "default") -> list[dict[str, str]]:
    ensure_vendor_alias_schema()
    with get_engine().connect() as connection:
        rows = connection.execute(text("""
            SELECT alias_key, canonical_key, canonical_name, created_by, created_at
            FROM finance_vendor_aliases
            WHERE scope_key=:scope AND revoked_at IS NULL AND alias_key <> canonical_key
            ORDER BY canonical_name, alias_key
        """), {"scope": scope}).fetchall()
    return [{
        key: str(value or "")
        for key, value in dict(row._mapping).items()
    } for row in rows]


def resolve_vendor_key(
    key: str, *, scope: str = "default", aliases: dict[str, dict[str, str]] | None = None
) -> str:
    """Follow a merchant key to the vendor it has been combined into.

    Pass ``aliases`` when resolving many keys in a loop. Without it every call
    queries the alias table, which is one round trip per transaction: grouping a
    1,700 row queue cost roughly 3,500 of them.
    """
    current = str(key or "").strip().lower()
    if aliases is None:
        aliases = alias_map(scope=scope)
    seen: set[str] = set()
    while current not in seen:
        seen.add(current)
        matched = current if current in aliases else next(
            (
                alias for alias in sorted(aliases, key=len, reverse=True)
                if current.startswith(alias + " ")
            ),
            "",
        )
        if not matched:
            break
        current = aliases[matched]["canonical_key"]
    return current


def canonical_name(
    key: str, fallback: str = "", *, scope: str = "default",
    aliases: dict[str, dict[str, str]] | None = None,
) -> str:
    """The display name for a combined vendor. Pass ``aliases`` inside a loop."""
    if aliases is None:
        aliases = alias_map(scope=scope)
    resolved = resolve_vendor_key(key, scope=scope, aliases=aliases)
    for value in aliases.values():
        if value["canonical_key"] == resolved and value["canonical_name"]:
            return value["canonical_name"]
    return fallback or resolved.title()


def combine_vendor_keys(
    keys: Iterable[str],
    *,
    canonical_key: str,
    canonical_name_value: str,
    actor: str,
    scope: str = "default",
    connection: Any | None = None,
) -> dict[str, Any]:
    cleaned = sorted({str(key or "").strip().lower() for key in keys if str(key or "").strip()})
    canonical_key = str(canonical_key or "").strip().lower()
    canonical_name_value = str(canonical_name_value or "").strip()[:255]
    if len(cleaned) < 2:
        raise ValueError("choose at least two vendors to combine")
    if canonical_key not in cleaned:
        raise ValueError("the kept vendor must be one of the selected vendors")
    if not canonical_name_value:
        raise ValueError("give the combined vendor a name")

    ensure_vendor_alias_schema()
    owns = connection is None
    manager = get_engine().begin() if owns else None
    conn = manager.__enter__() if manager else connection
    now = datetime.now(timezone.utc)
    try:
        for key in cleaned:
            conn.execute(text("""
                INSERT INTO finance_vendor_aliases (
                    id, scope_key, alias_key, canonical_key, canonical_name,
                    created_by, created_at, revoked_by, revoked_at
                ) VALUES (
                    :id, :scope, :alias, :canonical, :name, :actor, :now, NULL, NULL
                )
                ON CONFLICT(scope_key, alias_key) DO UPDATE SET
                    canonical_key=excluded.canonical_key,
                    canonical_name=excluded.canonical_name,
                    created_by=excluded.created_by,
                    created_at=excluded.created_at,
                    revoked_by=NULL,
                    revoked_at=NULL
            """), {
                "id": str(uuid4()), "scope": scope, "alias": key,
                "canonical": canonical_key, "name": canonical_name_value,
                "actor": actor or "finance-operator", "now": now,
            })
        if manager:
            manager.__exit__(None, None, None)
    except Exception as exc:
        if manager:
            manager.__exit__(type(exc), exc, exc.__traceback__)
        raise
    return {
        "keys": cleaned, "canonical_key": canonical_key,
        "canonical_name": canonical_name_value, "created_at": now.isoformat(),
    }


def revoke_vendor_alias(
    alias_key: str, *, actor: str, scope: str = "default"
) -> bool:
    ensure_vendor_alias_schema()
    with get_engine().begin() as connection:
        result = connection.execute(text("""
            UPDATE finance_vendor_aliases
            SET revoked_by=:actor, revoked_at=:now
            WHERE scope_key=:scope AND alias_key=:alias AND revoked_at IS NULL
        """), {
            "scope": scope, "alias": str(alias_key or "").strip().lower(),
            "actor": actor or "finance-operator", "now": datetime.now(timezone.utc),
        })
    return bool(result.rowcount)


__all__ = [
    "alias_map", "canonical_name", "clean_vendor_display_name", "combine_vendor_keys",
    "ensure_vendor_alias_schema", "list_vendor_aliases", "resolve_vendor_key",
    "revoke_vendor_alias",
]
