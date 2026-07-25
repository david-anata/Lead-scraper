"""Tunable lead-sourcing settings, with a change log and a config version.

Two jobs:

1. Let an operator retune the pull recipes (windows, caps, how many churn tools
   a day, which recipes are live) without a code change.
2. Make every change **attributable**. A date and a note alone cannot tell you
   whether last week's results came from the old settings or the new ones, so
   every change bumps a **config version**, and every pull records the version it
   ran under. Leads inherit that version through their run, which is what makes
   "did widening the window actually help?" answerable instead of a guess.

Backed by two small isolated tables created on first use. Everything fails open:
if the store is unreachable we fall back to the shipped defaults rather than
stopping a pull.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

_SETTINGS_TABLE = "outbound_settings"
_CHANGES_TABLE = "outbound_setting_changes"

_CREATE_SETTINGS = f"""
CREATE TABLE IF NOT EXISTS {_SETTINGS_TABLE} (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_CHANGES = f"""
CREATE TABLE IF NOT EXISTS {_CHANGES_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER,
    key TEXT,
    old_value TEXT,
    new_value TEXT,
    note TEXT,
    changed_by TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_CHANGES_PG = _CREATE_CHANGES.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")

_UPSERT = (
    f"INSERT INTO {_SETTINGS_TABLE} (key, value) VALUES (:key, :value) "
    "ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = CURRENT_TIMESTAMP"
)
_INSERT_CHANGE = (
    f"INSERT INTO {_CHANGES_TABLE} (version, key, old_value, new_value, note, changed_by) "
    "VALUES (:version, :key, :old_value, :new_value, :note, :changed_by)"
)


def _is_pg(engine) -> bool:
    return "postgres" in str(getattr(engine, "url", "")).lower()


def ensure_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SETTINGS))
        conn.execute(text(_CREATE_CHANGES_PG if _is_pg(engine) else _CREATE_CHANGES))


def load_settings(engine) -> dict[str, Any]:
    """Current overrides. Empty dict means 'use the shipped defaults'."""
    if engine is None:
        return {}
    try:
        ensure_tables(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT key, value FROM {_SETTINGS_TABLE}")).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:  # noqa: BLE001 — never block a pull on settings
        logger.exception("[outbound-settings] load failed; using defaults")
        return {}


def config_version(engine) -> int:
    """How many changes have been made. This is the tag that segments results."""
    if engine is None:
        return 0
    try:
        ensure_tables(engine)
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT COUNT(*) FROM {_CHANGES_TABLE}")).fetchone()
        return int(row[0] or 0)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-settings] version read failed")
        return 0


def apply_changes(engine, updates: dict[str, Any], *, note: str = "",
                  changed_by: str = "") -> dict[str, Any]:
    """Save only the values that actually changed, log each one against a new
    version, and return a summary. A change with no real difference is ignored,
    so the log stays meaningful."""
    if engine is None:
        return {"ok": False, "reason": "No database available.", "changed": 0}
    try:
        ensure_tables(engine)
        current = load_settings(engine)
        real = {k: str(v) for k, v in updates.items()
                if v is not None and str(v) != str(current.get(k, ""))}
        if not real:
            return {"ok": True, "changed": 0, "version": config_version(engine),
                    "reason": "Nothing changed."}
        new_version = config_version(engine) + 1
        with engine.begin() as conn:
            for key, value in real.items():
                conn.execute(text(_UPSERT), {"key": key, "value": value})
                conn.execute(text(_INSERT_CHANGE), {
                    "version": new_version, "key": key,
                    "old_value": str(current.get(key, "")), "new_value": value,
                    "note": note or "", "changed_by": changed_by or "",
                })
        return {"ok": True, "changed": len(real), "version": new_version,
                "keys": sorted(real)}
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound-settings] apply failed")
        return {"ok": False, "reason": str(exc), "changed": 0}


def load_changes(engine, limit: int = 25) -> list[dict[str, Any]]:
    """The change log: what changed, when, and why."""
    if engine is None:
        return []
    try:
        ensure_tables(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT version, key, old_value, new_value, note, changed_by, changed_at "
                f"FROM {_CHANGES_TABLE} ORDER BY id DESC"
            )).fetchall()
        return [{"version": r[0], "key": r[1], "old_value": r[2], "new_value": r[3],
                 "note": r[4], "changed_by": r[5], "changed_at": r[6]} for r in rows[:limit]]
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-settings] change log read failed")
        return []


def effective(engine, defaults: dict[str, Any]) -> dict[str, Any]:
    """Defaults with any saved overrides applied, coerced to the default's type."""
    saved = load_settings(engine)
    out = dict(defaults)
    for k, v in saved.items():
        if k not in out:
            out[k] = v
            continue
        try:
            out[k] = type(defaults[k])(v) if isinstance(defaults[k], int) else v
        except (TypeError, ValueError):
            out[k] = v
    return out
