"""Never-email-twice memory for the outbound machine.

Persists every brand domain we have already handed out (via a CSV export or a
Clay push) so the next pull skips it. Backed by the app's existing database
engine through a single isolated table created on first use, so it survives
redeploys without touching the ORM models or any migration.

Everything here fails open: if the DB is unavailable the CSV still builds, just
without dedup. Losing a run's dedup is a nuisance; a 500 on send-day is not.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from sqlalchemy import text

logger = logging.getLogger(__name__)

_TABLE = "outbound_contacted_domains"

# Portable across Postgres (production) and SQLite (tests): both support
# CREATE TABLE IF NOT EXISTS and INSERT ... ON CONFLICT DO NOTHING.
_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    domain TEXT PRIMARY KEY,
    source TEXT,
    tier TEXT,
    signals TEXT,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_SELECT_SQL = f"SELECT domain FROM {_TABLE}"
_SELECT_PUSHED_SQL = f"SELECT domain, tier, signals FROM {_TABLE}"
_INSERT_SQL = f"INSERT INTO {_TABLE} (domain, source) VALUES (:domain, :source) ON CONFLICT (domain) DO NOTHING"
_INSERT_LEAD_SQL = (
    f"INSERT INTO {_TABLE} (domain, source, tier, signals) "
    "VALUES (:domain, :source, :tier, :signals) ON CONFLICT (domain) DO NOTHING"
)
# Best-effort upgrade for tables created before tier/signals existed. SQLite and
# Postgres both accept ADD COLUMN; we swallow the "already exists" error.
_ALTERS = (
    f"ALTER TABLE {_TABLE} ADD COLUMN tier TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN signals TEXT",
)


_RUNS_TABLE = "outbound_pull_runs"

# One row per pull, so we can see what we pulled, when, from which recipe, and
# how it converted from scanned -> matched -> fresh.
_CREATE_RUNS_SQL = f"""
CREATE TABLE IF NOT EXISTS {_RUNS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recipe TEXT,
    scanned INTEGER,
    matched INTEGER,
    fresh INTEGER,
    skipped_seen INTEGER,
    partial INTEGER,
    note TEXT,
    ran_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
# Postgres needs a different auto-increment keyword than SQLite.
_CREATE_RUNS_SQL_PG = _CREATE_RUNS_SQL.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"
)
_INSERT_RUN_SQL = (
    f"INSERT INTO {_RUNS_TABLE} (recipe, scanned, matched, fresh, skipped_seen, partial, note) "
    "VALUES (:recipe, :scanned, :matched, :fresh, :skipped_seen, :partial, :note)"
)
_SELECT_RUNS_SQL = (
    f"SELECT recipe, scanned, matched, fresh, skipped_seen, partial, note, ran_at "
    f"FROM {_RUNS_TABLE} ORDER BY ran_at DESC"
)


def _norm(domain: str) -> str:
    return str(domain or "").strip().lower()


def ensure_runs_table(engine) -> None:
    is_pg = "postgres" in str(getattr(engine, "url", "")).lower()
    sql = _CREATE_RUNS_SQL_PG if is_pg else _CREATE_RUNS_SQL
    with engine.begin() as conn:
        conn.execute(text(sql))


def record_run(engine, *, recipe: str, scanned: int, matched: int, fresh: int,
               skipped_seen: int, partial: bool = False, note: str = "") -> bool:
    """Log one pull. Best-effort: a failure here must never break a pull."""
    try:
        ensure_runs_table(engine)
        with engine.begin() as conn:
            conn.execute(text(_INSERT_RUN_SQL), {
                "recipe": recipe or "", "scanned": int(scanned), "matched": int(matched),
                "fresh": int(fresh), "skipped_seen": int(skipped_seen),
                "partial": 1 if partial else 0, "note": note or "",
            })
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_run failed")
        return False


def load_runs(engine, limit: int = 30) -> list[dict[str, Any]]:
    """Most recent pulls first. Empty list on error."""
    try:
        ensure_runs_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(_SELECT_RUNS_SQL)).fetchall()
        out = []
        for r in rows[:limit]:
            out.append({
                "recipe": r[0], "scanned": r[1], "matched": r[2], "fresh": r[3],
                "skipped_seen": r[4], "partial": bool(r[5]), "note": r[6], "ran_at": r[7],
            })
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_runs failed")
        return []


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SQL))
    for stmt in _ALTERS:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 — column already exists on upgraded DBs
            pass


def load_contacted(engine) -> set[str]:
    """Return every domain we have already exported/contacted. Empty set on error."""
    try:
        ensure_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(_SELECT_SQL)).fetchall()
        return {_norm(r[0]) for r in rows if r and r[0]}
    except Exception:  # noqa: BLE001 — dedup is best-effort; never break the pull
        logger.exception("[outbound-memory] load_contacted failed; proceeding without dedup")
        return set()


def record_contacted(engine, domains: Iterable[str], *, source: str = "csv_export") -> int:
    """Remember these domains so future pulls skip them. Returns count attempted.
    Best-effort: logs and returns 0 on error rather than raising."""
    payload = [{"domain": d, "source": source} for d in {_norm(x) for x in domains} if d]
    if not payload:
        return 0
    try:
        ensure_table(engine)
        with engine.begin() as conn:
            conn.execute(text(_INSERT_SQL), payload)
        return len(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_contacted failed; %s domains not saved", len(payload))
        return 0


def record_leads(engine, leads: Iterable[dict[str, Any]], *, source: str = "csv_export") -> int:
    """Remember pushed leads WITH their tier + signals, for dedup AND per-signal
    efficacy. De-dupes by domain within the batch; existing domains are left as-is.
    Best-effort: returns 0 on error rather than raising."""
    seen: dict[str, dict[str, Any]] = {}
    for lead in leads:
        dom = _norm(lead.get("domain"))
        if not dom or dom in seen:
            continue
        seen[dom] = {
            "domain": dom,
            "source": source,
            "tier": lead.get("tier"),
            "signals": json.dumps(lead.get("signals") or []),
        }
    payload = list(seen.values())
    if not payload:
        return 0
    try:
        ensure_table(engine)
        with engine.begin() as conn:
            conn.execute(text(_INSERT_LEAD_SQL), payload)
        return len(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_leads failed; %s leads not saved", len(payload))
        return 0


def load_pushed(engine) -> list[dict[str, Any]]:
    """Return [{domain, tier, signals[]}] for every pushed brand. Empty on error."""
    try:
        ensure_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(_SELECT_PUSHED_SQL)).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                sigs = json.loads(r[2]) if r[2] else []
            except (ValueError, TypeError):
                sigs = []
            out.append({"domain": r[0], "tier": r[1], "signals": sigs})
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_pushed failed; returning empty")
        return []
