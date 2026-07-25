"""Never-email-twice memory for the outbound machine.

Persists every brand domain we have already handed out (via a CSV export or a
Clay push) so the next pull skips it. Backed by the app's existing database
engine through a single isolated table created on first use, so it survives
redeploys without touching the ORM models or any migration.

Everything here fails open: if the DB is unavailable the CSV still builds, just
without dedup. Losing a run's dedup is a nuisance; a 500 on send-day is not.
"""

from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy import text

logger = logging.getLogger(__name__)

_TABLE = "outbound_contacted_domains"

# Portable across Postgres (production) and SQLite (tests): both support
# CREATE TABLE IF NOT EXISTS and INSERT ... ON CONFLICT DO NOTHING.
_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    domain TEXT PRIMARY KEY,
    source TEXT,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_SELECT_SQL = f"SELECT domain FROM {_TABLE}"
_INSERT_SQL = f"INSERT INTO {_TABLE} (domain, source) VALUES (:domain, :source) ON CONFLICT (domain) DO NOTHING"


def _norm(domain: str) -> str:
    return str(domain or "").strip().lower()


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_CREATE_SQL))


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
