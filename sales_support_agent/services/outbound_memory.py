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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import text

logger = logging.getLogger(__name__)


class OutboundPersistenceError(RuntimeError):
    """A pull could not be durably recorded and must not be reported complete."""


@dataclass(frozen=True)
class PersistedPull:
    run_id: int
    company_count: int
    membership_count: int

_TABLE = "outbound_contacted_domains"

# Portable across Postgres (production) and SQLite (tests): both support
# CREATE TABLE IF NOT EXISTS and INSERT ... ON CONFLICT DO NOTHING.
_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    domain TEXT PRIMARY KEY,
    source TEXT,
    tier TEXT,
    signals TEXT,
    brand TEXT,
    niche TEXT,
    country TEXT,
    score INTEGER,
    reason TEXT,
    recipe TEXT,
    revenue_cents BIGINT,
    categories TEXT,
    config_version INTEGER,
    amazon_facts TEXT,
    amazon_confidence TEXT,
    amazon_marketplace TEXT,
    amazon_checked_at TEXT,
    amazon_absent INTEGER,
    amazon_sellers_unknown INTEGER,
    amazon_skipped_reason TEXT,
    video_url TEXT,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_SELECT_SQL = f"SELECT domain FROM {_TABLE}"
_SELECT_PUSHED_SQL = f"SELECT domain, tier, signals FROM {_TABLE}"
_INSERT_SQL = f"INSERT INTO {_TABLE} (domain, source) VALUES (:domain, :source) ON CONFLICT (domain) DO NOTHING"
# The columns every lead has had since before the Amazon check existed. Kept
# separate so a table we could not widen still stores and returns its leads.
_CORE_LEAD_COLS = ("domain", "source", "tier", "signals", "brand", "niche", "country",
                   "score", "reason", "recipe", "revenue_cents", "categories", "config_version")
# What the Amazon check found, and how old the finding is. Stored here because
# a finding we cannot reproduce later is a finding we have lost.
_AMAZON_LEAD_COLS = ("amazon_facts", "amazon_confidence", "amazon_marketplace", "amazon_checked_at",
                     "amazon_absent", "amazon_sellers_unknown", "amazon_skipped_reason")
_LEAD_COLS = _CORE_LEAD_COLS + _AMAZON_LEAD_COLS
# Read but never inserted. A Tape link is pasted in by hand long after the lead
# was sourced, so it belongs to the update path, not the insert one - keeping it
# out of _LEAD_COLS means record_leads does not have to invent a value for it.
_SELECT_LEAD_COLS = _LEAD_COLS + ("video_url",)


def _insert_lead_sql(cols: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO {_TABLE} ({', '.join(cols)}) "
        f"VALUES ({', '.join(':' + c for c in cols)}) ON CONFLICT (domain) DO NOTHING"
    )


def _select_leads_sql(cols: tuple[str, ...]) -> str:
    return (
        f"SELECT {', '.join(cols)}, first_seen_at FROM {_TABLE} "
        "ORDER BY first_seen_at DESC"
    )


_INSERT_LEAD_SQL = _insert_lead_sql(_LEAD_COLS)
_INSERT_LEAD_CORE_SQL = _insert_lead_sql(_CORE_LEAD_COLS)
_SELECT_LEADS_SQL = _select_leads_sql(_SELECT_LEAD_COLS)
_SELECT_LEADS_CORE_SQL = _select_leads_sql(_CORE_LEAD_COLS)
# Best-effort upgrade for tables created before tier/signals existed. SQLite and
# Postgres both accept ADD COLUMN; we swallow the "already exists" error.
_ALTERS = (
    f"ALTER TABLE {_TABLE} ADD COLUMN tier TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN signals TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN brand TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN niche TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN country TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN score INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN reason TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN recipe TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN revenue_cents BIGINT",
    f"ALTER TABLE {_TABLE} ADD COLUMN categories TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN config_version INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_facts TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_confidence TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_marketplace TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_checked_at TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_absent INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_sellers_unknown INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_skipped_reason TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN video_url TEXT",
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
    config_version INTEGER,
    delivery TEXT,
    delivered INTEGER,
    note TEXT,
    ran_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
# Postgres needs a different auto-increment keyword than SQLite.
_CREATE_RUNS_SQL_PG = _CREATE_RUNS_SQL.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"
)
_INSERT_RUN_SQL = (
    f"INSERT INTO {_RUNS_TABLE} (recipe, scanned, matched, fresh, skipped_seen, partial, config_version, delivery, delivered, note) "
    "VALUES (:recipe, :scanned, :matched, :fresh, :skipped_seen, :partial, :config_version, :delivery, :delivered, :note)"
)
# Best-effort upgrade for runs tables created before versioning existed.
_RUN_ALTERS = (
    f"ALTER TABLE {_RUNS_TABLE} ADD COLUMN config_version INTEGER",
    f"ALTER TABLE {_RUNS_TABLE} ADD COLUMN delivery TEXT",
    f"ALTER TABLE {_RUNS_TABLE} ADD COLUMN delivered INTEGER",
)
_SELECT_RUNS_SQL = (
    f"SELECT id, recipe, scanned, matched, fresh, skipped_seen, partial, note, ran_at, config_version, delivery, delivered "
    f"FROM {_RUNS_TABLE} ORDER BY ran_at DESC"
)

_RUN_LEADS_TABLE = "outbound_pull_run_leads"
_CREATE_RUN_LEADS_SQL = f"""
CREATE TABLE IF NOT EXISTS {_RUN_LEADS_TABLE} (
    run_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    lead_json TEXT NOT NULL,
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, domain)
)
"""

_DELIVERY_TABLE = "outbound_delivery_settings"
_CREATE_DELIVERY_SQL = f"""
CREATE TABLE IF NOT EXISTS {_DELIVERY_TABLE} (
    id INTEGER PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 0,
    email_enabled INTEGER NOT NULL DEFAULT 0,
    slack_enabled INTEGER NOT NULL DEFAULT 0,
    frequency TEXT NOT NULL DEFAULT 'daily',
    email_recipients TEXT NOT NULL DEFAULT '',
    slack_channel TEXT NOT NULL DEFAULT '',
    content_mode TEXT NOT NULL DEFAULT 'link',
    updated_by TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

_EXPORTS_TABLE = "outbound_export_history"
_CREATE_EXPORTS_SQL = f"""
CREATE TABLE IF NOT EXISTS {_EXPORTS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL DEFAULT '',
    run_ids TEXT NOT NULL,
    source_rows INTEGER NOT NULL DEFAULT 0,
    unique_companies INTEGER NOT NULL DEFAULT 0,
    duplicates_removed INTEGER NOT NULL DEFAULT 0,
    include_duplicates INTEGER NOT NULL DEFAULT 0,
    filename TEXT NOT NULL DEFAULT '',
    exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_EXPORTS_SQL_PG = _CREATE_EXPORTS_SQL.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"
)
_DELIVERY_HISTORY_TABLE = "outbound_delivery_history"
_CREATE_DELIVERY_HISTORY_SQL = f"""
CREATE TABLE IF NOT EXISTS {_DELIVERY_HISTORY_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    recipe TEXT NOT NULL DEFAULT '',
    destination TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CREATE_DELIVERY_HISTORY_SQL_PG = _CREATE_DELIVERY_HISTORY_SQL.replace(
    "INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY"
)


def _norm(domain: str) -> str:
    return str(domain or "").strip().lower()


def _text(value: Any) -> str:
    """Anything at all as a stored string. None and junk become empty, never an error."""
    return "" if value is None else str(value).strip()


def _whole(value: Any) -> int:
    """A count we can store. Anything unreadable counts as none seen."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


_FALSEY_TEXT = {"", "0", "false", "no", "none", "null"}


def _flag(value: Any) -> int:
    """A yes/no we can store, tolerant of the string forms a CSV or an API sends."""
    if isinstance(value, str):
        return 0 if value.strip().lower() in _FALSEY_TEXT else 1
    return 1 if value else 0


def _amazon_values(lead: dict[str, Any]) -> dict[str, Any]:
    """The Amazon check as stored columns.

    Flat amazon_* keys lead; a whole brand_control result parked under "amazon"
    is read as a fallback so a finding never falls off the lead on its way in.
    """
    nested = lead.get("amazon")
    nested = nested if isinstance(nested, dict) else {}
    findings = nested.get("findings")
    findings = findings if isinstance(findings, dict) else {}

    def pick(flat: str, *fallbacks: Any) -> Any:
        val = lead.get(flat)
        if val not in (None, ""):
            return val
        for candidate in fallbacks:
            if candidate not in (None, ""):
                return candidate
        return None

    # The bucketed facts Clay writes from, kept as one blob so adding a fact
    # later needs no migration and the export cannot drift from the scan.
    facts = lead.get("amazon_facts")
    if not facts and nested:
        try:
            from outbound_amazon import clay_facts
            facts = json.dumps(clay_facts(nested))
        except Exception:  # noqa: BLE001
            facts = ""
    if isinstance(facts, dict):
        facts = json.dumps(facts)

    return {
        "amazon_facts": _text(facts),
        "amazon_confidence": _text(pick("amazon_confidence", nested.get("confidence"))),
        "amazon_marketplace": _text(pick("amazon_marketplace", nested.get("marketplace"))),
        "amazon_checked_at": _text(pick("amazon_checked_at", nested.get("checked_at"))),
        "amazon_absent": _flag(pick("amazon_absent", findings.get("absent"))),
        "amazon_sellers_unknown": _whole(pick("amazon_sellers_unknown",
                                              nested.get("sellers_unknown"))),
        "amazon_skipped_reason": _text(pick("amazon_skipped_reason",
                                            nested.get("skipped_reason"))),
    }


def ensure_runs_table(engine, *, force: bool = False) -> None:
    is_pg = "postgres" in str(getattr(engine, "url", "")).lower()
    if is_pg and not force:
        return
    sql = _CREATE_RUNS_SQL_PG if is_pg else _CREATE_RUNS_SQL
    with engine.begin() as conn:
        conn.execute(text(sql))
    for stmt in _RUN_ALTERS:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 — column already present
            pass
    with engine.begin() as conn:
        conn.execute(text(_CREATE_RUN_LEADS_SQL))
        conn.execute(text(_CREATE_DELIVERY_SQL))
        conn.execute(text(_CREATE_EXPORTS_SQL_PG if is_pg else _CREATE_EXPORTS_SQL))
        conn.execute(text(_CREATE_DELIVERY_HISTORY_SQL_PG if is_pg else _CREATE_DELIVERY_HISTORY_SQL))
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {_DELIVERY_TABLE} ADD COLUMN slack_channel TEXT NOT NULL DEFAULT ''"))
    except Exception:  # noqa: BLE001 — additive column already exists
        pass


def ensure_outbound_schema(engine) -> None:
    """Create or upgrade every table required by the outbound workflow."""
    if engine is None:
        raise OutboundPersistenceError("Database engine is unavailable.")
    ensure_table(engine, force=True)
    ensure_runs_table(engine, force=True)


def persistence_health(engine, *, require_runs: bool = True) -> dict[str, Any]:
    """Return truthful outbound database readiness without mutating schema."""
    if engine is None:
        return {"ready": False, "code": "engine_unavailable", "detail": "Database engine is unavailable."}
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SELECT COUNT(*) FROM {_TABLE}"))
            if require_runs:
                conn.execute(text(f"SELECT COUNT(*) FROM {_RUNS_TABLE}"))
                conn.execute(text(f"SELECT COUNT(*) FROM {_RUN_LEADS_TABLE}"))
        return {"ready": True, "code": "ready", "detail": "Outbound persistence is ready."}
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound-memory] persistence health check failed")
        return {"ready": False, "code": "schema_unavailable", "detail": exc.__class__.__name__}


def record_run(engine, *, recipe: str, scanned: int, matched: int, fresh: int,
               skipped_seen: int, partial: bool = False, note: str = "",
               config_version: int = 0, delivery: str = "file",
               delivered: int = 0) -> int:
    """Log one pull and return its id. Best-effort: zero on failure."""
    if engine is None:
        return False
    try:
        ensure_runs_table(engine)
        is_pg = "postgres" in str(getattr(engine, "url", "")).lower()
        with engine.begin() as conn:
            result = conn.execute(text(_INSERT_RUN_SQL + (" RETURNING id" if is_pg else "")), {
                "recipe": recipe or "", "scanned": int(scanned), "matched": int(matched),
                "fresh": int(fresh), "skipped_seen": int(skipped_seen),
                "partial": 1 if partial else 0, "note": note or "",
                "config_version": int(config_version or 0),
                "delivery": delivery or "file", "delivered": int(delivered or 0),
            })
            run_id = result.scalar_one() if is_pg else getattr(result, "lastrowid", None)
        return int(run_id or 0)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_run failed")
        return 0


def total_delivered(engine) -> int:
    """How many rows we have actually pushed to Clay, for the budget guard."""
    if engine is None:
        return 0
    try:
        ensure_runs_table(engine)
        with engine.connect() as conn:
            row = conn.execute(text(
                f"SELECT COALESCE(SUM(delivered), 0) FROM {_RUNS_TABLE} WHERE delivery = 'clay'"
            )).fetchone()
        return int(row[0] or 0)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] total_delivered failed")
        return 0


def load_runs(engine, limit: int = 30) -> list[dict[str, Any]]:
    """Most recent pulls first. Empty list when there is no database or on error."""
    if engine is None:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(_SELECT_RUNS_SQL)).fetchall()
        out = []
        for r in rows[:limit]:
            out.append({
                "id": int(r[0]), "recipe": r[1], "scanned": r[2], "matched": r[3], "fresh": r[4],
                "skipped_seen": r[5], "partial": bool(r[6]), "note": r[7], "ran_at": r[8],
                "config_version": r[9] if len(r) > 9 else 0,
                "delivery": (r[10] if len(r) > 10 else None) or "file",
                "delivered": (r[11] if len(r) > 11 else 0) or 0,
            })
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_runs failed")
        return []


def record_run_leads(engine, run_id: int, leads: Iterable[dict[str, Any]]) -> int:
    """Persist the exact company membership of a pull without changing suppression."""
    if engine is None or not run_id:
        return 0
    payload = []
    seen: set[str] = set()
    for lead in leads:
        domain = _norm(lead.get("domain"))
        website = _norm(lead.get("website")).replace("https://", "").replace("http://", "").rstrip("/")
        brand = _norm(lead.get("brand") or lead.get("company"))
        identity = domain or website or (f"name:{brand}" if brand else "")
        if not identity or identity in seen:
            continue
        seen.add(identity)
        payload.append({"run_id": int(run_id), "domain": identity, "lead_json": json.dumps(lead, default=str)})
    if not payload:
        return 0
    try:
        ensure_runs_table(engine)
        with engine.begin() as conn:
            conn.execute(text(
                f"INSERT INTO {_RUN_LEADS_TABLE} (run_id, domain, lead_json) "
                "VALUES (:run_id, :domain, :lead_json) ON CONFLICT (run_id, domain) DO NOTHING"
            ), payload)
        return len(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_run_leads failed")
        return 0


def load_run_leads(engine, run_ids: Iterable[int]) -> list[dict[str, Any]]:
    """Return exact pull rows, including provenance, for selected run ids."""
    wanted = sorted({int(x) for x in run_ids if int(x) > 0})
    if engine is None or not wanted:
        return []
    try:
        placeholders = ",".join(f":r{i}" for i in range(len(wanted)))
        params = {f"r{i}": value for i, value in enumerate(wanted)}
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT l.run_id, l.lead_json, r.recipe, r.ran_at, r.partial, r.config_version "
                f"FROM {_RUN_LEADS_TABLE} l JOIN {_RUNS_TABLE} r ON r.id=l.run_id "
                f"WHERE l.run_id IN ({placeholders}) ORDER BY r.ran_at DESC, r.id DESC, l.domain"
            ), params).fetchall()
        out = []
        for row in rows:
            lead = json.loads(row[1] or "{}")
            lead.update({"pull_id": int(row[0]), "pull_recipe": row[2] or "",
                         "pull_date": str(row[3] or ""),
                         "pull_status": "cut short" if row[4] else "complete",
                         "settings_version": int(row[5] or 0), "source": "StoreLeads"})
            out.append(lead)
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_run_leads failed")
        return []


def run_lead_counts(engine, run_ids: Iterable[int]) -> dict[int, int]:
    wanted = sorted({int(x) for x in run_ids if int(x) > 0})
    if engine is None or not wanted:
        return {}
    try:
        placeholders = ",".join(f":r{i}" for i in range(len(wanted)))
        params = {f"r{i}": value for i, value in enumerate(wanted)}
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT run_id, COUNT(*) FROM {_RUN_LEADS_TABLE} WHERE run_id IN ({placeholders}) GROUP BY run_id"), params).fetchall()
        return {int(row[0]): int(row[1]) for row in rows}
    except Exception:  # noqa: BLE001
        return {}


def backfill_legacy_run_leads(engine) -> int:
    """Recover exact older pull membership when the stored counts prove it.

    Before per-run snapshots existed, full lead rows were inserted immediately
    before the run record.  The interval between consecutive run timestamps is
    therefore a safe candidate set.  We persist it only when the candidate
    count exactly equals the run's recorded fresh count; ambiguous pulls remain
    unavailable instead of returning a plausible-but-wrong file.
    """
    if engine is None:
        return 0
    try:
        runs = sorted(load_runs(engine, limit=1000), key=lambda item: (str(item["ran_at"]), item["id"]))
        leads = sorted(load_leads(engine, limit=None), key=lambda item: str(item.get("first_seen_at") or ""))
        existing = run_lead_counts(engine, [run["id"] for run in runs])
        recovered = 0
        previous_at = ""
        for run in runs:
            ran_at = str(run.get("ran_at") or "")
            fresh = int(run.get("fresh") or 0)
            if fresh > 0 and not existing.get(run["id"]):
                recipe = str(run.get("recipe") or "")
                candidates = [lead for lead in leads
                              if previous_at < str(lead.get("first_seen_at") or "") <= ran_at
                              and str(lead.get("recipe") or lead.get("source") or "") == recipe]
                if len(candidates) == fresh:
                    recovered += record_run_leads(engine, run["id"], candidates)
            previous_at = max(previous_at, ran_at)
        return recovered
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] legacy pull recovery failed")
        return 0


def load_delivery_settings(engine) -> dict[str, Any]:
    defaults = {"enabled": False, "email_enabled": False, "slack_enabled": False,
                "frequency": "daily", "email_recipients": "", "slack_channel": "",
                "content_mode": "link", "updated_by": "", "updated_at": ""}
    if engine is None:
        return defaults
    ensure_runs_table(engine)
    try:
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT enabled,email_enabled,slack_enabled,frequency,email_recipients,slack_channel,content_mode,updated_by,updated_at FROM {_DELIVERY_TABLE} WHERE id=1")).fetchone()
    except Exception:  # noqa: BLE001
        # The production table predates slack_channel and the Vercel database
        # role cannot ALTER it during a request. Keep delivery operational on
        # that additive schema version until the owner migration runs.
        try:
            with engine.connect() as conn:
                row = conn.execute(text(f"SELECT enabled,email_enabled,slack_enabled,frequency,email_recipients,content_mode,updated_by,updated_at FROM {_DELIVERY_TABLE} WHERE id=1")).fetchone()
            if not row:
                return defaults
            return {"enabled": bool(row[0]), "email_enabled": bool(row[1]), "slack_enabled": bool(row[2]),
                    "frequency": row[3] or "daily", "email_recipients": row[4] or "",
                    "slack_channel": "", "content_mode": row[5] or "link",
                    "updated_by": row[6] or "", "updated_at": str(row[7] or "")}
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-memory] load_delivery_settings failed")
            return defaults
    if not row:
        return defaults
    return {"enabled": bool(row[0]), "email_enabled": bool(row[1]), "slack_enabled": bool(row[2]),
            "frequency": row[3] or "daily", "email_recipients": row[4] or "",
            "slack_channel": row[5] or "", "content_mode": row[6] or "link",
            "updated_by": row[7] or "", "updated_at": str(row[8] or "")}


def save_delivery_settings(engine, values: dict[str, Any], *, actor: str = "") -> bool:
    if engine is None:
        return False
    ensure_runs_table(engine)
    payload = {"enabled": _flag(values.get("enabled")), "email_enabled": _flag(values.get("email_enabled")),
               "slack_enabled": _flag(values.get("slack_enabled")),
               "frequency": str(values.get("frequency") or "daily") if str(values.get("frequency") or "daily") in {"daily", "every_pull"} else "daily",
               "email_recipients": _text(values.get("email_recipients")),
               "slack_channel": _text(values.get("slack_channel")),
               "content_mode": values.get("content_mode") if values.get("content_mode") in {"link", "summary"} else "link",
               "updated_by": _text(actor)}
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""INSERT INTO {_DELIVERY_TABLE}
                (id,enabled,email_enabled,slack_enabled,frequency,email_recipients,slack_channel,content_mode,updated_by,updated_at)
                VALUES (1,:enabled,:email_enabled,:slack_enabled,:frequency,:email_recipients,:slack_channel,:content_mode,:updated_by,CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET enabled=:enabled,email_enabled=:email_enabled,slack_enabled=:slack_enabled,
                frequency=:frequency,email_recipients=:email_recipients,slack_channel=:slack_channel,content_mode=:content_mode,
                updated_by=:updated_by,updated_at=CURRENT_TIMESTAMP"""), payload)
        return True
    except Exception:  # noqa: BLE001
        try:
            with engine.begin() as conn:
                conn.execute(text(f"""INSERT INTO {_DELIVERY_TABLE}
                    (id,enabled,email_enabled,slack_enabled,frequency,email_recipients,content_mode,updated_by,updated_at)
                    VALUES (1,:enabled,:email_enabled,:slack_enabled,:frequency,:email_recipients,:content_mode,:updated_by,CURRENT_TIMESTAMP)
                    ON CONFLICT (id) DO UPDATE SET enabled=:enabled,email_enabled=:email_enabled,slack_enabled=:slack_enabled,
                    frequency=:frequency,email_recipients=:email_recipients,content_mode=:content_mode,
                    updated_by=:updated_by,updated_at=CURRENT_TIMESTAMP"""), payload)
            return True
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-memory] save_delivery_settings failed")
            return False


def record_export(engine, *, actor: str, run_ids: Iterable[int], source_rows: int,
                  unique_companies: int, duplicates_removed: int,
                  include_duplicates: bool, filename: str) -> bool:
    if engine is None:
        return False
    try:
        ensure_runs_table(engine)
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO {_EXPORTS_TABLE} (actor,run_ids,source_rows,unique_companies,duplicates_removed,include_duplicates,filename) VALUES (:actor,:run_ids,:source_rows,:unique_companies,:duplicates_removed,:include_duplicates,:filename)"),
                         {"actor": actor, "run_ids": ",".join(str(int(x)) for x in run_ids),
                          "source_rows": source_rows, "unique_companies": unique_companies,
                          "duplicates_removed": duplicates_removed, "include_duplicates": 1 if include_duplicates else 0,
                          "filename": filename})
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_export failed")
        return False


def load_exports(engine, limit: int = 8) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        ensure_runs_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT actor,run_ids,source_rows,unique_companies,duplicates_removed,filename,exported_at FROM {_EXPORTS_TABLE} ORDER BY exported_at DESC")).fetchall()
        return [{"actor": r[0], "run_ids": r[1], "source_rows": r[2], "unique_companies": r[3],
                 "duplicates_removed": r[4], "filename": r[5], "exported_at": r[6]} for r in rows[:limit]]
    except Exception:  # noqa: BLE001
        return []


def record_delivery_attempt(engine, *, run_id: int = 0, recipe: str = "",
                            destination: str, target: str = "", status: str,
                            detail: str = "") -> bool:
    if engine is None:
        return False
    try:
        ensure_runs_table(engine)
        with engine.begin() as conn:
            conn.execute(text(f"INSERT INTO {_DELIVERY_HISTORY_TABLE} (run_id,recipe,destination,target,status,detail) VALUES (:run_id,:recipe,:destination,:target,:status,:detail)"),
                         {"run_id": int(run_id or 0), "recipe": _text(recipe),
                          "destination": _text(destination), "target": _text(target),
                          "status": _text(status), "detail": _text(detail)[:500]})
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_delivery_attempt failed")
        return False


def load_delivery_history(engine, limit: int = 8) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        ensure_runs_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT recipe,destination,target,status,attempted_at FROM {_DELIVERY_HISTORY_TABLE} ORDER BY attempted_at DESC")).fetchall()
        return [{"recipe": r[0], "destination": r[1], "target": r[2],
                 "status": r[3], "attempted_at": r[4]} for r in rows[:limit]]
    except Exception:  # noqa: BLE001
        return []
def ensure_table(engine, *, force: bool = False) -> None:
    if "postgres" in str(getattr(engine, "url", "")).lower() and not force:
        return
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
    if engine is None:
        return set()
    try:
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


def release_contacted(engine, domains: Iterable[str] | None = None) -> int:
    """Un-mark brands so they can be sourced again.

    Use ONLY for brands that were pulled but never actually contacted, e.g. a
    test pull whose file was discarded. Releasing a brand that really was emailed
    would let us email it twice, which is the one thing this memory exists to
    prevent, so this is never automatic.
    Returns how many were released.
    """
    if engine is None:
        return 0
    try:
        ensure_table(engine)
        with engine.begin() as conn:
            if domains is None:
                row = conn.execute(text(f"SELECT COUNT(*) FROM {_TABLE}")).fetchone()
                count = int(row[0] or 0)
                conn.execute(text(f"DELETE FROM {_TABLE}"))
                return count
            wanted = [{"d": _norm(x)} for x in domains if _norm(x)]
            if not wanted:
                return 0
            conn.execute(text(f"DELETE FROM {_TABLE} WHERE domain = :d"), wanted)
            return len(wanted)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] release_contacted failed")
        return 0


def _write_leads(engine, payload: list[dict[str, Any]]) -> None:
    """Write the widest row the table accepts.

    A database we could not widen still keeps the lead, minus the Amazon
    columns, because a lead is worth more than the finding attached to it.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(_INSERT_LEAD_SQL), payload)
    except Exception:  # noqa: BLE001, table predates the Amazon columns
        core = [{c: row.get(c) for c in _CORE_LEAD_COLS} for row in payload]
        with engine.begin() as conn:
            conn.execute(text(_INSERT_LEAD_CORE_SQL), core)


def _read_leads(engine) -> tuple[list[Any], tuple[str, ...]]:
    """Rows plus the column names they carry, narrowing on an older table."""
    try:
        with engine.connect() as conn:
            return conn.execute(text(_SELECT_LEADS_SQL)).fetchall(), _SELECT_LEAD_COLS
    except Exception:  # noqa: BLE001, table predates the Amazon columns
        with engine.connect() as conn:
            return conn.execute(text(_SELECT_LEADS_CORE_SQL)).fetchall(), _CORE_LEAD_COLS


def _lead_payload(leads: Iterable[dict[str, Any]], *, source: str,
                  config_version: int) -> list[dict[str, Any]]:
    """Normalize a lead batch once for both legacy and atomic write paths."""
    seen: dict[str, dict[str, Any]] = {}
    for lead in leads:
        dom = _norm(lead.get("domain"))
        if not dom or dom in seen:
            continue
        cats = lead.get("categories")
        if isinstance(cats, (list, tuple)):
            cats = ", ".join(str(c) for c in cats)
        try:
            score = int(lead.get("score") or 0)
        except (TypeError, ValueError):
            score = 0
        try:
            revenue = int(lead.get("estimated_sales_yearly_cents") or 0)
        except (TypeError, ValueError):
            revenue = 0
        seen[dom] = {
            "domain": dom, "source": source, "tier": lead.get("tier"),
            "signals": json.dumps(lead.get("signals") or []), "brand": lead.get("brand"),
            "niche": lead.get("niche"), "country": lead.get("country"), "score": score,
            "reason": lead.get("reason"), "recipe": lead.get("recipe"),
            "revenue_cents": revenue, "categories": cats,
            "config_version": int(config_version or 0), **_amazon_values(lead),
        }
    return list(seen.values())


def persist_pull(engine, *, leads: Iterable[dict[str, Any]], recipe: str,
                 scanned: int, matched: int, fresh: int, skipped_seen: int,
                 partial: bool = False, note: str = "", config_version: int = 0,
                 delivery: str = "file", delivered: int = 0,
                 library_leads: Iterable[dict[str, Any]] | None = None) -> PersistedPull:
    """Atomically store a run, its companies, and exact membership."""
    if engine is None:
        raise OutboundPersistenceError("Database engine is unavailable.")
    lead_list = list(leads)
    membership_by_domain = {
        _norm(lead.get("domain")): lead for lead in lead_list if _norm(lead.get("domain"))
    }
    library_list = lead_list if library_leads is None else list(library_leads)
    company_payload = _lead_payload(
        library_list, source=recipe or "csv_export", config_version=config_version,
    )
    expected_memberships = len(membership_by_domain)
    expected_companies = len(company_payload)
    if int(fresh or 0) != expected_memberships:
        raise OutboundPersistenceError(
            f"Fresh count mismatch: result={int(fresh or 0)} unique_companies={expected_memberships}."
        )
    is_pg = "postgres" in str(getattr(engine, "url", "")).lower()
    try:
        with engine.begin() as conn:
            run_result = conn.execute(
                text(_INSERT_RUN_SQL + (" RETURNING id" if is_pg else "")),
                {"recipe": recipe or "", "scanned": int(scanned), "matched": int(matched),
                 "fresh": expected_memberships, "skipped_seen": int(skipped_seen),
                 "partial": 1 if partial else 0, "note": note or "",
                 "config_version": int(config_version or 0), "delivery": delivery or "file",
                 "delivered": int(delivered or 0)},
            )
            run_id = run_result.scalar_one() if is_pg else getattr(run_result, "lastrowid", None)
            run_id = int(run_id or 0)
            if not run_id:
                raise OutboundPersistenceError("Run insert did not return an id.")
            if company_payload:
                conn.execute(text(_INSERT_LEAD_SQL), company_payload)
            memberships = [
                {"run_id": run_id, "domain": domain,
                 "lead_json": json.dumps(lead, default=str)}
                for domain, lead in membership_by_domain.items()
            ]
            if memberships:
                conn.execute(text(
                    f"INSERT INTO {_RUN_LEADS_TABLE} (run_id, domain, lead_json) "
                    "VALUES (:run_id, :domain, :lead_json)"
                ), memberships)
            membership_count = int(conn.execute(text(
                f"SELECT COUNT(*) FROM {_RUN_LEADS_TABLE} WHERE run_id=:run_id"
            ), {"run_id": run_id}).scalar_one() or 0)
            library_count = 0
            if expected_companies:
                placeholders = ",".join(f":d{i}" for i in range(expected_companies))
                library_count = int(conn.execute(text(
                    f"SELECT COUNT(*) FROM {_TABLE} WHERE domain IN ({placeholders})"
                ), {f"d{i}": row["domain"] for i, row in enumerate(company_payload)}).scalar_one() or 0)
            if membership_count != expected_memberships or library_count != expected_companies:
                raise OutboundPersistenceError(
                    "Persistence invariant failed: "
                    f"expected_memberships={expected_memberships} membership={membership_count} "
                    f"expected_companies={expected_companies} library={library_count}."
                )
        return PersistedPull(run_id=run_id, company_count=library_count,
                             membership_count=membership_count)
    except OutboundPersistenceError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("[outbound-memory] atomic pull persistence failed")
        raise OutboundPersistenceError("Atomic pull persistence failed.") from exc


def record_leads(engine, leads: Iterable[dict[str, Any]], *, source: str = "csv_export",
                 config_version: int = 0) -> int:
    """Store the FULL sourced lead, not just the domain.

    This table is our own record of every brand we have sourced: what it is, why
    we picked it, which recipe found it and under which settings. Clay and
    Instantly process these leads, but the record of them lives here, so losing
    access to either tool never loses the leads. The Amazon check rides along
    with the time it was made, so a stale finding is never read as a fresh one.

    De-dupes by domain within the batch; existing domains are left as-is.
    Best-effort: returns 0 on error rather than raising."""
    payload = _lead_payload(leads, source=source, config_version=config_version)
    if not payload:
        return 0
    try:
        ensure_table(engine)
        _write_leads(engine, payload)
        return len(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_leads failed; %s leads not saved", len(payload))
        return 0


def load_leads(engine, limit: int | None = 500) -> list[dict[str, Any]]:
    """Every sourced lead with its full record, newest first.

    ``limit=None`` is used only by explicit full-library exports. Interactive
    pages keep a numeric limit so their initial render remains bounded.
    """
    if engine is None:
        return []
    try:
        rows, cols = _read_leads(engine)
        out = []
        selected_rows = rows if limit is None else rows[:max(0, int(limit))]
        for r in selected_rows:
            d = dict(zip(cols, r))
            try:
                d["signals"] = json.loads(d.get("signals") or "[]")
            except (ValueError, TypeError):
                d["signals"] = []
            # Rows written before the Amazon check read back as "never checked"
            # rather than as a missing key or a None the callers have to guard.
            d["amazon_confidence"] = _text(d.get("amazon_confidence"))
            d["amazon_marketplace"] = _text(d.get("amazon_marketplace"))
            d["amazon_checked_at"] = _text(d.get("amazon_checked_at"))
            d["amazon_absent"] = bool(_flag(d.get("amazon_absent")))
            d["amazon_sellers_unknown"] = _whole(d.get("amazon_sellers_unknown"))
            d["amazon_skipped_reason"] = _text(d.get("amazon_skipped_reason"))
            d["video_url"] = _text(d.get("video_url"))
            # Flatten the facts blob back onto the lead so leads_to_csv finds the
            # amz_* columns exactly where a freshly built lead would have them.
            try:
                for key, val in (json.loads(d.get("amazon_facts") or "{}") or {}).items():
                    d[key] = val
            except (ValueError, TypeError):
                pass
            d["first_seen_at"] = r[-1]
            out.append(d)
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_leads failed")
        return []


_UPDATE_AMAZON_SQL = (
    f"UPDATE {_TABLE} SET "
    "amazon_facts = :amazon_facts, "
    "amazon_confidence = :amazon_confidence, "
    "amazon_marketplace = :amazon_marketplace, "
    "amazon_checked_at = :amazon_checked_at, "
    "amazon_absent = :amazon_absent, "
    "amazon_sellers_unknown = :amazon_sellers_unknown, "
    "amazon_skipped_reason = :amazon_skipped_reason, "
    "reason = :reason "
    "WHERE domain = :domain"
)


def update_amazon_finding(engine, domain: str, amazon: dict[str, Any] | None) -> bool:
    """Write one brand's Amazon result onto its existing lead row.

    record_leads inserts and leaves existing rows alone, which is right for
    dedup and useless for a scan that revisits brands we already hold. Written
    one brand at a time so a scan that stops halfway keeps everything it found.

    The opening line is only overwritten when the check actually produced one,
    so a skipped brand keeps whatever reason it already had.
    """
    if engine is None or not isinstance(amazon, dict):
        return False
    key = _norm(domain)
    if not key:
        return False
    # Shape it through the same fold the pull uses, so a scanned brand and a
    # pulled one are stored identically instead of drifting apart.
    from outbound_pipeline import apply_amazon
    shaped = apply_amazon({}, amazon)
    vals = _amazon_values(shaped)
    reason = _text(shaped.get("reason"))
    try:
        ensure_table(engine)
        with engine.begin() as conn:
            existing = conn.execute(
                text(f"SELECT reason FROM {_TABLE} WHERE domain = :d"), {"d": key}
            ).fetchone()
            if existing is None:
                return False
            conn.execute(text(_UPDATE_AMAZON_SQL), {
                "domain": key,
                "reason": reason or _text(existing[0]),
                **vals,
            })
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not save the Amazon finding for %s", key)
        return False


def leads_needing_amazon(engine, *, limit: int = 3, max_age_days: int = 7) -> list[dict[str, Any]]:
    """The brands worth checking next: best first, never checked or gone stale.

    We only look up brands we would actually email, rather than every brand we
    hold, because each one costs minutes and real money at the data provider.
    """
    leads = load_leads(engine, limit=1000)
    if not leads:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(max_age_days or 7)))

    def stale(lead: dict[str, Any]) -> bool:
        stamp = _text(lead.get("amazon_checked_at"))
        if not stamp:
            return True
        try:
            when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return True
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return when < cutoff

    tier_rank = {"A": 0, "B": 1, "C": 2, "X": 9}
    pending = [l for l in leads if stale(l)]
    pending.sort(key=lambda l: (tier_rank.get(_text(l.get("tier")).upper(), 5),
                                -_whole(l.get("score"))))
    return pending[:max(1, int(limit or 1))]


def load_pushed(engine) -> list[dict[str, Any]]:
    """Return [{domain, tier, signals[]}] for every pushed brand. Empty on error."""
    if engine is None:
        return []
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


def set_video_url(engine, domain: str, url: str) -> bool:
    """Attach a Tape recording to a brand we already hold.

    Stored against the brand rather than pasted into Clay, so the link survives
    a re-import and rides out on every future export instead of being retyped.
    An empty url clears it, which is how a bad link gets taken back out.
    """
    if engine is None:
        return False
    key = _norm(domain)
    if not key:
        return False
    link = _text(url)
    # Only our own recordings. A pasted link is the one place someone could put
    # an arbitrary URL into an email we send, so the host is checked, not trusted.
    if link and not link.lower().startswith(("https://tape.anatainc.com/", "http://tape.anatainc.com/")):
        return False
    try:
        ensure_table(engine)
        with engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE {_TABLE} SET video_url = :v WHERE domain = :d"),
                {"v": link, "d": key},
            )
            return bool(getattr(result, "rowcount", 0))
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not set video_url for %s", key)
        return False


# --- HeyReach: who has already been sent a LinkedIn request -------------------
# Separate table from the leads, because these are PEOPLE and the leads are
# COMPANIES. One brand can have several contacts, and a contact can move brand.
_HR_TABLE = "outbound_heyreach_sent"
_HR_CREATE = f"""
CREATE TABLE IF NOT EXISTS {_HR_TABLE} (
    lead_key TEXT PRIMARY KEY,
    campaign_id TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_heyreach_table(engine) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(text(_HR_CREATE))


def load_heyreach_sent(engine, campaign_id: str = "") -> set[str]:
    """Every profile we have already pushed. Empty on any error.

    Failing open here would mean re-sending everyone, so a read failure must
    never look like "nobody has been contacted". Callers treat an empty set as
    a reason to be careful, not as permission.
    """
    if engine is None:
        return set()
    try:
        ensure_heyreach_table(engine)
        sql = f"SELECT lead_key FROM {_HR_TABLE}"
        params: dict[str, Any] = {}
        if str(campaign_id or "").strip():
            sql += " WHERE campaign_id = :c"
            params["c"] = str(campaign_id).strip()
        with engine.connect() as conn:
            return {r[0] for r in conn.execute(text(sql), params).fetchall() if r[0]}
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not read the HeyReach sent list")
        return set()


def record_heyreach_sent(engine, keys: Iterable[str], campaign_id: str = "") -> int:
    """Mark profiles as contacted. Idempotent, so a replay adds nothing."""
    if engine is None:
        return 0
    rows = [{"k": str(k).strip(), "c": str(campaign_id or "").strip()}
            for k in keys if str(k or "").strip()]
    if not rows:
        return 0
    try:
        ensure_heyreach_table(engine)
        with engine.begin() as conn:
            for row in rows:
                conn.execute(text(
                    f"INSERT INTO {_HR_TABLE} (lead_key, campaign_id) "
                    f"VALUES (:k, :c) ON CONFLICT (lead_key) DO NOTHING"), row)
        return len(rows)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not record %s HeyReach sends", len(rows))
        return 0


# --- Contacts and the LinkedIn queue -----------------------------------------
# People, not companies. The leads table holds brands; this holds the humans
# Clay found inside them, because the email/LinkedIn join is keyed on a person.
_CT_TABLE = "outbound_contacts"
_CT_CREATE = f"""
CREATE TABLE IF NOT EXISTS {_CT_TABLE} (
    email TEXT PRIMARY KEY,
    linkedin_url TEXT,
    first_name TEXT,
    last_name TEXT,
    company TEXT,
    state TEXT,
    eligible_at TEXT,
    reason TEXT,
    email_blocked INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
_CT_COLS = ("email", "linkedin_url", "first_name", "last_name", "company",
            "state", "eligible_at", "reason", "email_blocked")


def ensure_contacts_table(engine) -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(text(_CT_CREATE))


def _email(value: Any) -> str:
    return str(value or "").strip().lower()


def record_contacts(engine, contacts: Iterable[dict[str, Any]]) -> int:
    """Remember the people we are about to email, so a webhook can find them.

    Upsert on the profile fields only. The queue state is deliberately NOT
    touched here: re-uploading the same Clay export must not reset someone who
    has already replied back to "waiting", which would queue a LinkedIn request
    at a person who already said no.
    """
    if engine is None:
        return 0
    rows = []
    for c in contacts:
        addr = _email(c.get("email"))
        if not addr:
            continue
        rows.append({
            "email": addr,
            "linkedin_url": _text(c.get("linkedin_url")),
            "first_name": _text(c.get("first_name")),
            "last_name": _text(c.get("last_name")),
            "company": _text(c.get("company")),
        })
    if not rows:
        return 0
    try:
        ensure_contacts_table(engine)
        with engine.begin() as conn:
            for row in rows:
                conn.execute(text(
                    f"INSERT INTO {_CT_TABLE} "
                    f"(email, linkedin_url, first_name, last_name, company, state) "
                    f"VALUES (:email, :linkedin_url, :first_name, :last_name, :company, 'new') "
                    f"ON CONFLICT (email) DO UPDATE SET "
                    f"linkedin_url = :linkedin_url, first_name = :first_name, "
                    f"last_name = :last_name, company = :company"), row)
        return len(rows)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not record %s contacts", len(rows))
        return 0


def load_contacts(engine, limit: int = 2000) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        ensure_contacts_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT {', '.join(_CT_COLS)} FROM {_CT_TABLE} "
                f"ORDER BY updated_at DESC")).fetchall()
        out = []
        for r in rows[:limit]:
            d = dict(zip(_CT_COLS, r))
            d["email_blocked"] = bool(_flag(d.get("email_blocked")))
            for k in ("linkedin_url", "first_name", "last_name", "company", "state",
                      "eligible_at", "reason"):
                d[k] = _text(d.get(k))
            out.append(d)
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not read contacts")
        return []


def apply_queue_decision(engine, email: str, decision: dict[str, Any]) -> bool:
    """Move one contact along the LinkedIn state machine.

    Creates the row if the webhook arrives for someone we have not stored, so a
    contact uploaded straight into Instantly is still tracked - just without a
    LinkedIn URL, which keeps them out of the queue rather than losing them.

    A stop never un-stops: once someone has replied or declined, a later
    email_sent from a different campaign must not put them back in the queue.
    """
    if engine is None or not isinstance(decision, dict):
        return False
    addr = _email(email)
    if not addr:
        return False
    try:
        ensure_contacts_table(engine)
        with engine.begin() as conn:
            current = conn.execute(
                text(f"SELECT state FROM {_CT_TABLE} WHERE email = :e"), {"e": addr}
            ).fetchone()
            if current is not None and str(current[0] or "") in ("stopped", "sent") \
                    and decision.get("state") == "waiting":
                return False
            params = {
                "e": addr,
                "state": _text(decision.get("state")),
                "eligible_at": _text(decision.get("eligible_at")),
                "reason": _text(decision.get("reason")),
                "blocked": 1 if decision.get("block_email") else 0,
            }
            if current is None:
                conn.execute(text(
                    f"INSERT INTO {_CT_TABLE} "
                    f"(email, state, eligible_at, reason, email_blocked) "
                    f"VALUES (:e, :state, :eligible_at, :reason, :blocked)"), params)
            else:
                conn.execute(text(
                    f"UPDATE {_CT_TABLE} SET state = :state, eligible_at = :eligible_at, "
                    f"reason = :reason, email_blocked = "
                    f"CASE WHEN :blocked = 1 THEN 1 ELSE email_blocked END "
                    f"WHERE email = :e"), params)
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not apply queue decision for %s", addr)
        return False


def mark_linkedin_sent(engine, emails: Iterable[str]) -> int:
    """Close the loop after a HeyReach push, so they leave the queue."""
    if engine is None:
        return 0
    addrs = [_email(e) for e in emails if _email(e)]
    if not addrs:
        return 0
    try:
        ensure_contacts_table(engine)
        with engine.begin() as conn:
            for addr in addrs:
                conn.execute(text(
                    f"UPDATE {_CT_TABLE} SET state = 'sent', eligible_at = '' "
                    f"WHERE email = :e"), {"e": addr})
        return len(addrs)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] could not mark %s contacts sent", len(addrs))
        return 0
