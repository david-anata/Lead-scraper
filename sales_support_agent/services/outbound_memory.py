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
from datetime import datetime, timedelta, timezone
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
    brand TEXT,
    niche TEXT,
    country TEXT,
    score INTEGER,
    reason TEXT,
    recipe TEXT,
    revenue_cents BIGINT,
    categories TEXT,
    config_version INTEGER,
    amazon_confidence TEXT,
    amazon_marketplace TEXT,
    amazon_checked_at TEXT,
    amazon_absent INTEGER,
    amazon_sellers_unknown INTEGER,
    amazon_skipped_reason TEXT,
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
_AMAZON_LEAD_COLS = ("amazon_confidence", "amazon_marketplace", "amazon_checked_at",
                     "amazon_absent", "amazon_sellers_unknown", "amazon_skipped_reason")
_LEAD_COLS = _CORE_LEAD_COLS + _AMAZON_LEAD_COLS


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
_SELECT_LEADS_SQL = _select_leads_sql(_LEAD_COLS)
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
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_confidence TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_marketplace TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_checked_at TEXT",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_absent INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_sellers_unknown INTEGER",
    f"ALTER TABLE {_TABLE} ADD COLUMN amazon_skipped_reason TEXT",
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
    f"SELECT recipe, scanned, matched, fresh, skipped_seen, partial, note, ran_at, config_version, delivery, delivered "
    f"FROM {_RUNS_TABLE} ORDER BY ran_at DESC"
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

    return {
        "amazon_confidence": _text(pick("amazon_confidence", nested.get("confidence"))),
        "amazon_marketplace": _text(pick("amazon_marketplace", nested.get("marketplace"))),
        "amazon_checked_at": _text(pick("amazon_checked_at", nested.get("checked_at"))),
        "amazon_absent": _flag(pick("amazon_absent", findings.get("absent"))),
        "amazon_sellers_unknown": _whole(pick("amazon_sellers_unknown",
                                              nested.get("sellers_unknown"))),
        "amazon_skipped_reason": _text(pick("amazon_skipped_reason",
                                            nested.get("skipped_reason"))),
    }


def ensure_runs_table(engine) -> None:
    is_pg = "postgres" in str(getattr(engine, "url", "")).lower()
    sql = _CREATE_RUNS_SQL_PG if is_pg else _CREATE_RUNS_SQL
    with engine.begin() as conn:
        conn.execute(text(sql))
    for stmt in _RUN_ALTERS:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 — column already present
            pass


def record_run(engine, *, recipe: str, scanned: int, matched: int, fresh: int,
               skipped_seen: int, partial: bool = False, note: str = "",
               config_version: int = 0, delivery: str = "file",
               delivered: int = 0) -> bool:
    """Log one pull. Best-effort: a failure here must never break a pull."""
    if engine is None:
        return False
    try:
        ensure_runs_table(engine)
        with engine.begin() as conn:
            conn.execute(text(_INSERT_RUN_SQL), {
                "recipe": recipe or "", "scanned": int(scanned), "matched": int(matched),
                "fresh": int(fresh), "skipped_seen": int(skipped_seen),
                "partial": 1 if partial else 0, "note": note or "",
                "config_version": int(config_version or 0),
                "delivery": delivery or "file", "delivered": int(delivered or 0),
            })
        return True
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_run failed")
        return False


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
        ensure_runs_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(_SELECT_RUNS_SQL)).fetchall()
        out = []
        for r in rows[:limit]:
            out.append({
                "recipe": r[0], "scanned": r[1], "matched": r[2], "fresh": r[3],
                "skipped_seen": r[4], "partial": bool(r[5]), "note": r[6], "ran_at": r[7],
                "config_version": r[8] if len(r) > 8 else 0,
                "delivery": (r[9] if len(r) > 9 else None) or "file",
                "delivered": (r[10] if len(r) > 10 else 0) or 0,
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
    if engine is None:
        return set()
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
            return conn.execute(text(_SELECT_LEADS_SQL)).fetchall(), _LEAD_COLS
    except Exception:  # noqa: BLE001, table predates the Amazon columns
        with engine.connect() as conn:
            return conn.execute(text(_SELECT_LEADS_CORE_SQL)).fetchall(), _CORE_LEAD_COLS


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
            "domain": dom,
            "source": source,
            "tier": lead.get("tier"),
            "signals": json.dumps(lead.get("signals") or []),
            "brand": lead.get("brand"),
            "niche": lead.get("niche"),
            "country": lead.get("country"),
            "score": score,
            "reason": lead.get("reason"),
            "recipe": lead.get("recipe"),
            "revenue_cents": revenue,
            "categories": cats,
            "config_version": int(config_version or 0),
            **_amazon_values(lead),
        }
    payload = list(seen.values())
    if not payload:
        return 0
    try:
        ensure_table(engine)
        _write_leads(engine, payload)
        return len(payload)
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] record_leads failed; %s leads not saved", len(payload))
        return 0


def load_leads(engine, limit: int = 500) -> list[dict[str, Any]]:
    """Every sourced lead with its full record, newest first."""
    if engine is None:
        return []
    try:
        ensure_table(engine)
        rows, cols = _read_leads(engine)
        out = []
        for r in rows[:limit]:
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
            d["first_seen_at"] = r[-1]
            out.append(d)
        return out
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-memory] load_leads failed")
        return []


_UPDATE_AMAZON_SQL = (
    f"UPDATE {_TABLE} SET "
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
