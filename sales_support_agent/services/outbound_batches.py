"""Durable daily outbound batches and operator-managed recipe schedules."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any, Iterable

from sqlalchemy import text


@dataclass(frozen=True)
class DailyBatch:
    id: int
    business_date: str
    status: str
    trigger: str
    recipe_count: int
    completed_count: int
    failed_count: int
    scanned: int
    matched: int
    skipped_seen: int
    unique_companies: int
    duplicates_removed: int
    artifact_sha256: str
    email_status: str
    slack_status: str
    error: str
    started_at: str
    completed_at: str


_BATCHES = "outbound_daily_batches"
_BATCH_RUNS = "outbound_daily_batch_runs"
_BATCH_COMPANIES = "outbound_daily_batch_companies"
_RECIPES = "outbound_recipe_definitions"


def _is_pg(engine: Any) -> bool:
    return "postgres" in str(getattr(engine, "url", "")).lower()


def ensure_tables(engine: Any, *, force: bool = False) -> None:
    """Create daily-batch schema during deployment, never in Postgres requests."""
    is_pg = _is_pg(engine)
    if is_pg and not force:
        return
    pk = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
    statements = [
        f"""CREATE TABLE IF NOT EXISTS {_BATCHES} (
            id {pk}, business_date TEXT NOT NULL, timezone TEXT NOT NULL DEFAULT 'America/Denver',
            status TEXT NOT NULL, trigger TEXT NOT NULL, recipe_count INTEGER NOT NULL DEFAULT 0,
            completed_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0,
            scanned INTEGER NOT NULL DEFAULT 0, matched INTEGER NOT NULL DEFAULT 0,
            skipped_seen INTEGER NOT NULL DEFAULT 0, unique_companies INTEGER NOT NULL DEFAULT 0,
            duplicates_removed INTEGER NOT NULL DEFAULT 0, artifact_csv TEXT NOT NULL DEFAULT '',
            artifact_filename TEXT NOT NULL DEFAULT '', artifact_sha256 TEXT NOT NULL DEFAULT '',
            email_status TEXT NOT NULL DEFAULT 'not_requested',
            slack_status TEXT NOT NULL DEFAULT 'not_requested', error TEXT NOT NULL DEFAULT '',
            correlation_id TEXT NOT NULL DEFAULT '', started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (business_date, trigger)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {_BATCH_RUNS} (
            batch_id INTEGER NOT NULL, run_id INTEGER NOT NULL DEFAULT 0,
            recipe_key TEXT NOT NULL, recipe_label TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL, fresh INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '', PRIMARY KEY (batch_id, recipe_key)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {_BATCH_COMPANIES} (
            batch_id INTEGER NOT NULL, domain TEXT NOT NULL, lead_json TEXT NOT NULL,
            primary_recipe TEXT NOT NULL, matched_recipes TEXT NOT NULL DEFAULT '[]',
            PRIMARY KEY (batch_id, domain)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {_RECIPES} (
            recipe_key TEXT PRIMARY KEY, label TEXT NOT NULL, template_key TEXT NOT NULL,
            reason TEXT NOT NULL, tier TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 100,
            weekdays TEXT NOT NULL, cap INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1,
            include_in_daily INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL DEFAULT 1,
            updated_by TEXT NOT NULL DEFAULT '', updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    seed_recipes(engine)


def seed_recipes(engine: Any) -> None:
    """Seed the six current recipes without changing their behavior."""
    import outbound_recipes as source

    weekday_map = {"daily": "0,1,2,3,4", "weekly": "1,2"}
    rows = [
        {
            "recipe_key": recipe.key, "label": recipe.label, "template_key": recipe.key,
            "reason": recipe.reason, "tier": recipe.tier,
            "priority": index + 1, "weekdays": weekday_map[recipe.cadence],
            "cap": recipe.max_per_run,
        }
        for index, recipe in enumerate(source.RECIPES)
    ]
    sql = text(f"""INSERT INTO {_RECIPES}
        (recipe_key,label,template_key,reason,tier,priority,weekdays,cap)
        VALUES (:recipe_key,:label,:template_key,:reason,:tier,:priority,:weekdays,:cap)
        ON CONFLICT (recipe_key) DO NOTHING""")
    with engine.begin() as conn:
        conn.execute(sql, rows)


def load_recipe_definitions(engine: Any) -> list[dict[str, Any]]:
    if engine is None:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"""SELECT recipe_key,label,template_key,reason,tier,
                priority,weekdays,cap,active,include_in_daily,version,updated_by,updated_at
                FROM {_RECIPES} ORDER BY active DESC, priority, recipe_key""")).fetchall()
        return [
            {"key": r[0], "label": r[1], "template_key": r[2], "reason": r[3],
             "tier": r[4], "priority": int(r[5]), "weekdays": str(r[6]),
             "cap": int(r[7]), "active": bool(r[8]), "include_in_daily": bool(r[9]),
             "version": int(r[10]), "updated_by": r[11], "updated_at": str(r[12] or "")}
            for r in rows
        ]
    except Exception:
        return []


def save_recipe(engine: Any, values: dict[str, Any], *, actor: str) -> dict[str, Any]:
    """Create or version a safe recipe backed by an approved template."""
    import outbound_recipes as source

    key = str(values.get("key") or "").strip().lower().replace(" ", "_")
    template_key = str(values.get("template_key") or "").strip()
    if not key or not key.replace("_", "").isalnum():
        return {"ok": False, "reason": "Use letters, numbers, and underscores for the recipe key."}
    if source.recipe(template_key) is None:
        return {"ok": False, "reason": "Choose a supported signal template."}
    label = str(values.get("label") or "").strip()
    reason = str(values.get("reason") or "").strip()
    if not label or not reason:
        return {"ok": False, "reason": "Name and why-now explanation are required."}
    try:
        cap = max(1, min(int(values.get("cap") or 25), 150))
        priority = max(1, min(int(values.get("priority") or 100), 999))
    except (TypeError, ValueError):
        return {"ok": False, "reason": "Cap and priority must be whole numbers."}
    weekdays = sorted({int(x) for x in str(values.get("weekdays") or "").split(",") if x.strip().isdigit()})
    if not weekdays or any(day < 0 or day > 4 for day in weekdays):
        return {"ok": False, "reason": "Choose at least one weekday."}
    tier = str(values.get("tier") or "C").upper()
    if tier not in {"A", "B", "C"}:
        return {"ok": False, "reason": "Tier must be A, B, or C."}
    payload = {
        "key": key, "label": label, "template": template_key, "reason": reason,
        "tier": tier, "priority": priority, "weekdays": ",".join(map(str, weekdays)),
        "cap": cap, "active": 1 if str(values.get("active", "1")).lower() in {"1", "true", "on"} else 0,
        "include": 1 if str(values.get("include_in_daily", "1")).lower() in {"1", "true", "on"} else 0,
        "actor": actor or "unknown",
    }
    with engine.begin() as conn:
        conn.execute(text(f"""INSERT INTO {_RECIPES}
            (recipe_key,label,template_key,reason,tier,priority,weekdays,cap,active,include_in_daily,updated_by)
            VALUES (:key,:label,:template,:reason,:tier,:priority,:weekdays,:cap,:active,:include,:actor)
            ON CONFLICT (recipe_key) DO UPDATE SET label=:label,template_key=:template,reason=:reason,
            tier=:tier,priority=:priority,weekdays=:weekdays,cap=:cap,active=:active,
            include_in_daily=:include,version={_RECIPES}.version+1,updated_by=:actor,
            updated_at=CURRENT_TIMESTAMP"""), payload)
    return {"ok": True, "key": key}


def materialize_recipes(engine: Any, weekday: int, settings: dict[str, Any]) -> list[Any]:
    """Resolve active DB definitions into the existing deterministic Recipe type."""
    import outbound_recipes as source

    output: list[Any] = []
    for definition in load_recipe_definitions(engine):
        days = {int(x) for x in definition["weekdays"].split(",") if x.strip().isdigit()}
        if not definition["active"] or not definition["include_in_daily"] or weekday not in days:
            continue
        template = source.recipe(definition["template_key"])
        if template is None:
            continue
        output.append(replace(
            template, key=definition["key"], label=definition["label"],
            reason=definition["reason"], tier=definition["tier"],
            cadence="daily", max_per_run=definition["cap"],
        ))
    return output


def create_batch(engine: Any, *, business_date: str, trigger: str,
                 recipe_count: int, correlation_id: str) -> tuple[int, bool]:
    """Create a batch idempotently and return (id, created_now)."""
    with engine.begin() as conn:
        result = conn.execute(text(f"""INSERT INTO {_BATCHES}
            (business_date,status,trigger,recipe_count,correlation_id)
            VALUES (:date,'running',:trigger,:count,:correlation)
            ON CONFLICT (business_date,trigger) DO NOTHING"""),
            {"date": business_date, "trigger": trigger, "count": recipe_count,
             "correlation": correlation_id})
        created = bool(result.rowcount)
        batch_id = int(conn.execute(text(f"SELECT id FROM {_BATCHES} WHERE business_date=:date AND trigger=:trigger"),
                                    {"date": business_date, "trigger": trigger}).scalar_one())
    return batch_id, created


def finalize_batch(engine: Any, *, batch_id: int, recipe_runs: list[dict[str, Any]],
                   leads: Iterable[dict[str, Any]], filename: str) -> DailyBatch:
    """Atomically freeze exact membership and the one daily CSV artifact."""
    import outbound_pipeline

    by_domain: dict[str, dict[str, Any]] = {}
    matched: dict[str, list[str]] = {}
    for lead in leads:
        domain = str(lead.get("domain") or "").strip().lower()
        if not domain:
            continue
        recipe_key = str(lead.get("recipe") or "unknown")
        matched.setdefault(domain, [])
        if recipe_key not in matched[domain]:
            matched[domain].append(recipe_key)
        by_domain.setdefault(domain, dict(lead))
    ordered = list(by_domain.values())
    artifact = outbound_pipeline.leads_to_csv(ordered)
    checksum = hashlib.sha256(artifact.encode("utf-8")).hexdigest()
    total_recipe_rows = sum(int(run.get("fresh") or 0) for run in recipe_runs)
    completed = sum(1 for run in recipe_runs if run.get("status") == "complete")
    failed = sum(1 for run in recipe_runs if run.get("status") == "failed")
    partial = any(run.get("status") == "partial" for run in recipe_runs)
    status = "needs_review" if failed or partial else "ready"
    error = "; ".join(str(run.get("error") or "") for run in recipe_runs if run.get("error"))[:500]
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {_BATCH_RUNS} WHERE batch_id=:id"), {"id": batch_id})
        conn.execute(text(f"DELETE FROM {_BATCH_COMPANIES} WHERE batch_id=:id"), {"id": batch_id})
        if recipe_runs:
            conn.execute(text(f"""INSERT INTO {_BATCH_RUNS}
                (batch_id,run_id,recipe_key,recipe_label,status,fresh,error)
                VALUES (:batch_id,:run_id,:recipe_key,:recipe_label,:status,:fresh,:error)"""),
                [{"batch_id": batch_id, "run_id": int(run.get("run_id") or 0),
                  "recipe_key": str(run.get("recipe_key") or ""),
                  "recipe_label": str(run.get("recipe_label") or ""),
                  "status": str(run.get("status") or "failed"),
                  "fresh": int(run.get("fresh") or 0), "error": str(run.get("error") or "")[:300]}
                 for run in recipe_runs])
        if by_domain:
            conn.execute(text(f"""INSERT INTO {_BATCH_COMPANIES}
                (batch_id,domain,lead_json,primary_recipe,matched_recipes)
                VALUES (:batch_id,:domain,:lead_json,:primary_recipe,:matched_recipes)"""),
                [{"batch_id": batch_id, "domain": domain,
                  "lead_json": json.dumps(lead, default=str),
                  "primary_recipe": str(lead.get("recipe") or "unknown"),
                  "matched_recipes": json.dumps(matched[domain])}
                 for domain, lead in by_domain.items()])
        conn.execute(text(f"""UPDATE {_BATCHES} SET status=:status,
            completed_count=:completed,failed_count=:failed,scanned=:scanned,matched=:matched,
            skipped_seen=:skipped,unique_companies=:unique_count,duplicates_removed=:duplicates,
            artifact_csv=:artifact,artifact_filename=:filename,artifact_sha256=:checksum,
            error=:error,completed_at=CURRENT_TIMESTAMP WHERE id=:id"""),
            {"status": status, "completed": completed, "failed": failed,
             "scanned": sum(int(run.get("scanned") or 0) for run in recipe_runs),
             "matched": sum(int(run.get("matched") or 0) for run in recipe_runs),
             "skipped": sum(int(run.get("skipped_seen") or 0) for run in recipe_runs),
             "unique_count": len(by_domain), "duplicates": max(0, total_recipe_rows - len(by_domain)),
             "artifact": artifact, "filename": filename, "checksum": checksum,
             "error": error, "id": batch_id})
    return get_batch(engine, batch_id)


def update_delivery(engine: Any, batch_id: int, *, email_status: str, slack_status: str) -> None:
    status = "delivered" if "delivered" in {email_status, slack_status} else None
    with engine.begin() as conn:
        conn.execute(text(f"""UPDATE {_BATCHES} SET email_status=:email,slack_status=:slack,
            status=CASE WHEN :status IS NULL THEN status ELSE :status END WHERE id=:id"""),
            {"email": email_status, "slack": slack_status, "status": status, "id": batch_id})


def get_batch(engine: Any, batch_id: int) -> DailyBatch:
    with engine.connect() as conn:
        row = conn.execute(text(f"""SELECT id,business_date,status,trigger,recipe_count,
            completed_count,failed_count,scanned,matched,skipped_seen,unique_companies,
            duplicates_removed,artifact_sha256,email_status,slack_status,error,started_at,completed_at
            FROM {_BATCHES} WHERE id=:id"""), {"id": batch_id}).fetchone()
    if not row:
        raise LookupError("Daily batch not found")
    return DailyBatch(*[str(value or "") if index in {1, 2, 3, 12, 13, 14, 15, 16, 17}
                        else int(value or 0) for index, value in enumerate(row)])


def load_batches(engine: Any, *, page: int = 1, per_page: int = 10) -> tuple[list[DailyBatch], int]:
    page = max(1, int(page)); per_page = max(1, min(int(per_page), 50))
    with engine.connect() as conn:
        total = int(conn.execute(text(f"SELECT COUNT(*) FROM {_BATCHES}")).scalar_one() or 0)
        ids = [int(row[0]) for row in conn.execute(text(f"""SELECT id FROM {_BATCHES}
            ORDER BY business_date DESC,id DESC LIMIT :limit OFFSET :offset"""),
            {"limit": per_page, "offset": (page - 1) * per_page}).fetchall()]
    return [get_batch(engine, batch_id) for batch_id in ids], total


def load_batch_leads(engine: Any, batch_ids: Iterable[int]) -> list[dict[str, Any]]:
    wanted = sorted({int(value) for value in batch_ids if int(value) > 0})
    if not wanted:
        return []
    params = {f"id{i}": value for i, value in enumerate(wanted)}
    placeholders = ",".join(f":id{i}" for i in range(len(wanted)))
    with engine.connect() as conn:
        rows = conn.execute(text(f"""SELECT batch_id,lead_json,primary_recipe,matched_recipes
            FROM {_BATCH_COMPANIES} WHERE batch_id IN ({placeholders}) ORDER BY batch_id DESC,domain"""), params).fetchall()
    output = []
    for row in rows:
        lead = json.loads(row[1] or "{}")
        lead["daily_batch_id"] = int(row[0]); lead["primary_recipe"] = row[2]
        lead["matched_recipes"] = json.loads(row[3] or "[]")
        output.append(lead)
    return output


def batch_artifact(engine: Any, batch_id: int) -> tuple[str, str]:
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT artifact_filename,artifact_csv FROM {_BATCHES} WHERE id=:id"),
                           {"id": batch_id}).fetchone()
    if not row:
        raise LookupError("Daily batch not found")
    return str(row[0] or f"anata-daily-leads-{batch_id}.csv"), str(row[1] or "")


def batch_runs(engine: Any, batch_id: int) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"""SELECT recipe_key,recipe_label,status,fresh,error,run_id
            FROM {_BATCH_RUNS} WHERE batch_id=:id ORDER BY recipe_key"""), {"id": batch_id}).fetchall()
    return [{"recipe_key": r[0], "recipe_label": r[1], "status": r[2], "fresh": int(r[3]),
             "error": r[4], "run_id": int(r[5])} for r in rows]
