"""Cross-process leases for scheduled work.

Scheduled jobs are claimed in PostgreSQL before any external work begins. This
keeps Render retries, overlapping cron invocations, and multiple web instances
from performing the same paid API call or external write twice.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.services.schema_policy import schema_maintenance_allowed


@dataclass(frozen=True)
class JobLease:
    """A successfully claimed scheduled run."""

    job_key: str
    run_key: str
    owner_token: str


def ensure_job_lease_schema(engine: Any, *, force: bool = False) -> None:
    """Create the additive lease table used by cron-owned jobs."""

    if not schema_maintenance_allowed(engine, force=force):
        return

    ddl = """
        CREATE TABLE IF NOT EXISTS scheduled_job_runs (
            job_key VARCHAR(128) NOT NULL,
            run_key VARCHAR(128) NOT NULL,
            status VARCHAR(24) NOT NULL,
            owner_token VARCHAR(64) NOT NULL,
            started_at VARCHAR(64) NOT NULL,
            completed_at VARCHAR(64),
            lease_expires_at VARCHAR(64) NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (job_key, run_key)
        )
    """
    with engine.begin() as connection:
        connection.execute(text(ddl))


def claim_scheduled_job(
    engine: Any,
    *,
    job_key: str,
    run_key: str,
    lease_minutes: int = 90,
) -> JobLease | None:
    """Atomically claim a logical run, recovering failed or stale claims."""

    # Predeploy normally creates this table. Keeping the claim self-healing is
    # important for fresh preview databases and focused worker processes, and
    # avoids turning a missing additive table into an unaudited cron failure.
    ensure_job_lease_schema(engine)

    now = datetime.now(timezone.utc)
    now_text = now.isoformat()
    expires_text = (now + timedelta(minutes=max(1, lease_minutes))).isoformat()
    owner_token = uuid4().hex
    values = {
        "job_key": job_key,
        "run_key": run_key,
        "status": "running",
        "owner_token": owner_token,
        "started_at": now_text,
        "lease_expires_at": expires_text,
    }
    dialect = engine.dialect.name
    insert_sql = (
        """
        INSERT INTO scheduled_job_runs (
            job_key, run_key, status, owner_token, started_at, lease_expires_at
        ) VALUES (
            :job_key, :run_key, :status, :owner_token, :started_at, :lease_expires_at
        ) ON CONFLICT (job_key, run_key) DO NOTHING
        """
        if dialect == "postgresql"
        else
        """
        INSERT OR IGNORE INTO scheduled_job_runs (
            job_key, run_key, status, owner_token, started_at, lease_expires_at
        ) VALUES (
            :job_key, :run_key, :status, :owner_token, :started_at, :lease_expires_at
        )
        """
    )
    with engine.begin() as connection:
        inserted = connection.execute(text(insert_sql), values)
        if inserted.rowcount == 1:
            return JobLease(job_key, run_key, owner_token)

        recovered = connection.execute(
            text(
                """
                UPDATE scheduled_job_runs
                   SET status = :status,
                       owner_token = :owner_token,
                       started_at = :started_at,
                       completed_at = NULL,
                       lease_expires_at = :lease_expires_at,
                       details_json = '{}'
                 WHERE job_key = :job_key
                   AND run_key = :run_key
                   AND (
                        status = 'failed'
                        OR lease_expires_at < :started_at
                   )
                """
            ),
            values,
        )
        if recovered.rowcount == 1:
            return JobLease(job_key, run_key, owner_token)
    return None


def finish_scheduled_job(
    engine: Any,
    lease: JobLease,
    *,
    status: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Finish only the claim owned by this process."""

    final_status = "succeeded" if status == "succeeded" else "failed"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE scheduled_job_runs
                   SET status = :status,
                       completed_at = :completed_at,
                       details_json = :details_json
                 WHERE job_key = :job_key
                   AND run_key = :run_key
                   AND owner_token = :owner_token
                """
            ),
            {
                "status": final_status,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "details_json": json.dumps(details or {}, sort_keys=True, default=str),
                "job_key": lease.job_key,
                "run_key": lease.run_key,
                "owner_token": lease.owner_token,
            },
        )
