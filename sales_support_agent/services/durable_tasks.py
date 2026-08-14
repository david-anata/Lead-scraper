"""Durable, retryable task execution for serverless request handoffs.

FastAPI ``BackgroundTasks`` are convenient on a long-running web process but
are not a durability boundary on Vercel.  This module records the complete
task intent in PostgreSQL before a response is returned.  An opportunistic
in-request worker may execute it immediately; a protected cron worker repairs
anything left queued after function termination or a transient failure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any
from uuid import uuid4

from sqlalchemy import text

from sales_support_agent.services.schema_policy import schema_maintenance_allowed


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DurableTaskClaim:
    task_id: str
    task_type: str
    payload: dict[str, Any]
    owner_token: str
    attempts: int


def _now() -> datetime:
    return datetime.now(timezone.utc)


def app_task_engine(app: Any) -> Any:
    """Resolve the shared engine, including focused router-test applications."""

    factory = getattr(app.state, "session_factory", None)
    if factory is not None:
        engine = factory.kw.get("bind")
        if engine is not None:
            return engine
    from sales_support_agent.models.database import get_engine

    return get_engine()


def ensure_durable_task_schema(engine: Any, *, force: bool = False) -> None:
    """Create the additive queue table used by request-owned durable work."""

    if not schema_maintenance_allowed(engine, force=force):
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS durable_task_queue (
                    id VARCHAR(64) PRIMARY KEY,
                    task_type VARCHAR(96) NOT NULL,
                    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    status VARCHAR(24) NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 5,
                    available_at VARCHAR(64) NOT NULL,
                    lease_expires_at VARCHAR(64),
                    owner_token VARCHAR(64),
                    created_at VARCHAR(64) NOT NULL,
                    started_at VARCHAR(64),
                    completed_at VARCHAR(64),
                    last_error TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_durable_task_queue_ready
                    ON durable_task_queue (status, available_at)
                """
            )
        )


def enqueue_durable_task(
    engine: Any,
    *,
    task_type: str,
    idempotency_key: str,
    payload: dict[str, Any],
    max_attempts: int = 5,
) -> str:
    """Persist one logical task and return its stable task ID.

    Repeated requests with the same idempotency key return the existing task
    rather than scheduling the external effect twice.
    """

    ensure_durable_task_schema(engine)
    task_id = uuid4().hex
    now_text = _now().isoformat()
    values = {
        "id": task_id,
        "task_type": task_type.strip(),
        "idempotency_key": idempotency_key.strip(),
        "payload_json": json.dumps(payload, sort_keys=True, default=str),
        "status": "queued",
        "max_attempts": max(1, int(max_attempts)),
        "available_at": now_text,
        "created_at": now_text,
    }
    insert_sql = (
        """
        INSERT INTO durable_task_queue (
            id, task_type, idempotency_key, payload_json, status,
            max_attempts, available_at, created_at
        ) VALUES (
            :id, :task_type, :idempotency_key, :payload_json, :status,
            :max_attempts, :available_at, :created_at
        ) ON CONFLICT (idempotency_key) DO NOTHING
        """
        if engine.dialect.name == "postgresql"
        else
        """
        INSERT OR IGNORE INTO durable_task_queue (
            id, task_type, idempotency_key, payload_json, status,
            max_attempts, available_at, created_at
        ) VALUES (
            :id, :task_type, :idempotency_key, :payload_json, :status,
            :max_attempts, :available_at, :created_at
        )
        """
    )
    with engine.begin() as connection:
        inserted = connection.execute(text(insert_sql), values)
        if inserted.rowcount == 1:
            return task_id
        row = connection.execute(
            text(
                "SELECT id FROM durable_task_queue WHERE idempotency_key = :key"
            ),
            {"key": values["idempotency_key"]},
        ).first()
    if row is None:  # Defensive: a concurrent transaction should be visible.
        raise RuntimeError("Durable task could not be persisted.")
    return str(row[0])


def claim_durable_task(
    engine: Any,
    *,
    task_id: str | None = None,
    lease_minutes: int = 15,
) -> DurableTaskClaim | None:
    """Atomically claim one ready task, recovering expired workers."""

    ensure_durable_task_schema(engine)
    now = _now()
    now_text = now.isoformat()
    owner_token = uuid4().hex
    selector = "AND id = :task_id" if task_id else ""
    parameters: dict[str, Any] = {"now": now_text}
    if task_id:
        parameters["task_id"] = task_id

    # Select a candidate, then use a guarded update. If another worker wins the
    # update we return no claim and let the next cron iteration choose again.
    with engine.begin() as connection:
        row = connection.execute(
            text(
                f"""
                SELECT id, task_type, payload_json, attempts
                  FROM durable_task_queue
                 WHERE attempts < max_attempts
                   AND available_at <= :now
                   AND (
                        status IN ('queued', 'failed')
                        OR (status = 'running' AND lease_expires_at < :now)
                   )
                   {selector}
                 ORDER BY created_at, id
                 LIMIT 1
                """
            ),
            parameters,
        ).first()
        if row is None:
            return None
        task_id_value = str(row[0])
        attempts = int(row[3]) + 1
        updated = connection.execute(
            text(
                """
                UPDATE durable_task_queue
                   SET status = 'running', attempts = :attempts,
                       owner_token = :owner_token, started_at = :started_at,
                       lease_expires_at = :lease_expires_at, last_error = ''
                 WHERE id = :id
                   AND attempts = :previous_attempts
                   AND (
                        status IN ('queued', 'failed')
                        OR (status = 'running' AND lease_expires_at < :started_at)
                   )
                """
            ),
            {
                "attempts": attempts,
                "previous_attempts": int(row[3]),
                "owner_token": owner_token,
                "started_at": now_text,
                "lease_expires_at": (
                    now + timedelta(minutes=max(1, lease_minutes))
                ).isoformat(),
                "id": task_id_value,
            },
        )
        if updated.rowcount != 1:
            return None
    payload = json.loads(str(row[2]) or "{}")
    if not isinstance(payload, dict):
        payload = {}
    return DurableTaskClaim(
        task_id=task_id_value,
        task_type=str(row[1]),
        payload=payload,
        owner_token=owner_token,
        attempts=attempts,
    )


def finish_durable_task(
    engine: Any,
    claim: DurableTaskClaim,
    *,
    succeeded: bool,
    result: dict[str, Any] | None = None,
    error: str = "",
) -> None:
    """Finish a task owned by this worker, or make it eligible for retry."""

    now = _now()
    retry_delay = min(60, 2 ** max(0, claim.attempts - 1))
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE durable_task_queue
                   SET status = :status, completed_at = :completed_at,
                       available_at = :available_at, lease_expires_at = NULL,
                       result_json = :result_json, last_error = :last_error
                 WHERE id = :id AND owner_token = :owner_token
                """
            ),
            {
                "status": "succeeded" if succeeded else "failed",
                "completed_at": now.isoformat() if succeeded else None,
                "available_at": (
                    now if succeeded else now + timedelta(minutes=retry_delay)
                ).isoformat(),
                "result_json": json.dumps(result or {}, sort_keys=True, default=str),
                "last_error": str(error or "")[:4000],
                "id": claim.task_id,
                "owner_token": claim.owner_token,
            },
        )


def run_durable_recovery_probe(engine: Any, *, correlation_id: str) -> dict[str, Any]:
    """Prove the hosted queue's failure, retry, overlap, and replay contracts.

    The probe never dispatches a business task or calls an external provider.
    Its completed queue row is intentionally retained as an operator receipt.
    """

    probe_key = "".join(
        character for character in str(correlation_id) if character.isalnum() or character in "-_"
    )[:80]
    if not probe_key:
        raise ValueError("A correlation ID is required for the recovery probe.")
    task_id = enqueue_durable_task(
        engine,
        task_type="migration_recovery_probe",
        idempotency_key=f"migration-recovery-probe:{probe_key}",
        payload={"correlation_id": probe_key, "external_writes": False},
        max_attempts=2,
    )
    first = claim_durable_task(engine, task_id=task_id, lease_minutes=1)
    if first is None:
        raise RuntimeError("Recovery probe could not acquire its first lease.")
    overlap_blocked = claim_durable_task(engine, task_id=task_id) is None
    finish_durable_task(
        engine,
        first,
        succeeded=False,
        error="intentional staging recovery probe failure",
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE durable_task_queue SET available_at = :available_at "
                "WHERE id = :id AND status = 'failed'"
            ),
            {"available_at": _now().isoformat(), "id": task_id},
        )
    second = claim_durable_task(engine, task_id=task_id, lease_minutes=1)
    if second is None:
        raise RuntimeError("Recovery probe could not reacquire its failed task.")
    finish_durable_task(
        engine,
        second,
        succeeded=True,
        result={"probe": "passed", "external_writes": False},
    )
    replay_blocked = claim_durable_task(engine, task_id=task_id) is None
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, attempts, last_error, result_json "
                "FROM durable_task_queue WHERE id = :id"
            ),
            {"id": task_id},
        ).first()
    if row is None:
        raise RuntimeError("Recovery probe receipt was not retained.")
    passed = (
        str(row[0]) == "succeeded"
        and int(row[1]) == 2
        and overlap_blocked
        and replay_blocked
    )
    return {
        "status": "passed" if passed else "failed",
        "task_id": task_id,
        "correlation_id": probe_key,
        "attempts": int(row[1]),
        "overlap_blocked": overlap_blocked,
        "replay_blocked": replay_blocked,
        "failure_recorded": first.attempts == 1,
        "recovered": str(row[0]) == "succeeded",
        "external_writes": False,
    }


def _dispatch(app: Any, claim: DurableTaskClaim) -> dict[str, Any]:
    """Call the existing business service for one persisted task intent."""

    payload = claim.payload
    if claim.task_type == "plaid_item_sync":
        from sales_support_agent.services.cashflow.plaid import sync_item

        settings = getattr(app.state, "agent_settings", None) or app.state.settings
        sync_item(int(payload["local_item_id"]), settings=settings)
    elif claim.task_type == "marketing_analysis":
        from sales_support_agent.api.marketing_router import _run_analysis_and_deliver

        _run_analysis_and_deliver(app, **payload)
    elif claim.task_type == "marketing_store_unlock":
        from sales_support_agent.api.marketing_router import _deliver_store_unlock

        _deliver_store_unlock(app, **payload)
    elif claim.task_type == "marketing_build_shelf":
        from sales_support_agent.api.marketing_router import _build_shelf

        _build_shelf(app, int(payload["intake_run_id"]), str(payload["asin"]))
    elif claim.task_type == "fulfillment_finish_unlock":
        from sales_support_agent.api.fulfillment_public_router import _finish_unlock

        _finish_unlock(app, **payload)
    elif claim.task_type == "fulfillment_retry_handoffs":
        from sales_support_agent.api.fulfillment_public_router import retry_rate_sheet_handoffs

        retry_rate_sheet_handoffs(app, **payload)
    elif claim.task_type == "advertising_audit":
        from sales_support_agent.api.advertising_router import (
            _decode_pending,
            _do_run_background,
        )

        run_id = str(payload["run_id"])
        decoded = _decode_pending(dict(payload["inputs"]))
        _do_run_background(**decoded, confirmed=False, run_id=run_id)
    else:
        raise ValueError(f"Unsupported durable task type: {claim.task_type}")
    return {"task_type": claim.task_type, "attempt": claim.attempts}


def execute_durable_task(app: Any, task_id: str) -> bool:
    """Execute one task if claimable; safe for FastAPI BackgroundTasks."""

    engine = app_task_engine(app)
    claim = claim_durable_task(engine, task_id=task_id)
    if claim is None:
        return False
    try:
        result = _dispatch(app, claim)
    except Exception as exc:  # noqa: BLE001 — queue records the retry receipt.
        logger.exception("Durable task %s failed", claim.task_id)
        finish_durable_task(engine, claim, succeeded=False, error=str(exc))
        return False
    finish_durable_task(engine, claim, succeeded=True, result=result)
    return True


def drain_durable_tasks(app: Any, *, limit: int = 5) -> dict[str, int]:
    """Run a bounded batch for Vercel Cron without exceeding function time."""

    engine = app_task_engine(app)
    claimed = succeeded = failed = 0
    for _ in range(max(1, min(int(limit), 20))):
        claim = claim_durable_task(engine)
        if claim is None:
            break
        claimed += 1
        try:
            result = _dispatch(app, claim)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Durable task %s failed", claim.task_id)
            finish_durable_task(engine, claim, succeeded=False, error=str(exc))
            failed += 1
        else:
            finish_durable_task(engine, claim, succeeded=True, result=result)
            succeeded += 1
    return {"claimed": claimed, "succeeded": succeeded, "failed": failed}
