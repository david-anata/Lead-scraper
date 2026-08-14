from __future__ import annotations

import json
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from sales_support_agent.services.durable_tasks import (
    claim_durable_task,
    enqueue_durable_task,
    ensure_durable_task_schema,
    finish_durable_task,
    run_durable_recovery_probe,
)


def _engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'durable-tasks.db'}", future=True)
    ensure_durable_task_schema(engine)
    return engine


def test_enqueue_is_idempotent_and_claim_has_complete_payload(tmp_path) -> None:
    engine = _engine(tmp_path)
    first = enqueue_durable_task(
        engine,
        task_type="marketing_analysis",
        idempotency_key="analysis:42",
        payload={"intake_run_id": 42, "needs": ["advertising"]},
    )
    second = enqueue_durable_task(
        engine,
        task_type="marketing_analysis",
        idempotency_key="analysis:42",
        payload={"intake_run_id": 999},
    )

    assert second == first
    claim = claim_durable_task(engine, task_id=first)
    assert claim is not None
    assert claim.payload == {"intake_run_id": 42, "needs": ["advertising"]}
    assert claim_durable_task(engine, task_id=first) is None


def test_success_receipt_prevents_replay(tmp_path) -> None:
    engine = _engine(tmp_path)
    task_id = enqueue_durable_task(
        engine,
        task_type="plaid_item_sync",
        idempotency_key="plaid:7:sync",
        payload={"local_item_id": 7},
    )
    claim = claim_durable_task(engine, task_id=task_id)
    assert claim is not None
    finish_durable_task(engine, claim, succeeded=True, result={"synced": True})

    assert claim_durable_task(engine, task_id=task_id) is None
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, attempts, result_json FROM durable_task_queue WHERE id = :id"
            ),
            {"id": task_id},
        ).first()
    assert row is not None
    assert row[0] == "succeeded"
    assert row[1] == 1
    assert json.loads(row[2]) == {"synced": True}


def test_failed_task_records_error_and_can_be_reclaimed_when_due(tmp_path) -> None:
    engine = _engine(tmp_path)
    task_id = enqueue_durable_task(
        engine,
        task_type="fulfillment_finish_unlock",
        idempotency_key="fulfillment:9",
        payload={"run_id": 9},
    )
    first = claim_durable_task(engine, task_id=task_id)
    assert first is not None
    finish_durable_task(engine, first, succeeded=False, error="provider unavailable")

    with engine.begin() as connection:
        row = connection.execute(
            text(
                "SELECT status, last_error FROM durable_task_queue WHERE id = :id"
            ),
            {"id": task_id},
        ).first()
        assert row is not None
        assert row[0] == "failed"
        assert row[1] == "provider unavailable"
        connection.execute(
            text(
                "UPDATE durable_task_queue SET available_at = '2000-01-01T00:00:00+00:00' WHERE id = :id"
            ),
            {"id": task_id},
        )

    retry = claim_durable_task(engine, task_id=task_id)
    assert retry is not None
    assert retry.attempts == 2
    assert retry.owner_token != first.owner_token


def test_recovery_probe_proves_failure_retry_overlap_and_replay(tmp_path) -> None:
    engine = _engine(tmp_path)

    result = run_durable_recovery_probe(engine, correlation_id="release-candidate-1")

    assert result["status"] == "passed"
    assert result["attempts"] == 2
    assert result["failure_recorded"] is True
    assert result["recovered"] is True
    assert result["overlap_blocked"] is True
    assert result["replay_blocked"] is True
    assert result["external_writes"] is False


def test_restricted_postgres_runtime_never_attempts_queue_ddl(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_RUNTIME_SCHEMA_MAINTENANCE", "false")
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    ensure_durable_task_schema(engine)
