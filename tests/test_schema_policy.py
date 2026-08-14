from __future__ import annotations

from types import SimpleNamespace

from sales_support_agent.models.database import init_database
from sales_support_agent.services.durable_tasks import ensure_durable_task_schema
from sales_support_agent.services.fulfillment_report_storage import (
    ensure_fulfillment_report_storage_schema,
)
from sales_support_agent.services.job_lease import ensure_job_lease_schema
from sales_support_agent.services.website_ops_storage import (
    ensure_website_ops_storage_schema,
)


class _RestrictedPostgresEngine:
    dialect = SimpleNamespace(name="postgresql")

    def begin(self):
        raise AssertionError("restricted runtime attempted schema DDL")

    def connect(self):
        raise AssertionError("restricted runtime attempted schema inspection or locking")


def test_restricted_postgres_runtime_skips_all_request_time_schema_work(monkeypatch) -> None:
    monkeypatch.setenv("AGENT_RUNTIME_SCHEMA_MAINTENANCE", "false")
    engine = _RestrictedPostgresEngine()
    factory = SimpleNamespace(kw={"bind": engine})

    init_database(factory)
    ensure_job_lease_schema(engine)
    ensure_durable_task_schema(engine)
    ensure_website_ops_storage_schema(engine)
    ensure_fulfillment_report_storage_schema(engine)
