from __future__ import annotations

from sqlalchemy import create_engine, text

from sales_support_agent.services.job_lease import ensure_job_lease_schema
from sales_support_agent.services.schedule_shadow import (
    run_schedule_shadow_matrix,
    shadow_schedule_names,
)


def test_shadow_matrix_records_every_job_without_external_writes(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'shadow.db'}", future=True)
    ensure_job_lease_schema(engine)
    tables = {
        "website_ops_files",
        "content_job_runs",
        "content_artifacts",
        "hubspot_deals",
        "durable_task_queue",
        "hr_employees",
        "building_inquiries",
        "building_campaigns",
        "outbound_settings",
    }
    with engine.begin() as connection:
        for table_name in tables:
            connection.execute(text(f'CREATE TABLE "{table_name}" (id INTEGER PRIMARY KEY)'))

    result = run_schedule_shadow_matrix(
        engine,
        environment={},
        correlation_id="candidate-1",
    )

    assert result["status"] == "passed"
    assert result["external_writes"] is False
    assert [item["job"] for item in result["jobs"]] == list(shadow_schedule_names())
    assert all(item["database_ready"] for item in result["jobs"])
    assert all(item["external_writes"] is False for item in result["jobs"])
    with engine.connect() as connection:
        receipts = connection.execute(
            text("SELECT COUNT(*) FROM scheduled_job_runs WHERE job_key LIKE 'vercel-shadow-%'")
        ).scalar_one()
    assert receipts == len(shadow_schedule_names())
