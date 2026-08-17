"""Read-only shadow preflight for every Vercel-owned write schedule."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Mapping

from sqlalchemy import text

from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job


_SHADOW_CONTRACTS: dict[str, dict[str, Any]] = {
    "website-ops": {"tables": ("website_ops_files",), "config": ()},
    "content": {
        "tables": ("content_job_runs", "content_artifacts"),
        "config": (("RIVERSIDE_API_KEY", "CONTENT_RIVERSIDE_RELAY_ENABLED"),),
    },
    "stale-leads": {
        "tables": ("hubspot_deals",),
        "config": (
            ("HUBSPOT_API_TOKEN", "HUBSPOT_ACCESS_TOKEN", "HUBSPOT_PRIVATE_APP_TOKEN"),
            ("CLICKUP_API_TOKEN", "CLICKUP_API_KEY"),
        ),
    },
    "gmail-sync": {
        "tables": ("hubspot_deals",),
        "config": (
            ("GMAIL_ACCESS_TOKEN", "GMAIL_REFRESH_TOKEN", "GMAIL_INBOXES_JSON"),
        ),
    },
    "daily-digest": {
        "tables": ("scheduled_job_runs",),
        "config": (("SLACK_BOT_TOKEN",), ("SLACK_CHANNEL_ID",)),
    },
    "durable-tasks": {"tables": ("durable_task_queue",), "config": ()},
    "sales-operator": {
        "tables": ("hubspot_deals", "scheduled_job_runs"),
        "config": (
            ("HUBSPOT_API_TOKEN", "HUBSPOT_ACCESS_TOKEN", "HUBSPOT_PRIVATE_APP_TOKEN"),
            ("SLACK_BOT_TOKEN",),
        ),
    },
    "hr-reminders": {
        "tables": ("hr_employees", "scheduled_job_runs"),
        "config": (("SLACK_BOT_TOKEN",),),
    },
    "building-operations": {
        "tables": ("building_inquiries", "building_campaigns", "scheduled_job_runs"),
        "config": (),
    },
    "outbound-morning": {
        "tables": ("outbound_settings", "scheduled_job_runs"),
        "config": (("STORELEADS_API_KEY",),),
    },
}


def shadow_schedule_names() -> tuple[str, ...]:
    return tuple(_SHADOW_CONTRACTS)


def _configured(environment: Mapping[str, str], alternatives: tuple[str, ...]) -> bool:
    return any(str(environment.get(key) or "").strip() for key in alternatives)


def run_schedule_shadow_matrix(
    engine: Any,
    *,
    environment: Mapping[str, str] | None = None,
    correlation_id: str,
) -> dict[str, Any]:
    """Record one no-provider-call preflight receipt for every write schedule."""

    env = environment or os.environ
    results: list[dict[str, Any]] = []
    for job_key, contract in _SHADOW_CONTRACTS.items():
        table_counts: dict[str, int] = {}
        table_errors: list[str] = []
        with engine.connect() as connection:
            for table_name in contract["tables"]:
                try:
                    count = connection.execute(
                        text(f'SELECT COUNT(*) FROM "{table_name}"')
                    ).scalar_one()
                except Exception as exc:  # noqa: BLE001 - report type only, never credentials.
                    table_errors.append(f"{table_name}:{type(exc).__name__}")
                    connection.rollback()
                else:
                    table_counts[table_name] = int(count)
        config_groups = [
            {
                "alternatives": list(alternatives),
                "configured": _configured(env, alternatives),
            }
            for alternatives in contract["config"]
        ]
        details = {
            "mode": "shadow",
            "database_ready": not table_errors,
            "table_counts": table_counts,
            "table_errors": table_errors,
            "configuration": config_groups,
            "live_configuration_ready": all(item["configured"] for item in config_groups),
            "external_writes": False,
        }
        lease = claim_scheduled_job(
            engine,
            job_key=f"vercel-shadow-{job_key}",
            run_key=correlation_id,
            lease_minutes=5,
        )
        if lease is None:
            details["receipt"] = "already_recorded"
        else:
            finish_scheduled_job(
                engine,
                lease,
                status="succeeded" if not table_errors else "failed",
                details=details,
            )
            details["receipt"] = "recorded"
        results.append({"job": job_key, **details})
    passed = all(item["database_ready"] for item in results)
    return {
        "status": "passed" if passed else "failed",
        "correlation_id": correlation_id,
        "jobs": results,
        "external_writes": False,
    }
