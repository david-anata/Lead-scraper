"""The Vercel cutover must preserve every approved Agent schedule."""

from __future__ import annotations

import json
from pathlib import Path


EXPECTED_SCHEDULES = {
    "/api/vercel-cron/synthetic-health": "30 * * * *",
    "/api/vercel-cron/website-ops": "0 * * * *",
    "/api/vercel-cron/content": "0 * * * *",
    "/api/vercel-cron/stale-leads": "0 15 * * 1-5",
    "/api/vercel-cron/gmail-sync": "*/15 * * * *",
    "/api/vercel-cron/sales-operator": "5 * * * *",
    "/api/vercel-cron/hr-reminders": "5 * * * *",
    "/api/vercel-cron/durable-tasks": "*/5 * * * *",
    "/api/vercel-cron/daily-digest": "0 16 * * 1-5",
    "/api/vercel-cron/building-operations": "20 * * * *",
    # Replaces the two-hourly background loop that Render held open at boot and
    # serverless cannot. Offset off the hour so it does not contend with the
    # other schedules for the same cold start.
    "/api/vercel-cron/finance-sync": "10 */2 * * *",
    "/api/jobs/outbound-morning/run": "0 13,14 * * *",
}


def test_vercel_manifest_contains_the_complete_approved_schedule() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(root.joinpath("vercel.json").read_text(encoding="utf-8"))
    actual = {item["path"]: item["schedule"] for item in manifest["crons"]}

    assert actual == EXPECTED_SCHEDULES

