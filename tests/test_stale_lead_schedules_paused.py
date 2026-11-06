"""The weekday stale-lead scan is paused in both schedulers.

Manual dispatch of the GitHub workflow still calls the same job. Every other
Vercel cron keeps the path and schedule it had before this pause.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STALE_SCAN_WORKFLOW = ROOT / ".github/workflows" / "sales-support-stale-scan.yml"
PAUSED_VERCEL_CRON_PATH = "/api/vercel-cron/stale-leads"
PAUSED_SCHEDULE = "0 15 * * 1-5"

# vercel.json crons after removing only /api/vercel-cron/stale-leads.
UNCHANGED_VERCEL_CRONS = [
    {"path": "/api/vercel-cron/synthetic-health", "schedule": "30 * * * *"},
    {"path": "/api/jobs/website-ops/run", "schedule": "0 * * * *"},
    {"path": "/api/jobs/content/run", "schedule": "0 * * * *"},
    {"path": "/api/vercel-cron/gmail-sync", "schedule": "*/15 * * * *"},
    {"path": "/api/vercel-cron/sales-operator", "schedule": "5 * * * *"},
    {"path": "/api/vercel-cron/hr-reminders", "schedule": "5 * * * *"},
    {"path": "/api/vercel-cron/durable-tasks", "schedule": "*/5 * * * *"},
    {"path": "/api/vercel-cron/daily-digest", "schedule": "0 16 * * 1-5"},
    {"path": "/api/vercel-cron/building-operations", "schedule": "20 * * * *"},
    {"path": "/api/vercel-cron/finance-sync", "schedule": "10 */2 * * *"},
    {"path": "/api/jobs/outbound-morning/run", "schedule": "0 * * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/at-1415", "schedule": "15 14 * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/at-1515", "schedule": "15 15 * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/at-1615", "schedule": "15 16 * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/at-1715", "schedule": "15 17 * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/run", "schedule": "30 20 * * 1-5"},
    {"path": "/api/jobs/outbound-enrich/missed-run", "schedule": "30 15 * * 1-5"},
    {"path": "/api/jobs/abby/run", "schedule": "*/5 * * * *"},
    {"path": "/api/jobs/instantly/analytics-sync", "schedule": "0 12 * * *"},
    {"path": "/api/jobs/outbound-campaign-score/at-1300", "schedule": "0 13 * * 1"},
    {"path": "/api/jobs/outbound-campaign-score/at-1400", "schedule": "0 14 * * 1"},
    {"path": "/api/jobs/abby/followups", "schedule": "*/15 * * * *"},
    {"path": "/api/jobs/sales-call-prep/run", "schedule": "*/15 * * * *"},
    {"path": "/api/jobs/instantly/register-webhook", "schedule": "7 * * * *"},
]


def _workflow_on_block(text: str) -> list[str]:
    lines = text.splitlines()
    try:
        start = lines.index("on:") + 1
    except ValueError as exc:
        raise AssertionError("stale-scan workflow is missing an on: block") from exc
    block: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith((" ", "\t")):
            break
        block.append(line)
    while block and block[-1] == "":
        block.pop()
    return block


def test_stale_lead_schedules_are_paused_and_other_crons_are_unchanged() -> None:
    workflow = STALE_SCAN_WORKFLOW.read_text(encoding="utf-8")
    assert _workflow_on_block(workflow) == ["  workflow_dispatch:"]
    assert "schedule:" not in workflow
    assert f'cron: "{PAUSED_SCHEDULE}"' not in workflow
    assert "workflow_dispatch:" in workflow
    assert "/api/jobs/stale-leads/run" in workflow

    config = json.loads((ROOT / "vercel.json").read_text(encoding="utf-8"))
    crons = config["crons"]
    assert crons == UNCHANGED_VERCEL_CRONS
    assert PAUSED_VERCEL_CRON_PATH not in {item["path"] for item in crons}
    assert PAUSED_SCHEDULE not in {item["schedule"] for item in crons}
    assert len(crons) == 24
