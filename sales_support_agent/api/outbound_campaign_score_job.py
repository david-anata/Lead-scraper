"""Monday morning campaign score. America/Denver, 7:00.

Vercel cron is UTC, and a comma-separated hour on one path has been listed
and then never invoked. Two paths cover the two Denver offsets:

- ``/at-1300`` at 13:00 UTC is 7:00 AM MDT
- ``/at-1400`` at 14:00 UTC is 7:00 AM MST

The handler runs only on Monday at 7:00 AM local. The other offset returns
skipped. Render's Monday cron posts to ``/run`` on both hours and the same
check applies.

The score proposes actions. It does not activate campaigns, change caps,
or create Instantly campaigns.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/jobs/outbound-campaign-score", tags=["outbound-jobs"])

_TZ = ZoneInfo("America/Denver")
_RUN_WEEKDAY = 0  # Monday
_RUN_HOUR = 7


def _register(suffix: str) -> None:
    slug = suffix.strip("/").replace("-", "_") or "run"

    def run_scheduled(request: Request) -> dict:
        _require_scheduler_auth(request)
        now = datetime.now(_TZ)
        if now.weekday() != _RUN_WEEKDAY or now.hour != _RUN_HOUR:
            return {
                "status": "skipped",
                "reason": "waiting for Monday 7:00 AM America/Denver",
                "local_time": now.isoformat(),
            }
        from sales_support_agent.models.database import get_engine
        import outbound_campaign_score as score

        try:
            details = score.run_weekly_score(get_engine(), now=now)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-campaign-score] scheduled run failed")
            return {"status": "error", "reason": "weekly score failed"}
        return {"status": "ok", "details": details}

    for method in ("GET", "POST"):
        router.add_api_route(
            suffix,
            run_scheduled,
            methods=[method],
            name=f"outbound_campaign_score_{slug}_{method.lower()}",
            operation_id=f"outbound_campaign_score_{slug}_{method.lower()}",
        )


for _suffix in ("/run", "/at-1300", "/at-1400"):
    _register(_suffix)
