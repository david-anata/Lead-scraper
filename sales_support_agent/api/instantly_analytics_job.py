"""Daily Instantly campaign analytics. Vercel Cron, about 6:00 AM Mountain.

12:00 UTC is 6:00 AM America/Denver during daylight time and 5:00 AM during
standard time. The route accepts CRON_SECRET or X-Internal-Api-Key, the same
pair the other scheduled jobs use. A call with no start_date refreshes the
last few days. The first call, while the table is empty, backfills from
2026-10-01. Pass start_date=YYYY-MM-DD to choose the backfill yourself.
"""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Query, Request

from sales_support_agent.api.outbound_jobs import _require_scheduler_auth

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/instantly", tags=["outbound-jobs"])


def run_analytics_sync(*, start_date: str = "", days: int = 3) -> dict:
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services.instantly_campaign_analytics import sync_daily_analytics

    parsed = date.fromisoformat(start_date.strip()[:10]) if start_date.strip() else None
    return sync_daily_analytics(get_engine(), start_date=parsed, days=days)


@router.api_route("/analytics-sync", methods=["GET", "POST"])
def analytics_sync(
    request: Request,
    start_date: str = "",
    days: int = Query(default=3, ge=1, le=366),
) -> dict:
    _require_scheduler_auth(request)
    if start_date.strip():
        try:
            date.fromisoformat(start_date.strip()[:10])
        except ValueError:
            return {"ok": False, "reason": "start_date must be YYYY-MM-DD."}
    try:
        return run_analytics_sync(start_date=start_date, days=days)
    except Exception:  # noqa: BLE001
        logger.exception("[instantly-analytics] sync failed")
        return {"ok": False, "reason": "sync failed"}
