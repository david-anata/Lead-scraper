"""Weekly read-only Search Console export.

Vercel Cron is UTC. Monday 14:00 UTC is 7:00 AM America/Denver during standard
time and 8:00 AM during daylight time. The route also accepts an on-demand
GET or POST with the same credentials as the other scheduled jobs. It does not
run Website Ops and it does not change either website.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from sales_support_agent.api.website_ops_jobs_router import _require_internal_key
from sales_support_agent.services.search_console_export import (
    SearchConsoleExportError,
    run_search_console_export,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/jobs/search-console", tags=["search-console-jobs"])


@router.api_route("/export", methods=["GET", "POST"])
async def export_search_console(request: Request) -> dict:
    """Export Search Console queries and pages into the configured spreadsheet."""

    _require_internal_key(request)
    try:
        return await run_in_threadpool(
            run_search_console_export,
            request.app.state.settings,
        )
    except SearchConsoleExportError as exc:
        logger.error("Search Console export failed: %s", exc)
        raise HTTPException(status_code=503, detail=str(exc)) from None
