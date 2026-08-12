"""Vercel Cron adapters for the existing Agent scheduled services.

The adapters are deliberately inert until the cutover flag is enabled. They
use Vercel's bearer ``CRON_SECRET`` and delegate to the same handlers Render
uses, preserving business logic while avoiding duplicate schedulers.
"""

from __future__ import annotations

from datetime import datetime
import os
import secrets
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from sales_support_agent.api.content_router import ContentRunInput, content_run
from sales_support_agent.api.hr_jobs_router import hr_reminders_run
from sales_support_agent.api.router import (
    run_daily_digest_job,
    run_gmail_sync_job,
    run_stale_lead_job,
)
from sales_support_agent.api.sales_jobs_router import sales_operator_run_job
from sales_support_agent.api.website_ops_jobs_router import _run_embedded_pulse
from sales_support_agent.models.schemas import (
    DailyDigestRunRequest,
    GmailSyncRequest,
    StaleLeadRunRequest,
)


router = APIRouter(prefix="/api/vercel-cron", tags=["vercel-cron"])
_DENVER = ZoneInfo("America/Denver")


def _require_vercel_cron(authorization: str | None) -> None:
    configured = os.getenv("CRON_SECRET", "").strip()
    provided = str(authorization or "").strip()
    if (
        not configured
        or not provided.startswith("Bearer ")
        or not secrets.compare_digest(configured, provided[7:].strip())
    ):
        raise HTTPException(status_code=401, detail="Valid Vercel cron credential required.")


def _cron_writes_enabled() -> bool:
    return os.getenv("VERCEL_CRON_WRITES_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _disabled() -> JSONResponse:
    return JSONResponse(
        {
            "status": "disabled",
            "message": "Vercel scheduled writes remain disabled until cutover.",
        }
    )


def _authorize(authorization: str | None) -> JSONResponse | None:
    _require_vercel_cron(authorization)
    return None if _cron_writes_enabled() else _disabled()


def _internal_key(request: Request) -> str:
    return str(getattr(request.app.state.settings, "internal_api_key", "") or "")


@router.get("/website-ops")
def website_ops_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return _run_embedded_pulse(request.app.state.settings, datetime.now(_DENVER))


@router.get("/content")
def content_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return content_run(ContentRunInput(mode="scheduled"), request)


@router.get("/stale-leads")
def stale_leads_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return run_stale_lead_job(
        StaleLeadRunRequest(dry_run=False), request, _internal_key(request)
    )


@router.get("/gmail-sync")
def gmail_sync_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return run_gmail_sync_job(
        GmailSyncRequest(dry_run=False), request, _internal_key(request)
    )


@router.get("/daily-digest")
def daily_digest_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return run_daily_digest_job(
        DailyDigestRunRequest(), request, _internal_key(request)
    )


@router.get("/sales-operator")
async def sales_operator_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return await sales_operator_run_job(request, _internal_key(request))


@router.get("/hr-reminders")
async def hr_reminders_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return await hr_reminders_run(
        request,
        dry_run=False,
        x_internal_api_key=_internal_key(request),
    )

