"""Internal scheduled jobs for the HR control room."""

from __future__ import annotations

import secrets

from fastapi import APIRouter, Header, HTTPException, Request

from sales_support_agent.services.hr.notifications import run_daily_digest


router = APIRouter(prefix="/api/jobs", tags=["hr-jobs"])


@router.post("/hr-reminders/run")
async def hr_reminders_run(
    request: Request, dry_run: bool = False,
    x_internal_api_key: str | None = Header(default=None),
):
    configured = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    provided = str(x_internal_api_key or "").strip()
    if not configured or not secrets.compare_digest(configured, provided):
        raise HTTPException(status_code=403, detail="Valid internal API key required.")
    base_url = str(request.base_url).rstrip("/")
    if "localhost" not in base_url and "127.0.0.1" not in base_url:
        base_url = base_url.replace("http://", "https://")
    return run_daily_digest(
        request.app.state.settings, base_url=base_url, dry_run=dry_run
    )
