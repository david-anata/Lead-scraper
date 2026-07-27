"""Scheduled Website Ops job entrypoint owned by Anata Agent."""

from __future__ import annotations

import secrets
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request

from sales_support_agent.services.website_ops import (
    run_website_ops,
    send_website_ops_failure_email,
    website_ops_run_is_due,
    write_website_ops_run_state,
)


router = APIRouter(prefix="/api/jobs/website-ops", tags=["website-ops-jobs"])


def _require_internal_key(request: Request) -> None:
    expected = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


@router.post("/run")
async def run_scheduled_website_ops(request: Request) -> dict:
    _require_internal_key(request)
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    requested_mode = str(payload.get("mode", "scheduled") or "scheduled").strip().lower()
    if requested_mode not in {"scheduled", "daily", "weekly", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported run mode.")

    local_now = datetime.now(ZoneInfo("America/Denver"))
    if requested_mode == "scheduled" and local_now.hour != 8:
        return {
            "status": "skipped",
            "message": "Website Ops scheduler is waiting for 8:00 AM America/Denver.",
            "local_time": local_now.isoformat(),
        }

    modes = [requested_mode] if requested_mode != "scheduled" else ["daily"]
    if requested_mode == "scheduled" and local_now.weekday() == 0:
        modes.append("weekly")
        if local_now.day <= 7:
            modes.append("monthly")

    settings = request.app.state.settings
    results: dict[str, dict] = {}
    for mode in modes:
        if not website_ops_run_is_due(settings, mode):
            results[mode] = {"status": "skipped", "message": "Already completed for this period."}
            continue
        now = datetime.now(ZoneInfo("UTC"))
        write_website_ops_run_state(
            settings,
            mode,
            {
                "mode": mode,
                "status": "running",
                "run_date": now.date().isoformat(),
                "trigger": "render_cron",
                "last_started_at": now.isoformat(),
                "last_error": "",
            },
        )
        try:
            result = run_website_ops(settings, mode=mode)
        except Exception as exc:  # noqa: BLE001
            write_website_ops_run_state(
                settings,
                mode,
                {
                    "status": "failed",
                    "last_completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                    "last_error": str(exc),
                },
            )
            send_website_ops_failure_email(settings, mode=mode, error=str(exc))
            results[mode] = {"status": "failed", "message": str(exc)}
            continue
        completed_at = datetime.now(ZoneInfo("UTC"))
        write_website_ops_run_state(
            settings,
            mode,
            {
                "status": "succeeded",
                "last_completed_at": completed_at.isoformat(),
                "last_successful_date": completed_at.date().isoformat(),
                "last_error": "",
            },
        )
        results[mode] = {"status": "succeeded", "message": result.message}

    return {"status": "ok", "details": results}
