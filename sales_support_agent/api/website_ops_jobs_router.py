"""Scheduled Website Ops job entrypoint owned by Anata Agent."""

from __future__ import annotations

import os
import secrets
import logging
from datetime import datetime
from threading import Event, Thread
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Request

from sales_support_agent.services.website_ops import (
    load_website_ops_run_state,
    run_website_ops,
    send_website_ops_failure_email,
    website_ops_run_is_due,
    write_website_ops_run_state,
)
from sales_support_agent.services.website_ops_autonomy import (
    analytics_configuration_status,
)
from sales_support_agent.services.website_ops_query_intelligence import (
    citation_config,
    load_query_intelligence,
)


router = APIRouter(prefix="/api/jobs/website-ops", tags=["website-ops-jobs"])
logger = logging.getLogger(__name__)


def _require_internal_key(request: Request) -> None:
    expected = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _scheduled_modes(local_now: datetime) -> list[str]:
    modes = ["daily"]
    if local_now.weekday() == 0:
        modes.append("weekly")
        if local_now.day <= 7:
            modes.append("monthly")
    return modes


def _run_due_modes(settings: Any, modes: list[str], *, trigger: str) -> dict[str, dict]:
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
                "trigger": trigger,
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
    return results


def install_embedded_website_ops_scheduler(app: FastAPI) -> None:
    """Run the 8 AM sweep in-process, with restart catch-up and due-state locking."""

    settings = app.state.settings
    enabled = os.getenv("WEBSITE_OPS_EMBEDDED_SCHEDULER", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enabled or not str(getattr(settings, "internal_api_key", "") or "").strip():
        return
    if getattr(app.state, "website_ops_scheduler_thread", None):
        return

    stop_event = Event()

    def worker() -> None:
        while not stop_event.is_set():
            local_now = datetime.now(ZoneInfo("America/Denver"))
            if local_now.hour >= 8:
                try:
                    _run_due_modes(
                        settings,
                        _scheduled_modes(local_now),
                        trigger="embedded_scheduler",
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("Embedded Website Ops scheduler failed.")
            stop_event.wait(300)

    thread = Thread(target=worker, name="website-ops-scheduler", daemon=True)
    app.state.website_ops_scheduler_stop = stop_event
    app.state.website_ops_scheduler_thread = thread
    thread.start()


@router.get("/health")
def website_ops_runtime_health(request: Request) -> dict:
    """Expose non-secret scheduler readiness and persisted run freshness."""

    settings = request.app.state.settings
    recipients = os.getenv("WEBSITE_OPS_REPORT_EMAIL_TO", "david@anatainc.com").strip()
    allowed_host = os.getenv("WEBSITE_OPS_ALLOWED_HOST", "anatainc.com").strip().lower()
    github_repository = os.getenv(
        "WEBSITE_OPS_GITHUB_REPOSITORY",
        "david-anata/anata-website",
    ).strip()
    checks = {
        "internal_scheduler_key": bool(str(getattr(settings, "internal_api_key", "") or "").strip()),
        "report_recipient": bool(recipients),
        "email_delivery": bool(str(getattr(settings, "resend_api_key", "") or "").strip()),
        "marketing_scope": allowed_host == "anatainc.com",
        "github_autopush": bool(
            os.getenv("WEBSITE_OPS_GITHUB_TOKEN", "").strip()
            and github_repository == "david-anata/anata-website"
            and bool(getattr(settings, "website_ops_execute_approved", True))
        ),
    }
    analytics_readiness = analytics_configuration_status(settings)
    checks["search_console_configuration"] = bool(
        analytics_readiness["checks"]["google_service_account"]
        and analytics_readiness["checks"]["search_console_property"]
    )
    checks["ga4_configuration"] = bool(
        analytics_readiness["checks"]["google_service_account"]
        and analytics_readiness["checks"]["ga4_property"]
    )
    citation_readiness = citation_config(settings)
    checks["citation_testing"] = bool(
        citation_readiness.enabled and citation_readiness.api_key
    )
    query_intelligence = load_query_intelligence(settings)
    state = load_website_ops_run_state(settings)
    sanitized_runs = {
        mode: {
            key: str(value or "")
            for key, value in run.items()
            if key
            in {
                "mode",
                "status",
                "run_date",
                "trigger",
                "last_started_at",
                "last_completed_at",
                "last_successful_date",
            }
        }
        for mode, run in state.get("runs", {}).items()
    }
    return {
        "status": "ready" if all(checks.values()) else "blocked",
        "states": {
            "runtime": "ready"
            if all(
                checks[key]
                for key in (
                    "internal_scheduler_key",
                    "report_recipient",
                    "email_delivery",
                    "marketing_scope",
                )
            )
            else "blocked",
            "decision_data": analytics_readiness["status"],
            "publishing": "ready" if checks["github_autopush"] else "blocked",
            "query_intelligence": str(query_intelligence.get("status", "not-run")),
            "citation_testing": "ready" if checks["citation_testing"] else "blocked",
        },
        "blockers": analytics_readiness["blockers"]
        + ([] if checks["citation_testing"] else ["Citation testing needs OPENAI_API_KEY."]),
        "schedule": {
            "timezone": "America/Denver",
            "hour": 8,
            "trigger_path": "/api/jobs/website-ops/run",
        },
        "checks": checks,
        "runs": sanitized_runs,
        "state_updated_at": str(state.get("updated_at", "") or ""),
    }


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

    modes = [requested_mode] if requested_mode != "scheduled" else _scheduled_modes(local_now)
    results = _run_due_modes(request.app.state.settings, modes, trigger="render_cron")

    return {"status": "ok", "details": results}
