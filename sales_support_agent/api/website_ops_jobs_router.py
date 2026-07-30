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
    get_website_ops_run_state,
    load_website_ops_run_state,
    run_website_ops,
    send_website_ops_failure_email,
    website_ops_operating_state,
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
from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    finish_scheduled_job,
)


router = APIRouter(prefix="/api/jobs/website-ops", tags=["website-ops-jobs"])
logger = logging.getLogger(__name__)
WEBSITE_OPS_PULSE_HOURS = (8, 9, 10, 11, 12, 13, 14, 15)


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


def _run_due_modes(
    settings: Any,
    modes: list[str],
    *,
    trigger: str,
    force: bool = False,
    pulse_slot: str = "",
) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for mode in modes:
        current_state = get_website_ops_run_state(settings, mode)
        repeated_daily_pulse = (
            mode == "daily"
            and bool(pulse_slot)
            and current_state.get("last_pulse_slot") != pulse_slot
        )
        if not force and not repeated_daily_pulse and not website_ops_run_is_due(settings, mode):
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
                "last_pulse_slot": pulse_slot if mode == "daily" else "",
            },
        )
        max_attempts = max(
            1,
            min(int(os.getenv("WEBSITE_OPS_RUN_MAX_ATTEMPTS", "3") or "3"), 5),
        )
        result = None
        final_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            write_website_ops_run_state(
                settings,
                mode,
                {
                    "attempt_count": str(attempt),
                    "recovery_status": "retrying" if attempt > 1 else "primary",
                },
            )
            try:
                result = run_website_ops(settings, mode=mode)
                final_error = None
                break
            except Exception as exc:  # noqa: BLE001
                final_error = exc
                logger.exception(
                    "Website Ops %s attempt %s/%s failed.",
                    mode,
                    attempt,
                    max_attempts,
                )
        if final_error is not None or result is None:
            message = str(final_error or "Website Ops run did not return a result.")
            write_website_ops_run_state(
                settings,
                mode,
                {
                    "status": "failed",
                    "last_completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                    "last_error": message,
                    "recovery_status": "exhausted",
                },
            )
            send_website_ops_failure_email(settings, mode=mode, error=message)
            results[mode] = {
                "status": "failed",
                "message": message,
                "attempts": max_attempts,
            }
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
                "recovery_status": "recovered"
                if int(get_website_ops_run_state(settings, mode).get("attempt_count", "1") or "1") > 1
                else "not_needed",
            },
        )
        results[mode] = {
            "status": "succeeded",
            "message": result.message,
            "attempts": int(
                get_website_ops_run_state(settings, mode).get("attempt_count", "1") or "1"
            ),
        }
    return results


def install_embedded_website_ops_scheduler(app: FastAPI) -> None:
    """Run eight local production pulses, with restart catch-up and due-state locking."""

    settings = app.state.settings
    enabled = os.getenv("WEBSITE_OPS_EMBEDDED_SCHEDULER", "false").strip().lower() in {
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
            if local_now.hour in WEBSITE_OPS_PULSE_HOURS and local_now.minute < 5:
                try:
                    _run_due_modes(
                        settings,
                        _scheduled_modes(local_now),
                        trigger="embedded_scheduler",
                        pulse_slot=f"{local_now.date().isoformat()}:{local_now.hour:02d}",
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
    operating_state = website_ops_operating_state(settings)
    checks["search_console_configuration"] = bool(
        analytics_readiness["checks"]["google_service_account"]
        and analytics_readiness["checks"]["search_console_property"]
        and operating_state["search_console"] == "ready"
    )
    checks["ga4_configuration"] = bool(
        analytics_readiness["checks"]["google_service_account"]
        and analytics_readiness["checks"]["ga4_property"]
        and operating_state["ga4"] == "ready"
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
            "decision_data": operating_state["decision_data"],
            "publishing": "ready" if checks["github_autopush"] else "blocked",
            "query_intelligence": str(query_intelligence.get("status", "not-run")),
            "citation_testing": "ready" if checks["citation_testing"] else "blocked",
        },
        "blockers": operating_state["blockers"]
        + [
            item.get("message", "")
            for item in analytics_readiness["blockers"]
            if item.get("message")
        ]
        + (
            []
            if checks["citation_testing"]
            else ["Citation testing needs OPENAI_API_KEY or ANTHROPIC_API_KEY."]
        ),
        "schedule": {
            "timezone": "America/Denver",
            "hour": 8,
            "hours": list(WEBSITE_OPS_PULSE_HOURS),
            "trigger_path": "/api/jobs/website-ops/run",
        },
        "checks": checks,
        "runs": sanitized_runs,
        "state_updated_at": str(state.get("updated_at", "") or ""),
        "evidence": {
            "generated_at": operating_state["evidence_generated_at"],
            "age_hours": operating_state["evidence_age_hours"],
        },
        "user_todo": operating_state["support_requests"],
    }


@router.post("/run")
async def run_scheduled_website_ops(request: Request) -> dict:
    _require_internal_key(request)
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    requested_mode = str(payload.get("mode", "scheduled") or "scheduled").strip().lower()
    force = payload.get("force") is True
    if requested_mode not in {"scheduled", "daily", "weekly", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported run mode.")

    local_now = datetime.now(ZoneInfo("America/Denver"))
    if requested_mode == "scheduled" and local_now.hour not in WEBSITE_OPS_PULSE_HOURS:
        return {
            "status": "skipped",
            "message": "Website Ops scheduler is waiting for the 8 AM, 1 PM, or 6 PM America/Denver pulse.",
            "local_time": local_now.isoformat(),
        }

    from sales_support_agent.models.database import get_engine

    try:
        engine = get_engine()
    except RuntimeError:
        # Preserve compatibility for isolated/test router mounts. Production
        # always initializes the shared engine and therefore always uses the
        # cross-instance lease below.
        engine = None
    pulse_slot = (
        f"{local_now.date().isoformat()}:{local_now.hour:02d}"
        if requested_mode == "scheduled"
        else ""
    )
    run_key = f"{local_now.date().isoformat()}:{requested_mode}:{pulse_slot}"
    lease = (
        claim_scheduled_job(
            engine,
            job_key="website_ops",
            run_key=run_key,
            lease_minutes=180,
        )
        if engine is not None
        else None
    )
    if engine is not None and lease is None:
        return {
            "status": "skipped",
            "message": "This Website Ops period is already running or complete.",
            "run_key": run_key,
        }

    try:
        modes = (
            [requested_mode]
            if requested_mode != "scheduled"
            else _scheduled_modes(local_now)
        )
        results = _run_due_modes(
            request.app.state.settings,
            modes,
            trigger="internal_force" if force else "render_cron",
            force=force,
            pulse_slot=pulse_slot,
        )
        failed = any(item.get("status") == "failed" for item in results.values())
        if engine is not None and lease is not None:
            finish_scheduled_job(
                engine,
                lease,
                status="failed" if failed else "succeeded",
                details=results,
            )
        if failed:
            raise HTTPException(
                status_code=503,
                detail="One or more Website Ops modes failed.",
            )
        return {"status": "ok", "details": results, "run_key": run_key}
    except HTTPException:
        raise
    except Exception as exc:
        if engine is not None and lease is not None:
            finish_scheduled_job(
                engine,
                lease,
                status="failed",
                details={"error": str(exc)},
            )
        raise
