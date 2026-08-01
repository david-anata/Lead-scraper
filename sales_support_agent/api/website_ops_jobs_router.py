"""Scheduled Website Ops job entrypoint owned by Anata Agent."""

from __future__ import annotations

import os
import secrets
import logging
import hashlib
from datetime import datetime, timedelta
from threading import Event, Thread
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    load_feedback_records,
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
from sales_support_agent.services.website_ops_storage import (
    database_mirror_enabled,
    website_ops_cache_transaction,
)


router = APIRouter(prefix="/api/jobs/website-ops", tags=["website-ops-jobs"])
logger = logging.getLogger(__name__)
WEBSITE_OPS_PULSE_HOURS = (8, 9, 10, 11, 12, 13, 14, 15)
WEBSITE_OPS_RUNNING_STALE_AFTER = timedelta(minutes=45)


def _pulse_slot(local_now: datetime) -> str:
    return f"{local_now.date().isoformat()}:{local_now.hour:02d}"


def _daily_run_is_fresh(state: dict[str, Any], local_now: datetime) -> bool:
    """Require a durable, non-stalled current-day run after the first pulse."""

    if local_now.hour < WEBSITE_OPS_PULSE_HOURS[0]:
        return True
    daily = dict((state.get("runs") or {}).get("daily") or {})
    if str(daily.get("run_date", "")) != local_now.date().isoformat():
        return False
    status = str(daily.get("status", ""))
    if status == "succeeded":
        return True
    if status != "running":
        return False
    try:
        started_at = datetime.fromisoformat(str(daily.get("last_started_at", "")))
    except (TypeError, ValueError):
        return False
    if started_at.tzinfo is None:
        return False
    return local_now.astimezone(ZoneInfo("UTC")) - started_at.astimezone(
        ZoneInfo("UTC")
    ) <= WEBSITE_OPS_RUNNING_STALE_AFTER


def _daily_run_has_verified_outcome(
    state: dict[str, Any], local_now: datetime
) -> bool:
    """Do not call a completed pulse healthy until production changed."""

    if local_now.hour < WEBSITE_OPS_PULSE_HOURS[0]:
        return True
    daily = dict((state.get("runs") or {}).get("daily") or {})
    if str(daily.get("run_date", "")) != local_now.date().isoformat():
        return False
    if str(daily.get("status", "")) == "running":
        return True
    try:
        production_delta_count = int(daily.get("production_delta_count", 0) or 0)
    except (TypeError, ValueError):
        return False
    return (
        str(daily.get("status", "")) == "succeeded"
        and str(daily.get("outcome_status", "")) == "production_verified"
        and production_delta_count > 0
    )


def _latest_autonomous_execution_error(settings: Any) -> dict[str, str]:
    failed = [
        record
        for record in load_feedback_records(settings)
        if bool(record.get("auto_generated"))
        and str(record.get("status", "")).strip().lower() == "error"
        and str(record.get("execution_error", "")).strip()
    ]
    if not failed:
        return {}
    latest = max(
        failed,
        key=lambda item: str(
            item.get("last_execution_at", "") or item.get("submitted_at", "")
        ),
    )
    return {
        "action_type": str(
            latest.get("action_type", "") or latest.get("suggested_action_type", "")
        )[:80],
        "page_url": str(latest.get("page_url", ""))[:300],
        "error": str(latest.get("execution_error", ""))[:500],
        "last_execution_at": str(latest.get("last_execution_at", ""))[:64],
    }


def _run_embedded_pulse(settings: Any, local_now: datetime) -> dict[str, Any]:
    """Run one missed hourly slot with the same durable lease as Render cron."""

    if local_now.hour not in WEBSITE_OPS_PULSE_HOURS:
        return {"status": "skipped", "message": "Outside the Website Ops pulse window."}

    from sales_support_agent.models.database import get_engine

    try:
        engine = get_engine()
    except RuntimeError:
        engine = None
    mirror_enabled = engine is not None and database_mirror_enabled()

    def execute() -> dict[str, Any]:
        pulse_slot = _pulse_slot(local_now)
        current_state = get_website_ops_run_state(settings, "daily")
        failed_status = str(current_state.get("status", "")) in {
            "failed",
            "failed_outcome",
        }
        stalled_running = (
            str(current_state.get("status", "")) == "running"
            and not _daily_run_is_fresh(
                {"runs": {"daily": current_state}},
                local_now,
            )
        )
        recoverable_status = failed_status or stalled_running
        if (
            current_state.get("last_pulse_slot") == pulse_slot
            and not recoverable_status
        ):
            return {"status": "skipped", "message": "This Website Ops pulse already completed."}

        run_key = f"{local_now.date().isoformat()}:scheduled:{pulse_slot}"
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
        if (
            engine is not None
            and lease is None
            and (
                str(current_state.get("run_date", "")) != local_now.date().isoformat()
                or recoverable_status
            )
        ):
            # Recover one failed or stale attempt. Key the recovery lease to
            # that attempt's start timestamp so a deploy interruption cannot
            # permanently consume the slot's only recovery opportunity.
            recovery_attempt = str(
                current_state.get("last_started_at", "") or "missing-start"
            )
            recovery_key = hashlib.sha256(
                recovery_attempt.encode("utf-8")
            ).hexdigest()[:12]
            lease = claim_scheduled_job(
                engine,
                job_key="website_ops",
                run_key=f"{run_key}:stale-or-failed-recovery:{recovery_key}",
                lease_minutes=180,
            )
        if engine is not None and lease is None:
            return {"status": "skipped", "message": "This Website Ops pulse is already running."}

        try:
            results = _run_due_modes(
                settings,
                _scheduled_modes(local_now),
                trigger="embedded_scheduler",
                pulse_slot=pulse_slot,
            )
            failed = any(
                item.get("status") in {"failed", "failed_outcome"}
                for item in results.values()
            )
            if engine is not None and lease is not None:
                finish_scheduled_job(
                    engine,
                    lease,
                    status="failed" if failed else "succeeded",
                    details=results,
                )
            return {"status": "failed" if failed else "succeeded", "details": results}
        except Exception as exc:
            if engine is not None and lease is not None:
                finish_scheduled_job(
                    engine,
                    lease,
                    status="failed",
                    details={"error": str(exc)},
                )
            raise

    if mirror_enabled:
        with website_ops_cache_transaction(engine, settings.website_ops_root):
            return execute()
    return execute()


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
        last_attempt = 0
        for attempt in range(1, max_attempts + 1):
            last_attempt = attempt
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
                attempt_outcome = dict(
                    (getattr(result, "report", None) or {}).get("run_outcome") or {}
                )
                attempt_status = str(attempt_outcome.get("status", "") or "").strip()
                attempt_delta = int(
                    attempt_outcome.get("production_delta_count", 0) or 0
                )
                retryable_zero_delta = (
                    mode == "daily"
                    and attempt_status in {"work_in_progress", "failed_outcome"}
                    and attempt_delta == 0
                    and attempt < max_attempts
                )
                if retryable_zero_delta:
                    logger.warning(
                        "Website Ops daily attempt %s/%s produced no verified delta; retrying.",
                        attempt,
                        max_attempts,
                    )
                    continue
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
        outcome = dict((getattr(result, "report", None) or {}).get("run_outcome") or {})
        outcome_status = str(outcome.get("status", "") or "").strip()
        production_delta_count = int(outcome.get("production_delta_count", 0) or 0)
        outcome_updates = {
            "outcome_status": outcome_status,
            "outcome_message": str(outcome.get("summary", "") or ""),
            "expected_output": str(outcome.get("expected_output", "") or ""),
            "actual_output": str(outcome.get("actual_output", "") or ""),
            "production_delta_count": str(production_delta_count),
            "last_stage": str(outcome.get("last_stage", "") or ""),
            "next_operation": str(outcome.get("next_operation", "") or ""),
            "failure_stage": str(outcome.get("failure_stage", "") or ""),
        }
        daily_target_missed = mode == "daily" and not (
            outcome_status == "production_verified" and production_delta_count > 0
        )
        if (
            not bool(getattr(result, "ok", True))
            or outcome_status == "failed_outcome"
            or daily_target_missed
        ):
            message = result.message or "Website Ops completed without the required outcome."
            write_website_ops_run_state(
                settings,
                mode,
                {
                    **outcome_updates,
                    "status": "failed_outcome",
                    "last_completed_at": completed_at.isoformat(),
                    "last_error": message,
                    "recovery_status": "outcome_failed",
                },
            )
            send_website_ops_failure_email(settings, mode=mode, error=message)
            results[mode] = {
                "status": "failed_outcome",
                "message": message,
                "outcome": outcome,
                "attempts": last_attempt,
            }
            continue
        write_website_ops_run_state(
            settings,
            mode,
            {
                **outcome_updates,
                "status": "succeeded",
                "last_completed_at": completed_at.isoformat(),
                "last_successful_date": completed_at.date().isoformat(),
                "last_error": "",
                "recovery_status": "recovered" if last_attempt > 1 else "not_needed",
            },
        )
        results[mode] = {
            "status": "succeeded",
            "message": result.message,
            "outcome": outcome,
            "attempts": last_attempt,
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
            if local_now.hour in WEBSITE_OPS_PULSE_HOURS:
                try:
                    _run_embedded_pulse(settings, local_now)
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
    scheduler_thread = getattr(request.app.state, "website_ops_scheduler_thread", None)
    checks["embedded_scheduler"] = bool(
        scheduler_thread is not None
        and callable(getattr(scheduler_thread, "is_alive", None))
        and scheduler_thread.is_alive()
    )
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
    latest_execution_error = _latest_autonomous_execution_error(settings)
    local_now = datetime.now(ZoneInfo("America/Denver"))
    checks["daily_run_fresh"] = _daily_run_is_fresh(state, local_now)
    checks["daily_production_outcome"] = _daily_run_has_verified_outcome(
        state, local_now
    )
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
                "outcome_status",
                "outcome_message",
                "expected_output",
                "actual_output",
                "production_delta_count",
                "last_stage",
                "next_operation",
                "failure_stage",
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
        )
        + (
            []
            if checks["daily_run_fresh"]
            else [
                "The embedded scheduler has not persisted a successful Website Ops run "
                f"for {local_now.date().isoformat()}."
            ]
        )
        + (
            []
            if checks["daily_production_outcome"]
            else [
                "The latest completed Website Ops pulse has no production-verified change."
            ]
        )
        + (
            []
            if not latest_execution_error or checks["daily_production_outcome"]
            else [
                "Latest autonomous execution failed: "
                + latest_execution_error["error"]
            ]
        ),
        "schedule": {
            "timezone": "America/Denver",
            "hour": 8,
            "hours": list(WEBSITE_OPS_PULSE_HOURS),
            "source": "embedded_scheduler" if checks["embedded_scheduler"] else "unavailable",
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
        "latest_execution_error": latest_execution_error,
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
            "message": "Website Ops scheduler is waiting for an 8 AM through 3 PM America/Denver pulse.",
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
    if engine is not None and lease is None and force and requested_mode == "daily":
        current_state = get_website_ops_run_state(request.app.state.settings, "daily")
        state_status = str(current_state.get("status", ""))
        running_is_stale = state_status == "running" and not _daily_run_is_fresh(
            {"runs": {"daily": current_state}},
            local_now,
        )
        if state_status != "running" or running_is_stale:
            attempt_reference = "|".join(
                (
                    state_status,
                    str(current_state.get("last_started_at", "")),
                    str(current_state.get("last_completed_at", "")),
                )
            )
            recovery_key = hashlib.sha256(
                attempt_reference.encode("utf-8")
            ).hexdigest()[:12]
            lease = claim_scheduled_job(
                engine,
                job_key="website_ops",
                run_key=f"{run_key}:force-recovery:{recovery_key}",
                lease_minutes=180,
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
        results = await run_in_threadpool(
            _run_due_modes,
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
