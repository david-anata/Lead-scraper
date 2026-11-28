"""Scheduled Website Ops job entrypoint owned by Anata Agent."""

from __future__ import annotations

import json
import os
import secrets
import logging
import hashlib
import time
from datetime import date, datetime, timedelta, timezone
from threading import Event
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from fastapi import APIRouter, FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from sales_support_agent.services.website_ops import (
    get_website_ops_run_state,
    load_feedback_records,
    load_website_ops_run_state,
    run_website_ops,
    save_feedback_record,
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
    expire_stale_scheduled_jobs,
    finish_scheduled_job,
)
from sales_support_agent.services.website_ops_outcomes import (
    persisted_run_disposition,
)
from sales_support_agent.services.website_ops_storage import (
    database_mirror_enabled,
    website_ops_cache_transaction,
)


router = APIRouter(prefix="/api/jobs/website-ops", tags=["website-ops-jobs"])
logger = logging.getLogger(__name__)
WEBSITE_OPS_PULSE_HOURS = (8, 9, 10, 11, 12, 13, 14, 15)
WEBSITE_OPS_RUNNING_STALE_AFTER = timedelta(minutes=45)
# Stay well under Vercel's 800s maxDuration, including snapshot time after return.
WEBSITE_OPS_STEP_BUDGET_SECONDS = 540
WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS = 600
_RELEASE_LOG_HOSTS = {"anatainc.com", "anatabuilding.com"}


def _website_ops_step_budget_seconds() -> int:
    """Return the per-invocation budget, capped so the cron returns before 800s."""

    raw = os.getenv(
        "WEBSITE_OPS_STEP_BUDGET_SECONDS",
        str(WEBSITE_OPS_STEP_BUDGET_SECONDS),
    )
    try:
        budget = int(raw)
    except (TypeError, ValueError):
        budget = WEBSITE_OPS_STEP_BUDGET_SECONDS
    return max(30, min(budget, WEBSITE_OPS_STEP_BUDGET_MAX_SECONDS))


def _pulse_slot(local_now: datetime) -> str:
    return f"{local_now.date().isoformat()}:{local_now.hour:02d}"


_INTERRUPTED_LEASE_DETAILS = {
    "error": "Website Ops was interrupted or timed out before the lease was finished.",
    "reason": "interrupted_or_timeout",
}


def _scheduled_trigger(method: str, *, force: bool) -> str:
    """Label Vercel cron GETs separately from the Render POST cron."""

    if force:
        return "internal_force"
    if str(method or "").upper() == "GET":
        return "vercel_cron"
    return "render_cron"


def _finish_website_ops_lease(
    engine: Any,
    lease: Any,
    *,
    status: str,
    details: dict[str, Any] | None,
) -> bool:
    """Finish a claimed lease. Return False when the update itself fails."""

    if engine is None or lease is None:
        return True
    try:
        finish_scheduled_job(engine, lease, status=status, details=details)
    except Exception:
        logger.exception("Website Ops lease could not be finished.")
        return False
    return True


def _expire_stale_mode_run_files(settings: Any, now: datetime) -> list[str]:
    """Fail daily or weekly file state that has been queued or running too long."""

    expired: list[str] = []
    minutes = int(WEBSITE_OPS_RUNNING_STALE_AFTER.total_seconds() // 60)
    current = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    for mode in ("daily", "weekly"):
        state = get_website_ops_run_state(settings, mode)
        status = str(state.get("status", ""))
        if status not in {"queued", "running"}:
            continue
        started_raw = str(state.get("last_started_at", "") or "").strip()
        stale = True
        if started_raw:
            try:
                started_at = datetime.fromisoformat(started_raw)
            except ValueError:
                started_at = None
            if started_at is not None:
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                age = current.astimezone(timezone.utc) - started_at.astimezone(timezone.utc)
                stale = age > WEBSITE_OPS_RUNNING_STALE_AFTER
        if not stale:
            continue
        started_label = started_raw or "an unknown time"
        write_website_ops_run_state(
            settings,
            mode,
            {
                "status": "failed",
                "last_completed_at": datetime.now(timezone.utc).isoformat(),
                "last_error": (
                    f"Marked failed because this {mode} run stayed {status} "
                    f"after {started_label}, past the {minutes}-minute limit, "
                    "and never finished. The next pulse can start new work."
                ),
                "recovery_status": "exhausted",
            },
        )
        expired.append(mode)
        logger.info("Marked stale Website Ops %s file state failed.", mode)
    return expired


def _sweep_expired_website_ops_leases(engine: Any) -> None:
    """Fail website_ops leases that have already passed their expiry."""

    if engine is None:
        return
    try:
        expired = expire_stale_scheduled_jobs(
            engine,
            now=datetime.now(timezone.utc),
            job_key="website_ops",
        )
    except Exception:
        logger.exception("Could not expire stale Website Ops leases.")
        return
    if expired:
        logger.info("Expired %s stale website_ops lease(s).", expired)


def _daily_run_is_fresh(state: dict[str, Any], local_now: datetime) -> bool:
    """Require a durable, non-stalled current-day run after the first pulse."""

    if local_now.hour < WEBSITE_OPS_PULSE_HOURS[0]:
        return True
    daily = dict((state.get("runs") or {}).get("daily") or {})
    if str(daily.get("run_date", "")) != local_now.date().isoformat():
        return False
    status = str(daily.get("status", ""))
    if status in {"succeeded", "work_in_progress"}:
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
        _expire_stale_mode_run_files(settings, local_now)
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

        lease_finished = False
        try:
            results = _run_due_modes(
                settings,
                _scheduled_modes(local_now),
                trigger="embedded_scheduler",
                pulse_slot=pulse_slot,
                business_date=local_now.date(),
            )
            failed = any(
                item.get("status") in {"failed", "failed_outcome"}
                for item in results.values()
            )
            lease_finished = _finish_website_ops_lease(
                engine,
                lease,
                status="failed" if failed else "succeeded",
                details=results,
            )
            return {"status": "failed" if failed else "succeeded", "details": results}
        except Exception as exc:
            if not lease_finished:
                lease_finished = _finish_website_ops_lease(
                    engine,
                    lease,
                    status="failed",
                    details={"error": str(exc)},
                )
            raise
        finally:
            if not lease_finished:
                _finish_website_ops_lease(
                    engine,
                    lease,
                    status="failed",
                    details=dict(_INTERRUPTED_LEASE_DETAILS),
                )

    if mirror_enabled:
        with website_ops_cache_transaction(engine, settings.website_ops_root):
            return execute()
    return execute()


def _require_internal_key(request: Request) -> None:
    cron_secret = os.getenv("CRON_SECRET", "").strip()
    authorization = request.headers.get("Authorization", "").strip()
    if cron_secret and secrets.compare_digest(
        authorization,
        f"Bearer {cron_secret}",
    ):
        return

    expected = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


def _require_release_log_key(request: Request) -> None:
    """Allow release logging to use a narrowly scoped credential."""

    expected = os.getenv("SEO_RELEASE_LOG_KEY", "").strip()
    if not expected:
        _require_internal_key(request)
        return
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid SEO release-log key.")


def _release_log_host(value: object) -> str:
    """Return a supported production host from a site or production URL."""

    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.hostname or "").lower().removeprefix("www.")
    return host if host in _RELEASE_LOG_HOSTS else ""


def _release_log_urls(value: object, *, site_host: str) -> list[str]:
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail="live_urls must be a JSON array.")
    urls: list[str] = []
    for item in value:
        url = str(item or "").strip()
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower().removeprefix("www.")
        if parsed.scheme != "https" or host != site_host or not parsed.path:
            raise HTTPException(
                status_code=400,
                detail="Every live URL must be an HTTPS URL on the declared site.",
            )
        if url not in urls:
            urls.append(url)
    if not urls:
        raise HTTPException(status_code=400, detail="At least one live URL is required.")
    if len(urls) > 24:
        raise HTTPException(status_code=400, detail="A release log can contain at most 24 live URLs.")
    return urls


def _release_log_values(value: object, *, field: str, limit: int) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail=f"{field} must be a JSON array.")
    values = list(
        dict.fromkeys(
            str(item or "").strip() for item in value if str(item or "").strip()
        )
    )
    if len(values) > limit or any(len(item) > 300 for item in values):
        raise HTTPException(status_code=400, detail=f"{field} has an invalid value.")
    return values


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
    business_date: date | None = None,
) -> dict[str, dict]:
    results: dict[str, dict] = {}
    run_business_date = business_date or datetime.now(
        ZoneInfo("America/Denver")
    ).date()
    _expire_stale_mode_run_files(settings, datetime.now(timezone.utc))
    # One budget for every mode in this invocation. A later mode that does not
    # fit yields immediately and resumes on the next cron instead of crossing
    # Vercel's 800s maxDuration.
    deadline = time.monotonic() + _website_ops_step_budget_seconds()
    for mode in modes:
        current_state = get_website_ops_run_state(settings, mode)
        repeated_daily_pulse = (
            mode == "daily"
            and bool(pulse_slot)
            and current_state.get("last_pulse_slot") != pulse_slot
        )
        if (
            not force
            and not repeated_daily_pulse
            and not website_ops_run_is_due(settings, mode, today=run_business_date)
        ):
            results[mode] = {"status": "skipped", "message": "Already completed for this period."}
            continue
        now = datetime.now(ZoneInfo("UTC"))
        write_website_ops_run_state(
            settings,
            mode,
            {
                "mode": mode,
                "status": "running",
                "run_date": run_business_date.isoformat(),
                "trigger": trigger,
                "last_started_at": now.isoformat(),
                "last_error": "",
                "last_pulse_slot": pulse_slot if mode == "daily" else "",
            },
        )
        try:
            result = run_website_ops(
                settings,
                mode=mode,
                deadline=deadline,
                business_date=run_business_date,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Website Ops %s attempt failed.", mode)
            message = str(exc or "Website Ops run did not return a result.")
            write_website_ops_run_state(
                settings,
                mode,
                {
                    "status": "failed",
                    "last_completed_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                    "last_error": message,
                    "attempt_count": "1",
                    "recovery_status": "exhausted",
                },
            )
            send_website_ops_failure_email(settings, mode=mode, error=message)
            results[mode] = {
                "status": "failed",
                "message": message,
                "attempts": 1,
            }
            continue
        completed_at = datetime.now(ZoneInfo("UTC"))
        outcome = dict((getattr(result, "report", None) or {}).get("run_outcome") or {})
        outcome_status = str(outcome.get("status", "") or "").strip()
        production_delta_count = int(outcome.get("production_delta_count", 0) or 0)
        disposition = persisted_run_disposition(
            mode=mode,
            outcome=outcome,
            ok=bool(getattr(result, "ok", True)),
        )
        outcome_updates = {
            "outcome_status": outcome_status,
            "outcome_message": str(outcome.get("summary", "") or ""),
            "expected_output": str(outcome.get("expected_output", "") or ""),
            "actual_output": str(outcome.get("actual_output", "") or ""),
            "production_delta_count": str(production_delta_count),
            "last_stage": str(outcome.get("last_stage", "") or ""),
            "next_operation": str(outcome.get("next_operation", "") or ""),
            "failure_stage": str(outcome.get("failure_stage", "") or ""),
            "attempt_count": "1",
            "recovery_status": str(disposition["recovery_status"]),
        }
        if disposition["failure"]:
            message = str(disposition["last_error"] or result.message or "")
            write_website_ops_run_state(
                settings,
                mode,
                {
                    **outcome_updates,
                    "status": "failed_outcome",
                    "last_completed_at": completed_at.isoformat(),
                    "last_error": message,
                },
            )
            send_website_ops_failure_email(settings, mode=mode, error=message)
            results[mode] = {
                "status": "failed_outcome",
                "message": message,
                "outcome": outcome,
                "attempts": 1,
            }
            continue
        if disposition["status"] == "work_in_progress":
            logger.info(
                "Website Ops %s is in progress with no verified production delta; the next cron resumes it.",
                mode,
            )
        completion = {
            **outcome_updates,
            "status": str(disposition["status"]),
            "last_completed_at": completed_at.isoformat(),
            "last_error": "",
        }
        if disposition["successful_day"]:
            completion["last_successful_date"] = run_business_date.isoformat()
        write_website_ops_run_state(settings, mode, completion)
        results[mode] = {
            "status": str(disposition["status"]),
            "message": result.message,
            "outcome": outcome,
            "attempts": 1,
            "resume": bool(outcome.get("resume_required")),
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

    from sales_support_agent.services.llm_client import start_background_thread

    thread = start_background_thread(worker, name="website-ops-scheduler", daemon=True)
    app.state.website_ops_scheduler_stop = stop_event
    app.state.website_ops_scheduler_thread = thread


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


@router.api_route("/run", methods=["GET", "POST"])
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
    from sales_support_agent.models.database import get_engine

    try:
        engine = get_engine()
    except RuntimeError:
        # Preserve compatibility for isolated/test router mounts. Production
        # always initializes the shared engine and therefore always uses the
        # cross-instance lease below.
        engine = None
    _sweep_expired_website_ops_leases(engine)
    _expire_stale_mode_run_files(request.app.state.settings, local_now)
    if requested_mode == "scheduled" and local_now.hour not in WEBSITE_OPS_PULSE_HOURS:
        return {
            "status": "skipped",
            "message": "Website Ops scheduler is waiting for an 8 AM through 3 PM America/Denver pulse.",
            "local_time": local_now.isoformat(),
        }

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

    lease_finished = False
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
            trigger=_scheduled_trigger(request.method, force=force),
            force=force,
            pulse_slot=pulse_slot,
            business_date=local_now.date(),
        )
        failed = any(item.get("status") == "failed" for item in results.values())
        lease_finished = _finish_website_ops_lease(
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
        if not lease_finished:
            lease_finished = _finish_website_ops_lease(
                engine,
                lease,
                status="failed",
                details={"error": str(exc)},
            )
        raise
    finally:
        if not lease_finished:
            _finish_website_ops_lease(
                engine,
                lease,
                status="failed",
                details=dict(_INTERRUPTED_LEASE_DETAILS),
            )


@router.post("/release-log")
async def record_verified_seo_release(request: Request) -> dict:
    """Persist a verified SEO release without requiring an admin browser session."""

    _require_release_log_key(request)
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="A JSON release payload is required.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="A JSON object is required.")

    site_host = _release_log_host(payload.get("site"))
    if not site_host:
        raise HTTPException(
            status_code=400,
            detail="site must be anatainc.com or anatabuilding.com.",
        )
    release_key = str(payload.get("release_key", "")).strip()
    if not release_key or len(release_key) > 300:
        raise HTTPException(status_code=400, detail="A release_key of 1-300 characters is required.")
    pr_url = str(payload.get("pr_url", "")).strip()
    parsed_pr = urlparse(pr_url)
    if (
        parsed_pr.scheme != "https"
        or parsed_pr.hostname != "github.com"
        or not parsed_pr.path.startswith("/david-anata/")
        or "/pull/" not in parsed_pr.path
    ):
        raise HTTPException(status_code=400, detail="pr_url must be a david-anata GitHub pull request URL.")

    live_urls = _release_log_urls(payload.get("live_urls"), site_host=site_host)
    task_ids = _release_log_values(payload.get("task_ids"), field="task_ids", limit=100)
    deployment_id = str(payload.get("deployment_id", "")).strip()
    summary = str(payload.get("summary", "")).strip()
    if len(deployment_id) > 300 or not summary or len(summary) > 6000:
        raise HTTPException(status_code=400, detail="deployment_id or summary is invalid.")

    automation_key = f"seo-release:{release_key}"
    for existing in load_feedback_records(request.app.state.settings):
        if str(existing.get("automation_key", "")) == automation_key:
            return {
                "status": "duplicate",
                "feedback_id": str(existing.get("feedback_id", "")),
                "release_key": release_key,
            }

    record = save_feedback_record(
        request.app.state.settings,
        {
            "category": "SEO",
            "priority": "Low",
            "page_url": live_urls[0],
            "page_title": f"Verified SEO release — {site_host}",
            "summary": f"Verified SEO release — {site_host}",
            "details": json.dumps(
                {
                    "site": site_host,
                    "pr_url": pr_url,
                    "deployment_id": deployment_id,
                    "live_urls": live_urls,
                    "task_ids": task_ids,
                    "summary": summary,
                },
                indent=2,
                sort_keys=True,
            ),
            "desired_outcome": "Production release verified and recorded.",
            "recommended_fix": "Monitor the released target queries in the next weekly measurement.",
            "reporter_name": "Codex Scheduled SEO",
            "status": "done",
            "automation_key": automation_key,
            "auto_generated": True,
            "source_insight": "Scheduled SEO release",
        },
    )
    logger.info("Recorded verified SEO release site=%s release_key=%s", site_host, release_key)
    return {
        "status": "recorded",
        "feedback_id": str(record.get("feedback_id", "")),
        "release_key": release_key,
    }
