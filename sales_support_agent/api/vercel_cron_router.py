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
from sqlalchemy import text

from sales_support_agent.api.content_router import ContentRunInput, content_run
from sales_support_agent.api.building_booking_router import (
    CommunicationRunInput,
    run_booking_communications,
)
from sales_support_agent.api.building_crm_router import (
    ScheduledRunInput,
    run_scheduled_campaigns,
)
from sales_support_agent.api.hr_jobs_router import hr_reminders_run
from sales_support_agent.api.router import (
    run_daily_digest_job,
    run_gmail_sync_job,
    run_stale_lead_job,
)
from sales_support_agent.api.sales_jobs_router import sales_operator_run_job
from sales_support_agent.api.sales_jobs_router import (
    building_hold_expiration_job,
    building_lead_follow_up_job,
)
from sales_support_agent.api.website_ops_jobs_router import _run_embedded_pulse
from sales_support_agent.models.schemas import (
    DailyDigestRunRequest,
    GmailSyncRequest,
    StaleLeadRunRequest,
)
from sales_support_agent.services.job_lease import (
    claim_scheduled_job,
    finish_scheduled_job,
)
from sales_support_agent.services.durable_tasks import drain_durable_tasks
from sales_support_agent.services.durable_tasks import run_durable_recovery_probe
from sales_support_agent.services.schedule_shadow import (
    run_schedule_shadow_matrix,
    shadow_schedule_names,
)


router = APIRouter(prefix="/api/vercel-cron", tags=["vercel-cron"])
_DENVER = ZoneInfo("America/Denver")
_WRITE_SCHEDULES = (
    "website-ops",
    "content",
    "stale-leads",
    "gmail-sync",
    "daily-digest",
    "durable-tasks",
    "sales-operator",
    "hr-reminders",
    "building-operations",
    "daily-lead-build",
    "outbound-morning",
)


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


@router.get("/synthetic-health")
def synthetic_health_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Read-only service/database journey that remains safe before cutover."""

    _require_vercel_cron(authorization)
    checks = {
        "application_ready": bool(getattr(request.app.state, "ready", False)),
        "database": False,
    }
    queue = {"queued": 0, "failed": 0, "running": 0}
    try:
        with request.app.state.session_factory() as session:
            session.execute(text("SELECT 1"))
            checks["database"] = True
            try:
                rows = session.execute(
                    text(
                        "SELECT status, COUNT(*) FROM durable_task_queue "
                        "WHERE status IN ('queued', 'failed', 'running') GROUP BY status"
                    )
                ).all()
                for status, count in rows:
                    queue[str(status)] = int(count)
            except Exception:
                session.rollback()
                queue["available"] = False
            else:
                queue["available"] = True
    except Exception as exc:  # No credential or connection detail leaves the service.
        return JSONResponse(
            {
                "status": "failed",
                "checks": checks,
                "queue": queue,
                "reason": "database_unavailable",
                "error_type": type(exc).__name__,
            },
            status_code=503,
        )
    healthy = all(checks.values())
    return JSONResponse(
        {
            "status": "passed" if healthy else "degraded",
            "checks": checks,
            "queue": queue,
            "external_writes": False,
            "commit": getattr(request.app.state, "render_git_commit", "unknown"),
        },
        status_code=200 if healthy else 503,
    )


@router.get("/preflight")
def cron_preflight(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Prove the scheduler boundary is ready without running a scheduled job."""

    _require_vercel_cron(authorization)
    database_ready = False
    durable_queue_ready = False
    try:
        with request.app.state.session_factory() as session:
            session.execute(text("SELECT 1"))
            database_ready = True
            try:
                session.execute(text("SELECT 1 FROM durable_task_queue LIMIT 1"))
            except Exception:
                session.rollback()
            else:
                durable_queue_ready = True
    except Exception:
        pass
    checks = {
        "application_ready": bool(getattr(request.app.state, "ready", False)),
        "database_ready": database_ready,
        "durable_queue_ready": durable_queue_ready,
        "writes_disabled": not _cron_writes_enabled(),
    }
    passed = all(checks.values())
    return JSONResponse(
        {
            "status": "passed" if passed else "failed",
            "checks": checks,
            "write_schedules": list(_WRITE_SCHEDULES),
            "external_writes": False,
            "message": (
                "Scheduler prerequisites are ready; write schedules remain disabled."
                if passed
                else "One or more scheduler prerequisites are not ready."
            ),
        },
        status_code=200 if passed else 503,
    )


@router.get("/durable-recovery-probe")
def durable_recovery_probe(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Run a staging-only queue recovery proof with no external side effect."""

    _require_vercel_cron(authorization)
    is_staging = os.getenv("VERCEL_STAGING", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not is_staging:
        raise HTTPException(status_code=404, detail="Recovery probe is staging-only.")
    if _cron_writes_enabled():
        raise HTTPException(
            status_code=409,
            detail="Disable scheduled writes before running the recovery probe.",
        )
    correlation_id = f"vercel-{datetime.now(tz=_DENVER).strftime('%Y%m%dT%H%M%S%f')}"
    result = run_durable_recovery_probe(
        request.app.state.session_factory.kw.get("bind"),
        correlation_id=correlation_id,
    )
    return JSONResponse(result, status_code=200 if result["status"] == "passed" else 503)


@router.get("/shadow-preflight")
def schedule_shadow_preflight(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Record staging shadow receipts for every write schedule without providers."""

    _require_vercel_cron(authorization)
    is_staging = os.getenv("VERCEL_STAGING", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not is_staging:
        raise HTTPException(status_code=404, detail="Shadow preflight is staging-only.")
    if _cron_writes_enabled():
        raise HTTPException(
            status_code=409,
            detail="Disable scheduled writes before running shadow preflight.",
        )
    correlation_id = f"vercel-shadow-{datetime.now(tz=_DENVER).strftime('%Y%m%dT%H%M%S%f')}"
    result = run_schedule_shadow_matrix(
        request.app.state.session_factory.kw.get("bind"),
        environment=os.environ,
        correlation_id=correlation_id,
    )
    result["write_schedules"] = list(shadow_schedule_names())
    return JSONResponse(result, status_code=200 if result["status"] == "passed" else 503)


@router.get("/website-ops")
def website_ops_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    if response := _authorize(authorization):
        return response
    return _run_embedded_pulse(request.app.state.settings, datetime.now(_DENVER))


@router.get("/daily-lead-build")
def daily_lead_build_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Run the original weekday lead build with Render's exact input contract."""

    if response := _authorize(authorization):
        return response
    import main as lead_builder

    payload = lead_builder.ICPBuildRequest(
        date=datetime.now(_DENVER).date().isoformat(),
        max_domains=150,
    )
    result = lead_builder.execute_lead_build(
        payload,
        scheduler_source="vercel_cron",
    )
    return {
        "status": "succeeded",
        "date": payload.date,
        "max_domains": payload.max_domains,
        "domains_scanned": result.raw_scanned,
        "accepted_lead_target": result.accepted_lead_target,
        "personal_contacts_found": result.successful_contacts,
    }


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


@router.get("/durable-tasks")
def durable_tasks_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Repair request-owned work that did not finish in its first function."""

    if response := _authorize(authorization):
        return response
    result = drain_durable_tasks(request.app, limit=5)
    return {"status": "succeeded", **result}


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


@router.get("/building-operations")
async def building_operations_cron(
    request: Request,
    authorization: str | None = Header(default=None),
):
    """Run the four Render Building steps from one Vercel invocation.

    Each underlying service retains its own transaction and audit contract.
    The global cutover switch prevents this adapter from running while Render
    remains authoritative.
    """

    if response := _authorize(authorization):
        return response
    internal_key = _internal_key(request)
    engine = request.app.state.session_factory.kw.get("bind")
    local_now = datetime.now(_DENVER)
    lease = claim_scheduled_job(
        engine,
        job_key="vercel-building-operations",
        run_key=local_now.strftime("%Y-%m-%dT%H"),
        lease_minutes=55,
    )
    if lease is None:
        return {"status": "skipped", "message": "Building operations already ran this hour."}
    try:
        holds = await building_hold_expiration_job(request, internal_key)
        leads = await building_lead_follow_up_job(request, internal_key)
        campaigns = run_scheduled_campaigns(
            ScheduledRunInput(
                dry_run=False,
                max_campaigns=10,
                actor="job:building-campaign-scheduler",
            ),
            request,
            internal_key,
        )
        communications = run_booking_communications(
            CommunicationRunInput(
                execute=True,
                actor="job:building-event-communications",
            ),
            request,
            internal_key,
        )
        steps = {
            "holds": holds.body.decode("utf-8") if hasattr(holds, "body") else holds,
            "leads": leads.body.decode("utf-8") if hasattr(leads, "body") else leads,
            "campaigns": campaigns,
            "communications": communications,
        }
        finish_scheduled_job(engine, lease, status="succeeded", details=steps)
        return {"status": "succeeded", "steps": steps}
    except Exception as exc:
        finish_scheduled_job(
            engine,
            lease,
            status="failed",
            details={"error": str(exc)[:500]},
        )
        raise
