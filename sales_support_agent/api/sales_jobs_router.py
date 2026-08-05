"""Internal sales job routes with API-key auth."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.jobs.sales_operator_review import SalesOperatorReviewJob
from sales_support_agent.services.building_holds import expire_building_holds
from sales_support_agent.services.building_lead_follow_up import (
    process_building_lead_follow_up,
)
from sales_support_agent.services.job_lease import claim_scheduled_job, finish_scheduled_job


router = APIRouter(prefix="/api/jobs", tags=["sales-jobs"])


def _enforce_internal_api_key(request: Request, internal_api_key: str | None) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    provided = str(internal_api_key or "").strip()
    if configured and provided != configured:
        raise PermissionError("Invalid internal API key.")


@router.post("/building-leads/run")
async def building_lead_follow_up_job(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> JSONResponse:
    """Escalate overdue, unanswered Building leads without contacting customers."""

    try:
        _enforce_internal_api_key(request, x_internal_api_key)
    except PermissionError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    dry_run = bool(payload.get("dry_run", False))
    lease = None
    if not dry_run:
        engine = request.app.state.session_factory.kw.get("bind")
        lease = claim_scheduled_job(
            engine,
            job_key="building-lead-follow-up",
            run_key=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H"),
            lease_minutes=55,
        )
        if lease is None:
            return JSONResponse(
                {
                    "ok": True,
                    "status": "ok",
                    "message": "Building lead follow-up already processed for this hour.",
                    "details": {"status": "already_processed", "overdue_count": 0},
                }
            )
    try:
        result = process_building_lead_follow_up(
            request.app.state.session_factory,
            settings=request.app.state.settings,
            dry_run=dry_run,
        )
    except Exception as exc:
        if lease is not None:
            finish_scheduled_job(
                request.app.state.session_factory.kw.get("bind"),
                lease,
                status="failed",
                details={"error": str(exc)[:500]},
            )
        raise
    if lease is not None:
        finish_scheduled_job(
            request.app.state.session_factory.kw.get("bind"),
            lease,
            status="succeeded" if result.get("status") != "failed" else "failed",
            details=result,
        )
    return JSONResponse(
        {
            "ok": True,
            "status": "ok",
            "message": "Building lead follow-up completed.",
            "details": result,
        }
    )


@router.post("/building-holds/run")
async def building_hold_expiration_job(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> JSONResponse:
    try:
        _enforce_internal_api_key(request, x_internal_api_key)
    except PermissionError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=401)
    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    result = expire_building_holds(
        request.app.state.session_factory,
        dry_run=bool(payload.get("dry_run", False)),
    )
    return JSONResponse(
        {
            "ok": True,
            "status": "ok",
            "message": "Building hold expiration completed.",
            "details": result,
        }
    )


@router.post("/sales-operator/run")
async def sales_operator_run_job(
    request: Request,
    x_internal_api_key: str | None = Header(default=None),
) -> JSONResponse:
    try:
        _enforce_internal_api_key(request, x_internal_api_key)
    except PermissionError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=401)

    payload = {}
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    try:
        limit = max(1, min(int(payload.get("limit", 25) or 25), 25))
    except (TypeError, ValueError):
        limit = 25
    try:
        max_messages = int(payload["max_messages"]) if payload.get("max_messages") not in (None, "") else None
    except (TypeError, ValueError):
        max_messages = None

    result = SalesOperatorReviewJob(
        request.app.state.settings,
        request.app.state.session_factory,
    ).run(
        dry_run=bool(payload.get("dry_run", False)),
        limit=limit,
        run_hubspot_sync=bool(payload.get("run_hubspot_sync", True)),
        run_mailbox_sync=bool(payload.get("run_mailbox_sync", False)),
        max_messages=max_messages,
        trigger=("scheduled" if str(payload.get("trigger") or "").strip() == "scheduled" else "manual"),
    )
    return JSONResponse(
        {"ok": True, "status": "ok", "message": "Sales operator review completed.", "details": result}
    )
