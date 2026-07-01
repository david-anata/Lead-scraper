"""Internal sales job routes with API-key auth."""

from __future__ import annotations

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from sales_support_agent.jobs.sales_operator_review import SalesOperatorReviewJob


router = APIRouter(prefix="/api/jobs", tags=["sales-jobs"])


def _enforce_internal_api_key(request: Request, internal_api_key: str | None) -> None:
    configured = str(getattr(request.app.state.settings, "internal_api_key", "") or "").strip()
    provided = str(internal_api_key or "").strip()
    if configured and provided != configured:
        raise PermissionError("Invalid internal API key.")


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
