"""Authenticated control room and trusted job routes for Content Operations."""

from __future__ import annotations

import secrets
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.auth_deps import require_any_tool
from sales_support_agent.services.content_engine import (
    control_room_data,
    dependency_health,
    ingest_source_assets,
    record_orchestration_check,
    render_content_control_room,
    render_run_detail,
)


router = APIRouter(tags=["content-operations"])
CONTENT_VIEW = require_any_tool("content.view", "content.operate", "content.admin")


class SourceAssetInput(BaseModel):
    """Normalized source asset accepted from a trusted Riverside relay."""

    asset_id: str = Field(min_length=1, max_length=255)
    asset_type: Literal[
        "video",
        "audio",
        "transcript",
        "clip",
        "chapter",
        "speaker_track",
    ]
    status: str = Field(default="ready", max_length=32)
    title: str = Field(default="", max_length=500)
    speaker: str = Field(default="", max_length=255)
    transcript_start_ms: int | None = Field(default=None, ge=0)
    transcript_end_ms: int | None = Field(default=None, ge=0)
    source_url: str = Field(default="", max_length=4000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceBatchInput(BaseModel):
    """One normalized Riverside episode payload."""

    episode_id: str = Field(min_length=1, max_length=255)
    assets: list[SourceAssetInput] = Field(min_length=1, max_length=250)


class ContentRunInput(BaseModel):
    """Trusted scheduler request."""

    mode: Literal[
        "scheduled",
        "daily_brief",
        "episode_harvest",
        "social_distribution",
        "weekly_retrospective",
    ] = "scheduled"
    force: bool = False


def _require_internal_key(request: Request) -> None:
    expected = str(
        getattr(request.app.state.settings, "internal_api_key", "") or ""
    ).strip()
    supplied = request.headers.get("X-Internal-Api-Key", "").strip()
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid internal API key.")


@router.get("/admin/content", response_class=HTMLResponse)
def content_control_room(
    request: Request,
    user: dict = Depends(CONTENT_VIEW),
) -> HTMLResponse:
    """Render the canonical Content Operations control room."""

    with session_scope(request.app.state.session_factory) as session:
        page = render_content_control_room(
            session,
            request.app.state.settings,
            user=user,
        )
    return HTMLResponse(page)


@router.get("/admin/content/runs/{run_id}", response_class=HTMLResponse)
def content_run_detail(
    run_id: str,
    request: Request,
    user: dict = Depends(CONTENT_VIEW),
) -> HTMLResponse:
    """Render one content run with safe audit evidence."""

    with session_scope(request.app.state.session_factory) as session:
        page = render_run_detail(session, run_id, user=user)
    if page is None:
        raise HTTPException(status_code=404, detail="Content run not found.")
    return HTMLResponse(page)


@router.get("/admin/api/content/status")
def content_status(
    request: Request,
    _user: dict = Depends(CONTENT_VIEW),
) -> dict[str, Any]:
    """Return a safe control-room snapshot without credentials."""

    with session_scope(request.app.state.session_factory) as session:
        data = control_room_data(session, request.app.state.settings)
        return {
            "status": data["overall_status"],
            "generated_at": data["generated_at"].isoformat(),
            "source_asset_count": data["source_asset_count"],
            "publication_count": data["publication_count"],
            "run_count": len(data["runs"]),
            "dependencies": data["dependencies"],
        }


@router.post("/api/jobs/content/source-assets")
def content_source_assets(
    payload: SourceBatchInput,
    request: Request,
) -> dict[str, Any]:
    """Ingest normalized Riverside assets from a trusted MCP/API relay."""

    _require_internal_key(request)
    with session_scope(request.app.state.session_factory) as session:
        result = ingest_source_assets(
            session,
            episode_id=payload.episode_id,
            assets=[item.model_dump() for item in payload.assets],
            actor="trusted:riverside-relay",
        )
    return {"status": "ok", **result}


@router.post("/api/jobs/content/run")
def content_run(
    payload: ContentRunInput,
    request: Request,
) -> dict[str, Any]:
    """Run a safe orchestration preflight and persist its truthful state."""

    _require_internal_key(request)
    local_now = datetime.now(ZoneInfo("America/Denver"))
    if payload.mode == "scheduled" and local_now.hour != 6:
        return {
            "status": "skipped",
            "message": "Content scheduler is waiting for 6:00 AM America/Denver.",
            "local_time": local_now.isoformat(),
        }

    job_keys = (
        ["daily_brief", "weekly_retrospective"]
        if payload.mode == "scheduled" and local_now.weekday() == 0
        else (
            ["daily_brief"]
            if payload.mode == "scheduled"
            else [payload.mode]
        )
    )
    results: dict[str, dict[str, Any]] = {}
    with session_scope(request.app.state.session_factory) as session:
        source_count = int(control_room_data(session, request.app.state.settings)["source_asset_count"])
        health = dependency_health(
            request.app.state.settings,
            source_asset_count=source_count,
        )
        state_by_key = {item["key"]: item["status"] for item in health}
        requirements = {
            "daily_brief": ("drive", "gmail", "slack"),
            "episode_harvest": ("riverside", "drive"),
            "social_distribution": (
                "riverside",
                "linkedin",
                "youtube",
                "instagram",
            ),
            "weekly_retrospective": ("linkedin", "youtube", "instagram", "drive"),
        }
        for job_key in job_keys:
            blockers = [
                key
                for key in requirements[job_key]
                if state_by_key.get(key) != "ready"
            ]
            run_key = (
                f"{local_now.date().isoformat()}:{job_key}"
                if not payload.force
                else f"{local_now.isoformat()}:{job_key}"
            )
            row = record_orchestration_check(
                session,
                job_key=job_key,
                run_key=run_key,
                trigger="scheduled" if payload.mode == "scheduled" else "internal",
                actor="job:content-orchestrator",
                blockers=blockers,
            )
            results[job_key] = {
                "run_id": row.id,
                "status": row.status,
                "blockers": list((row.summary_json or {}).get("blockers") or []),
            }
    return {
        "status": (
            "blocked"
            if any(result["status"] == "blocked" for result in results.values())
            else "ready"
        ),
        "details": results,
    }
