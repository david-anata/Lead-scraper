"""Authenticated control room and trusted job routes for Content Operations."""

from __future__ import annotations

import secrets
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.auth_deps import require_any_tool
from sales_support_agent.services.content_engine import (
    control_room_data,
    ingest_source_assets,
    render_content_control_room,
    render_run_detail,
)
from sales_support_agent.services.content_automation import run_content_cycle
from sales_support_agent.services.content_automation import stage_daily_brief
from sales_support_agent.services.content_engine import (
    dependency_health,
    record_orchestration_check,
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
    transcript_text: str = Field(default="", max_length=200_000)
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
        "seo_blog",
        "episode_harvest",
        "social_distribution",
        "weekly_retrospective",
        "newsletter_issue",
    ] = "scheduled"
    force: bool = False


class DailyNewsInput(BaseModel):
    """One evidence-led rapid-fire item."""

    title: str = Field(min_length=1, max_length=300)
    what_happened: str = Field(min_length=1, max_length=2000)
    why_it_matters: str = Field(min_length=1, max_length=2000)
    anata_angle: str = Field(min_length=1, max_length=2000)
    talking_point: str = Field(min_length=1, max_length=2000)


class DailyDeepDiveInput(BaseModel):
    """One operator-led deep-dive option."""

    title: str = Field(min_length=1, max_length=300)
    skill: str = Field(min_length=1, max_length=2000)
    common_mistake: str = Field(min_length=1, max_length=2000)
    framework: str = Field(min_length=1, max_length=3000)
    questions: str = Field(min_length=1, max_length=5000)
    keyword: str = Field(default="", max_length=300)


class DailyBriefInput(BaseModel):
    """Trusted research payload for Job 1 staging."""

    date_key: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    theme: str = Field(min_length=1, max_length=500)
    recording_time: str = Field(min_length=1, max_length=255)
    cold_open: str = Field(min_length=1, max_length=5000)
    news_items: list[DailyNewsInput] = Field(min_length=3, max_length=3)
    deep_dives: list[DailyDeepDiveInput] = Field(min_length=1, max_length=3)
    source_urls: list[str] = Field(min_length=1, max_length=30)


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
            "next_daily_run": data["next_daily_run"].isoformat(),
            "source_asset_count": data["source_asset_count"],
            "artifact_count": data["artifact_count"],
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


@router.post("/api/jobs/content/daily-brief")
def content_daily_brief(
    payload: DailyBriefInput,
    request: Request,
) -> dict[str, Any]:
    """Stage validated Job 1 outputs from a trusted MCP or research relay."""

    _require_internal_key(request)
    with session_scope(request.app.state.session_factory) as session:
        source_count = int(
            control_room_data(
                session,
                request.app.state.settings,
            )["source_asset_count"]
        )
        health = dependency_health(
            request.app.state.settings,
            source_asset_count=source_count,
        )
        state_by_key = {item["key"]: item["status"] for item in health}
        blockers = [
            key
            for key in ("drive", "gmail", "slack")
            if state_by_key.get(key) != "ready"
        ]
        run = record_orchestration_check(
            session,
            job_key="daily_brief",
            run_key=f"{payload.date_key}:daily_brief",
            trigger="relay",
            actor="trusted:content-research-relay",
            blockers=blockers,
        )
        result = stage_daily_brief(
            session,
            run=run,
            theme=payload.theme,
            recording_time=payload.recording_time,
            cold_open=payload.cold_open,
            news_items=[item.model_dump() for item in payload.news_items],
            deep_dives=[item.model_dump() for item in payload.deep_dives],
            source_urls=payload.source_urls,
            actor="trusted:content-research-relay",
        )
        return {
            **result,
            "run_id": run.id,
            "blockers": blockers,
        }


@router.post("/api/jobs/content/run")
def content_run(
    payload: ContentRunInput,
    request: Request,
) -> dict[str, Any]:
    """Run a leased, catch-up-aware orchestration preflight in shadow mode."""

    _require_internal_key(request)
    return run_content_cycle(
        request.app.state.session_factory,
        request.app.state.settings,
        mode=payload.mode,
        force=payload.force,
    )
