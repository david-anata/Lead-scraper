"""Authenticated control room and trusted job routes for Content Operations."""

from __future__ import annotations

import os
import secrets
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sales_support_agent.models.database import session_scope
from sales_support_agent.services.auth_deps import require_any_tool
from sales_support_agent.services.content_engine import (
    content_video_resources,
    control_room_data,
    ingest_source_assets,
    render_content_control_room,
    render_run_detail,
)
from sales_support_agent.services.content_publishing import publish_artifact
from sales_support_agent.services.content_intelligence import (
    record_performance_observation,
)
from sales_support_agent.services.content_automation import run_content_cycle
from sales_support_agent.services.content_automation import stage_daily_brief
from sales_support_agent.services.content_engine import (
    dependency_health,
    record_orchestration_check,
)


router = APIRouter(tags=["content-operations"])
CONTENT_VIEW = require_any_tool("content.view", "content.operate", "content.admin")
CONTENT_OPERATE = require_any_tool("content.operate", "content.admin")


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
    """One normalized owned-source episode payload."""

    provider: Literal["riverside", "youtube"] = "riverside"
    episode_id: str = Field(min_length=1, max_length=255)
    assets: list[SourceAssetInput] = Field(min_length=1, max_length=250)


class ContentRunInput(BaseModel):
    """Trusted scheduler request."""

    mode: Literal[
        "scheduled",
        "daily_brief",
        "seo_blog",
        "episode_harvest",
        "youtube_harvest",
        "social_distribution",
        "personal_distribution",
        "daily_distribution",
        "weekly_retrospective",
        "newsletter_issue",
    ] = "scheduled"
    force: bool = False


class PublishArtifactInput(BaseModel):
    """Deliberate approval for one channel-specific publication."""

    artifact_id: str = Field(min_length=1, max_length=64)
    confirmed: bool = False


class PerformanceObservationInput(BaseModel):
    """Normalized provider analytics for one comparable observation window."""

    publication_id: str = Field(min_length=1, max_length=64)
    platform: Literal[
        "linkedin_personal", "linkedin_company", "youtube", "instagram"
    ]
    format_name: str = Field(default="", max_length=64)
    objective: str = Field(default="", max_length=64)
    window_started_at: str
    window_ended_at: str
    metrics: dict[str, float | None]
    sample_confidence: Literal["low", "medium", "high"] = "low"


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
    cron_secret = os.getenv("CRON_SECRET", "").strip()
    authorization = request.headers.get("Authorization", "").strip()
    if cron_secret and secrets.compare_digest(
        authorization,
        f"Bearer {cron_secret}",
    ):
        return

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
            "video_resource_count": data["video_resource_count"],
            "awaiting_transcript_count": data["awaiting_transcript_count"],
            "artifact_count": data["artifact_count"],
            "publication_count": data["publication_count"],
            "coverage_missing_count": data["coverage_missing_count"],
            "daily_backlog_days": data["daily_backlog_days"],
            "personal_cadence": data["personal_cadence"],
            "run_count": len(data["runs"]),
            "dependencies": data["dependencies"],
        }


@router.post("/api/jobs/content/source-assets")
def content_source_assets(
    payload: SourceBatchInput,
    request: Request,
) -> dict[str, Any]:
    """Ingest normalized owned-source assets from a trusted relay."""

    _require_internal_key(request)
    with session_scope(request.app.state.session_factory) as session:
        result = ingest_source_assets(
            session,
            episode_id=payload.episode_id,
            assets=[item.model_dump() for item in payload.assets],
            actor=f"trusted:{payload.provider}-relay",
            provider=payload.provider,
        )
    return {"status": "ok", **result}


@router.get("/admin/api/content/video-resources")
def video_resources(
    request: Request,
    _user: dict = Depends(CONTENT_VIEW),
) -> dict[str, Any]:
    """Expose transcript-backed resources to the authenticated Codex routine."""

    with session_scope(request.app.state.session_factory) as session:
        items = content_video_resources(session, include_transcript=True)
    return {"count": len(items), "resources": items}


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


@router.post("/api/jobs/content/performance")
def content_performance(
    payload: PerformanceObservationInput,
    request: Request,
) -> dict[str, Any]:
    """Record comparable channel analytics from a trusted provider relay."""

    from datetime import datetime

    _require_internal_key(request)
    try:
        started = datetime.fromisoformat(payload.window_started_at.replace("Z", "+00:00"))
        ended = datetime.fromisoformat(payload.window_ended_at.replace("Z", "+00:00"))
        with session_scope(request.app.state.session_factory) as session:
            row = record_performance_observation(
                session,
                publication_id=payload.publication_id,
                platform=payload.platform,
                format_name=payload.format_name,
                objective=payload.objective,
                window_started_at=started,
                window_ended_at=ended,
                metrics=payload.metrics,
                sample_confidence=payload.sample_confidence,
                actor="trusted:content-analytics-relay",
            )
        return {
            "status": "recorded",
            "observation_id": row.id,
            "selection_score": row.metrics_json.get("selection_score"),
        }
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


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


@router.get("/api/jobs/content/run")
def scheduled_content_run(request: Request) -> dict[str, Any]:
    """Run the due content jobs from Vercel Cron."""

    _require_internal_key(request)
    return run_content_cycle(
        request.app.state.session_factory,
        request.app.state.settings,
        mode="scheduled",
        force=False,
    )


@router.post("/admin/api/content/publish")
def content_publish(
    payload: PublishArtifactInput,
    request: Request,
    user: dict = Depends(CONTENT_OPERATE),
) -> dict[str, Any]:
    """Approve and execute one audited production publication."""

    actor = str(user.get("email") or user.get("id") or "content-operator")
    try:
        with session_scope(request.app.state.session_factory) as session:
            row = publish_artifact(
                session,
                artifact_id=payload.artifact_id,
                actor=actor,
                confirmed=payload.confirmed,
            )
            return {
                "status": row.status,
                "publication_id": row.id,
                "public_url": row.public_url,
                "verified": bool(row.verified_at),
            }
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
