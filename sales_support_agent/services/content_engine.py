"""Content & Growth Engine operating state and server-rendered control room."""

from __future__ import annotations

import hashlib
import html
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.content import (
    ContentAuditEvent,
    ContentJobRun,
    ContentPublication,
    ContentSourceAsset,
)
from sales_support_agent.services.admin_nav import (
    render_agent_favicon_links,
    render_agent_nav,
    render_agent_nav_styles,
)


PLAYBOOKS: tuple[dict[str, Any], ...] = (
    {
        "channel": "LinkedIn personal",
        "priority": "Primary authority",
        "cadence": "3 strong posts / week",
        "format": "Operator lesson, framework, or informed contrarian take",
        "metrics": "Comments, qualified actions, leads",
        "state": "blocked",
        "dependency": "Production publisher and analytics read",
    },
    {
        "channel": "LinkedIn company",
        "priority": "B2B proof",
        "cadence": "2 posts / week",
        "format": "Useful operating insight with Anata evidence",
        "metrics": "Engagement, qualified visits, leads",
        "state": "blocked",
        "dependency": "Company publisher and analytics read",
    },
    {
        "channel": "YouTube",
        "priority": "Depth and search",
        "cadence": "1 episode + up to 3 Shorts / week",
        "format": "Full episode, story-led education, qualified vertical clip",
        "metrics": "CTR, retention, watch time, conversion",
        "state": "blocked",
        "dependency": "Upload relay and analytics read",
    },
    {
        "channel": "Instagram",
        "priority": "Discovery and relationship",
        "cadence": "Up to 3 Reels or carousels / week",
        "format": "Native Reel, carousel, or visible-action moment",
        "metrics": "Watch %, shares, saves, qualified actions",
        "state": "blocked",
        "dependency": "Publisher and analytics read",
    },
    {
        "channel": "Newsletter",
        "priority": "Owned collection",
        "cadence": "1 useful issue / week",
        "format": "Framework, breakdown, or specific operator lesson",
        "metrics": "Delivery, opens, clicks, replies, unsubscribes",
        "state": "blocked",
        "dependency": "Consent-safe audience and delivery provider",
    },
    {
        "channel": "X",
        "priority": "Experiment",
        "cadence": "Stage up to 5 candidates / week",
        "format": "Sharp opinion, prediction, question, or concise lesson",
        "metrics": "Replies and qualified actions",
        "state": "staged",
        "dependency": "Publishing intentionally disabled",
    },
)

PIPELINE = (
    ("signals", "Signals", "Research demand and audience questions"),
    ("brief", "Brief", "Choose the strongest original Anata angle"),
    ("riverside", "Riverside", "Record and process the source episode"),
    ("harvest", "Harvest", "Ingest transcript, tracks, and candidate clips"),
    ("transform", "Transform", "Create separately for each native playbook"),
    ("quality", "Quality gate", "Verify claims, source lineage, and channel fit"),
    ("publish", "Distribute", "Publish through verified runtime connectors"),
    ("calibrate", "Calibrate", "Measure Start, Stay, Signal, and business impact"),
)


def _configured(*names: str) -> bool:
    return all(bool(os.getenv(name, "").strip()) for name in names)


def _enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_source_url(value: str) -> str:
    """Drop signed query strings and fragments before durable storage."""

    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def dependency_health(settings: Any, *, source_asset_count: int = 0) -> list[dict]:
    """Return safe, non-secret readiness for each content dependency."""

    gmail_ready = bool(
        getattr(settings, "gmail_access_token", "")
        or (
            getattr(settings, "gmail_client_id", "")
            and getattr(settings, "gmail_client_secret", "")
            and getattr(settings, "gmail_refresh_token", "")
        )
    )
    dependencies = [
        {
            "key": "drive",
            "label": "Google Drive",
            "status": "ready"
            if _configured("GOOGLE_SERVICE_ACCOUNT_JSON", "CONTENT_DRIVE_PARENT_ID")
            and _enabled("CONTENT_DRIVE_VERIFIED")
            else "blocked",
            "message": "Briefs and artifacts"
            if _configured("GOOGLE_SERVICE_ACCOUNT_JSON", "CONTENT_DRIVE_PARENT_ID")
            and _enabled("CONTENT_DRIVE_VERIFIED")
            else "Grant and verify service-account access to the content parent.",
        },
        {
            "key": "gmail",
            "label": "Gmail drafts",
            "status": "ready" if gmail_ready else "blocked",
            "message": "Draft-only delivery is configured."
            if gmail_ready
            else "Connect the Gmail draft account. Sending remains disabled.",
        },
        {
            "key": "slack",
            "label": "Slack",
            "status": "ready"
            if bool(getattr(settings, "slack_bot_token", "") and getattr(settings, "slack_channel_id", ""))
            else "blocked",
            "message": "Notification delivery is configured."
            if bool(getattr(settings, "slack_bot_token", "") and getattr(settings, "slack_channel_id", ""))
            else "Configure the bot token and content notification channel.",
        },
        {
            "key": "riverside",
            "label": "Riverside source",
            "status": "ready"
            if _enabled("CONTENT_RIVERSIDE_RELAY_ENABLED") and source_asset_count
            else ("stale" if source_asset_count else "blocked"),
            "message": f"{source_asset_count} normalized source asset(s) available."
            if source_asset_count
            else "Connect a production MCP/API relay and ingest the first episode.",
        },
        {
            "key": "linkedin",
            "label": "LinkedIn",
            "status": "ready"
            if _configured("CONTENT_LINKEDIN_CONNECTOR_URL", "CONTENT_LINKEDIN_CONNECTOR_KEY")
            and _enabled("CONTENT_LINKEDIN_CONNECTOR_VERIFIED")
            else "blocked",
            "message": "Personal and company destinations must verify separately.",
        },
        {
            "key": "youtube",
            "label": "YouTube",
            "status": "ready"
            if _configured("CONTENT_YOUTUBE_CONNECTOR_URL", "CONTENT_YOUTUBE_CONNECTOR_KEY")
            and _enabled("CONTENT_YOUTUBE_CONNECTOR_VERIFIED")
            else "blocked",
            "message": "Upload and analytics-read contracts are required.",
        },
        {
            "key": "instagram",
            "label": "Instagram",
            "status": "ready"
            if _configured("CONTENT_INSTAGRAM_CONNECTOR_URL", "CONTENT_INSTAGRAM_CONNECTOR_KEY")
            and _enabled("CONTENT_INSTAGRAM_CONNECTOR_VERIFIED")
            else "blocked",
            "message": "Publishing and analytics-read contracts are required.",
        },
        {
            "key": "newsletter",
            "label": "Newsletter",
            "status": "ready"
            if _configured("CONTENT_NEWSLETTER_PROVIDER", "CONTENT_NEWSLETTER_AUDIENCE_ID")
            else "blocked",
            "message": "A consent-safe audience and unsubscribe contract are required.",
        },
        {
            "key": "visuals",
            "label": "Selective visuals",
            "status": "ready" if _configured("GEMINI_API_KEY") else "blocked",
            "message": "Gemini is optional. Authentic Riverside media remains the default.",
        },
        {
            "key": "x",
            "label": "X",
            "status": "staged",
            "message": "Staging only. Automatic publishing is intentionally disabled.",
        },
    ]
    return dependencies


def ingest_source_assets(
    session: Session,
    *,
    episode_id: str,
    assets: list[dict[str, Any]],
    actor: str,
) -> dict[str, int]:
    """Idempotently persist normalized source assets from a trusted relay."""

    created = 0
    existing = 0
    for raw in assets:
        external_id = str(raw.get("asset_id") or "").strip()
        asset_type = str(raw.get("asset_type") or "").strip().lower()
        if not external_id or asset_type not in {
            "video",
            "audio",
            "transcript",
            "clip",
            "chapter",
            "speaker_track",
        }:
            continue
        row = session.execute(
            select(ContentSourceAsset).where(
                ContentSourceAsset.provider == "riverside",
                ContentSourceAsset.external_asset_id == external_id,
            )
        ).scalar_one_or_none()
        if row:
            existing += 1
            continue
        fingerprint = hashlib.sha256(
            f"riverside:{episode_id}:{external_id}".encode("utf-8")
        ).hexdigest()
        row = ContentSourceAsset(
            id=uuid4().hex,
            provider="riverside",
            episode_external_id=episode_id,
            external_asset_id=external_id,
            asset_type=asset_type,
            processing_status=str(raw.get("status") or "ready").strip().lower(),
            title=str(raw.get("title") or "").strip()[:500],
            speaker=str(raw.get("speaker") or "").strip()[:255],
            transcript_start_ms=raw.get("transcript_start_ms"),
            transcript_end_ms=raw.get("transcript_end_ms"),
            source_url=_safe_source_url(str(raw.get("source_url") or "")),
            source_fingerprint=fingerprint,
            metadata_json={
                key: value
                for key, value in dict(raw.get("metadata") or {}).items()
                if key
                in {"duration_ms", "aspect_ratio", "width", "height", "language"}
            },
        )
        session.add(row)
        session.add(
            ContentAuditEvent(
                id=uuid4().hex,
                actor_type="connector",
                actor_id=actor[:255],
                event_type="source_asset_ingested",
                object_type="content_source_asset",
                object_id=row.id,
                details_json={
                    "episode_id": episode_id,
                    "asset_type": asset_type,
                    "source_fingerprint": fingerprint,
                },
            )
        )
        created += 1
    session.commit()
    return {"created": created, "existing": existing}


def record_orchestration_check(
    session: Session,
    *,
    job_key: str,
    run_key: str,
    trigger: str,
    actor: str,
    blockers: list[str],
) -> ContentJobRun:
    """Create one durable run check without starting unconfigured writes."""

    row = session.execute(
        select(ContentJobRun).where(
            ContentJobRun.job_key == job_key,
            ContentJobRun.run_key == run_key,
        )
    ).scalar_one_or_none()
    if row:
        return row
    now = datetime.now(timezone.utc)
    status = "blocked" if blockers else "ready"
    row = ContentJobRun(
        id=uuid4().hex,
        job_key=job_key,
        run_key=run_key,
        trigger=trigger,
        status=status,
        scheduled_for=now,
        started_at=now,
        completed_at=now,
        attempt_count=1,
        idempotency_key=hashlib.sha256(
            f"{job_key}:{run_key}".encode("utf-8")
        ).hexdigest(),
        safe_error_code="dependencies_blocked" if blockers else "",
        safe_error_message=(
            "Connect the required production dependencies before this job can run."
            if blockers
            else ""
        ),
        summary_json={"blockers": blockers},
        created_by=actor,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.add(
        ContentAuditEvent(
            id=uuid4().hex,
            run_id=row.id,
            actor_type="scheduler" if trigger == "scheduled" else "operator",
            actor_id=actor,
            event_type="orchestration_preflight_completed",
            object_type="content_job_run",
            object_id=row.id,
            details_json={"status": status, "blockers": blockers},
        )
    )
    session.commit()
    return row


def _status_badge(status: str) -> str:
    normalized = status.replace("_", " ").strip().lower() or "blocked"
    css = {
        "ready": "ready",
        "delivered": "delivered",
        "confirmed": "confirmed",
        "running": "running",
        "queued": "queued",
        "stale": "stale",
        "staged": "review",
        "needs review": "review",
        "failed": "failed",
        "blocked": "blocked",
    }.get(normalized, "blocked")
    return f'<span class="app-status app-status--{css}">{html.escape(normalized.title())}</span>'


def _format_time(value: Optional[datetime]) -> str:
    if value is None:
        return "Not started"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%b %d, %Y %H:%M UTC")


def control_room_data(session: Session, settings: Any) -> dict[str, Any]:
    """Build a single safe snapshot for the page and status endpoint."""

    source_count = int(
        session.scalar(select(func.count()).select_from(ContentSourceAsset)) or 0
    )
    publication_count = int(
        session.scalar(select(func.count()).select_from(ContentPublication)) or 0
    )
    runs = list(
        session.execute(
            select(ContentJobRun)
            .order_by(ContentJobRun.created_at.desc())
            .limit(50)
        ).scalars()
    )
    deps = dependency_health(settings, source_asset_count=source_count)
    blockers = [item for item in deps if item["status"] == "blocked"]
    return {
        "generated_at": datetime.now(timezone.utc),
        "source_asset_count": source_count,
        "publication_count": publication_count,
        "runs": runs,
        "dependencies": deps,
        "blockers": blockers,
        "overall_status": "blocked" if blockers else "ready",
    }


def render_content_control_room(
    session: Session,
    settings: Any,
    *,
    user: dict,
) -> str:
    """Render the canonical authenticated Content Operations control room."""

    data = control_room_data(session, settings)
    runs = data["runs"]
    run_rows = "".join(
        f"""
        <tr>
          <td><a href="/admin/content/runs/{html.escape(row.id)}">{html.escape(row.job_key.replace('_', ' ').title())}</a></td>
          <td>{_status_badge(row.status)}</td>
          <td>{html.escape(row.trigger.title())}</td>
          <td>{_format_time(row.started_at)}</td>
          <td>{html.escape(row.safe_error_message or 'Preflight complete.')}</td>
        </tr>
        """
        for row in runs
    )
    if not run_rows:
        run_workspace = """
        <div class="app-state-panel">
          <h2>No content runs yet</h2>
          <p>The engine is ready to record preflight and ingestion activity. No external publishing is enabled.</p>
        </div>
        """
    else:
        run_workspace = f"""
        <table class="app-table app-table--sticky">
          <thead><tr><th>Job</th><th>State</th><th>Trigger</th><th>Started</th><th>Next action</th></tr></thead>
          <tbody>{run_rows}</tbody>
        </table>
        """

    pipeline = "".join(
        f"""
        <li>
          <span class="content-step__index">{index}</span>
          <div><strong>{html.escape(label)}</strong><span>{html.escape(note)}</span></div>
        </li>
        """
        for index, (_, label, note) in enumerate(PIPELINE, start=1)
    )
    dependencies = "".join(
        f"""
        <tr>
          <td><strong>{html.escape(item['label'])}</strong></td>
          <td>{_status_badge(item['status'])}</td>
          <td>{html.escape(item['message'])}</td>
        </tr>
        """
        for item in data["dependencies"]
    )
    playbooks = "".join(
        f"""
        <tr>
          <td><strong>{html.escape(item['channel'])}</strong><br><span class="content-muted">{html.escape(item['priority'])}</span></td>
          <td>{html.escape(item['cadence'])}</td>
          <td>{html.escape(item['format'])}</td>
          <td>{html.escape(item['metrics'])}</td>
          <td>{_status_badge(item['state'])}</td>
        </tr>
        """
        for item in PLAYBOOKS
    )
    overall_copy = (
        f"{len(data['blockers'])} production dependencies need configuration."
        if data["blockers"]
        else "All configured dependencies passed their latest readiness check."
    )
    nav = render_agent_nav(
        "content",
        permissions=set(user.get("permissions") or ()),
        is_superadmin=bool(user.get("is_superadmin")),
        user=user,
        include_content_target=False,
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Content Control Room · Anata Agent</title>
  {render_agent_favicon_links()}
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700;800&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="/static/admin.css?v=3">
  <style>{render_agent_nav_styles()}</style>
</head>
<body>
  {nav}
  <main id="agent-main-content" class="app-container app-page content-page" tabindex="-1">
    <header class="app-page-header">
      <div>
        <p class="content-eyebrow">Content Operations</p>
        <h1>Content Control Room</h1>
        <p>Plan, produce, publish, and verify Anata content from one operating view.</p>
        <p class="content-freshness">State refreshed {_format_time(data['generated_at'])}. Schedule timezone: America/Denver.</p>
      </div>
      <div class="app-page-actions">
        <a class="admin-btn admin-btn--secondary" href="#dependencies">Resolve blockers</a>
      </div>
    </header>

    <section class="content-readiness content-readiness--{html.escape(data['overall_status'])}" aria-labelledby="readiness-title">
      <div>
        <p class="content-eyebrow">Operating state</p>
        <h2 id="readiness-title">{html.escape(data['overall_status'].title())}</h2>
        <p>{html.escape(overall_copy)}</p>
      </div>
      {_status_badge(data['overall_status'])}
    </section>

    <section class="app-metric-strip" aria-label="Content engine summary">
      <div class="app-metric"><div class="app-metric__value">{data['source_asset_count']}</div><div class="app-metric__label">Riverside source assets</div></div>
      <div class="app-metric"><div class="app-metric__value">{len(runs)}</div><div class="app-metric__label">Recorded runs</div></div>
      <div class="app-metric"><div class="app-metric__value">{data['publication_count']}</div><div class="app-metric__label">Verified publications</div></div>
      <div class="app-metric"><div class="app-metric__value">{len(data['blockers'])}</div><div class="app-metric__label">Dependencies blocked</div></div>
    </section>

    <section class="content-section" aria-labelledby="pipeline-title">
      <div class="content-section__head"><div><p class="content-eyebrow">Automation</p><h2 id="pipeline-title">Riverside-to-growth production line</h2></div><p>Research → creation → native distribution → learning</p></div>
      <ol class="content-pipeline">{pipeline}</ol>
    </section>

    <section class="content-section" aria-labelledby="runs-title">
      <div class="content-section__head"><div><p class="content-eyebrow">Execution ledger</p><h2 id="runs-title">Recent runs</h2></div><p>{len(runs)} result(s)</p></div>
      <div class="app-data-workspace">{run_workspace}</div>
    </section>

    <section id="dependencies" class="content-section" aria-labelledby="dependencies-title">
      <div class="content-section__head"><div><p class="content-eyebrow">Production truth</p><h2 id="dependencies-title">Connection readiness</h2></div><p>Developer-only MCP access is not counted as production-ready.</p></div>
      <div class="app-data-workspace"><table class="app-table"><thead><tr><th>Dependency</th><th>State</th><th>Required action</th></tr></thead><tbody>{dependencies}</tbody></table></div>
    </section>

    <section class="content-section" aria-labelledby="playbooks-title">
      <div class="content-section__head"><div><p class="content-eyebrow">Channel</p><h2 id="playbooks-title">Native publishing playbooks</h2></div><p>Cadence is a controlled experiment, not a quota.</p></div>
      <div class="app-data-workspace"><table class="app-table content-playbook-table"><thead><tr><th>Channel</th><th>Cadence</th><th>Native format</th><th>Calibration</th><th>State</th></tr></thead><tbody>{playbooks}</tbody></table></div>
    </section>

    <section class="content-six-cs" aria-labelledby="six-cs-title">
      <div><p class="content-eyebrow">Strategy system</p><h2 id="six-cs-title">The Six C's</h2></div>
      <dl>
        <div><dt>Channel</dt><dd>Create natively for the destination.</dd></div>
        <div><dt>Credibility</dt><dd>Use earned experience and visible action.</dd></div>
        <div><dt>Category</dt><dd>Reinforce what Anata should be known for.</dd></div>
        <div><dt>Content</dt><dd>Use demand evidence and original operator substance.</dd></div>
        <div><dt>Calibration</dt><dd>Learn from Start, Stay, Signal, and business impact.</dd></div>
        <div><dt>Collection</dt><dd>Move qualified attention toward an owned relationship.</dd></div>
      </dl>
    </section>
  </main>
</body>
</html>"""


def render_run_detail(session: Session, run_id: str, *, user: dict) -> str | None:
    """Render one safe run record and its audit history."""

    row = session.get(ContentJobRun, run_id)
    if row is None:
        return None
    events = list(
        session.execute(
            select(ContentAuditEvent)
            .where(ContentAuditEvent.run_id == run_id)
            .order_by(ContentAuditEvent.created_at.desc())
        ).scalars()
    )
    event_rows = "".join(
        f"<tr><td>{_format_time(event.created_at)}</td><td>{html.escape(event.event_type.replace('_', ' ').title())}</td><td>{html.escape(event.actor_id)}</td></tr>"
        for event in events
    ) or "<tr><td colspan='3'>No audit events recorded.</td></tr>"
    nav = render_agent_nav(
        "content",
        permissions=set(user.get("permissions") or ()),
        is_superadmin=bool(user.get("is_superadmin")),
        user=user,
        include_content_target=False,
    )
    blockers = list((row.summary_json or {}).get("blockers") or [])
    blocker_items = "".join(f"<li>{html.escape(str(item))}</li>" for item in blockers)
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Content run · Anata Agent</title>{render_agent_favicon_links()}<link rel="stylesheet" href="/static/admin.css?v=3"><style>{render_agent_nav_styles()}</style></head><body>{nav}<main id="agent-main-content" class="app-container app-page content-page" tabindex="-1"><p><a href="/admin/content">← Content Control Room</a></p><header class="app-page-header"><div><p class="content-eyebrow">Content run</p><h1>{html.escape(row.job_key.replace('_', ' ').title())}</h1><p>{html.escape(row.run_key)}</p></div>{_status_badge(row.status)}</header><section class="content-section"><h2>Run evidence</h2><dl class="content-detail-grid"><div><dt>Trigger</dt><dd>{html.escape(row.trigger.title())}</dd></div><div><dt>Started</dt><dd>{_format_time(row.started_at)}</dd></div><div><dt>Attempts</dt><dd>{row.attempt_count}</dd></div><div><dt>Idempotency</dt><dd>{html.escape(row.idempotency_key[:16])}…</dd></div></dl>{f'<h3>Blocking dependencies</h3><ul>{blocker_items}</ul>' if blockers else '<p>Preflight found no blocking dependency.</p>'}</section><section class="content-section"><h2>Audit history</h2><div class="app-data-workspace"><table class="app-table"><thead><tr><th>Time</th><th>Event</th><th>Actor</th></tr></thead><tbody>{event_rows}</tbody></table></div></section></main></body></html>"""
