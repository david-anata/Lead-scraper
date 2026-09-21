"""Idempotent ClickUp projection for Anata Building inquiries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select

from sales_support_agent.integrations.clickup import ClickUpClient
from sales_support_agent.models.entities import BuildingAuditEvent, BuildingInquiry


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _task_name(inquiry: BuildingInquiry) -> str:
    return f"Anata Building lead · {inquiry.email.strip().lower()}"


def _state(inquiry: BuildingInquiry) -> dict[str, Any]:
    return dict((inquiry.payload_json or {}).get("_clickup") or {})


def _source_label(inquiry: BuildingInquiry) -> str:
    attribution = dict((inquiry.payload_json or {}).get("_attribution") or {})
    source = str(attribution.get("source") or inquiry.source or "unknown").strip()
    referrer = str(
        attribution.get("source_reference")
        or (inquiry.payload_json or {}).get("referrer")
        or ""
    ).lower()
    if source in {"anata-building", "direct", "unknown"}:
        if any(host in referrer for host in ("chatgpt.com", "openai.com")):
            source = "AI · ChatGPT"
        elif "perplexity.ai" in referrer:
            source = "AI · Perplexity"
        elif any(host in referrer for host in ("gemini.google.com", "copilot.microsoft.com", "claude.ai")):
            source = "AI assistant"
        elif any(host in referrer for host in ("google.", "bing.com", "duckduckgo.com", "yahoo.com")):
            source = "Organic search"
        elif not referrer:
            source = "Direct"
    parts = [source]
    parts.extend(
        value
        for value in (
            str(attribution.get("medium") or "").strip(),
            str(attribution.get("campaign") or "").strip(),
        )
        if value
    )
    return " / ".join(parts)


def _summary(inquiry: BuildingInquiry) -> str:
    payload = dict(inquiry.payload_json or {})
    attribution = dict(payload.get("_attribution") or {})
    lines = [
        f"## {inquiry.name}",
        f"- **Email:** {inquiry.email}",
        f"- **Phone:** {inquiry.phone or 'Not provided'}",
        f"- **Journey:** {inquiry.kind}",
        f"- **Source:** {_source_label(inquiry)}",
        f"- **Source reference:** {inquiry.source_reference or 'Not provided'}",
        f"- **Preferred date:** {inquiry.preferred_date.isoformat() if inquiry.preferred_date else 'Not provided'}",
        f"- **Offering:** {inquiry.offering_id or 'Not selected'}",
        f"- **Agent inquiry ID:** {inquiry.id}",
        f"- **Received:** {inquiry.created_at.isoformat()}",
    ]
    if attribution.get("landing_page"):
        lines.append(f"- **Landing page:** {attribution['landing_page']}")
    details = [
        (str(key).replace("_", " ").strip().title(), str(value).strip())
        for key, value in payload.items()
        if not str(key).startswith("_") and str(value or "").strip()
    ]
    if details:
        lines.extend(["", "### Form details"])
        lines.extend(f"- **{label}:** {value}" for label, value in details)
    lines.extend(
        [
            "",
            "### Follow-up",
            f"Owner: {inquiry.assigned_owner or 'Unassigned'}",
            f"Respond by: {inquiry.response_due_at.isoformat() if inquiry.response_due_at else 'Not set'}",
            "Agent remains the source of truth. Use this ClickUp task to manage outreach.",
        ]
    )
    return "\n".join(lines)


def _record(
    session,
    inquiry: BuildingInquiry,
    *,
    status: str,
    actor: str,
    task_id: str = "",
    task_url: str = "",
    error: str = "",
) -> None:
    payload = dict(inquiry.payload_json or {})
    before = dict(payload.get("_clickup") or {})
    after = {
        "status": status,
        "task_id": task_id or str(before.get("task_id") or ""),
        "task_url": task_url or str(before.get("task_url") or ""),
        "error": error[:1000],
        "attempt_count": int(before.get("attempt_count") or 0) + 1,
        "updated_at": _now().isoformat(),
    }
    payload["_clickup"] = after
    inquiry.payload_json = payload
    inquiry.updated_at = _now()
    session.add(inquiry)
    session.add(
        BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry.id,
            action=f"clickup_projection_{status}",
            actor=actor,
            before_json=before,
            after_json=after,
        )
    )


def _linked_task(session, inquiry: BuildingInquiry) -> tuple[str, str]:
    rows = session.execute(
        select(BuildingInquiry)
        .where(
            func.lower(BuildingInquiry.email) == inquiry.email.strip().lower(),
            BuildingInquiry.id != inquiry.id,
        )
        .order_by(BuildingInquiry.created_at.desc())
    ).scalars().all()
    for row in rows:
        prior = _state(row)
        if str(prior.get("task_id") or "").strip():
            return str(prior["task_id"]), str(prior.get("task_url") or "")
    return "", ""


def _find_task(client: ClickUpClient, list_id: str, name: str) -> dict[str, Any] | None:
    for page in range(20):
        tasks = client.get_tasks(list_id, include_closed=True, page=page)
        for task in tasks:
            if str(task.get("name") or "").strip().casefold() == name.casefold():
                return task
        if len(tasks) < 100:
            break
    return None


def project_building_inquiry_to_clickup(
    *,
    session,
    inquiry: BuildingInquiry,
    client: ClickUpClient,
    list_id: str,
    actor: str,
) -> bool:
    """Create or reuse exactly one ClickUp task for an inquiry email."""

    if str(_state(inquiry).get("task_id") or "").strip():
        return True
    if not client.settings.clickup_api_token or not list_id:
        _record(session, inquiry, status="not_configured", actor=actor)
        return False
    try:
        task_id, task_url = _linked_task(session, inquiry)
        if not task_id:
            existing = _find_task(client, list_id, _task_name(inquiry))
            if existing:
                task_id = str(existing.get("id") or "").strip()
                task_url = str(existing.get("url") or "").strip()
        if task_id:
            client.create_task_comment(
                task_id,
                "New Anata Building form fill linked to this lead.\n\n" + _summary(inquiry),
            )
            _record(
                session,
                inquiry,
                status="linked_existing",
                actor=actor,
                task_id=task_id,
                task_url=task_url,
            )
            return True
        created = client.create_task(
            list_id,
            {
                "name": _task_name(inquiry),
                "description": _summary(inquiry),
            },
        )
        task_id = str(created.get("id") or "").strip()
        if not task_id:
            raise RuntimeError("ClickUp did not return a task ID.")
        _record(
            session,
            inquiry,
            status="created",
            actor=actor,
            task_id=task_id,
            task_url=str(created.get("url") or ""),
        )
        return True
    except Exception as exc:  # noqa: BLE001 - intake must survive a CRM outage
        _record(session, inquiry, status="error", actor=actor, error=str(exc))
        return False


def backfill_building_inquiries_to_clickup(
    session_factory,
    *,
    settings,
    actor: str = "system:building-clickup-backfill",
) -> dict[str, int]:
    """Project all historical unsynced Building inquiries."""

    stats = {"scanned": 0, "projected": 0, "failed": 0}
    if not settings.clickup_api_token or not settings.clickup_list_id:
        return stats
    client = ClickUpClient(settings)
    with session_factory() as session:
        rows = session.execute(
            select(BuildingInquiry).order_by(BuildingInquiry.created_at)
        ).scalars().all()
        for inquiry in rows:
            if str(_state(inquiry).get("task_id") or "").strip():
                continue
            stats["scanned"] += 1
            ok = project_building_inquiry_to_clickup(
                session=session,
                inquiry=inquiry,
                client=client,
                list_id=settings.clickup_list_id,
                actor=actor,
            )
            stats["projected" if ok else "failed"] += 1
            session.commit()
    return stats
