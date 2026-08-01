"""Idempotent Slack alerts for recoverable public-tool failures."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.entities import AutomationRun

logger = logging.getLogger(__name__)

SALES_CONTROL_ROOM_URL = "https://agent.anatainc.com/admin/sales"
ALERT_MARKER_KEY = "public_tool_slack_alert"
PUBLIC_RUN_TYPES = (
    "marketing_intake",
    "marketing_analysis_intake",
    "fulfillment_rate_sheet",
)


@dataclass(frozen=True)
class PublicToolBlocker:
    run: AutomationRun
    tool: str
    blockers: tuple[str, ...]
    fingerprint: str


def _classify(run: AutomationRun) -> PublicToolBlocker | None:
    metadata = dict(run.metadata_json or {})
    summary = dict(run.summary_json or {})
    run_type = str(run.run_type or "")

    if run_type == "marketing_analysis_intake":
        if metadata.get("tool") != "advertising_audit":
            return None
        tool = "Advertising Audit"
    elif run_type == "marketing_intake":
        tool = "Website Analysis"
    elif run_type == "fulfillment_rate_sheet":
        tool = "Rate Sheet"
    else:
        return None

    # Only notify for unlocked public requests. Values are deliberately used
    # as presence checks only and never copied into the Slack payload.
    if not str(metadata.get("email") or summary.get("public_unlock_email") or "").strip():
        return None

    blockers: list[str] = []
    if run_type == "fulfillment_rate_sheet":
        rate_status = str(summary.get("public_rate_sheet_status") or run.status or "")
        email_status = str(summary.get("public_email_status") or "")
        crm_status = str(summary.get("public_sales_status") or "")
        if rate_status == "failed":
            blockers.append("rate sheet build failed")
        if rate_status == "ready" and email_status == "failed":
            blockers.append("final email failed")
        if rate_status == "ready" and crm_status == "failed":
            blockers.append("CRM handoff failed")
        states = (rate_status, email_status, crm_status)
    else:
        email_status = str(summary.get("email_delivery") or "")
        crm_status = str(summary.get("hubspot_handoff") or "")
        if str(run.status or "") == "failed":
            blockers.append("report build failed")
        if email_status == "failed":
            blockers.append("final email failed")
        if crm_status == "failed":
            blockers.append("CRM handoff failed")
        states = (str(run.status or ""), email_status, crm_status)

    if not blockers:
        return None
    fingerprint = "|".join((run_type, *states, *blockers))
    marker = summary.get(ALERT_MARKER_KEY)
    if isinstance(marker, dict) and marker.get("fingerprint") == fingerprint:
        return None
    return PublicToolBlocker(run=run, tool=tool, blockers=tuple(blockers), fingerprint=fingerprint)


def send_public_tool_failure_alerts(
    session: Session,
    settings: Any,
    *,
    as_of: datetime | None = None,
    query_limit: int = 100,
    alert_limit: int = 10,
) -> dict[str, Any]:
    """Post one PII-free digest and durably mark the exact states reported."""
    client = SlackClient(settings)
    if not client.is_configured():
        return {"sent": False, "skipped": True, "reason": "Slack not configured"}

    runs = session.scalars(
        select(AutomationRun)
        .where(AutomationRun.run_type.in_(PUBLIC_RUN_TYPES))
        .order_by(AutomationRun.id.desc())
        .limit(max(1, min(query_limit, 500)))
    ).all()
    blockers = [item for run in runs if (item := _classify(run)) is not None]
    blockers = blockers[: max(1, min(alert_limit, 20))]
    if not blockers:
        return {"sent": False, "skipped": True, "reason": "no new public-tool failures"}

    lines = [
        f"*{item.tool}* — run `{item.run.id}` — {', '.join(item.blockers)}"
        for item in blockers
    ]
    text = (
        f"Public tool blocker alert: {len(blockers)} run(s) need attention. "
        f"Recover them in {SALES_CONTROL_ROOM_URL}"
    )
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Public tool blockers need attention"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Open Sales Control Room"},
                    "url": SALES_CONTROL_ROOM_URL,
                    "action_id": "open_public_tool_recovery",
                }
            ],
        },
    ]
    try:
        client.post_message(text=text, blocks=blocks)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[public_tool_alerts] failed to post message")
        return {"sent": False, "error": str(exc), "run_count": len(blockers)}

    sent_at = (as_of or datetime.now(timezone.utc)).isoformat()
    for item in blockers:
        item.run.summary_json = {
            **dict(item.run.summary_json or {}),
            ALERT_MARKER_KEY: {"fingerprint": item.fingerprint, "sent_at": sent_at},
        }
    session.flush()
    return {"sent": True, "run_count": len(blockers)}
