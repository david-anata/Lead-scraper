"""Slack Critical Deal Alerts — post a Block Kit digest of at-risk deals.

A 24-hour KV cooldown prevents spam. Call ``send_critical_deal_alerts``
from the sales router. Returns a result dict describing what was sent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.database import kv_get_json, kv_set_json
from sales_support_agent.models.entities import HubSpotDeal

logger = logging.getLogger(__name__)

ALERT_COOLDOWN_KEY = "sales:slack:alerts:last_sent"
ALERT_COOLDOWN_HOURS = 24


@dataclass
class DealAlert:
    deal_id: str
    deal_name: str
    owner_email: str
    issues: list[str]
    close_date: Optional[datetime]
    amount_cents: int


@dataclass
class AlertBatch:
    alerts: list[DealAlert] = field(default_factory=list)
    by_rep: dict[str, list[DealAlert]] = field(default_factory=dict)
    skipped_cooldown: bool = False


def _days_overdue(cd: Optional[datetime], as_of: datetime) -> int:
    if cd is None:
        return 0
    if cd.tzinfo is None:
        cd = cd.replace(tzinfo=timezone.utc)
    delta = (as_of - cd).days
    return max(0, delta)


def build_alert_batch(
    session: Session,
    *,
    as_of: datetime | None = None,
    stale_days: int = 14,
) -> AlertBatch:
    as_of = as_of or datetime.now(timezone.utc)
    stale_cutoff = as_of - timedelta(days=stale_days)

    deals = session.scalars(
        __import__("sqlalchemy", fromlist=["select"]).select(HubSpotDeal)
        .where(HubSpotDeal.is_closed.is_(False))
    ).all()

    batch = AlertBatch()
    for d in deals:
        issues: list[str] = []

        cd = d.close_date
        if cd is not None:
            if cd.tzinfo is None:
                cd = cd.replace(tzinfo=timezone.utc)
            if cd < as_of:
                days = (as_of - cd).days
                issues.append(f"overdue by {days}d")

        touch = d.last_meaningful_touch_at
        if touch is not None and touch.tzinfo is None:
            touch = touch.replace(tzinfo=timezone.utc)
        if touch is None or touch < stale_cutoff:
            issues.append(f"no touch in {stale_days}+ days")

        if (d.amount_cents or 0) <= 0:
            issues.append("missing amount")

        if not issues:
            continue

        alert = DealAlert(
            deal_id=d.hubspot_deal_id,
            deal_name=d.deal_name or d.hubspot_deal_id,
            owner_email=(d.owner_email or "").strip() or "unassigned",
            issues=issues,
            close_date=d.close_date,
            amount_cents=d.amount_cents or 0,
        )
        batch.alerts.append(alert)
        batch.by_rep.setdefault(alert.owner_email, []).append(alert)

    return batch


def _build_blocks(batch: AlertBatch, as_of: datetime) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":rotating_light: Sales Alert — {len(batch.alerts)} deal(s) need attention",
            },
        },
        {"type": "divider"},
    ]

    for rep_email, alerts in sorted(batch.by_rep.items()):
        rep_label = rep_email.split("@")[0].replace(".", " ").title()
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{rep_label}* (`{rep_email}`) — {len(alerts)} deal(s)",
            },
        })
        for alert in alerts[:5]:  # cap per-rep at 5 to avoid Slack block limit
            issue_str = " • ".join(alert.issues)
            cd_str = ""
            if alert.close_date:
                cd = alert.close_date
                if cd.tzinfo is None:
                    cd = cd.replace(tzinfo=timezone.utc)
                cd_str = f" | Close: {cd.strftime('%b %d')}"
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"> *{alert.deal_name}*{cd_str}\n> _{issue_str}_",
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View"},
                    "url": f"https://agent.anatainc.com/admin/sales/deals/{alert.deal_id}",
                    "action_id": f"view_deal_{alert.deal_id}",
                },
            })
        if len(alerts) > 5:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"… and {len(alerts) - 5} more deals for {rep_label}"}],
            })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"Generated {as_of.strftime('%Y-%m-%d %H:%M UTC')} | <https://agent.anatainc.com/admin/sales/deals|View full board>"},
        ],
    })
    return blocks


def send_critical_deal_alerts(
    session: Session,
    settings: Any,
    *,
    as_of: datetime | None = None,
    force: bool = False,
    stale_days: int = 14,
) -> dict[str, Any]:
    as_of = as_of or datetime.now(timezone.utc)

    if not force:
        last = kv_get_json(ALERT_COOLDOWN_KEY)
        if last:
            last_sent_str = last.get("sent_at") if isinstance(last, dict) else str(last)
            try:
                from datetime import datetime as _dt
                last_sent = _dt.fromisoformat(last_sent_str)
                if last_sent.tzinfo is None:
                    last_sent = last_sent.replace(tzinfo=timezone.utc)
                if (as_of - last_sent).total_seconds() < ALERT_COOLDOWN_HOURS * 3600:
                    return {
                        "sent": False,
                        "skipped": True,
                        "reason": f"cooldown ({ALERT_COOLDOWN_HOURS}h between alerts)",
                        "last_sent": last_sent_str,
                    }
            except Exception:
                pass

    client = SlackClient(settings)
    if not client.is_configured():
        return {"sent": False, "skipped": True, "reason": "Slack not configured"}

    batch = build_alert_batch(session, as_of=as_of, stale_days=stale_days)
    if not batch.alerts:
        return {"sent": False, "skipped": True, "reason": "no critical deals found"}

    blocks = _build_blocks(batch, as_of)
    fallback = f"Sales alert: {len(batch.alerts)} deal(s) need attention. View at https://agent.anatainc.com/admin/sales/deals"

    try:
        client.post_message(text=fallback, blocks=blocks)
    except Exception as exc:
        logger.exception("[slack_alerts] failed to post message")
        return {"sent": False, "error": str(exc), "deal_count": len(batch.alerts)}

    kv_set_json(ALERT_COOLDOWN_KEY, {"sent_at": as_of.isoformat()})
    logger.info("[slack_alerts] posted alert for %d deals", len(batch.alerts))
    return {
        "sent": True,
        "deal_count": len(batch.alerts),
        "rep_count": len(batch.by_rep),
    }
