"""Read-only notifications for completed outbound pulls."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_LEAD_OPS_URL = "https://agent.anatainc.com/admin/outbound/lead-ops"


def summary_text(run: dict[str, Any], *, include_link: bool = True, test: bool = False) -> str:
    status = "cut short" if run.get("partial") else "complete"
    prefix = "TEST — " if test else ""
    body = (
        f"{prefix}Outbound pull {run.get('recipe') or 'Unknown'} is {status}.\n"
        f"Scanned: {int(run.get('scanned') or 0):,}\n"
        f"Fit ICP: {int(run.get('matched') or 0):,}\n"
        f"Fresh companies: {int(run.get('fresh') or 0):,}\n"
        f"Already seen: {int(run.get('skipped_seen') or 0):,}\n"
        f"Settings: v{int(run.get('config_version') or 0)}"
    )
    return body + (f"\n\nReview and download: {_LEAD_OPS_URL}" if include_link else "")


def deliver_completed_pull(engine, run: dict[str, Any], *, force: bool = False,
                           test: bool = False) -> dict[str, Any]:
    """Deliver a summary without modifying lead, suppression, or Clay state."""
    from sales_support_agent.config import load_settings
    from sales_support_agent.integrations.slack import SlackClient
    from sales_support_agent.services import outbound_memory
    from sales_support_agent.services.access import notify

    prefs = outbound_memory.load_delivery_settings(engine)
    result: dict[str, Any] = {"email": "skipped", "slack": "skipped", "sent": 0}
    if not force and (not prefs["enabled"] or prefs["frequency"] != "every_pull"):
        result["reason"] = "automatic delivery is off or set to daily"
        return result

    settings = load_settings()
    text = summary_text(run, include_link=prefs.get("content_mode") == "link", test=test)
    if prefs["email_enabled"]:
        recipients = [x.strip() for x in prefs["email_recipients"].replace(";", ",").split(",") if x.strip()]
        sent = 0
        for recipient in recipients:
            ok = notify._send(settings, to_email=recipient,
                            subject=f"Outbound: {run.get('fresh', 0)} fresh companies from {run.get('recipe') or 'pull'}",
                            text=text)
            outbound_memory.record_delivery_attempt(engine, run_id=int(run.get("id") or 0),
                recipe=str(run.get("recipe") or ""), destination="email", target=recipient,
                status="sent" if ok else "failed")
            if ok:
                sent += 1
        result["email"] = "sent" if sent == len(recipients) and recipients else ("partial" if sent else "failed")
        result["sent"] += sent

    if prefs["slack_enabled"]:
        try:
            response = SlackClient(settings).post_message(text=text)
            result["slack"] = "sent" if response.get("ok") else "failed"
            result["sent"] += 1 if response.get("ok") else 0
            outbound_memory.record_delivery_attempt(engine, run_id=int(run.get("id") or 0),
                recipe=str(run.get("recipe") or ""), destination="slack",
                target=str(getattr(settings, "slack_channel_id", "") or "configured channel"),
                status="sent" if response.get("ok") else "failed")
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-delivery] Slack delivery failed")
            result["slack"] = "failed"
            outbound_memory.record_delivery_attempt(engine, run_id=int(run.get("id") or 0),
                recipe=str(run.get("recipe") or ""), destination="slack", status="failed",
                detail="Slack delivery raised an error")
    return result
