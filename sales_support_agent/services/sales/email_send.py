"""Send a follow-up email via Gmail and log it to HubSpot CRM.

Phase 5 of the Sales Operational Director. Flow:
  1. Draft page → user edits subject/body → clicks "Send via Anata"
  2. Confirmation preview page (no send yet)
  3. Confirm → gmail.send_message() → hubspot.log_email_engagement() (best-effort)
  4. Redirect to deal detail with "sent" flash; HubSpot timeline updates on next sync.

HubSpot engagement logging is deliberately best-effort: if it fails the email
was still sent, and the rep can manually log it in HubSpot. We never silently
retry or queue — just log a warning and return ok=True.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sales_support_agent.integrations.gmail import GmailClient
from sales_support_agent.integrations.hubspot import HubSpotClient

logger = logging.getLogger(__name__)


@dataclass
class SendResult:
    ok: bool
    gmail_message_id: str = ""
    hubspot_engagement_id: str = ""
    from_email: str = ""
    error: str = ""
    sent_at: Optional[datetime] = None


def send_followup_email(
    *,
    gmail_client: GmailClient,
    hubspot_client: HubSpotClient,
    deal_id: str,
    contact_ids: list[str],
    to_emails: list[str],
    subject: str,
    body_text: str,
) -> SendResult:
    """Send a follow-up email via Gmail and log it to HubSpot.

    Returns SendResult with ok=True on Gmail success regardless of whether
    the HubSpot engagement log succeeded.
    """
    to_list = [e.strip() for e in to_emails if e.strip()]
    if not to_list:
        return SendResult(ok=False, error="No recipients specified.")
    if not (subject or "").strip():
        return SendResult(ok=False, error="Subject is required.")
    if not (body_text or "").strip():
        return SendResult(ok=False, error="Body is required.")

    # Get the sender address for HubSpot logging (best-effort, non-blocking).
    from_email = ""
    try:
        profile = gmail_client.get_profile()
        from_email = str(profile.get("emailAddress") or "").strip()
    except Exception as exc:
        logger.debug("[email_send] profile fetch skipped: %s", exc)

    # Send via Gmail.
    sent_at = datetime.now(timezone.utc)
    try:
        send_resp = gmail_client.send_message(
            to=tuple(to_list),
            subject=subject,
            text=body_text,
        )
    except Exception as exc:
        logger.warning("[email_send] Gmail send failed: %s", exc)
        return SendResult(ok=False, error=str(exc)[:200], from_email=from_email)

    gmail_message_id = str(
        send_resp.get("id") or (send_resp.get("message") or {}).get("id") or ""
    )
    logger.info("[email_send] sent message %s for deal %s", gmail_message_id, deal_id)

    # Log to HubSpot (best-effort — failure does not fail the send).
    hubspot_engagement_id = ""
    if hubspot_client.is_configured:
        try:
            sent_at_ms = int(sent_at.timestamp() * 1000)
            log_resp = hubspot_client.log_email_engagement(
                deal_id=deal_id,
                contact_ids=list(contact_ids),
                from_email=from_email,
                to_emails=to_list,
                subject=subject,
                body_html=body_text,
                sent_at_ms=sent_at_ms,
            )
            hubspot_engagement_id = str(log_resp.get("id") or "")
            logger.info(
                "[email_send] HubSpot engagement %s logged for deal %s",
                hubspot_engagement_id, deal_id,
            )
        except Exception as exc:
            logger.warning(
                "[email_send] HubSpot engagement log failed (non-critical): %s", exc
            )

    return SendResult(
        ok=True,
        gmail_message_id=gmail_message_id,
        hubspot_engagement_id=hubspot_engagement_id,
        from_email=from_email,
        sent_at=sent_at,
    )
