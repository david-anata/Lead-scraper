"""Access-flow email notifications (invites + approvals).

Sent via the existing GmailClient when GMAIL_* credentials are configured;
every helper degrades to returning False (never raises) so the access flows
keep working without email — the UI falls back to copyable links.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _client(settings):
    from sales_support_agent.integrations.gmail import GmailClient
    client = GmailClient(settings)
    return client if client.is_configured() else None


def send_invite_email(settings, *, to_email: str, invite_link: str,
                      invited_by: str = "", role_name: str = "") -> bool:
    """Email an invite link. Returns True only if the send succeeded."""
    if settings is None:
        return False
    try:
        client = _client(settings)
        if client is None:
            return False
        role_part = f" with the «{role_name}» role" if role_name else ""
        by_part = f" by {invited_by}" if invited_by else ""
        client.send_message(
            to=(to_email,),
            subject="You've been invited to the Anata agent dashboard",
            text=(
                f"Hi,\n\n"
                f"You've been invited{by_part} to the Anata agent dashboard{role_part}.\n\n"
                f"Accept your invite (valid 7 days):\n{invite_link}\n\n"
                f"You'll be asked to sign in with your @anatainc.com Google account.\n"
            ),
        )
        return True
    except Exception:  # noqa: BLE001 — email must never block the invite flow
        logger.exception("Invite email to %s failed", to_email)
        return False


def send_approval_email(settings, *, to_email: str, base_url: str = "",
                        decided_by: str = "") -> bool:
    """Email an access-request approval. Returns True only if sent."""
    if settings is None:
        return False
    try:
        client = _client(settings)
        if client is None:
            return False
        login_url = (base_url.rstrip("/") + "/admin/login") if base_url else "the dashboard login page"
        by_part = f" by {decided_by}" if decided_by else ""
        client.send_message(
            to=(to_email,),
            subject="Your Anata agent dashboard access was approved",
            text=(
                f"Hi,\n\n"
                f"Your access request was approved{by_part}.\n\n"
                f"Sign in with your @anatainc.com Google account:\n{login_url}\n"
            ),
        )
        return True
    except Exception:  # noqa: BLE001 — email must never block the approval flow
        logger.exception("Approval email to %s failed", to_email)
        return False
