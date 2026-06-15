"""Access-flow email notifications (invites + approvals).

Preferred sender is Resend (RESEND_API_KEY + RESEND_FROM — a single key and a
verified domain). Falls back to the GmailClient when Resend isn't configured but
GMAIL_* credentials are. Every helper degrades to returning False (never raises)
so the access flows keep working without any email provider — the UI falls back
to copyable links.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _send(settings, *, to_email: str, subject: str, text: str) -> bool:
    """Send via the first configured provider (Resend, then Gmail).

    Returns True only if a provider accepted the message. Never raises — a send
    failure logs and returns False so the invite/approval flow keeps working
    (the UI then falls back to the copyable link).
    """
    if settings is None:
        return False

    # 1) Resend — preferred (simple API key + verified domain).
    try:
        from sales_support_agent.integrations.resend import ResendClient
        resend = ResendClient(settings)
        if resend.is_configured():
            resend.send_message(to=(to_email,), subject=subject, text=text)
            return True
    except Exception:  # noqa: BLE001 — fall through to Gmail / copy-link
        logger.exception("Resend send to %s failed; trying Gmail", to_email)

    # 2) Gmail — legacy fallback when Resend isn't configured.
    try:
        from sales_support_agent.integrations.gmail import GmailClient
        gmail = GmailClient(settings)
        if gmail.is_configured():
            gmail.send_message(to=(to_email,), subject=subject, text=text)
            return True
    except Exception:  # noqa: BLE001 — email must never block the access flow
        logger.exception("Gmail send to %s failed", to_email)

    return False


def send_invite_email(settings, *, to_email: str, invite_link: str,
                      invited_by: str = "", role_name: str = "") -> bool:
    """Email an invite link. Returns True only if a provider sent it."""
    role_part = f" with the «{role_name}» role" if role_name else ""
    by_part = f" by {invited_by}" if invited_by else ""
    return _send(
        settings,
        to_email=to_email,
        subject="You've been invited to the Anata agent dashboard",
        text=(
            f"Hi,\n\n"
            f"You've been invited{by_part} to the Anata agent dashboard{role_part}.\n\n"
            f"Accept your invite (valid 7 days):\n{invite_link}\n\n"
            f"Open the link and sign in with the Google account this invite was sent to.\n"
        ),
    )


def send_approval_email(settings, *, to_email: str, base_url: str = "",
                        decided_by: str = "") -> bool:
    """Email an access-request approval. Returns True only if sent."""
    login_url = (base_url.rstrip("/") + "/admin/login") if base_url else "the dashboard login page"
    by_part = f" by {decided_by}" if decided_by else ""
    return _send(
        settings,
        to_email=to_email,
        subject="Your Anata agent dashboard access was approved",
        text=(
            f"Hi,\n\n"
            f"Your access request was approved{by_part}.\n\n"
            f"Sign in with your @anatainc.com Google account:\n{login_url}\n"
        ),
    )
