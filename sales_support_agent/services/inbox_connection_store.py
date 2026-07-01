"""Persisted inbox connection helpers."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import GmailMailboxAccount
from sales_support_agent.models.entities import InboxConnection
from sales_support_agent.services.token_seal import seal_token, unseal_token


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _token_secret(settings: Any) -> str:
    return str(
        getattr(settings, "canva_token_secret", "")
        or getattr(settings, "admin_session_secret", "")
        or ""
    ).strip()


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return cleaned or "inbox"


def load_user_inbox_connections(session: Session) -> list[InboxConnection]:
    return list(
        session.scalars(
            select(InboxConnection).order_by(InboxConnection.owner_user_email.asc(), InboxConnection.created_at.asc())
        ).all()
    )


def load_active_gmail_mailbox_accounts(session: Session, settings: Any) -> list[GmailMailboxAccount]:
    accounts: list[GmailMailboxAccount] = list(getattr(settings, "gmail_mailbox_accounts", ()) or ())
    client_id = str(getattr(settings, "google_oauth_client_id", "") or "").strip()
    client_secret = str(getattr(settings, "google_oauth_client_secret", "") or "").strip()
    secret = _token_secret(settings)
    for row in load_user_inbox_connections(session):
        if str(row.provider or "") != "gmail":
            continue
        if str(row.status or "") not in {"connected", "attention"}:
            continue
        refresh_token = ""
        access_token = ""
        try:
            if row.sealed_refresh_token and secret:
                refresh_token = unseal_token(secret, row.sealed_refresh_token)
            if row.sealed_access_token and secret:
                access_token = unseal_token(secret, row.sealed_access_token)
        except Exception:
            row.status = "attention"
            row.last_error = "Stored Gmail token could not be unsealed."
            row.updated_at = _now()
            continue
        accounts.append(
            GmailMailboxAccount(
                account_key=str(row.account_key or "").strip(),
                label=str(row.account_label or row.account_email or row.owner_user_email or "Inbox").strip(),
                access_token=access_token,
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
                user_id=str(row.gmail_user_id or "me").strip() or "me",
                poll_query=str(row.poll_query or getattr(settings, "gmail_poll_query", "newer_than:2d")).strip(),
                poll_max_messages=int(row.poll_max_messages or getattr(settings, "gmail_poll_max_messages", 25) or 25),
                source_domains=tuple(str(item).strip() for item in (row.source_domains_json or []) if str(item).strip()),
            )
        )
    return accounts


def upsert_user_gmail_connection(
    session: Session,
    settings: Any,
    *,
    owner_user_id: str,
    owner_user_email: str,
    owner_user_name: str,
    account_email: str,
    account_label: str,
    access_token: str,
    refresh_token: str,
    source_domains: list[str] | None = None,
) -> InboxConnection:
    email = str(account_email or owner_user_email or "").strip().lower()
    owner_email = str(owner_user_email or "").strip().lower()
    account_key = _slug(email or owner_email)
    row = session.scalar(select(InboxConnection).where(InboxConnection.owner_user_email == owner_email))
    if row is None:
        row = InboxConnection(
            provider="gmail",
            connection_source="user_oauth",
            account_key=account_key,
            owner_user_id=str(owner_user_id or "").strip(),
            owner_user_email=owner_email,
            owner_user_name=str(owner_user_name or "").strip(),
        )
        session.add(row)

    secret = _token_secret(settings)
    row.provider = "gmail"
    row.connection_source = "user_oauth"
    row.account_key = account_key
    row.account_email = email
    row.account_label = str(account_label or email or owner_email or "Inbox").strip()
    row.owner_user_id = str(owner_user_id or "").strip()
    row.owner_user_email = owner_email
    row.owner_user_name = str(owner_user_name or "").strip()
    row.gmail_user_id = "me"
    if access_token and secret:
        row.sealed_access_token = seal_token(secret, access_token)
    if refresh_token and secret:
        row.sealed_refresh_token = seal_token(secret, refresh_token)
    row.poll_query = str(getattr(settings, "gmail_poll_query", "newer_than:2d") or "newer_than:2d").strip()
    row.poll_max_messages = int(getattr(settings, "gmail_poll_max_messages", 25) or 25)
    row.source_domains_json = [str(item).strip() for item in (source_domains or getattr(settings, "gmail_source_domains", ()) or []) if str(item).strip()]
    row.status = "connected"
    row.last_error = ""
    row.last_validated_at = _now()
    row.disconnected_at = None
    row.updated_at = _now()
    return row


def disconnect_user_inbox_connection(session: Session, *, owner_user_email: str) -> bool:
    row = session.scalar(
        select(InboxConnection).where(InboxConnection.owner_user_email == str(owner_user_email or "").strip().lower())
    )
    if row is None:
        return False
    row.status = "disconnected"
    row.disconnected_at = _now()
    row.updated_at = _now()
    return True
