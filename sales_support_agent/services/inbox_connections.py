"""Mailbox connection summaries for /admin/settings."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import MailboxSignal


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    aware = _aware(value)
    return aware.isoformat() if aware is not None else None


def _has_credentials(account: Any) -> bool:
    access_token = str(getattr(account, "access_token", "") or "").strip()
    client_id = str(getattr(account, "client_id", "") or "").strip()
    client_secret = str(getattr(account, "client_secret", "") or "").strip()
    refresh_token = str(getattr(account, "refresh_token", "") or "").strip()
    return bool(access_token or (client_id and client_secret and refresh_token))


def build_inbox_connection_summary(
    session: Session,
    settings: Any,
    *,
    as_of: datetime | None = None,
    stale_days: int = 7,
) -> dict[str, Any]:
    as_of = _aware(as_of) or datetime.now(timezone.utc)
    stale_cutoff = as_of - timedelta(days=max(int(stale_days), 1))
    configured_accounts = list(getattr(settings, "gmail_mailbox_accounts", ()) or ())

    grouped_signals: dict[str, list[MailboxSignal]] = defaultdict(list)
    for signal in session.scalars(
        select(MailboxSignal).order_by(MailboxSignal.received_at.desc(), MailboxSignal.id.desc())
    ).all():
        raw_payload = dict(signal.raw_payload or {})
        account_key = str(raw_payload.get("gmail_account_key") or "").strip()
        if not account_key:
            continue
        grouped_signals[account_key].append(signal)

    rows: list[dict[str, Any]] = []
    connected_count = 0
    attention_count = 0
    invalid_count = 0
    unseen_count = 0

    for account in configured_accounts:
        account_key = str(getattr(account, "account_key", "") or "").strip()
        label = str(getattr(account, "label", "") or account_key or "Inbox").strip()
        signals = grouped_signals.get(account_key, [])
        latest_signal = signals[0] if signals else None
        has_credentials = _has_credentials(account)
        matched_deal_ids = {
            str(signal.matched_deal_id or "").strip()
            for signal in signals
            if str(signal.matched_deal_id or "").strip()
        }
        last_received_at = _aware(getattr(latest_signal, "received_at", None))

        if not has_credentials:
            status = "invalid"
            invalid_count += 1
        elif latest_signal is None:
            status = "configured_not_seen"
            unseen_count += 1
        elif last_received_at is not None and last_received_at < stale_cutoff:
            status = "attention"
            attention_count += 1
        else:
            status = "connected"
            connected_count += 1

        rows.append(
            {
                "account_key": account_key,
                "label": label,
                "configured": True,
                "has_credentials": has_credentials,
                "message_count": len(signals),
                "matched_deal_count": len(matched_deal_ids),
                "last_received_at": _iso(last_received_at),
                "last_sender_email": str(getattr(latest_signal, "sender_email", "") or "").strip() or None,
                "last_subject": str(getattr(latest_signal, "subject", "") or "").strip() or None,
                "poll_query": str(getattr(account, "poll_query", "") or "").strip(),
                "source_domains": list(getattr(account, "source_domains", ()) or ()),
                "status": status,
                "status_label": {
                    "connected": "Connected",
                    "configured_not_seen": "Configured, No Traffic Yet",
                    "attention": "Needs Attention",
                    "invalid": "Invalid",
                }[status],
            }
        )

    rows.sort(key=lambda item: (item["status"] != "connected", item["status"] == "invalid", item["label"].lower()))
    return {
        "total_configured": len(configured_accounts),
        "connected_count": connected_count,
        "attention_count": attention_count,
        "invalid_count": invalid_count,
        "configured_not_seen_count": unseen_count,
        "accounts": rows,
    }
