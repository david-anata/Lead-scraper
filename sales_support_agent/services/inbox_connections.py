"""Mailbox connection summaries for /admin/settings."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import MailboxSignal
from sales_support_agent.services.inbox_connection_store import load_user_inbox_connections


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


def _account_key_expr(session: Session):
    bind = getattr(session, "bind", None)
    dialect = getattr(getattr(bind, "dialect", None), "name", "")
    if dialect == "postgresql":
        return MailboxSignal.raw_payload.op("->>")("gmail_account_key")
    if dialect == "sqlite":
        return func.json_extract(MailboxSignal.raw_payload, "$.gmail_account_key")
    return None


def _default_summary(configured_accounts: list[Any]) -> dict[str, Any]:
    return {
        "total_configured": len(configured_accounts),
        "legacy_configured_count": 0,
        "user_configured_count": 0,
        "connected_count": 0,
        "attention_count": 0,
        "invalid_count": 0,
        "configured_not_seen_count": 0,
        "accounts": [],
    }


def _load_signal_snapshot(
    session: Session,
    *,
    account_keys: list[str],
) -> dict[str, dict[str, Any]]:
    if not account_keys:
        return {}

    account_key_expr = _account_key_expr(session)
    if account_key_expr is None:
        return {}

    stats_rows = session.execute(
        select(
            account_key_expr.label("account_key"),
            func.count(MailboxSignal.id).label("message_count"),
            func.count(distinct(func.nullif(MailboxSignal.matched_deal_id, ""))).label("matched_deal_count"),
            func.max(MailboxSignal.received_at).label("last_received_at"),
        )
        .where(account_key_expr.in_(account_keys))
        .group_by(account_key_expr)
    ).all()

    latest_rows = session.execute(
        select(
            MailboxSignal.id.label("signal_id"),
            account_key_expr.label("account_key"),
            func.row_number()
            .over(
                partition_by=account_key_expr,
                order_by=(MailboxSignal.received_at.desc(), MailboxSignal.id.desc()),
            )
            .label("position"),
        )
        .where(account_key_expr.in_(account_keys))
        .subquery()
        .select()
    ).all()

    latest_signal_ids = [int(row.signal_id) for row in latest_rows if int(row.position or 0) == 1]
    latest_signal_map = {}
    if latest_signal_ids:
        signal_rows = session.execute(
            select(MailboxSignal).where(MailboxSignal.id.in_(latest_signal_ids))
        ).scalars().all()
        latest_signal_map = {signal.id: signal for signal in signal_rows}

    snapshot: dict[str, dict[str, Any]] = {}
    latest_by_account = {
        str(row.account_key or "").strip(): latest_signal_map.get(int(row.signal_id))
        for row in latest_rows
        if int(row.position or 0) == 1
    }
    for row in stats_rows:
        account_key = str(row.account_key or "").strip()
        if not account_key:
            continue
        latest_signal = latest_by_account.get(account_key)
        snapshot[account_key] = {
            "message_count": int(row.message_count or 0),
            "matched_deal_count": int(row.matched_deal_count or 0),
            "last_received_at": _aware(row.last_received_at),
            "last_sender_email": (
                str(getattr(latest_signal, "sender_email", "") or "").strip() or None
                if latest_signal is not None
                else None
            ),
            "last_subject": (
                str(getattr(latest_signal, "subject", "") or "").strip() or None
                if latest_signal is not None
                else None
            ),
        }
    return snapshot


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
    persisted_accounts = list(load_user_inbox_connections(session))
    summary = _default_summary(configured_accounts)
    summary["legacy_configured_count"] = len(configured_accounts)
    summary["user_configured_count"] = len(persisted_accounts)
    summary["total_configured"] = len(configured_accounts) + len(persisted_accounts)
    configured_account_keys = [
        str(getattr(account, "account_key", "") or "").strip()
        for account in configured_accounts
        if str(getattr(account, "account_key", "") or "").strip()
    ]
    configured_account_keys.extend(
        str(getattr(account, "account_key", "") or "").strip()
        for account in persisted_accounts
        if str(getattr(account, "account_key", "") or "").strip()
    )
    signal_snapshot = _load_signal_snapshot(session, account_keys=configured_account_keys)

    rows: list[dict[str, Any]] = []

    for account in configured_accounts:
        account_key = str(getattr(account, "account_key", "") or "").strip()
        label = str(getattr(account, "label", "") or account_key or "Inbox").strip()
        signal_data = signal_snapshot.get(account_key, {})
        has_credentials = _has_credentials(account)
        last_received_at = _aware(signal_data.get("last_received_at"))

        if not has_credentials:
            status = "invalid"
            summary["invalid_count"] += 1
        elif int(signal_data.get("message_count") or 0) <= 0:
            status = "configured_not_seen"
            summary["configured_not_seen_count"] += 1
        elif last_received_at is not None and last_received_at < stale_cutoff:
            status = "attention"
            summary["attention_count"] += 1
        else:
            status = "connected"
            summary["connected_count"] += 1

        rows.append(
            {
                "account_key": account_key,
                "label": label,
                "configured": True,
                "has_credentials": has_credentials,
                "message_count": int(signal_data.get("message_count") or 0),
                "matched_deal_count": int(signal_data.get("matched_deal_count") or 0),
                "last_received_at": _iso(last_received_at),
                "last_sender_email": signal_data.get("last_sender_email"),
                "last_subject": signal_data.get("last_subject"),
                "poll_query": str(getattr(account, "poll_query", "") or "").strip(),
                "source_domains": list(getattr(account, "source_domains", ()) or ()),
                "source": "legacy_env",
                "source_label": "Legacy system inbox",
                "owner_user_email": None,
                "owner_user_name": "System-managed",
                "status": status,
                "status_label": {
                    "connected": "Connected",
                    "configured_not_seen": "Configured, No Traffic Yet",
                    "attention": "Needs Attention",
                    "invalid": "Invalid",
                }[status],
            }
        )

    for account in persisted_accounts:
        account_key = str(getattr(account, "account_key", "") or "").strip()
        label = str(getattr(account, "account_label", "") or getattr(account, "account_email", "") or account_key or "Inbox").strip()
        signal_data = signal_snapshot.get(account_key, {})
        has_credentials = bool(str(getattr(account, "sealed_refresh_token", "") or "").strip())
        last_received_at = _aware(signal_data.get("last_received_at"))
        row_status = str(getattr(account, "status", "") or "").strip() or "connected"

        if row_status == "disconnected":
            status = "invalid"
            summary["invalid_count"] += 1
        elif not has_credentials:
            status = "invalid"
            summary["invalid_count"] += 1
        elif int(signal_data.get("message_count") or 0) <= 0:
            status = "configured_not_seen"
            summary["configured_not_seen_count"] += 1
        elif last_received_at is not None and last_received_at < stale_cutoff:
            status = "attention"
            summary["attention_count"] += 1
        else:
            status = "connected"
            summary["connected_count"] += 1

        rows.append(
            {
                "account_key": account_key,
                "label": label,
                "configured": True,
                "has_credentials": has_credentials,
                "message_count": int(signal_data.get("message_count") or 0),
                "matched_deal_count": int(signal_data.get("matched_deal_count") or 0),
                "last_received_at": _iso(last_received_at),
                "last_sender_email": signal_data.get("last_sender_email"),
                "last_subject": signal_data.get("last_subject"),
                "poll_query": str(getattr(account, "poll_query", "") or "").strip(),
                "source_domains": list(getattr(account, "source_domains_json", ()) or ()),
                "source": "user_oauth",
                "source_label": "User-connected inbox",
                "owner_user_email": str(getattr(account, "owner_user_email", "") or "").strip() or None,
                "owner_user_name": str(getattr(account, "owner_user_name", "") or "").strip() or None,
                "status": status,
                "status_label": {
                    "connected": "Connected",
                    "configured_not_seen": "Configured, No Traffic Yet",
                    "attention": "Needs Attention",
                    "invalid": "Disconnected",
                }[status],
            }
        )

    rows.sort(key=lambda item: (item["status"] != "connected", item["status"] == "invalid", item["label"].lower()))
    summary["accounts"] = rows
    return summary
