"""Bounce and send outcomes for the outbound campaigns.

Webhooks are the precise source: Instantly posts email_sent, email_bounced,
and lead_unsubscribed to the existing webhook. A bounded poll of lead status
backfills bounces and unsubscribes the webhook missed. The first observation
wins, except a webhook timestamp replaces a poll timestamp for the same
bounce or unsubscribe.

Bounce rate is hard bounces divided by sends, per Denver day and per campaign.
A day with no recorded sends has a count and no rate. Soft bounces are stored
and are not suppressions.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

import outbound_suppression as suppression
from sales_support_agent.integrations.instantly_payloads import (
    build_external_event_key,
    extract_email,
    parse_occurred_at,
)

logger = logging.getLogger(__name__)

LEADS_URL = "https://api.instantly.ai/api/v2/leads/list"
_EVENTS = "outbound_delivery_events"
_TZ = ZoneInfo("America/Denver")

_HARD = "hard_bounce"
_SOFT = "soft_bounce"
_SENT = "sent"
_UNSUB = "unsubscribe"


def _api_key() -> str:
    return (os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip()


def _stamp(value: datetime | str | None) -> str:
    if value is None or value == "":
        value = datetime.now(timezone.utc)
    if isinstance(value, str):
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return raw[:19].replace("T", " ")
        value = parsed
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _denver_day(stamp: str) -> str:
    try:
        dt = datetime.strptime(stamp[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return stamp[:10]
    return dt.astimezone(_TZ).date().isoformat()


def _is_soft(payload: dict[str, Any]) -> bool:
    blob = " ".join(str(payload.get(key) or "") for key in ("bounce_type", "bounce_reason", "reason")).lower()
    return "soft" in blob and "hard" not in blob


def record_event(engine, *, email: str, campaign_id: str, event_type: str,
                 occurred_at: datetime | str | None, source: str, detail: str,
                 external_key: str) -> None:
    suppression.ensure_tables(engine)
    address = (email or "").strip().lower()
    if not address or not external_key:
        return
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {_EVENTS} (email, campaign_id, event_type, occurred_at, source, detail, external_key) "
            "VALUES (:email, :campaign, :event_type, :occurred, :source, :detail, :key) "
            "ON CONFLICT (external_key) DO UPDATE SET "
            "occurred_at = CASE WHEN outbound_delivery_events.source = 'instantly_poll' "
            "AND excluded.source = 'instantly_webhook' THEN excluded.occurred_at "
            "ELSE outbound_delivery_events.occurred_at END, "
            "source = CASE WHEN outbound_delivery_events.source = 'instantly_poll' "
            "AND excluded.source = 'instantly_webhook' THEN excluded.source "
            "ELSE outbound_delivery_events.source END"),
            {"email": address, "campaign": campaign_id or "", "event_type": event_type,
             "occurred": _stamp(occurred_at), "source": source,
             "detail": (detail or "")[:300], "key": external_key})


def record_webhook(engine, payload: dict[str, Any]) -> dict[str, Any]:
    """Store one Instantly webhook if it is a send, bounce, or unsubscribe."""
    event = str(payload.get("event_type") or "").strip()
    mapping = {"email_sent": _SENT, "email_bounced": _HARD, "lead_unsubscribed": _UNSUB}
    if event not in mapping:
        return {"recorded": False}
    email = extract_email(payload).strip().lower()
    if "@" not in email:
        return {"recorded": False, "reason": "missing email"}
    campaign = str(payload.get("campaign_id") or "")
    occurred = parse_occurred_at(payload)
    kind = _SOFT if event == "email_bounced" and _is_soft(payload) else mapping[event]
    if kind == _SENT:
        key = build_external_event_key(payload)
    elif kind == _SOFT:
        key = "soft|" + build_external_event_key(payload)
    else:
        key = f"{kind}|{campaign}|{email}"
    detail = str(payload.get("bounce_reason") or payload.get("reason") or "")
    record_event(engine, email=email, campaign_id=campaign, event_type=kind,
                 occurred_at=occurred, source="instantly_webhook", detail=detail,
                 external_key=key)
    if kind == _HARD:
        suppression.add(engine, kind="email", value=email, reason="hard_bounce",
                        source="instantly_webhook", detail=detail)
    elif kind == _UNSUB:
        suppression.add(engine, kind="email", value=email, reason="unsubscribe",
                        source="instantly_webhook")
    return {"recorded": True, "event_type": kind}


def _rate(bounces: int, sent: int) -> Optional[float]:
    if not sent:
        return None
    return round(100.0 * bounces / sent, 1)


def bounce_report(engine, *, days: int = 14) -> dict[str, Any]:
    """Hard-bounce rate per Denver day and per campaign, from stored events."""
    suppression.ensure_tables(engine)
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, days))).strftime("%Y-%m-%d %H:%M:%S")
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT campaign_id, event_type, occurred_at FROM {_EVENTS} WHERE occurred_at >= :t"),
            {"t": since}).fetchall()
    by_day: dict[str, dict[str, int]] = {}
    by_campaign: dict[str, dict[str, int]] = {}
    by_both: dict[tuple[str, str], dict[str, int]] = {}

    def bump(bucket: dict[str, int], event_type: str) -> None:
        if event_type == _SENT:
            bucket["sent"] += 1
        elif event_type == _HARD:
            bucket["bounces"] += 1
        elif event_type == _SOFT:
            bucket["soft_bounces"] += 1
        elif event_type == _UNSUB:
            bucket["unsubscribes"] += 1

    def fresh() -> dict[str, int]:
        return {"sent": 0, "bounces": 0, "soft_bounces": 0, "unsubscribes": 0}

    for campaign_id, event_type, occurred_at in rows:
        day = _denver_day(str(occurred_at or ""))
        campaign = str(campaign_id or "")
        by_day.setdefault(day, fresh())
        by_campaign.setdefault(campaign, fresh())
        by_both.setdefault((day, campaign), fresh())
        bump(by_day[day], str(event_type))
        bump(by_campaign[campaign], str(event_type))
        bump(by_both[(day, campaign)], str(event_type))

    def pack(day: str, campaign: str, counts: dict[str, int]) -> dict[str, Any]:
        return {
            "day": day, "campaign_id": campaign,
            "sent": counts["sent"], "bounces": counts["bounces"],
            "soft_bounces": counts["soft_bounces"], "unsubscribes": counts["unsubscribes"],
            "bounce_rate": _rate(counts["bounces"], counts["sent"]),
        }

    return {
        "by_day": [pack(day, "", counts) for day, counts in sorted(by_day.items())],
        "by_campaign": [pack("", campaign, counts) for campaign, counts in sorted(by_campaign.items())],
        "by_day_campaign": [
            pack(day, campaign, counts) for (day, campaign), counts in sorted(by_both.items())
        ],
    }


def today_slice(report: dict[str, Any], now: datetime) -> Optional[dict[str, Any]]:
    day = now.astimezone(_TZ).date().isoformat()
    for row in report.get("by_day") or []:
        if row.get("day") == day and (row.get("sent") or row.get("bounces")):
            return row
    return None


def _redact(message: str, key: str) -> str:
    if key and len(key) > 6:
        return message.replace(key, "<instantly-key>")
    return message


def _poll_filter(engine, *, key: str, lead_filter: str, event_type: str, reason: str,
                 expected_status: int, post: Callable[..., Any], max_pages: int) -> int:
    cursor_key = f"instantly_leads:{lead_filter}"
    cursor = suppression.get_cursor(engine, cursor_key)
    recorded = 0
    for _ in range(max(1, max_pages)):
        body: dict[str, Any] = {"filter": lead_filter, "limit": 100}
        if cursor:
            body["starting_after"] = cursor
        try:
            resp = post(LEADS_URL, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                        json=body, timeout=30)
            status = int(getattr(resp, "status_code", 0) or 0)
            data = resp.json() if status == 200 else {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("[bounces] lead poll failed: %s", _redact(str(exc), key))
            return recorded
        if status >= 300 or not isinstance(data, dict):
            logger.warning("[bounces] lead poll returned %s", status)
            return recorded
        items = data.get("items") or []
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_status = item.get("status")
            if raw_status is not None and int(raw_status) != expected_status:
                continue
            email = str(item.get("email") or "").strip().lower()
            if "@" not in email:
                continue
            campaign = str(item.get("campaign") or "")
            when = item.get("timestamp_updated") or item.get("timestamp_last_contact") or item.get("timestamp_created")
            record_event(engine, email=email, campaign_id=campaign, event_type=event_type,
                         occurred_at=str(when or ""), source="instantly_poll", detail="",
                         external_key=f"{event_type}|{campaign}|{email}")
            suppression.add(engine, kind="email", value=email, reason=reason,
                            source="instantly_poll", overwrite=False)
            recorded += 1
        nxt = str(data.get("next_starting_after") or "")
        if not nxt or not items:
            cursor = ""
            break
        cursor = nxt
    suppression.set_cursor(engine, cursor_key, cursor)
    return recorded


def poll_lead_outcomes(engine, *, key: str = "", post: Optional[Callable[..., Any]] = None,
                       max_pages: int = 3) -> dict[str, Any]:
    """Backfill hard bounces and unsubscribes from Instantly lead status. Never raises."""
    key = (key or _api_key()).strip()
    if not key:
        return {"bounces": 0, "unsubscribes": 0, "reason": "no key"}
    post = post or requests.post
    try:
        suppression.ensure_tables(engine)
        bounces = _poll_filter(engine, key=key, lead_filter="FILTER_VAL_BOUNCED", event_type=_HARD,
                               reason="hard_bounce", expected_status=-1, post=post, max_pages=max_pages)
        unsubs = _poll_filter(engine, key=key, lead_filter="FILTER_VAL_UNSUBSCRIBED", event_type=_UNSUB,
                              reason="unsubscribe", expected_status=-2, post=post, max_pages=max_pages)
    except Exception:  # noqa: BLE001
        logger.exception("[bounces] lead poll failed")
        return {"bounces": 0, "unsubscribes": 0, "reason": "poll failed"}
    return {"bounces": bounces, "unsubscribes": unsubs, "reason": ""}
