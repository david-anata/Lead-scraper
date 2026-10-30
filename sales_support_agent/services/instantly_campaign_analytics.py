"""Pull Instantly's per-day campaign analytics into instantly_campaign_daily.

The daily route is GET /api/v2/campaigns/analytics/daily, once per campaign.
Campaign names come from GET /api/v2/campaigns. Nothing here sends email.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

DAILY_PATH = "/api/v2/campaigns/analytics/daily"
CAMPAIGNS_PATH = "/api/v2/campaigns"
DEFAULT_DAYS = 3
BACKFILL_START = date(2026, 10, 1)
_TABLE = "instantly_campaign_daily"

_CREATE = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
    date TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    campaign_name TEXT NOT NULL DEFAULT '',
    sent INTEGER NOT NULL DEFAULT 0,
    contacted INTEGER NOT NULL DEFAULT 0,
    new_leads_contacted INTEGER NOT NULL DEFAULT 0,
    replies INTEGER NOT NULL DEFAULT 0,
    unique_replies INTEGER NOT NULL DEFAULT 0,
    positive INTEGER NOT NULL DEFAULT 0,
    bounced INTEGER NOT NULL DEFAULT 0,
    unsubscribed INTEGER NOT NULL DEFAULT 0,
    opportunities INTEGER NOT NULL DEFAULT 0,
    meetings_booked INTEGER,
    pulled_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (date, campaign_id)
)
"""


def ensure_table(engine) -> None:
    """Create the table outside Vercel. Production uses the SQL migration."""

    if engine is None:
        return
    if os.getenv("VERCEL") and getattr(getattr(engine, "dialect", None), "name", "") == "postgresql":
        return
    with engine.begin() as conn:
        conn.execute(text(_CREATE))


def row_count(engine) -> int:
    ensure_table(engine)
    with engine.connect() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar() or 0)


def _whole(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _first_present(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return None


def positive_count(row: dict[str, Any]) -> int:
    """Interested when Instantly reports it, otherwise the opportunity count."""

    chosen = _first_present(row, ("positive", "interested", "total_interested"))
    if chosen is None:
        chosen = _first_present(row, ("opportunities", "total_opportunities"))
    return _whole(chosen)


def meetings_booked_count(row: dict[str, Any]) -> Optional[int]:
    chosen = _first_present(row, ("meetings_booked", "total_meeting_booked"))
    if chosen is None:
        return None
    return _whole(chosen)


def normalize_daily_row(
    row: dict[str, Any],
    *,
    campaign_id: str,
    campaign_name: str,
    pulled_at: str,
) -> Optional[dict[str, Any]]:
    """One Instantly daily object, shaped for instantly_campaign_daily."""

    raw_date = str(row.get("date") or "")[:10]
    if len(raw_date) != 10:
        return None
    stored_id = str(row.get("campaign_id") or campaign_id or "").strip()
    if not stored_id:
        return None
    name = str(row.get("campaign_name") or campaign_name or "").strip()
    return {
        "date": raw_date,
        "campaign_id": stored_id,
        "campaign_name": name,
        "sent": _whole(_first_present(row, ("sent", "emails_sent_count"))),
        "contacted": _whole(_first_present(row, ("contacted", "contacted_count"))),
        "new_leads_contacted": _whole(_first_present(row, ("new_leads_contacted", "new_leads_contacted_count"))),
        "replies": _whole(_first_present(row, ("replies", "reply_count"))),
        "unique_replies": _whole(_first_present(row, ("unique_replies", "reply_count_unique"))),
        "positive": positive_count(row),
        "bounced": _whole(_first_present(row, ("bounced", "bounces", "bounced_count"))),
        "unsubscribed": _whole(_first_present(row, ("unsubscribed", "unsubscribes", "unsubscribed_count"))),
        "opportunities": _whole(_first_present(row, ("opportunities", "total_opportunities"))),
        "meetings_booked": meetings_booked_count(row),
        "pulled_at": pulled_at,
    }


def _window(today: date, *, start_date: Optional[date], days: int, empty: bool) -> tuple[date, date]:
    if start_date is not None:
        return start_date, today
    if empty:
        return BACKFILL_START, today
    span = DEFAULT_DAYS if days < 1 else days
    return today - timedelta(days=span - 1), today


def _items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        raw = payload.get("items") or payload.get("data") or []
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
    return []


def _campaigns(get: Callable[..., tuple[int, Any]]) -> tuple[list[dict[str, str]], str]:
    found: list[dict[str, str]] = []
    params: dict[str, Any] = {"limit": 100}
    for _ in range(20):
        status, payload = get(CAMPAIGNS_PATH, params)
        if status != 200:
            return [], f"campaign list returned {status}"
        page = _items(payload)
        for item in page:
            campaign_id = str(item.get("id") or "").strip()
            if campaign_id:
                found.append({"id": campaign_id, "name": str(item.get("name") or "")})
        nxt = ""
        if isinstance(payload, dict):
            nxt = str(payload.get("next_starting_after") or "").strip()
        if not nxt:
            break
        params["starting_after"] = nxt
    return found, ""


def _upsert(engine, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = f"""
        INSERT INTO {_TABLE} (
            date, campaign_id, campaign_name, sent, contacted, new_leads_contacted,
            replies, unique_replies, positive, bounced, unsubscribed, opportunities,
            meetings_booked, pulled_at
        ) VALUES (
            :date, :campaign_id, :campaign_name, :sent, :contacted, :new_leads_contacted,
            :replies, :unique_replies, :positive, :bounced, :unsubscribed, :opportunities,
            :meetings_booked, :pulled_at
        )
        ON CONFLICT (date, campaign_id) DO UPDATE SET
            campaign_name = excluded.campaign_name,
            sent = excluded.sent,
            contacted = excluded.contacted,
            new_leads_contacted = excluded.new_leads_contacted,
            replies = excluded.replies,
            unique_replies = excluded.unique_replies,
            positive = excluded.positive,
            bounced = excluded.bounced,
            unsubscribed = excluded.unsubscribed,
            opportunities = excluded.opportunities,
            meetings_booked = excluded.meetings_booked,
            pulled_at = excluded.pulled_at
    """
    with engine.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)


def sync_daily_analytics(
    engine,
    *,
    start_date: Optional[date] = None,
    days: int = DEFAULT_DAYS,
    today: Optional[date] = None,
    api_key: Optional[str] = None,
    get: Optional[Callable[..., tuple[int, Any]]] = None,
) -> dict[str, Any]:
    """Upsert daily rows. An empty table backfills from 2026-10-01."""

    today = today or datetime.now(timezone.utc).date()
    out: dict[str, Any] = {
        "ok": False, "upserted": 0, "campaigns": 0, "reason": "",
        "start_date": "", "end_date": today.isoformat(),
    }
    if engine is None:
        out["reason"] = "No database."
        return out
    try:
        ensure_table(engine)
        empty = row_count(engine) == 0
    except Exception:  # noqa: BLE001
        logger.exception("[instantly-analytics] could not read the daily table")
        out["reason"] = "Could not read the daily table."
        return out
    start, end = _window(today, start_date=start_date, days=days, empty=empty and start_date is None)
    if start > end:
        out["reason"] = "start_date is after today."
        return out
    out["start_date"] = start.isoformat()
    out["end_date"] = end.isoformat()
    if api_key is None:
        import outbound_instantly_push as push
        api_key = push.api_key()
    if not api_key:
        out["reason"] = "INSTANTLY_API_KEY is not set."
        return out
    if get is None:
        get = _requests_get(api_key)

    campaigns, problem = _campaigns(get)
    if problem:
        out["reason"] = problem
        return out
    out["campaigns"] = len(campaigns)
    pulled_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for campaign in campaigns:
        status, payload = get(DAILY_PATH, {
            "campaign_id": campaign["id"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        })
        if status != 200:
            out["reason"] = f"daily analytics returned {status}"
            return out
        for item in _items(payload):
            shaped = normalize_daily_row(
                item, campaign_id=campaign["id"], campaign_name=campaign["name"],
                pulled_at=pulled_at,
            )
            if shaped is not None:
                rows.append(shaped)
    try:
        out["upserted"] = _upsert(engine, rows)
    except Exception:  # noqa: BLE001
        logger.exception("[instantly-analytics] upsert failed")
        out["reason"] = "Could not save daily analytics."
        return out
    out["ok"] = True
    return out


def maybe_backfill_if_empty(engine, **kwargs: Any) -> dict[str, Any]:
    """Backfill from 2026-10-01 once. Later calls leave the table alone."""

    try:
        ensure_table(engine)
        if row_count(engine) > 0:
            return {"ok": True, "ran": False, "reason": "already loaded", "upserted": 0}
    except Exception:  # noqa: BLE001
        logger.exception("[instantly-analytics] could not check for a backfill")
        return {"ok": False, "ran": False, "reason": "Could not read the daily table.", "upserted": 0}
    result = sync_daily_analytics(engine, start_date=BACKFILL_START, **kwargs)
    result["ran"] = bool(result.get("ok"))
    return result


def _requests_get(api_key: str) -> Callable[..., tuple[int, Any]]:
    import requests

    def get(path: str, params: dict[str, Any]) -> tuple[int, Any]:
        try:
            response = requests.get(
                f"https://api.instantly.ai{path}",
                params=params,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=60,
            )
            try:
                body = response.json() if response.content else None
            except ValueError:
                body = None
            return response.status_code, body
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[instantly-analytics] %s failed: %s",
                path,
                str(exc).replace(api_key, "<instantly-key>"),
            )
            return 0, None

    return get
