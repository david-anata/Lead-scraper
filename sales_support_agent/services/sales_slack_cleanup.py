"""One-off removal of this bot's old outbound status posts in #sales.

The Sales Agent app can delete its own messages with ``chat.delete`` and its
own bot token. Dry-run is the default. A post is eligible only when its first
line is exactly ``Outbound contacts (replaces Clay)`` and the author is this
bot. Daily Leads, send failures, held replies, and nudges stay.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)

SALES_CHANNEL = "CQRMXS339"
BOT_USER = "U0AK49FRQ3D"
BOT_APP = "B0AJV7Q9TNK"
STATUS_HEADER = "Outbound contacts (replaces Clay)"
LOOKBACK_DAYS = 30
PAGE_LIMIT = 200
MAX_PAGES = 20
DELETE_PAUSE_SECONDS = 1.1

HISTORY_URL = "https://slack.com/api/conversations.history"
DELETE_URL = "https://slack.com/api/chat.delete"


def is_outbound_status_post(message: dict[str, Any]) -> bool:
    """True only for this bot's hourly status post, not a quote or another alert."""

    if not isinstance(message, dict):
        return False
    if str(message.get("subtype") or "") in {"channel_join", "channel_leave", "tombstone"}:
        return False
    thread_ts = str(message.get("thread_ts") or "")
    ts = str(message.get("ts") or "")
    if thread_ts and ts and thread_ts != ts:
        return False
    first = str(message.get("text") or "").split("\n", 1)[0].strip()
    if first != STATUS_HEADER:
        return False
    user = str(message.get("user") or "")
    bot_id = str(message.get("bot_id") or "")
    if user and user != BOT_USER:
        return False
    if bot_id and bot_id != BOT_APP:
        return False
    return bool(user == BOT_USER or bot_id == BOT_APP)


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _call(method: str, url: str, *, token: str, payload: dict[str, Any],
          sleep: Callable[[float], None]) -> dict[str, Any]:
    """One Slack Web API call. Honor Retry-After, then surface the error."""

    for attempt in range(3):
        response = requests.request(
            method, url, headers=_headers(token),
            params=payload if method == "GET" else None,
            json=payload if method != "GET" else None,
            timeout=30,
        )
        if response.status_code == 429 and attempt < 2:
            retry_after = response.headers.get("Retry-After", "2")
            try:
                wait = min(30.0, max(1.0, float(retry_after)))
            except ValueError:
                wait = 2.0
            sleep(wait)
            continue
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Slack returned a non-object response")
        return data
    raise RuntimeError("Slack rate limit did not clear")


def _default_history(token: str, channel: str, oldest: str, cursor: str,
                     sleep: Callable[[float], None]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "channel": channel, "oldest": oldest, "limit": PAGE_LIMIT, "inclusive": "true",
    }
    if cursor:
        payload["cursor"] = cursor
    return _call("GET", HISTORY_URL, token=token, payload=payload, sleep=sleep)


def _default_delete(token: str, channel: str, ts: str,
                    sleep: Callable[[float], None]) -> dict[str, Any]:
    return _call("POST", DELETE_URL, token=token, payload={"channel": channel, "ts": ts}, sleep=sleep)


def cleanup_outbound_status_posts(
    *,
    token: str,
    now: datetime | None = None,
    apply: bool = False,
    channel: str = SALES_CHANNEL,
    days: int = LOOKBACK_DAYS,
    history: Callable[..., dict[str, Any]] | None = None,
    delete: Callable[..., dict[str, Any]] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """List matching posts. Delete them only when ``apply`` is true.

    ``history(token, channel, oldest, cursor, sleep)`` and
    ``delete(token, channel, ts, sleep)`` are injectable so tests never call Slack.
    """

    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    oldest_dt = moment - timedelta(days=max(1, days))
    oldest = f"{oldest_dt.timestamp():.6f}"
    pause = sleep or time.sleep
    read = history or _default_history
    remove = delete or _default_delete
    result: dict[str, Any] = {
        "dry_run": not apply,
        "channel": channel,
        "bot_user": BOT_USER,
        "header": STATUS_HEADER,
        "since": oldest_dt.isoformat(),
        "matched": 0,
        "deleted": 0,
        "failed": 0,
        "truncated": False,
        "posts": [],
        "errors": [],
    }
    if not token:
        result["errors"].append("Slack bot token is not configured")
        return result

    cursor = ""
    matched: list[dict[str, str]] = []
    for _page in range(MAX_PAGES):
        page = read(token, channel, oldest, cursor, pause)
        if not page.get("ok"):
            result["errors"].append(str(page.get("error") or "history failed"))
            break
        for message in page.get("messages") or []:
            if not is_outbound_status_post(message):
                continue
            matched.append({
                "ts": str(message.get("ts") or ""),
                "text": STATUS_HEADER,
            })
        cursor = str((page.get("response_metadata") or {}).get("next_cursor") or "")
        if not cursor:
            break
    else:
        result["truncated"] = True
    result["matched"] = len(matched)
    result["posts"] = matched
    if not apply or result["errors"]:
        return result

    for post in matched:
        pause(DELETE_PAUSE_SECONDS)
        try:
            outcome = remove(token, channel, post["ts"], pause)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[sales-slack-cleanup] delete failed ts=%s", post["ts"])
            result["failed"] += 1
            result["errors"].append(f"{post['ts']}: {exc}")
            continue
        if outcome.get("ok"):
            result["deleted"] += 1
        else:
            result["failed"] += 1
            result["errors"].append(f"{post['ts']}: {outcome.get('error') or 'delete failed'}")
    logger.info(
        "[sales-slack-cleanup] channel=%s matched=%s deleted=%s failed=%s dry_run=%s",
        channel, result["matched"], result["deleted"], result["failed"], result["dry_run"],
    )
    return result
