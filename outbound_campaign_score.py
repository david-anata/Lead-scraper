"""Monday score of each outbound campaign.

Reads ``instantly_campaign_daily`` when that table exists, and
``abby_handled.campaign_id`` when that column exists. Writes one row per
campaign to ``outbound_campaign_scores``. A campaign with fewer than 150
contacted is not judged.

Boost, throttle, and cut are proposals on the score row. A new trait
combination is inserted only as ``status='proposed'``. This module does
not activate a campaign, change ``daily_cap``, or call Instantly.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import inspect, text

import outbound_campaigns as campaigns

logger = logging.getLogger(__name__)

_DENVER = ZoneInfo("America/Denver")
_SAFE_COLUMN = re.compile(r"^[a-z][a-z0-9_]*$")
_DAILY = "instantly_campaign_daily"
_ABBY = "abby_handled"

_CONTACTED = ("contacted", "sent", "emails_sent", "leads_contacted")
_POSITIVE = ("positive", "positive_replies", "interested")
_MEETINGS = ("meetings", "meetings_booked", "meeting_booked")
_BOUNCE = ("bounced", "bounces", "bounce")
_OPT_OUT = ("opt_out", "opt_outs", "unsubscribed", "optouts")
_DAY = ("day", "stat_date", "bucket_date", "date")
_POSITIVE_CATEGORIES = frozenset({"interested", "price", "referral", "question"})


def _columns(engine: Any, table: str) -> set[str]:
    from sales_support_agent.services.outbound_ddl import relation_exists

    if not relation_exists(engine, table):
        return set()
    try:
        return {str(column["name"]) for column in inspect(engine).get_columns(table)}
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-campaign-score] could not inspect %s", table)
        return set()


def _pick(columns: set[str], options: tuple[str, ...]) -> Optional[str]:
    for name in options:
        if name in columns and _SAFE_COLUMN.match(name):
            return name
    return None


def _quote(name: str) -> str:
    """Quote a whitelisted column. ``date`` is reserved in Postgres."""

    return '"' + name.replace('"', "") + '"'


def previous_week(now: datetime) -> tuple[str, datetime, datetime]:
    """The Monday-to-Monday window that just finished, in America/Denver.

    Returns the week-start date (``YYYY-MM-DD``), and the start and end
    instants. End is exclusive.
    """

    local = now.astimezone(_DENVER) if now.tzinfo else now.replace(tzinfo=_DENVER)
    this_monday = local.date() - timedelta(days=local.weekday())
    start_date = this_monday - timedelta(days=7)
    start = datetime.combine(start_date, dtime.min, tzinfo=_DENVER)
    end = datetime.combine(this_monday, dtime.min, tzinfo=_DENVER)
    return start_date.isoformat(), start, end


def _rate(count: Optional[float], contacted: int) -> Optional[float]:
    if count is None or contacted <= 0:
        return None
    return round(float(count) / contacted * 100, 2)


def _sum_daily(
    engine: Any,
    columns: set[str],
    start_day: str,
    end_day: str,
) -> tuple[dict[str, dict[str, Optional[float]]], list[str]]:
    """Sum one week of ``instantly_campaign_daily`` by campaign.

    Missing metric columns stay None so the score does not invent a zero.
    """

    notes: list[str] = []
    contacted = _pick(columns, _CONTACTED)
    if not contacted:
        return {}, ["instantly_campaign_daily has no contacted column"]
    wanted = {
        "contacted": contacted,
        "positive": _pick(columns, _POSITIVE),
        "meetings": _pick(columns, _MEETINGS),
        "bounced": _pick(columns, _BOUNCE),
        "opt_out": _pick(columns, _OPT_OUT),
    }
    for label, column in wanted.items():
        if column is None and label != "contacted":
            notes.append(f"instantly_campaign_daily has no {label} column")
    selects = [_quote("campaign_id")]
    for label, column in wanted.items():
        if column:
            selects.append(f"COALESCE(SUM({_quote(column)}), 0) AS {label}")
    day = _pick(columns, _DAY)
    where = ""
    params: dict[str, Any] = {}
    if day:
        quoted_day = _quote(day)
        where = (
            f"WHERE substr(CAST({quoted_day} AS TEXT), 1, 10) >= :start "
            f"AND substr(CAST({quoted_day} AS TEXT), 1, 10) < :end"
        )
        params = {"start": start_day, "end": end_day}
    else:
        notes.append("instantly_campaign_daily has no day column; summed every row")
    sql = f"SELECT {', '.join(selects)} FROM {_DAILY} {where} GROUP BY {_quote('campaign_id')}"
    totals: dict[str, dict[str, Optional[float]]] = {}
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        keys = list(result.keys())
        for row in result.fetchall():
            item = dict(zip(keys, row))
            campaign_id = str(item.get("campaign_id") or "").strip()
            if not campaign_id:
                continue
            totals[campaign_id] = {
                label: (float(item[label]) if label in item else None)
                for label in ("contacted", "positive", "meetings", "bounced", "opt_out")
            }
            if "contacted" not in item:
                totals[campaign_id]["contacted"] = None
    return totals, notes


def _abby_counts(
    engine: Any,
    columns: set[str],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, dict[str, int]], list[str]]:
    """Positive, meeting, and opt-out counts from Abby, by campaign.

    An absent ``campaign_id`` column is reported and not guessed from the
    lead address.
    """

    if "campaign_id" not in columns:
        return {}, ["abby_handled.campaign_id is not on the table yet"]
    # Abby stores CURRENT_TIMESTAMP. Compare it to naive UTC strings.
    start_naive = start.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    end_naive = end.astimezone(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    sql = text(
        f"""
        SELECT campaign_id, category, action, detail
        FROM {_ABBY}
        WHERE created_at >= :start AND created_at < :end
        """
    )
    counts: dict[str, dict[str, int]] = {}
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start": start_naive, "end": end_naive}).fetchall()
    for campaign_id, category, action, detail in rows:
        key = str(campaign_id or "").strip()
        if not key:
            continue
        bucket = counts.setdefault(key, {"positive": 0, "meetings": 0, "opt_out": 0})
        cat = str(category or "").strip().lower()
        if cat in _POSITIVE_CATEGORIES:
            bucket["positive"] += 1
        if cat == "opt_out":
            bucket["opt_out"] += 1
        blob = f"{action or ''} {detail or ''}".lower()
        if "meeting booked" in blob or str(action or "").strip().lower() == "meeting":
            bucket["meetings"] += 1
    return counts, []


def _upsert_score(engine: Any, row: dict[str, Any]) -> None:
    sql = text(
        f"""
        INSERT INTO {campaigns.SCORES} (
            campaign_id, week_start, contacted, positive_per_100, meetings_per_100,
            bounce_pct, opt_out_pct, judged, action, detail
        ) VALUES (
            :campaign_id, :week_start, :contacted, :positive_per_100, :meetings_per_100,
            :bounce_pct, :opt_out_pct, :judged, :action, :detail
        )
        ON CONFLICT (campaign_id, week_start) DO UPDATE SET
            contacted = :contacted,
            positive_per_100 = :positive_per_100,
            meetings_per_100 = :meetings_per_100,
            bounce_pct = :bounce_pct,
            opt_out_pct = :opt_out_pct,
            judged = :judged,
            action = :action,
            detail = :detail
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, row)


def run_weekly_score(
    engine: Any,
    *,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Score the week that just finished. Never raises.

    Does not update ``outbound_campaigns.status`` or ``daily_cap`` except
    by inserting new ``proposed`` rows.
    """

    moment = now or datetime.now(_DENVER)
    result: dict[str, Any] = {
        "ran": False,
        "blocked": [],
        "week_start": "",
        "scores": [],
        "proposed": [],
        "reason": "",
    }
    if engine is None:
        result["reason"] = "no database"
        result["blocked"].append("database")
        return result
    try:
        campaigns.ensure_tables(engine)
        daily_columns = _columns(engine, _DAILY)
        if not daily_columns:
            result["blocked"].append(_DAILY)
            result["reason"] = (
                "instantly_campaign_daily does not exist yet. "
                "No scores were invented."
            )
            logger.info("[outbound-campaign-score] %s", result["reason"])
            return result
        if "campaign_id" not in daily_columns:
            result["blocked"].append(f"{_DAILY}.campaign_id")
            result["reason"] = "instantly_campaign_daily has no campaign_id column"
            return result

        week_start, start, end = previous_week(moment)
        result["week_start"] = week_start
        totals, notes = _sum_daily(engine, daily_columns, week_start, end.date().isoformat())
        if notes and notes[0].startswith("instantly_campaign_daily has no contacted"):
            result["blocked"].append(f"{_DAILY}.contacted")
            result["reason"] = notes[0]
            return result
        abby_columns = _columns(engine, _ABBY)
        abby, abby_notes = _abby_counts(engine, abby_columns, start, end)
        notes.extend(abby_notes)

        parents = [
            row for row in campaigns.list_campaigns(engine)
            if row.get("status") in {"active", "throttled", "cut"}
        ]
        by_id = {str(row["campaign_id"]): row for row in parents}
        campaign_ids = list(dict.fromkeys([*by_id.keys(), *totals.keys()]))
        for campaign_id in campaign_ids:
            parent = by_id.get(campaign_id)
            if parent is None and campaign_id not in totals:
                continue
            if parent is None:
                # Analytics for a campaign we do not own. Score it, do not
                # invent a variant for it.
                parent = {
                    "campaign_id": campaign_id,
                    "name": "",
                    "recipe_key": "",
                    "storeleads_filters": None,
                    "status": "",
                    "daily_cap": 0,
                    "template_ref": "",
                }
            daily = totals.get(campaign_id, {})
            handled = abby.get(campaign_id, {})
            contacted = int(daily.get("contacted") or 0) if daily.get("contacted") is not None else 0
            # Instantly's daily columns are the rate numerator when they
            # exist. Abby fills a rate only when that column was not synced.
            if daily.get("positive") is not None:
                positive_count: Optional[float] = daily.get("positive")
            elif "campaign_id" in abby_columns:
                positive_count = float(handled.get("positive") or 0)
            else:
                positive_count = None
            if daily.get("meetings") is not None:
                meetings_count: Optional[float] = daily.get("meetings")
            elif "campaign_id" in abby_columns:
                meetings_count = float(handled.get("meetings") or 0)
            else:
                meetings_count = None
            if daily.get("opt_out") is not None:
                opt_count: Optional[float] = daily.get("opt_out")
            elif "campaign_id" in abby_columns:
                opt_count = float(handled.get("opt_out") or 0)
            else:
                opt_count = None
            bounce_count = daily.get("bounced")
            positive_per_100 = _rate(positive_count, contacted)
            meetings_per_100 = _rate(meetings_count, contacted)
            bounce_pct = _rate(bounce_count, contacted)
            opt_out_pct = _rate(opt_count, contacted)
            judged, action = campaigns.judge_metrics(
                contacted, positive_per_100, meetings_per_100, bounce_pct, opt_out_pct,
            )
            abby_note = ""
            if "campaign_id" in abby_columns:
                abby_note = (
                    f"Abby classified positive={handled.get('positive', 0)} "
                    f"meetings={handled.get('meetings', 0)} "
                    f"opt_out={handled.get('opt_out', 0)}."
                )
            detail = " ".join(part for part in (*notes, abby_note) if part).strip()
            detail = (
                f"{detail} Proposal only. David approves activation, volume, "
                "and any Instantly campaign."
            ).strip()
            score = {
                "campaign_id": campaign_id,
                "week_start": week_start,
                "contacted": contacted,
                "positive_per_100": positive_per_100,
                "meetings_per_100": meetings_per_100,
                "bounce_pct": bounce_pct,
                "opt_out_pct": opt_out_pct,
                "judged": 1 if judged else 0,
                "action": action,
                "detail": detail[:1000],
            }
            _upsert_score(engine, score)
            result["scores"].append(score)
            if judged and parent.get("storeleads_filters"):
                proposal = campaigns.variant_proposal(parent, action, week_start)
                if proposal is not None and campaigns.resolve_filters(proposal["storeleads_filters"], moment):
                    if campaigns.insert_proposed(engine, proposal):
                        result["proposed"].append({
                            "campaign_id": proposal["campaign_id"],
                            "recipe_key": proposal["recipe_key"],
                            "name": proposal["name"],
                            "action": action,
                        })
        result["ran"] = True
        result["reason"] = "scored"
        logger.info(
            "[outbound-campaign-score] week=%s scores=%s proposed=%s",
            week_start, len(result["scores"]), len(result["proposed"]),
        )
        return result
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-campaign-score] weekly score failed")
        result["reason"] = "weekly score failed"
        return result
