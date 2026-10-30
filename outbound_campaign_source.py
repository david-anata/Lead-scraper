"""Source brands for active campaigns from their saved StoreLeads filters.

Each active campaign with filters is queried server-side. Domains already
in ``outbound_contacted_domains`` are skipped. New leads are stored with
that campaign's recipe key. Volume stays inside the daily company cap
(300) and stops when the daily credit cap (1100) is already spent.
Sourcing does not call Prospeo and does not call Instantly.

The morning routine runs this before the older recipe pulls so ``amz_on``
and ``amz_off`` are filled from these filters.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, time as dtime, timezone
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

import outbound_campaigns as campaigns

logger = logging.getLogger(__name__)

COMPANY_CAP = 300
CREDIT_CAP = 1100
_SOURCE_ORDER = ("amz_on", "amz_off", "plan_upgrade", "replatform")
_MAX_PAGES = 5
_DENVER = ZoneInfo("America/Denver")

_REASONS = {
    "amz_on": "They sell on Amazon",
    "amz_off": "They do not sell on Amazon",
    "plan_upgrade": "They upgraded their store plan recently",
    "replatform": "They moved platforms recently",
}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _start_of_denver_day_utc(now: datetime) -> datetime:
    local = now.astimezone(_DENVER) if now.tzinfo else now.replace(tzinfo=_DENVER)
    midnight = datetime.combine(local.date(), dtime.min, tzinfo=_DENVER)
    return midnight.astimezone(timezone.utc).replace(tzinfo=None)


def company_room_today(
    engine: Any,
    now: datetime,
    *,
    company_room: Optional[int] = None,
) -> tuple[int, str]:
    """How many new companies sourcing may record right now.

    The ceiling is the smaller of 300 and ``OUTBOUND_ENRICH_DAILY_COMPANIES``
    when that variable is set, minus companies already enriched today.
    When today's Prospeo credits are already at the 1100 cap, the room is
    zero. Sourcing itself does not spend credits.
    """

    company_cap = min(COMPANY_CAP, _env_int("OUTBOUND_ENRICH_DAILY_COMPANIES", COMPANY_CAP))
    credit_cap = min(CREDIT_CAP, _env_int("OUTBOUND_ENRICH_DAILY_CREDITS", CREDIT_CAP))
    try:
        import outbound_enrich as enrich

        today = enrich.enriched_since(engine, _start_of_denver_day_utc(now))
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-campaign-source] could not read today's caps")
        return 0, "could not read today's caps"
    if int(today.get("credits") or 0) >= credit_cap:
        return 0, "daily credit cap reached"
    remaining = company_cap - int(today.get("companies") or 0)
    if company_room is not None:
        remaining = min(remaining, int(company_room))
    if remaining <= 0:
        return 0, "daily company cap reached"
    return remaining, ""


def _domains_from(page: Any) -> tuple[list[dict[str, Any]], Optional[str], str]:
    if isinstance(page, list):
        return [item for item in page if isinstance(item, dict)], None, ""
    domains = getattr(page, "domains", None) or []
    cursor = str(getattr(page, "next_cursor", "") or "").strip() or None
    error = str(getattr(page, "error", "") or "")
    return [item for item in domains if isinstance(item, dict)], cursor, error


def _lead_from(store: dict[str, Any], recipe_key: str) -> Optional[dict[str, Any]]:
    domain = str(store.get("name") or "").strip().lower()
    if "." not in domain or " " in domain:
        return None
    yearly = store.get("estimated_sales_yearly")
    return {
        "domain": domain,
        "brand": str(store.get("merchant_name") or store.get("name") or domain).strip(),
        "country": str(store.get("country_code") or "").strip().upper(),
        "recipe": recipe_key,
        "tier": "B",
        "score": 0,
        "reason": _REASONS.get(recipe_key, "They match this campaign's StoreLeads filters"),
        "signals": [recipe_key],
        "categories": store.get("categories") or "",
        "niche": "",
        "estimated_sales_yearly_cents": yearly if isinstance(yearly, (int, float)) else 0,
    }


def _ordered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {key: index for index, key in enumerate(_SOURCE_ORDER)}
    return sorted(rows, key=lambda row: (rank.get(str(row.get("recipe_key")), len(rank)), str(row.get("recipe_key"))))


def source_active_campaigns(
    engine: Any,
    *,
    api_key: str,
    now: Optional[datetime] = None,
    contacted: Optional[set[str]] = None,
    company_room: Optional[int] = None,
    fetch: Optional[Callable[..., Any]] = None,
    record: Optional[Callable[..., int]] = None,
) -> dict[str, Any]:
    """Pull and record leads for each active campaign that has filters.

    ``fetch`` is the List Domains call (``fetch_storeleads_result`` by
    default) so tests never need a live key. ``record`` writes the library
    and defaults to ``outbound_contacted_domains``.
    """

    moment = now or datetime.now(_DENVER)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_DENVER)
    out: dict[str, Any] = {
        "recorded": 0,
        "leads": [],
        "recipes": [],
        "scanned": 0,
        "skipped_seen": 0,
        "campaigns": [],
        "stopped_reason": "",
    }
    if engine is None:
        out["stopped_reason"] = "no database"
        return out
    campaigns.ensure_tables(engine)
    if contacted is None:
        try:
            from sales_support_agent.services.outbound_memory import load_contacted
            contacted = load_contacted(engine)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-campaign-source] could not read contacted domains")
            contacted = set()
    if not (api_key or "").strip():
        out["stopped_reason"] = "STORELEADS_API_KEY is not set"
        return out
    room, reason = company_room_today(engine, moment, company_room=company_room)
    if room <= 0:
        out["stopped_reason"] = reason or "daily company cap reached"
        return out

    rows = [
        row for row in campaigns.list_campaigns(engine)
        if row.get("status") == "active" and int(row.get("daily_cap") or 0) > 0
        and row.get("storeleads_filters")
    ]
    if not rows:
        out["stopped_reason"] = "no active campaign filters"
        return out

    if fetch is None:
        from outbound_pipeline import fetch_storeleads_result
        fetch = fetch_storeleads_result
    if record is None:
        from sales_support_agent.services.outbound_memory import record_leads

        def record(leads: list[dict[str, Any]], recipe_key: str) -> int:
            return record_leads(engine, leads, source=f"campaign:{recipe_key}")

    seen = set(contacted or set())
    for row in _ordered(rows):
        if room <= 0:
            break
        filters = campaigns.resolve_filters(row.get("storeleads_filters"), moment)
        if not filters:
            continue
        cap = min(int(row["daily_cap"]), room)
        fresh: list[dict[str, Any]] = []
        cursor: Optional[str] = None
        scanned = 0
        skipped = 0
        error = ""
        for _page in range(_MAX_PAGES):
            if len(fresh) >= cap:
                break
            try:
                page = fetch(api_key, extra_params=filters, cursor=cursor, page_size=100)
            except Exception as exc:  # noqa: BLE001
                error = str(exc)[:200]
                logger.exception("[outbound-campaign-source] StoreLeads failed for %s", row["recipe_key"])
                break
            domains, cursor, error = _domains_from(page)
            if error:
                break
            for store in domains:
                scanned += 1
                lead = _lead_from(store, str(row["recipe_key"]))
                if lead is None:
                    continue
                if lead["domain"] in seen:
                    skipped += 1
                    continue
                seen.add(lead["domain"])
                fresh.append(lead)
                if len(fresh) >= cap:
                    break
            if not cursor:
                break
        saved = 0
        if fresh and not error:
            try:
                saved = int(record(fresh, str(row["recipe_key"])) or 0)
            except Exception:  # noqa: BLE001
                logger.exception("[outbound-campaign-source] could not record %s", row["recipe_key"])
                saved = 0
            if saved <= 0:
                error = error or "could not record leads"
                saved = 0
        if saved <= 0:
            for lead in fresh:
                seen.discard(lead["domain"])
            fresh = []
        kept = fresh[:saved] if saved else []
        room -= len(kept)
        out["recorded"] += len(kept)
        out["leads"].extend(kept)
        out["scanned"] += scanned
        out["skipped_seen"] += skipped
        if kept:
            out["recipes"].append(row["recipe_key"])
        out["campaigns"].append({
            "recipe_key": row["recipe_key"],
            "campaign_id": row["campaign_id"],
            "recorded": len(kept),
            "scanned": scanned,
            "skipped_seen": skipped,
            "filters": filters,
            "error": error,
        })
    return out
