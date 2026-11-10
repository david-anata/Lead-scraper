"""Source brands for active campaigns from their saved StoreLeads filters.

Each active campaign with filters is queried server-side. Domains already
in ``outbound_contacted_domains`` are skipped. New leads are stored with
that campaign's recipe key. Volume stays inside the daily company cap
(300) and stops when the daily credit cap (1100) is already spent.
Each campaign also stops at its own ``daily_cap``. When those caps add up
to more than the room left in the 300, the room is shared so a later
recipe such as ``replatform`` still receives leads. Sourcing does not
call Prospeo and does not call Instantly.
New domains still have to pass the company-fit gate (US, own-brand
physical product, revenue on file, niche and exclusion screens).

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
import outbound_pipeline as pipeline

logger = logging.getLogger(__name__)

COMPANY_CAP = 300
CREDIT_CAP = 1100
_SOURCE_ORDER = ("amz_on", "amz_off", "plan_upgrade", "replatform")
_ENRICH_FIRST = ("amz_on", "plan_upgrade", "replatform")
_MAX_PAGES = 5
# About $1M a year, the ICP floor. The live amz_on row is $10k-$20k a month.
# That band produced 4 verified buyers out of 166 enriched companies on Nov 6.
# Raising only the ceiling reaches the ICP door. The floor, amz_off, and the
# 300 company cap stay put. This does not write the database.
AMZ_ON_MONTHLY_MAX_CENTS = 8_333_333
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
    import outbound_contacts as contacts

    domain = contacts.company_domain(str(store.get("name") or ""))
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
        "niche": pipeline.store_niche(store),
        "estimated_sales_yearly_cents": yearly if isinstance(yearly, (int, float)) else 0,
        "store_profile": pipeline.store_profile(store),
    }


def _platform_queries(filters: dict[str, Any]) -> list[dict[str, Any]]:
    """One List Domains query per previous platform.

    f:last_platform is mutually exclusive. A comma list returned an empty
    200 for the replatform recipe on Oct 28, Oct 30, Nov 4, and Nov 6.
    StoreLeads documents one value, or several values with op=or. Sending
    one documented value per call is the repair that does not invent rows.
    """

    raw = str(filters.get("f:last_platform") or "")
    if "," not in raw:
        return [filters]
    queries = []
    for platform in raw.split(","):
        platform = platform.strip().lower()
        if not platform:
            continue
        query = dict(filters)
        query["f:last_platform"] = platform
        query.pop("f:last_platform:op", None)
        queries.append(query)
    return queries or [filters]


def _widen_amz_on(recipe_key: str, filters: dict[str, Any]) -> dict[str, Any]:
    """Raise only the amz_on monthly ceiling, and only when it is still narrow."""

    if recipe_key != "amz_on":
        return filters
    try:
        current = int(filters.get("f:ermax") or 0)
    except (TypeError, ValueError):
        return filters
    if current <= 0 or current >= AMZ_ON_MONTHLY_MAX_CENTS:
        return filters
    widened = dict(filters)
    widened["f:ermax"] = AMZ_ON_MONTHLY_MAX_CENTS
    return widened


def _seen_companies(contacted: Optional[set[str]]) -> set[str]:
    """Contacted hosts plus their company keys, so a subdomain is not a new brand."""

    import outbound_contacts as contacts

    seen: set[str] = set()
    for item in contacted or set():
        raw = str(item or "").strip().lower()
        if not raw:
            continue
        seen.add(raw)
        company = contacts.company_domain(raw)
        if company:
            seen.add(company)
    return seen


def _ordered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {key: index for index, key in enumerate(_SOURCE_ORDER)}
    return sorted(rows, key=lambda row: (rank.get(str(row.get("recipe_key")), len(rank)), str(row.get("recipe_key"))))


def share_company_room(caps: list[int], room: int) -> list[int]:
    """Split ``room`` across per-campaign caps.

    When the caps fit, each campaign gets its cap. When they do not, the
    room is split as evenly as possible without passing a cap. Earlier
    campaigns receive a remainder of one before later ones. A campaign is
    left at zero only when ``room`` is smaller than the number of campaigns
    still waiting.

    Five seeded campaigns at a ``daily_cap`` of 100 would be 500 companies.
    ``abby_reengage`` has no StoreLeads filters, so it never takes room.
    The other four at 100 are 400, which does not fit in the 300 ceiling.
    Sharing that 300 gives each of those four 75, including ``replatform``.
    Paying every sourcing cap in full would mean raising ``COMPANY_CAP``.
    """

    safe = [max(0, int(cap)) for cap in caps]
    if room <= 0 or not safe:
        return [0] * len(safe)
    if sum(safe) <= room:
        return safe
    assigned = [0] * len(safe)
    left = int(room)
    while left > 0:
        open_idx = [index for index, cap in enumerate(safe) if assigned[index] < cap]
        if not open_idx:
            break
        share = left // len(open_idx)
        if share == 0:
            for index in open_idx:
                if left <= 0:
                    break
                assigned[index] += 1
                left -= 1
            break
        for index in open_idx:
            take = min(safe[index] - assigned[index], share)
            assigned[index] += take
            left -= take
    return assigned


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
        "skipped_unfit": 0,
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

    seen = _seen_companies(contacted)
    # Shares are locked up front. A campaign that finds nothing does not
    # hand its room to amz_off. Unused room stays inside the 300 ceiling
    # for the recipes that follow this sourcing pass.
    pending = _ordered(rows)
    shares = share_company_room(
        [int(row.get("daily_cap") or 0) for row in pending], room,
    )
    for row, allowance in zip(pending, shares):
        allowance = min(int(allowance), room)
        if allowance <= 0 or room <= 0:
            continue
        filters = _widen_amz_on(
            str(row["recipe_key"]),
            campaigns.resolve_filters(row.get("storeleads_filters"), moment),
        )
        if not filters:
            continue
        cap = allowance
        fresh: list[dict[str, Any]] = []
        scanned = 0
        skipped = 0
        unfit = 0
        error = ""
        pages_left = _MAX_PAGES
        for variant in _platform_queries(filters):
            cursor = None
            while pages_left > 0 and len(fresh) < cap:
                pages_left -= 1
                try:
                    page = fetch(api_key, extra_params=variant, cursor=cursor, page_size=100)
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)[:200]
                    logger.exception("[outbound-campaign-source] StoreLeads failed for %s", row["recipe_key"])
                    break
                domains, cursor, page_error = _domains_from(page)
                if page_error:
                    error = page_error
                    break
                for store in domains:
                    scanned += 1
                    lead = _lead_from(store, str(row["recipe_key"]))
                    if lead is None:
                        continue
                    if lead["domain"] in seen:
                        skipped += 1
                        continue
                    if not pipeline.passes_company_fit(store):
                        unfit += 1
                        continue
                    seen.add(lead["domain"])
                    fresh.append(lead)
                    if len(fresh) >= cap:
                        break
                if not cursor:
                    break
            if error or len(fresh) >= cap:
                break
        saved = 0
        if fresh:
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
        out["skipped_unfit"] += unfit
        if kept:
            out["recipes"].append(row["recipe_key"])
        out["campaigns"].append({
            "recipe_key": row["recipe_key"],
            "campaign_id": row["campaign_id"],
            "recorded": len(kept),
            "scanned": scanned,
            "skipped_seen": skipped,
            "skipped_unfit": unfit,
            "filters": filters,
            "error": error,
        })
    return out


def source_sales_org(
    engine: Any,
    *,
    api_key: str,
    now: Optional[datetime] = None,
    contacted: Optional[set[str]] = None,
    company_room: Optional[int] = None,
    fetch: Optional[Callable[..., Any]] = None,
    record: Optional[Callable[..., int]] = None,
) -> dict[str, Any]:
    """Pull US brands for the sales-org segment. Off unless the flag and campaign id are set.

    No category filter. Company fit still requires a US store, revenue on
    file, and the retailer and exclusion screens. This does not create an
    Instantly campaign and does not add a lead.
    """

    import outbound_instantly_push as push

    out: dict[str, Any] = {
        "recorded": 0, "leads": [], "recipes": [], "scanned": 0,
        "skipped_seen": 0, "skipped_unfit": 0, "stopped_reason": "",
    }
    if not push.sales_org_enabled():
        out["stopped_reason"] = "OUTBOUND_SALES_ORG_ENABLED is not true"
        return out
    if not push.sales_org_campaign_id():
        out["stopped_reason"] = "OUTBOUND_SALES_ORG_CAMPAIGN_ID is empty"
        return out
    if engine is None:
        out["stopped_reason"] = "no database"
        return out
    if not (api_key or "").strip():
        out["stopped_reason"] = "STORELEADS_API_KEY is not set"
        return out
    room = 25 if company_room is None else min(25, int(company_room))
    if room <= 0:
        out["stopped_reason"] = "daily company cap reached"
        return out
    if fetch is None:
        from outbound_pipeline import fetch_storeleads_result
        fetch = fetch_storeleads_result
    if record is None:
        from sales_support_agent.services.outbound_memory import record_leads

        def record(leads: list[dict[str, Any]], recipe_key: str) -> int:
            return record_leads(engine, leads, source=f"campaign:{recipe_key}")
    seen = _seen_companies(contacted)
    fresh: list[dict[str, Any]] = []
    cursor = None
    filters = {
        "f:p": "shopify", "f:ds": "Active", "f:it": "email",
        "f:cc": "US", "f:cc:op": "or",
    }
    for _page in range(3):
        if len(fresh) >= room:
            break
        try:
            page = fetch(api_key, extra_params=filters, cursor=cursor, page_size=100)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-campaign-source] sales-org StoreLeads failed")
            out["stopped_reason"] = "StoreLeads failed"
            break
        domains, cursor, error = _domains_from(page)
        if error:
            out["stopped_reason"] = error
            break
        for store in domains:
            out["scanned"] += 1
            lead = _lead_from(store, "sales_org")
            if lead is None:
                continue
            if lead["domain"] in seen:
                out["skipped_seen"] += 1
                continue
            if not pipeline.passes_company_fit(store, require_niche=False):
                out["skipped_unfit"] += 1
                continue
            seen.add(lead["domain"])
            fresh.append(lead)
            if len(fresh) >= room:
                break
        if not cursor:
            break
    if fresh:
        try:
            saved = int(record(fresh, "sales_org") or 0)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-campaign-source] could not record sales_org")
            saved = 0
        kept = fresh[:saved] if saved else []
        out["recorded"] = len(kept)
        out["leads"] = kept
        if kept:
            out["recipes"] = ["sales_org"]
    return out
