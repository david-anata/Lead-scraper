"""Source brands for active campaigns from their saved StoreLeads filters.

Each active campaign with filters is queried server-side. Domains already
in ``outbound_contacted_domains`` are skipped. New leads are stored with
that campaign's recipe key. Volume stays inside the daily company cap
(300) and stops when the daily credit cap (1100) is already spent.
When the active caps add up to more than that room, the room is shared
so a later recipe such as ``replatform`` still receives leads. No
campaign receives more than its own ``daily_cap``. Sourcing does not
call Prospeo and does not call Instantly.

The morning routine runs this before the older recipe pulls so the signal
campaigns are filled from these filters before baseline.
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


def ordered_campaigns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``amz_on``, ``amz_off``, ``plan_upgrade``, ``replatform``, then the rest."""

    return _ordered(rows)


def allocate_source_caps(caps: list[int], room: int) -> list[int]:
    """Share ``room`` across campaigns without passing any campaign's cap.

    Each campaign that still has cap left gets an equal share of the
    companies still unassigned. A campaign whose remaining cap is smaller
    than that share leaves the rest for the others. A single leftover
    company goes to the earliest campaign that can take it. When every
    cap fits inside ``room``, each campaign receives its full cap.

    Four caps of 100 inside a room of 300 yield 75 each, so ``replatform``
    is included. Five caps of 100 yield 60 each. The seeded 40, 40, 30,
    and 25 still receive those full caps inside a room of 300.
    """

    remaining = max(0, int(room))
    limits = [max(0, int(cap)) for cap in caps]
    assigned = [0 for _ in limits]
    while remaining > 0:
        open_indexes = [index for index, cap in enumerate(limits) if assigned[index] < cap]
        if not open_indexes:
            break
        share, extra = divmod(remaining, len(open_indexes))
        if share == 0:
            for index in open_indexes:
                if remaining <= 0:
                    break
                assigned[index] += 1
                remaining -= 1
            break
        before = remaining
        for offset, index in enumerate(open_indexes):
            if remaining <= 0:
                break
            want = share + (1 if offset < extra else 0)
            take = min(want, limits[index] - assigned[index], remaining)
            assigned[index] += take
            remaining -= take
        if remaining == before:
            break
    return assigned


def _blank_pull(row: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        "row": row,
        "filters": filters,
        "cursor": None,
        "offset": 0,
        "page": None,
        "next_cursor": None,
        "fetched_pages": 0,
        "recorded": 0,
        "scanned": 0,
        "skipped_seen": 0,
        "error": "",
        "exhausted": not bool(filters),
        "leads": [],
        "attempted": False,
    }


def _ensure_page(state: dict[str, Any], fetch: Callable[..., Any], api_key: str) -> bool:
    """Load the page the pull is reading. False when this recipe is done."""

    page = state["page"]
    if page is not None and state["offset"] < len(page):
        return True
    if state["fetched_pages"] >= _MAX_PAGES:
        state["exhausted"] = True
        return False
    if page is not None:
        if not state["next_cursor"]:
            state["exhausted"] = True
            return False
        state["cursor"] = state["next_cursor"]
        state["offset"] = 0
    try:
        fetched = fetch(
            api_key, extra_params=state["filters"], cursor=state["cursor"], page_size=100,
        )
    except Exception as exc:  # noqa: BLE001
        state["error"] = str(exc)[:200]
        state["exhausted"] = True
        logger.exception(
            "[outbound-campaign-source] StoreLeads failed for %s", state["row"]["recipe_key"],
        )
        return False
    domains, cursor, error = _domains_from(fetched)
    state["fetched_pages"] += 1
    if error:
        state["error"] = error
        state["exhausted"] = True
        state["page"] = []
        state["next_cursor"] = None
        return False
    state["page"] = domains
    state["next_cursor"] = cursor
    state["offset"] = 0
    if not domains:
        state["exhausted"] = True
        return False
    return True


def _take_leads(
    state: dict[str, Any],
    ask: int,
    fetch: Callable[..., Any],
    seen: set[str],
    api_key: str,
) -> list[dict[str, Any]]:
    """Take up to ``ask`` new leads, continuing a page that was only partly used."""

    fresh: list[dict[str, Any]] = []
    state["attempted"] = True
    while len(fresh) < ask and not state["exhausted"] and not state["error"]:
        if not _ensure_page(state, fetch, api_key):
            break
        page = state["page"] or []
        while state["offset"] < len(page) and len(fresh) < ask:
            store = page[state["offset"]]
            state["offset"] += 1
            state["scanned"] += 1
            lead = _lead_from(store, str(state["row"]["recipe_key"]))
            if lead is None:
                continue
            if lead["domain"] in seen:
                state["skipped_seen"] += 1
                continue
            seen.add(lead["domain"])
            fresh.append(lead)
        if state["offset"] >= len(page) and not state["next_cursor"] and len(fresh) < ask:
            state["exhausted"] = True
    return fresh


def _record_taken(
    state: dict[str, Any],
    fresh: list[dict[str, Any]],
    seen: set[str],
    record: Callable[..., int],
) -> list[dict[str, Any]]:
    """Persist one slice. A failed write puts those domains back."""

    saved = 0
    if fresh and not state["error"]:
        try:
            saved = int(record(fresh, str(state["row"]["recipe_key"])) or 0)
        except Exception:  # noqa: BLE001
            logger.exception("[outbound-campaign-source] could not record %s", state["row"]["recipe_key"])
            saved = 0
        if saved <= 0:
            state["error"] = state["error"] or "could not record leads"
            saved = 0
    if saved <= 0:
        for lead in fresh:
            seen.discard(lead["domain"])
        if fresh or state["error"]:
            state["exhausted"] = True
        return []
    kept = fresh[:saved]
    state["recorded"] += len(kept)
    state["leads"].extend(kept)
    return kept


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
    states = [
        _blank_pull(row, campaigns.resolve_filters(row.get("storeleads_filters"), moment))
        for row in ordered_campaigns(rows)
    ]
    while room > 0:
        runnable = [
            state for state in states
            if state["filters"] and not state["exhausted"] and not state["error"]
            and state["recorded"] < int(state["row"]["daily_cap"])
        ]
        if not runnable:
            break
        asks = allocate_source_caps(
            [int(state["row"]["daily_cap"]) - int(state["recorded"]) for state in runnable],
            room,
        )
        if sum(asks) <= 0:
            break
        progressed = False
        for state, ask in zip(runnable, asks):
            if ask <= 0 or room <= 0:
                continue
            fresh = _take_leads(state, ask, fetch, seen, api_key)
            if len(fresh) < ask:
                state["exhausted"] = True
            if state["error"]:
                for lead in fresh:
                    seen.discard(lead["domain"])
                state["exhausted"] = True
                progressed = True
                continue
            kept = _record_taken(state, fresh, seen, record)
            room -= len(kept)
            if kept or state["exhausted"] or state["error"] or not fresh:
                progressed = True
        if not progressed:
            break
    for state in states:
        if not state["filters"] or not state["attempted"]:
            continue
        kept = state["leads"]
        out["recorded"] += len(kept)
        out["leads"].extend(kept)
        out["scanned"] += int(state["scanned"])
        out["skipped_seen"] += int(state["skipped_seen"])
        if kept:
            out["recipes"].append(state["row"]["recipe_key"])
        out["campaigns"].append({
            "recipe_key": state["row"]["recipe_key"],
            "campaign_id": state["row"]["campaign_id"],
            "recorded": len(kept),
            "scanned": int(state["scanned"]),
            "skipped_seen": int(state["skipped_seen"]),
            "filters": state["filters"],
            "error": state["error"],
        })
    return out
