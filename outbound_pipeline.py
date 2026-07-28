"""StoreLeads -> Clay outbound pipeline.

Pulls ecommerce brands that match Anata's ICP from the StoreLeads API, dedupes
against brands already contacted, and pushes the fresh ones into a Clay table
(via Clay's inbound webhook). Clay then finds the decision-maker, verifies the
email, qualifies, personalizes, and hands off to Instantly.

This module deliberately does NOT send any email. It only sources brands and
hands them to Clay. The ICP gate here is the authoritative filter, matched to
the target already defined in the app and in docs/outbound/03-storeleads-filter-recipe.md.

Network calls (fetch_storeleads_page, push_to_clay) are thin and injectable so
the ICP logic can be tested without a live key. Live behaviour is verified once
STORELEADS_API_KEY and CLAY_WEBHOOK_URL are set on Render.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

STORELEADS_BASE_URL = "https://storeleads.app/json/api/v1/all"
STORELEADS_DOMAIN_PATH = "/domain"

# ---- ICP (mirrors the coded target + docs/outbound/03) -----------------------
ICP_MIN_YEARLY_SALES_CENTS = 1_000_000_00      # ~$1M/yr floor
ICP_MAX_YEARLY_SALES_CENTS = 20_000_000_00     # ~$20M/yr ceiling (David: "under 20M is who we want")
ICP_ALLOWED_COUNTRIES = {"US", "GB", "UK", "CA", "AU"}
ICP_ALLOWED_PLATFORMS = {"shopify"}

# StoreLeads category text -> our niche label (keyword match, lowercased)
ICP_NICHE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "beauty_wellness": ("beauty", "skincare", "cosmetic", "wellness", "supplement", "personal care"),
    "food_beverage": ("food", "beverage", "drink", "snack", "coffee", "tea"),
    "apparel_accessories": ("apparel", "fashion", "clothing", "footwear", "jewelry", "accessories"),
    "home_lifestyle": ("home", "furniture", "decor", "lifestyle", "kitchen", "bedding"),
    "pets": ("pet", "pets", "animal"),
    "family_gifts": ("baby", "kids", "toys", "gift", "stationery"),
}

# Words that disqualify a store outright (agencies, resellers, etc.)
ICP_EXCLUDE_KEYWORDS = (
    "agency", "consulting", "consultancy", "software", "saas", "wholesale",
    "distributor", "manufacturer", "b2b", "dropship", "drop shipping",
    "print on demand", "print-on-demand", "printful", "printify",
    "etsy seller", "reseller", "marketplace",
)


def _normalize_country(code: str) -> str:
    code = (code or "").strip().upper()
    return "GB" if code == "UK" else code


def store_niche(store: dict[str, Any]) -> str:
    """Return the ICP niche label for a store, or '' if it fits none."""
    text = " ".join(
        str(part) for part in (
            store.get("categories") or "",
            store.get("tags") or "",
            store.get("merchant_name") or "",
            store.get("name") or "",
        )
    ).lower()
    for niche, keywords in ICP_NICHE_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return niche
    return ""


def store_matches_icp(store: dict[str, Any], settings: Optional[dict[str, Any]] = None) -> bool:
    """Authoritative ICP gate applied to a StoreLeads store record.

    The revenue band is tunable on the Lead Ops page; settings are in dollars a
    year, while StoreLeads reports cents, so the conversion happens here.
    """
    lo, hi = ICP_MIN_YEARLY_SALES_CENTS, ICP_MAX_YEARLY_SALES_CENTS
    if settings:
        try:
            lo = int(settings.get("icp.revenue_min_usd") or lo // 100) * 100
            hi = int(settings.get("icp.revenue_max_usd") or hi // 100) * 100
        except (TypeError, ValueError):
            pass
    domain = str(store.get("name") or "").strip().lower()
    if not domain:
        return False

    platform = str(store.get("platform") or "").strip().lower()
    if ICP_ALLOWED_PLATFORMS and platform and platform not in ICP_ALLOWED_PLATFORMS:
        return False

    country = _normalize_country(str(store.get("country_code") or ""))
    if country and country not in ICP_ALLOWED_COUNTRIES:
        return False

    yearly = store.get("estimated_sales_yearly")
    if isinstance(yearly, (int, float)) and yearly > 0:
        if yearly < lo or yearly > hi:
            return False

    # Headcount cross-check. StoreLeads call their sales figures directional
    # only, so a low revenue estimate must not be the sole thing keeping a
    # 500-person company out of the list.
    emp_lo, emp_hi = 2, 80
    if settings:
        try:
            emp_lo = int(settings.get("icp.employees_min") or emp_lo)
            emp_hi = int(settings.get("icp.employees_max") or emp_hi)
        except (TypeError, ValueError):
            pass
    employees = store.get("employee_count")
    if isinstance(employees, (int, float)) and employees > 0:
        if employees < emp_lo or employees > emp_hi:
            return False

    searchable = " ".join(
        str(part) for part in (
            domain,
            store.get("merchant_name") or "",
            store.get("categories") or "",
            store.get("tags") or "",
        )
    ).lower()
    if any(bad in searchable for bad in ICP_EXCLUDE_KEYWORDS):
        return False

    # Must land in one of the six niches, and must have a contact email listed
    # (StoreLeads has no emails itself, but a listed email means a live domain
    # Clay can work from).
    if not store_niche(store):
        return False
    if not _has_email(store):
        return False

    return True


def _has_email(store: dict[str, Any]) -> bool:
    for item in store.get("contact_info") or []:
        if isinstance(item, dict) and str(item.get("type", "")).lower() == "email":
            if str(item.get("value", "")).strip():
                return True
    return False


# ---- signal scoring (docs/outbound/08) ---------------------------------------
# Each brand gets a fit score from weighted signals, a Tier (A/B/C), and a
# human "why now" reason that also seeds Clay's personalization. Pure function of
# the store record so it is fully unit-testable without a live key.

ICP_SCORE_RECENT_DAYS = 45          # "installed recently" window for the trigger
ICP_SCORE_PLAN_CHANGE_DAYS = 180    # "upgraded plan recently" window
ICP_TIER_A_MIN = 8
ICP_TIER_B_MIN = 4

# Ad pixels that prove live paid spend (StoreLeads technology names, lowercased).
_AD_PIXEL_TECHS = (
    "facebook pixel", "google ads pixel", "tiktok pixel", "pinterest pixel",
    "bing ads", "criteo", "snap pixel",
)
# CRO / testing / analytics apps: brand already cares about conversion.
_CRO_APP_HINTS = (
    "intelligems", "rebuy", "triple whale", "triplewhale", "polar", "lucky orange",
    "hotjar", "clarity", "kno", "octane", "boost ai", "boost commerce",
)
# App-store categories that mean "acquisition budget" or "conversion focus".
_ADS_APP_CATS = ("ads", "affiliate")
_CRO_APP_CATS = (
    "analytics", "site optimization", "pricing optimization", "conversion",
    "upsell", "search and filters", "surveys",
)
# Enterprise analytics that signal an existing agency / in-house team (deprioritize).
_ENTERPRISE_TECH = ("monetate", "ometria", "bloomreach", "dynamic yield")


def _parse_sl_date(value: Any) -> Optional[datetime]:
    """Parse a StoreLeads date ('2026-07-07T00:00:00' or '...Z') to aware UTC."""
    if not value:
        return None
    raw = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _tech_names(store: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for t in store.get("technologies") or []:
        if isinstance(t, dict) and t.get("name"):
            out.add(str(t["name"]).strip().lower())
    return out


def _app_records(store: dict[str, Any]) -> list[dict[str, Any]]:
    return [a for a in (store.get("apps") or []) if isinstance(a, dict)]


def _app_text(app: dict[str, Any]) -> str:
    cats = app.get("categories") or []
    return " ".join([str(app.get("name") or ""), str(app.get("token") or ""),
                     " ".join(str(c) for c in cats)]).lower()


def _has_ad_pixels(store: dict[str, Any]) -> int:
    techs = _tech_names(store)
    return sum(1 for p in _AD_PIXEL_TECHS if any(p in t for t in techs))


def _has_cro_app(store: dict[str, Any]) -> bool:
    for app in _app_records(store):
        text = _app_text(app)
        if any(h in text for h in _CRO_APP_HINTS):
            return True
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        if any(c in cats for c in _CRO_APP_CATS):
            return True
    return False


def _has_ads_or_affiliate_app(store: dict[str, Any]) -> bool:
    for app in _app_records(store):
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        if any(c in cats for c in _ADS_APP_CATS):
            return True
    return False


def _recent_growth_app_install(store: dict[str, Any], now: datetime) -> bool:
    """Any ads / analytics / CRO app installed within the recent window = a fresh
    project and moving budget: the strongest 'why now' trigger."""
    for app in _app_records(store):
        installed = _parse_sl_date(app.get("installed_at"))
        if not installed:
            continue
        if (now - installed).days > ICP_SCORE_RECENT_DAYS or (now - installed).days < 0:
            continue
        cats = " ".join(str(c) for c in (app.get("categories") or [])).lower()
        text = _app_text(app)
        if any(c in cats for c in (*_ADS_APP_CATS, *_CRO_APP_CATS)) or any(h in text for h in _CRO_APP_HINTS):
            return True
    return False


def _recent_plan_upgrade(store: dict[str, Any], now: datetime) -> bool:
    changed = _parse_sl_date(store.get("last_plan_change_at"))
    if not changed or (now - changed).days > ICP_SCORE_PLAN_CHANGE_DAYS or (now - changed).days < 0:
        return False
    return "plus" in str(store.get("plan") or "").lower()


def _healthy_app_spend(store: dict[str, Any]) -> bool:
    spend = store.get("monthly_app_spend")
    return isinstance(spend, (int, float)) and 300_00 <= spend <= 3_000_00


def _has_trending_tag(store: dict[str, Any]) -> bool:
    tags = " ".join(str(t) for t in (store.get("tags") or [])).lower()
    return "trending" in tags


def _is_public_company(store: dict[str, Any]) -> bool:
    feats = " ".join(str(f) for f in (store.get("features") or [])).lower()
    return "public company" in feats


def _has_enterprise_stack(store: dict[str, Any]) -> bool:
    techs = _tech_names(store)
    return any(e in t for e in _ENTERPRISE_TECH for t in techs)


def score_store(store: dict[str, Any], *, now: Optional[datetime] = None) -> dict[str, Any]:
    """Return {score, tier, reason, signals[], excluded} for a store.

    Ordered so the most compelling 'why now' trigger becomes the reason/opener.
    """
    now = now or datetime.now(timezone.utc)
    score = 0
    signals: list[str] = []

    # Timing triggers first (they make the best opener).
    if _recent_growth_app_install(store, now):
        score += 3
        signals.append("Added a growth or CRO app in the last 45 days")
    if _recent_plan_upgrade(store, now):
        score += 2
        signals.append("Upgraded their store plan recently")
    if _has_trending_tag(store):
        score += 1
        signals.append("Trending on social right now")

    # Spend-is-happening (there is waste to audit).
    pixels = _has_ad_pixels(store)
    if pixels >= 2:
        score += 3
        signals.append("Runs Meta and Google ads")
    if pixels >= 3:
        score += 2
        signals.append("Advertises across three or more channels")
    if _has_ads_or_affiliate_app(store):
        score += 2
        signals.append("Has ad or affiliate apps installed")
    if _healthy_app_spend(store):
        score += 1
        signals.append("Invests in a healthy growth app stack")

    # Believes in CVR (easy conversation).
    if _has_cro_app(store):
        score += 3
        signals.append("Already uses conversion or testing tools")

    # Anti-signals.
    excluded = _is_public_company(store)
    if _has_enterprise_stack(store):
        score -= 2
        signals.append("Runs an enterprise analytics stack")
    if pixels == 0:
        score -= 2
        signals.append("No ad pixel found")

    if excluded:
        tier = "X"
    elif score >= ICP_TIER_A_MIN:
        tier = "A"
    elif score >= ICP_TIER_B_MIN:
        tier = "B"
    else:
        tier = "C"

    positive = [s for s in signals if s not in
                ("Runs an enterprise analytics stack", "No ad pixel found")]
    reason = positive[0] if positive else "Fits our ICP, thin buying signals"
    return {"score": score, "tier": tier, "reason": reason, "signals": signals, "excluded": excluded}


def _amazon_unknown_sellers(findings: Any) -> int:
    """Total unknown third-party sellers across the matched listings."""
    if not isinstance(findings, dict):
        return 0
    total = 0
    for listing in findings.get("listings") or []:
        if not isinstance(listing, dict):
            continue
        try:
            total += int(listing.get("sellers_unknown") or 0)
        except (TypeError, ValueError):
            continue
    return total


def to_clay_lead(store: dict[str, Any], *, now: Optional[datetime] = None,
                 amazon: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Shape a matched store into the payload Clay's table expects, with the
    fit score, Tier, and 'why now' reason attached for ranking + personalization.

    amazon is the result of outbound_amazon.brand_control(). A brand losing
    control of its own Amazon listings beats any StoreLeads signal as an opener,
    so when that check produced a reason it takes over the reason line. When the
    check was skipped, switched off, or never run, the StoreLeads reason stands.
    """
    scored = score_store(store, now=now)
    lead = {
        "domain": str(store.get("name") or "").strip().lower(),
        "brand": str(store.get("merchant_name") or store.get("name") or "").strip(),
        "niche": store_niche(store),
        "country": _normalize_country(str(store.get("country_code") or "")),
        "tier": scored["tier"],
        "score": scored["score"],
        "reason": scored["reason"],
        "signals": scored["signals"],
        "estimated_sales_yearly_cents": store.get("estimated_sales_yearly"),
        "categories": store.get("categories"),
        "apps": store.get("apps"),
    }
    apply_amazon(lead, amazon)
    return lead


def apply_amazon(lead: dict[str, Any], amazon: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Fold an Amazon brand-control result onto a lead, in place.

    Applied LAST in the pull, after the recipe has had its say, because a real
    finding about a brand's own listings beats a guess made from a store plan
    change. Getting this order wrong silently reverts every lead to the old
    reason and the Amazon work never reaches the email.
    """
    if not isinstance(amazon, dict):
        return lead
    findings = amazon.get("findings")
    lead["amazon_confidence"] = str(amazon.get("confidence") or "none")
    lead["amazon_marketplace"] = str(amazon.get("marketplace") or "")
    lead["amazon_checked_at"] = str(amazon.get("checked_at") or "")
    lead["amazon_absent"] = bool(findings.get("absent")) if isinstance(findings, dict) else False
    lead["amazon_sellers_unknown"] = _amazon_unknown_sellers(findings)
    lead["amazon_skipped_reason"] = str(amazon.get("skipped_reason") or "")
    amazon_reason = str(amazon.get("reason") or "").strip()
    if amazon_reason:
        lead["reason"] = amazon_reason
        lead["signals"] = [amazon_reason] + [
            s for s in lead.get("signals") or [] if s != amazon_reason
        ]

    # The bucketed facts Clay writes the real sentence from. Our reason above is
    # only the fallback for when Clay's column has not run.
    try:
        import json as _json

        from outbound_amazon import clay_facts
        facts = clay_facts(amazon)
        lead.update(facts)
        # Also as one blob, because this lead gets stored and re-read later and
        # the facts have to survive that round trip to reach Clay at all.
        lead["amazon_facts"] = _json.dumps(facts)
    except Exception:  # noqa: BLE001 — a missing fact must not lose the lead
        logger.warning("[outbound] could not build Clay facts for %s", lead.get("domain"))
    return lead


# ---- thin network layer (injectable for tests) -------------------------------

def _retry_after_seconds(resp: "requests.Response") -> Optional[float]:
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def fetch_storeleads_page(
    api_key: str,
    *,
    page: int = 0,
    page_size: int = 50,
    timeout: int = 30,
    max_retries: int = 4,
    extra_params: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """Fetch one page of Shopify stores from StoreLeads, newest-ranked first.

    extra_params carries a recipe's server-side filters (see outbound_recipes),
    which is what turns a static list into a timed trigger. The authoritative ICP
    filtering still happens client-side in store_matches_icp, so a StoreLeads
    filter quirk can never widen our target.

    Retries politely on 429 (rate limit), honoring Retry-After when present, with
    exponential backoff otherwise.
    """
    params = {
        "f:p": "shopify",
        "page": page,
        "page_size": page_size,
        "sort": "-rank",
        "fields": ",".join((
            "name", "merchant_name", "platform", "country_code",
            "estimated_sales_yearly", "categories", "tags", "contact_info", "apps",
            # Richer fields the signal scorer reads (docs/outbound/08).
            "technologies", "monthly_app_spend", "last_plan_change_at",
            "plan", "created_at", "features", "employee_count",
        )),
    }
    if extra_params:
        # Recipe filters win over the defaults (e.g. a recipe's own sort order).
        params.update({k: v for k, v in extra_params.items() if v is not None})
    for attempt in range(max_retries + 1):
        resp = requests.get(
            f"{STORELEADS_BASE_URL}{STORELEADS_DOMAIN_PATH}",
            headers={"Authorization": f"Bearer {api_key}"},
            params=params,
            timeout=timeout,
        )
        if resp.status_code == 429 and attempt < max_retries:
            wait = _retry_after_seconds(resp) or (2 ** attempt)
            logger.warning("[outbound] StoreLeads rate-limited (page %s); waiting %ss", page, wait)
            time.sleep(min(wait, 30))
            continue
        resp.raise_for_status()
        payload = resp.json()
        # StoreLeads returns the store array under "domains" (list endpoint).
        if isinstance(payload, dict):
            return payload.get("domains") or payload.get("results") or []
        if isinstance(payload, list):
            return payload
        return []
    return []


def push_to_clay(webhook_url: str, leads: list[dict[str, Any]], *, timeout: int = 30) -> dict[str, Any]:
    """Post a batch of leads to the Clay table's inbound webhook."""
    if not webhook_url:
        return {"pushed": 0, "skipped": True, "reason": "no Clay webhook configured"}
    if not leads:
        return {"pushed": 0, "skipped": True, "reason": "no leads to push"}
    resp = requests.post(webhook_url, json={"leads": leads}, timeout=timeout)
    resp.raise_for_status()
    return {"pushed": len(leads)}


# ---- orchestration -----------------------------------------------------------

@dataclass
class PipelineResult:
    scanned: int = 0
    matched_icp: int = 0
    fresh: int = 0
    pushed: int = 0
    skipped_already_contacted: int = 0
    leads: list[dict[str, Any]] = field(default_factory=list)
    dry_run: bool = False
    partial: bool = False  # True if StoreLeads cut us off (rate limit) mid-run
    recipe: str = ""       # which pull recipe sourced this batch


def run_storeleads_to_clay(
    *,
    api_key: str,
    clay_webhook_url: str,
    processed_domains: set[str],
    max_new: int = 100,
    max_pages: int = 40,
    dry_run: bool = False,
    throttle_seconds: float = 1.5,
    recipe: Optional[Any] = None,
    now: Optional[datetime] = None,
    settings: Optional[dict[str, Any]] = None,
    fetch_page: Optional[Callable[..., list[dict[str, Any]]]] = None,
    push: Optional[Callable[..., dict[str, Any]]] = None,
    amazon_check: Optional[Callable[[dict[str, Any]], Optional[dict[str, Any]]]] = None,
) -> PipelineResult:
    """Pull ICP brands from StoreLeads, drop already-contacted ones, push the
    rest to Clay. With dry_run=True (or no Clay webhook) nothing is pushed; the
    matched leads are returned for preview.

    Paces requests (throttle_seconds between pages) to stay under StoreLeads'
    rate limit. If StoreLeads still cuts us off, returns the brands gathered so
    far and flags the result partial, instead of failing the whole pull.
    """
    fetch_page = fetch_page or fetch_storeleads_page
    push = push or push_to_clay

    result = PipelineResult(dry_run=dry_run or not clay_webhook_url)
    result.recipe = getattr(recipe, "key", "") if recipe else ""
    seen_this_run: set[str] = set()

    # A recipe supplies the server-side filters that make this pull a timed
    # trigger (see outbound_recipes); without one we pull the broad ICP list.
    extra = recipe.params(now, settings) if recipe is not None else None

    for page in range(max_pages):
        if result.fresh >= max_new:
            break
        if page > 0 and throttle_seconds:
            time.sleep(throttle_seconds)
        try:
            stores = fetch_page(api_key, page=page, extra_params=extra)
        except Exception as exc:  # noqa: BLE001 — return partial instead of failing
            logger.warning("[outbound] StoreLeads page %s failed, returning %s brands gathered: %s",
                           page, len(result.leads), exc)
            result.partial = True
            break
        if not stores:
            break
        for store in stores:
            result.scanned += 1
            # A recipe may re-check the trigger here when StoreLeads cannot
            # express it as a single filter (see outbound_recipes.Recipe.keeps).
            if recipe is not None and not recipe.keeps(store, now, settings):
                continue
            if not store_matches_icp(store, settings):
                continue
            result.matched_icp += 1
            lead = to_clay_lead(store, now=now)
            if recipe is not None:
                # The recipe is why this brand surfaced now, so it leads the
                # reason and is carried through for per-recipe measurement.
                reason = recipe.reason_for(settings)
                lead["recipe"] = recipe.key
                lead["reason"] = reason
                lead["signals"] = [reason] + [
                    s for s in lead.get("signals", []) if s != reason
                ]
            # Amazon last, so a real finding about their own listings wins over
            # the recipe's guess at why now.
            if amazon_check is not None:
                try:
                    apply_amazon(lead, amazon_check(lead))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[outbound] amazon check failed for %s: %s",
                                   lead.get("domain"), exc)
            if lead.get("tier") == "X":
                continue  # public company / out of profile — drop
            domain = lead["domain"]
            if not domain or domain in seen_this_run:
                continue
            if domain in processed_domains:
                result.skipped_already_contacted += 1
                continue
            seen_this_run.add(domain)
            result.leads.append(lead)
            result.fresh += 1
            if result.fresh >= max_new:
                break

    # Rank hottest first so Tier A brands lead the CSV / the push.
    result.leads.sort(key=lambda l: l.get("score", 0), reverse=True)

    if not result.dry_run and result.leads:
        outcome = push(clay_webhook_url, result.leads)
        result.pushed = int(outcome.get("pushed", 0) or 0)

    logger.info(
        "[outbound] scanned=%s matched=%s fresh=%s pushed=%s skipped_contacted=%s dry_run=%s",
        result.scanned, result.matched_icp, result.fresh, result.pushed,
        result.skipped_already_contacted, result.dry_run,
    )
    return result


def load_config_from_env() -> tuple[str, str]:
    """Read the two values David sets on Render. Never hardcoded."""
    api_key = (os.getenv("STORELEADS_API_KEY") or "").strip()
    clay_webhook_url = (os.getenv("CLAY_WEBHOOK_URL") or "").strip()
    return api_key, clay_webhook_url


# What Clay's AI reads to write the opening sentence. Mirrors
# outbound_amazon.CLAY_FACT_COLUMNS; kept as a literal so this module never has to
# import outbound_amazon, which imports this one.
AMAZON_FACT_COLUMNS = (
    "amz_situation", "amz_product", "amz_sellers_band", "amz_undercut",
    "amz_rivals_on_name", "amz_marketplace", "amz_confidence",
)

# Column order for the CSV that gets imported into Clay (used on the Launch plan,
# before webhooks unlock on Growth). These are the fields Clay needs to run its
# enrichment and the two prompts.
#
# NEVER RENAME an existing entry here or in CLAY_CSV_HEADERS. The Clay table's import
# mapping is keyed on these exact headers, so a rename silently creates a second set
# of columns that the enrichment never reads.
#
# ADDING a column is a deliberate act: it appears as a new column to map, once, on the
# next Clay import. The amz_* facts below were added on purpose, because Clay writes
# the opening sentence now and needs something true to write it from.
#
# The facts are BUCKETED on our side ("a handful of other sellers", never 18). Once an
# AI is holding a number there is nothing left to stop it quoting one, and a brand that
# reads "19 sellers" and counts 14 stops reading. Send words, not counts.
CLAY_CSV_COLUMNS = (
    "tier", "brand", "domain", "niche", "country", "reason", "recipe", "score",
    "revenue_usd", "categories",
) + AMAZON_FACT_COLUMNS

# The CSV header we WRITE for each field. These are named to match the Clay table's
# existing columns exactly, so Clay auto-maps on import instead of creating a second
# set of columns. Case matters: a header of "domain" makes a new text column, while
# "Domain" matches the existing one the enrichment actually reads from.
CLAY_CSV_HEADERS = {
    "brand": "Merchant Name",
    "domain": "Domain",
    "reason": "personalization",
    "revenue_usd": "revenue_usd",
}


def clay_header(field: str) -> str:
    return CLAY_CSV_HEADERS.get(field, field)


def leads_to_csv(leads: list[dict[str, Any]]) -> str:
    """Serialize matched leads to CSV text for manual import into Clay."""
    import csv
    import io

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CLAY_CSV_COLUMNS, extrasaction="ignore")
    # Write Clay's own column names as the header row so the import auto-maps.
    writer.writerow({f: clay_header(f) for f in CLAY_CSV_COLUMNS})
    for lead in leads:
        row = dict(lead)
        # Revenue is stored in cents; a person reading the sheet needs dollars,
        # otherwise $8.4M reads as 835227012 and looks a hundred times too big.
        try:
            # Freshly pulled leads carry the StoreLeads field name; leads read back
            # out of our own table carry ours. Reading only one silently exports
            # every scanned brand at $0.
            cents = lead.get("estimated_sales_yearly_cents")
            if cents in (None, ""):
                cents = lead.get("revenue_cents")
            row["revenue_usd"] = int(cents or 0) // 100
        except (TypeError, ValueError):
            row["revenue_usd"] = 0
        # Flatten anything non-scalar so the CSV stays clean.
        cats = row.get("categories")
        if isinstance(cats, (list, tuple)):
            row["categories"] = ", ".join(str(c) for c in cats)
        writer.writerow(row)
    return buf.getvalue()
