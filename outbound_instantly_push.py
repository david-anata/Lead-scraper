"""Push enriched leads straight into an Instantly campaign (replaces Clay's
"Add Lead to Campaign" column).

Safety, in order of importance:
  * DRY RUN is the default. Nothing leaves this process unless the caller passes
    ``dry_run=False`` AND ``OUTBOUND_PUSH_ENABLED`` is "true" on Render.
  * Protected campaigns. Campaigns listed in ``OUTBOUND_PROTECTED_CAMPAIGN_IDS``
    are refused. There is no built-in protected list.
  * Adding leads to a paused campaign does not send anything; the campaign has
    to be started in Instantly by a person.
  * The API key is never logged or returned.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests

logger = logging.getLogger(__name__)

INSTANTLY_ADD_LEADS_URL = "https://api.instantly.ai/api/v2/leads/add"
CHUNK = 100

# Retired "AGENT SDR // MARCH '26" campaign. Older read-only tools still name it.
# Pushes must not fall back to this id. Mapped recipes use RECIPE_CAMPAIGN_MAP.
# OUTBOUND_INSTANTLY_CAMPAIGN_ID is not a destination, even when it still
# names Trial Offer Oct.
LIVE_CAMPAIGN_ID = "4f118329-c07e-4279-9c08-12059f2a49f4"
MISSING_CAMPAIGN_MESSAGE = (
    "OUTBOUND_INSTANTLY_CAMPAIGN_ID is unset. Skipping the Instantly push. "
    "The retired campaign is not used as a fallback."
)
DEFAULT_PROTECTED: tuple[str, ...] = ()

# Contacts whose recipe column is one of these go to that Instantly campaign.
# This is a code map on purpose: no new env var and no schema change.
# These are the v2 campaigns. The completed v1 ids are not a fallback.
# push_pending prefers an active outbound_campaigns row over this map.
# replatform v2 is status completed in Instantly, so leads added there do
# not send until that campaign is restarted.
# Unmapped recipes are not sent to OUTBOUND_INSTANTLY_CAMPAIGN_ID. That
# variable is Trial Offer Oct in production. A filler lead is queued unless
# its stored signals prove one of these recipes.
RECIPE_CAMPAIGN_MAP: dict[str, str] = {
    "amz_on": "8e0df427-1f18-43ac-b0c6-e8ea787123d7",
    "amz_off": "138a162d-6654-4916-9959-20c11e31214b",
    "plan_upgrade": "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72",
    "replatform": "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
    "replatformed": "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
    "abby_reengage": "907d32fd-d6db-4ce6-8edc-485b78cce120",
}

# Partner-list contacts are stored for a separate pass and stay out of push_pending.
REDO_PARTNER_RECIPE = "redo_partner"
PUSH_EXCLUDED_RECIPES: frozenset[str] = frozenset({REDO_PARTNER_RECIPE})

# Filler recipes are not v2 campaigns. Production marked these cut, cap 0,
# name "|hold|no_trial_offer_ingest", so unmatched contacts stay queued.
# A stored eligibility token can still select a real v2 campaign. The recipe
# name alone never does.
FILLER_RECIPES = frozenset({
    "icp_baseline", "social_surge", "churned_tool", "new_growth_app",
})
HOLD_INGEST_CAMPAIGN_IDS = frozenset({
    "71add6d5-0096-52df-a6e2-66135fbe22e4",  # icp_baseline hold
    "75ae152d-4973-571a-a931-fcc95e6f2360",  # social_surge hold
    "a04a958c-6546-5ba0-a97d-586446d2b580",  # churned_tool hold
    "593df58d-f073-51c0-9cc9-b475a50a6a79",  # new_growth_app hold
})

# "Anata // Trial Offer // Oct 2026". Same id Abby still watches. New leads
# are not added to it. Keep this equal to abby_runner.TRIAL_CAMPAIGN_ID.
LEGACY_TRIAL_CAMPAIGN_ID = "e5f5046b-07e4-41ad-a395-79cf8766ccd0"
TRIAL_CLOSED_REASON = "Trial Offer Oct does not accept new leads."
QUEUED_NOTE = (
    "Queued contacts that do not match an active v2 recipe campaign. "
    "Trial Offer Oct is not used."
)

# Machine-readable tokens stored on a lead's signals list. A filler recipe
# (baseline, social, app install, churn) is routed only when one of these
# is present. The windows match the approved v2 campaign filters.
ROUTE_PLAN_UPGRADE = "route:plan_upgrade"
ROUTE_REPLATFORM = "route:replatform"
ROUTE_AMZ_ON = "route:amz_on"
ROUTE_AMZ_OFF = "route:amz_off"
_PLAN_UPGRADE_DAYS = 90
_REPLATFORM_DAYS = 120
_REPLATFORM_PREVIOUS = frozenset({"woocommerce", "magento", "bigcommerce"})
# The scoreboard already writes this sentence for a Shopify Plus change inside
# its 180-day window. It is evidence, so a stored lead can move off Trial Offer
# without a new pull. It is not a guess from the ICP recipe name.
_PLAN_UPGRADE_SENTENCE = "Upgraded their store plan recently"
_ROUTE_EVIDENCE: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("plan_upgrade", (ROUTE_PLAN_UPGRADE, _PLAN_UPGRADE_SENTENCE)),
    ("replatform", (ROUTE_REPLATFORM,)),
    ("amz_on", (ROUTE_AMZ_ON,)),
    ("amz_off", (ROUTE_AMZ_OFF,)),
)


def configured_campaign_id() -> str:
    """Return ``OUTBOUND_INSTANTLY_CAMPAIGN_ID`` when set.

    This is not a push destination. Unmapped recipes are queued.
    ``LIVE_CAMPAIGN_ID`` is not a substitute.
    """

    return (os.getenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID") or "").strip()


def is_legacy_trial_campaign(campaign_id: Any) -> bool:
    """True for Trial Offer Oct. Abby may still read it. Pushes may not."""

    return str(campaign_id or "").strip() == LEGACY_TRIAL_CAMPAIGN_ID


def _closed_to_new_leads(campaign_id: Any) -> bool:
    """Trial Offer and the reversible filler hold markers accept no new leads."""

    campaign = str(campaign_id or "").strip()
    return is_legacy_trial_campaign(campaign) or campaign in HOLD_INGEST_CAMPAIGN_IDS


SALES_ORG_RECIPE = "sales_org"
SALES_ORG_FLAG = "OUTBOUND_SALES_ORG_ENABLED"
SALES_ORG_CAMPAIGN_ENV = "OUTBOUND_SALES_ORG_CAMPAIGN_ID"


def sales_org_enabled() -> bool:
    """The sales-org segment stays off until this is set."""

    return (os.getenv(SALES_ORG_FLAG) or "").strip().lower() in {"1", "true", "yes", "on"}


def sales_org_campaign_id() -> str:
    """Instantly campaign for sales-org leads. Empty until David creates it."""

    return (os.getenv(SALES_ORG_CAMPAIGN_ENV) or "").strip()


def campaign_for_recipe(recipe: Any, default: str = "",
                        campaign_map: Optional[dict[str, str]] = None) -> str:
    """Campaign for one contact's recipe column.

    A mapped recipe wins. ``campaign_map`` is the live table when
    ``push_pending`` could read it. ``None`` uses ``RECIPE_CAMPAIGN_MAP``.
    ``default`` and ``OUTBOUND_INSTANTLY_CAMPAIGN_ID`` are ignored so a
    filler recipe cannot land in Trial Offer Oct. Empty means queue it.
    """

    del default  # kept so existing callers still pass a campaign id
    if str(recipe or "").strip() == SALES_ORG_RECIPE:
        if not sales_org_enabled():
            return ""
        mapped_sales = sales_org_campaign_id()
        if _closed_to_new_leads(mapped_sales):
            return ""
        return mapped_sales
    source = RECIPE_CAMPAIGN_MAP if campaign_map is None else campaign_map
    mapped = str(source.get(str(recipe or "").strip()) or "").strip()
    if _closed_to_new_leads(mapped):
        return ""
    return mapped


def _parse_store_date(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    text = raw.replace("Z", "+00:00")
    if "." in text:
        head, tail = text.split(".", 1)
        digits = []
        rest = ""
        for index, char in enumerate(tail):
            if char.isdigit():
                digits.append(char)
            else:
                rest = tail[index:]
                break
        text = f"{head}.{(''.join(digits) + '000000')[:6]}{rest}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _within_days(value: Any, now: datetime, days: int) -> bool:
    parsed = _parse_store_date(value)
    if parsed is None:
        return False
    age = (now - parsed).days
    return 0 <= age <= days


def _channel_names(store: dict[str, Any]) -> Optional[set[str]]:
    """Sales-channel names, or None when the store did not say.

    An empty list is unknown. It is not proof the brand is off Amazon.
    """

    raw = store.get("sales_channels")
    if raw is None:
        raw = store.get("scs")
    if raw is None:
        return None
    parts: list[str] = []
    if isinstance(raw, str):
        parts = [item.strip() for item in raw.split(",")]
    elif isinstance(raw, (list, tuple)):
        for item in raw:
            if isinstance(item, dict):
                parts.append(str(item.get("name") or item.get("title") or ""))
            else:
                parts.append(str(item))
    else:
        return None
    names = {part.strip().lower() for part in parts if part and part.strip()}
    return names or None


def route_tokens_for_store(store: dict[str, Any], *,
                           now: Optional[datetime] = None) -> list[str]:
    """Eligibility tokens for one StoreLeads row.

    Tokens are added only from fields on the row. A missing sales-channel
    list does not become "not on Amazon". A Shopify Plus plan without a
    recent plan-change date does not become a plan upgrade.
    """

    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    tokens: list[str] = []
    plan = str(store.get("plan") or "").lower()
    if "plus" in plan and _within_days(store.get("last_plan_change_at"), moment, _PLAN_UPGRADE_DAYS):
        tokens.append(ROUTE_PLAN_UPGRADE)
    previous = str(store.get("last_platform") or "").strip().lower()
    if previous in _REPLATFORM_PREVIOUS and _within_days(
        store.get("last_platform_change_at"), moment, _REPLATFORM_DAYS,
    ):
        tokens.append(ROUTE_REPLATFORM)
    channels = _channel_names(store)
    if channels is not None:
        if "amazon" in channels:
            tokens.append(ROUTE_AMZ_ON)
        else:
            tokens.append(ROUTE_AMZ_OFF)
    return tokens


def _evidence_recipes(signals: Iterable[Any]) -> list[str]:
    found: set[str] = set()
    text = {str(item or "").strip() for item in signals or [] if str(item or "").strip()}
    ordered: list[str] = []
    for recipe_key, markers in _ROUTE_EVIDENCE:
        if text.intersection(markers) and recipe_key not in found:
            found.add(recipe_key)
            ordered.append(recipe_key)
    return ordered


def resolve_recipe_campaign(
    recipe: Any,
    signals: Optional[Iterable[Any]] = None,
    *,
    campaign_map: Optional[dict[str, str]] = None,
    blocked: Optional[Iterable[str]] = None,
) -> tuple[str, str, str]:
    """Pick one v2 campaign, or queue the contact.

    Returns ``(campaign_id, status, route_recipe)``. ``status`` is
    ``routed``, ``queued``, or ``blocked``. A recipe that already names a
    v2 campaign uses that campaign. A filler recipe, including one held
    by a cut ``|hold|no_trial_offer_ingest`` row, uses stored evidence only.
    The filler name is not itself a destination. Plan upgrade wins over
    replatform, which wins over Amazon on or off. Amazon on and Amazon off
    together, with no stronger signal, stays queued. A blocked v2 recipe
    is not moved to a different campaign.
    """

    blocked_names = {str(name or "").strip() for name in (blocked or []) if str(name or "").strip()}
    name = str(recipe or "").strip()
    import outbound_campaigns as _campaigns

    if _campaigns.is_revenue_band_experiment(name):
        return "", "queued", ""
    if name == SALES_ORG_RECIPE:
        if name in blocked_names:
            return "", "blocked", ""
        target = campaign_for_recipe(name, campaign_map=campaign_map)
        if target:
            return target, "routed", name
        return "", "queued", ""
    known = set(RECIPE_CAMPAIGN_MAP)
    if campaign_map is not None:
        known.update(campaign_map)
    filler = name in FILLER_RECIPES or name not in known
    if name in blocked_names and not filler:
        return "", "blocked", ""
    if name in known and name not in blocked_names and name not in FILLER_RECIPES:
        target = campaign_for_recipe(name, campaign_map=campaign_map)
        if target:
            return target, "routed", name
        return "", "queued", ""

    matches = _evidence_recipes(signals or [])
    specific = [item for item in matches if item in {"plan_upgrade", "replatform"}]
    if specific:
        chosen = specific[0]
    else:
        amazon = [item for item in matches if item in {"amz_on", "amz_off"}]
        if len(set(amazon)) != 1:
            if name in blocked_names:
                return "", "blocked", ""
            return "", "queued", ""
        chosen = amazon[0]
    if chosen in blocked_names:
        return "", "blocked", ""
    target = campaign_for_recipe(chosen, campaign_map=campaign_map)
    if not target:
        return "", "queued", ""
    return target, "routed", chosen


def dry_run_route_plan(
    contacts: Iterable[dict[str, Any]],
    *,
    campaign_map: Optional[dict[str, str]] = None,
    blocked: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """Classify contacts without calling Instantly or spending credits.

    ``legacy_or_hold_targets`` must stay 0. A non-zero count means a contact
    was aimed at Trial Offer Oct or a filler hold marker.
    """

    routed: dict[str, int] = {}
    by_status = {"routed": 0, "queued": 0, "blocked": 0}
    legacy_or_hold = 0
    for contact in contacts:
        campaign_id, status, _route = resolve_recipe_campaign(
            contact.get("recipe"),
            contact.get("signals") or [],
            campaign_map=campaign_map,
            blocked=blocked,
        )
        if status not in by_status:
            status = "queued"
        by_status[status] += 1
        if _closed_to_new_leads(campaign_id):
            legacy_or_hold += 1
        elif status == "routed" and campaign_id:
            routed[campaign_id] = routed.get(campaign_id, 0) + 1
    return {
        "routed": routed,
        "queued": by_status["queued"],
        "blocked": by_status["blocked"],
        "routed_contacts": by_status["routed"],
        "legacy_or_hold_targets": legacy_or_hold,
    }


def friendly_category(raw: Any) -> str:
    """StoreLeads paths ("/Home & Garden/Gardening & Landscaping, /Business & ...")
    become the words the email reads: "gardening & landscaping"."""
    if isinstance(raw, (list, tuple)):
        raw = ", ".join(str(r) for r in raw)
    paths = [p.strip() for p in str(raw or "").split(",") if p.strip()]
    if not paths:
        return ""
    preferred = [p for p in paths if not p.lower().startswith("/business & industrial")] or paths
    leaf = [seg for seg in preferred[0].split("/") if seg.strip()]
    return leaf[-1].strip().lower() if leaf else ""


def protected_campaigns() -> set[str]:
    extra = [c.strip() for c in (os.getenv("OUTBOUND_PROTECTED_CAMPAIGN_IDS") or "").split(",")]
    return {c for c in (*DEFAULT_PROTECTED, *extra) if c}


def push_enabled() -> bool:
    return (os.getenv("OUTBOUND_PUSH_ENABLED") or "").strip().lower() == "true"


def api_key() -> str:
    return (os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip()


def _company_name(lead: dict[str, Any]) -> str:
    from outbound_pipeline import clean_brand_name

    return clean_brand_name(lead.get("brand") or lead.get("domain") or "")


def build_lead(contact: dict[str, Any], lead: dict[str, Any], opener: str) -> dict[str, Any]:
    """One Instantly lead. Field names match the sequence variables:
    {{firstName}}, {{companyName}}, {{personalization}}, {{category}}, {{brandAngle}}."""
    category = friendly_category(lead.get("categories") or "") or str(lead.get("niche") or "").strip().lower()
    angle = str(contact.get("brand_angle") or lead.get("brand_angle") or "").strip()
    return {
        "email": str(contact.get("email") or "").strip().lower(),
        "first_name": str(contact.get("first_name") or "").strip(),
        "last_name": str(contact.get("last_name") or "").strip(),
        "company_name": _company_name(lead),
        "website": str(lead.get("domain") or "").strip(),
        "personalization": opener,
        "custom_variables": {
            "category": category,
            "signal": str(lead.get("recipe") or "").strip(),
            "tier": str(lead.get("tier") or "").strip(),
            "contact_title": str(contact.get("title") or "").strip(),
            "brandAngle": angle,
        },
    }


@dataclass
class PushOutcome:
    attempted: int = 0
    created: int = 0
    skipped: int = 0
    dry_run: bool = True
    reason: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        if self.dry_run:
            return f"Dry run: {self.attempted} leads ready, nothing sent to Instantly."
        return f"Instantly accepted {self.created}, skipped {self.skipped} of {self.attempted}."


def push_leads(
    leads: list[dict[str, Any]],
    campaign_id: str,
    *,
    dry_run: bool = True,
    key: Optional[str] = None,
    post: Optional[Callable[..., Any]] = None,
) -> PushOutcome:
    out = PushOutcome(attempted=len(leads), dry_run=True)
    if not leads:
        out.reason = "Nothing to push."
        return out
    if not campaign_id:
        out.reason = "No campaign id."
        return out
    if _closed_to_new_leads(campaign_id):
        out.reason = TRIAL_CLOSED_REASON
        return out
    if campaign_id in protected_campaigns():
        out.reason = "That campaign is protected. Refusing to add leads to it."
        return out
    if dry_run or not push_enabled():
        out.reason = "Dry run." if dry_run else "OUTBOUND_PUSH_ENABLED is not true."
        return out

    key = key if key is not None else api_key()
    if not key:
        out.reason = "Instantly API key missing."
        return out
    post = post or requests.post
    out.dry_run = False
    for i in range(0, len(leads), CHUNK):
        chunk = leads[i:i + CHUNK]
        try:
            resp = post(
                INSTANTLY_ADD_LEADS_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "campaign_id": campaign_id,
                    "leads": chunk,
                    "verify_leads_on_import": False,
                    "skip_if_in_workspace": True,
                },
                timeout=60,
            )
            if resp.status_code >= 300:
                out.errors.append(f"Instantly returned {resp.status_code}")
                continue
            data = resp.json() or {}
            out.created += len(data.get("created_leads", []) or [])
            out.skipped += int(data.get("skipped_count", 0) or 0)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).replace(key, "<instantly-key>") if key else str(exc)
            out.errors.append(f"push failed: {msg}")
    return out


INSTANTLY_LIST_LEADS_URL = "https://api.instantly.ai/api/v2/leads/list"


def _same_company(item: dict[str, Any], domain: str) -> bool:
    """True when the lead belongs to this brand, including a store subdomain.

    ca.petlibro.com and petlibro.com are the same company. mybasin.com is not
    basin.com: comparison is the registrable domain, not a substring.
    """
    import outbound_contacts as _contacts

    target = _contacts.company_domain(domain)
    if not target:
        return False
    hosts = [
        str(item.get("company_domain") or ""),
        str(item.get("website") or ""),
    ]
    email = str(item.get("email") or "")
    if "@" in email:
        hosts.append(email.split("@", 1)[1])
    for host in hosts:
        raw = _contacts.normalize_host(host)
        reg = _contacts.registrable_domain(host)
        if not raw and not reg:
            continue
        if raw == target or reg == target or raw.endswith("." + target):
            return True
    return False


def domain_in_instantly(domain: str, *, key: Optional[str] = None,
                        post: Optional[Callable[..., Any]] = None) -> Optional[bool]:
    """Is anyone at this company already a lead anywhere in Instantly?

    True: yes, skip the company (Clay or an earlier run already worked it).
    False: no one found. None: the check failed; callers treat that as "do not
    touch this company this run" so a broken check never causes a double touch.
    """
    key = key if key is not None else api_key()
    import outbound_contacts as _contacts
    domain = _contacts.company_domain(domain)
    if not key or not domain:
        return None
    post = post or requests.post
    try:
        resp = post(INSTANTLY_LIST_LEADS_URL,
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                    json={"search": domain, "limit": 50}, timeout=30)
        if resp.status_code >= 300:
            logger.warning("[instantly] lead search returned %s", resp.status_code)
            return None
        items = (resp.json() or {}).get("items") or []
    except Exception as exc:  # noqa: BLE001
        logger.warning("[instantly] lead search failed: %s", str(exc).replace(key, "<instantly-key>"))
        return None
    return any(_same_company(i or {}, domain) for i in items)
