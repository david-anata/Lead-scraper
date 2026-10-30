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
from typing import Any, Callable, Optional

import requests

logger = logging.getLogger(__name__)

INSTANTLY_ADD_LEADS_URL = "https://api.instantly.ai/api/v2/leads/add"
CHUNK = 100

# Retired "AGENT SDR // MARCH '26" campaign. Older read-only tools still name it.
# Pushes must not fall back to this id. Unmapped recipes use
# OUTBOUND_INSTANTLY_CAMPAIGN_ID. Mapped recipes use RECIPE_CAMPAIGN_MAP.
LIVE_CAMPAIGN_ID = "4f118329-c07e-4279-9c08-12059f2a49f4"
MISSING_CAMPAIGN_MESSAGE = (
    "OUTBOUND_INSTANTLY_CAMPAIGN_ID is unset. Skipping the Instantly push. "
    "The retired campaign is not used as a fallback."
)
DEFAULT_PROTECTED: tuple[str, ...] = ()

# Contacts whose recipe column is one of these go to that Instantly campaign.
# Anything else uses the caller's campaign id, then OUTBOUND_INSTANTLY_CAMPAIGN_ID.
# This is a code map on purpose: no new env var and no schema change.
RECIPE_CAMPAIGN_MAP: dict[str, str] = {
    "amz_on": "a333e00e-bc3b-4abf-bcbf-4e0eb126090c",
    "amz_off": "45a521a0-68e2-439f-9622-1672f9ede214",
    "plan_upgrade": "1937c21a-89c4-458f-a949-306f90ae6258",
    "replatform": "d12b5512-d34c-4331-841f-8e59263f5b5e",
    "replatformed": "d12b5512-d34c-4331-841f-8e59263f5b5e",
    "abby_reengage": "8ebf1923-7ba4-4a63-96a8-adb18a58ddf6",
}

# Partner-list contacts are stored for a separate pass and stay out of push_pending.
REDO_PARTNER_RECIPE = "redo_partner"
PUSH_EXCLUDED_RECIPES: frozenset[str] = frozenset({REDO_PARTNER_RECIPE})


def configured_campaign_id() -> str:
    """Return the campaign for recipes that are not in ``RECIPE_CAMPAIGN_MAP``.

    Empty when ``OUTBOUND_INSTANTLY_CAMPAIGN_ID`` is unset. ``LIVE_CAMPAIGN_ID``
    is not a substitute.
    """

    return (os.getenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID") or "").strip()


def campaign_for_recipe(recipe: Any, default: str = "",
                        campaign_map: Optional[dict[str, str]] = None) -> str:
    """Campaign for one contact's recipe column.

    A mapped recipe always wins. ``campaign_map`` is the live table when
    ``push_pending`` could read it. ``None`` uses ``RECIPE_CAMPAIGN_MAP``.
    An unmapped recipe uses ``default`` when that is set, otherwise
    ``OUTBOUND_INSTANTLY_CAMPAIGN_ID``. Empty means there is no campaign
    to add this contact to.
    """

    source = RECIPE_CAMPAIGN_MAP if campaign_map is None else campaign_map
    mapped = source.get(str(recipe or "").strip())
    if mapped:
        return mapped
    explicit = str(default or "").strip()
    if explicit:
        return explicit
    return configured_campaign_id()


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


def build_lead(contact: dict[str, Any], lead: dict[str, Any], opener: str) -> dict[str, Any]:
    """One Instantly lead. Field names match the sequence variables:
    {{firstName}}, {{companyName}}, {{personalization}}, {{category}}."""
    category = friendly_category(lead.get("categories") or "") or str(lead.get("niche") or "").strip().lower()
    return {
        "email": str(contact.get("email") or "").strip().lower(),
        "first_name": str(contact.get("first_name") or "").strip(),
        "last_name": str(contact.get("last_name") or "").strip(),
        "company_name": str(lead.get("brand") or lead.get("domain") or "").strip(),
        "website": str(lead.get("domain") or "").strip(),
        "personalization": opener,
        "custom_variables": {
            "category": category,
            "signal": str(lead.get("recipe") or "").strip(),
            "tier": str(lead.get("tier") or "").strip(),
            "contact_title": str(contact.get("title") or "").strip(),
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

    target = _contacts.registrable_domain(domain)
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
    domain = _contacts.registrable_domain(domain)
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
