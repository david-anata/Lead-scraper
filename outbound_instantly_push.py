"""Push enriched leads straight into an Instantly campaign (replaces Clay's
"Add Lead to Campaign" column).

Safety, in order of importance:
  * DRY RUN is the default. Nothing leaves this process unless the caller passes
    ``dry_run=False`` AND ``OUTBOUND_PUSH_ENABLED`` is "true" on Render.
  * Protected campaigns. The live campaign is listed in
    ``OUTBOUND_PROTECTED_CAMPAIGN_IDS`` (plus a built-in default) and this module
    refuses to add leads to any protected campaign.
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

# The live "AGENT SDR // MARCH '26" campaign. Never written to by this module.
DEFAULT_PROTECTED = ("4f118329-c07e-4279-9c08-12059f2a49f4",)


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
    categories = lead.get("categories") or ""
    if isinstance(categories, (list, tuple)):
        categories = categories[0] if categories else ""
    return {
        "email": str(contact.get("email") or "").strip().lower(),
        "first_name": str(contact.get("first_name") or "").strip(),
        "last_name": str(contact.get("last_name") or "").strip(),
        "company_name": str(lead.get("brand") or lead.get("domain") or "").strip(),
        "website": str(lead.get("domain") or "").strip(),
        "personalization": opener,
        "custom_variables": {
            "category": str(categories or lead.get("niche") or "").strip(),
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
