"""Deep links into HubSpot's own UI.

The agent is a *companion* to HubSpot, not a replacement: records are edited in
HubSpot, and these helpers build the "Open in HubSpot" links. If no portal id is
configured the helpers return "" and callers simply omit the link.
"""

from __future__ import annotations

_BASE = "https://app.hubspot.com/contacts"

# HubSpot CRM object type ids used in record URLs.
_DEAL = "0-3"
_CONTACT = "0-1"
_COMPANY = "0-2"


def _record(portal_id: str, type_id: str, object_id: str) -> str:
    portal_id = (portal_id or "").strip()
    object_id = (object_id or "").strip()
    if not portal_id or not object_id:
        return ""
    return f"{_BASE}/{portal_id}/record/{type_id}/{object_id}"


def deal_url(portal_id: str, deal_id: str) -> str:
    return _record(portal_id, _DEAL, deal_id)


def contact_url(portal_id: str, contact_id: str) -> str:
    return _record(portal_id, _CONTACT, contact_id)


def company_url(portal_id: str, company_id: str) -> str:
    return _record(portal_id, _COMPANY, company_id)
