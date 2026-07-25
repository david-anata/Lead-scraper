"""HubSpot nurture enrollment for follow-up / no-show contacts (docs/outbound/09 B3).

When a replied contact asks to follow up later or no-shows a call, we stamp a
property on their HubSpot contact (creating it if needed) so a HubSpot workflow
enrolls them in the "Outbound Nurture" sequence. Keeping the warm nurture in
HubSpot protects the cold-send domains in Instantly.

This module only sets the trigger property. The actual nurture sequence and the
enroll-on-property-change workflow live in HubSpot and are David's to create. The
enrollment is best-effort and returns a clear result rather than raising.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

VALID_OUTCOMES = {"follow_up", "no_show"}
DEFAULT_NURTURE_PROPERTY = "outbound_nurture_status"


def _nurture_property() -> str:
    return (os.getenv("HUBSPOT_NURTURE_PROPERTY") or DEFAULT_NURTURE_PROPERTY).strip()


def enroll_contact(
    client: Any,
    *,
    email: str,
    outcome: str,
    brand: Optional[str] = None,
    property_name: Optional[str] = None,
) -> dict[str, Any]:
    """Create or update the HubSpot contact and stamp the nurture trigger.

    Returns {ok, action, reason}. ok=False (never raises) on bad input, HubSpot
    not connected, or an API error, with a plain reason.
    """
    email = str(email or "").strip().lower()
    outcome = str(outcome or "").strip().lower()

    if outcome not in VALID_OUTCOMES:
        return {"ok": False, "reason": f"Outcome must be one of {sorted(VALID_OUTCOMES)}."}
    if "@" not in email or "." not in email:
        return {"ok": False, "reason": "A valid contact email is required."}
    # HubSpotClient exposes is_configured as a property (bool); tests may use a
    # method. Support both so a real client is never mis-read as unconfigured.
    configured = getattr(client, "is_configured", False)
    if callable(configured):
        configured = configured()
    if not configured:
        return {"ok": False, "reason": "HubSpot is not connected on this service."}

    prop = property_name or _nurture_property()
    props = {prop: outcome}
    if brand:
        props["company"] = str(brand)

    try:
        existing = client.find_contact_by_email(email)
        if existing and existing.get("id"):
            client.update_contact(str(existing["id"]), props)
            action = "updated"
        else:
            client.create_contact({"email": email, **props})
            action = "created"
        return {"ok": True, "action": action, "outcome": outcome}
    except Exception as exc:  # noqa: BLE001 — surface a clean reason, never 500 the page
        logger.exception("[outbound-nurture] enroll failed for %s", email)
        return {"ok": False, "reason": f"HubSpot rejected the update: {exc}"}
