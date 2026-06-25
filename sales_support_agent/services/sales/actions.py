"""Confidence-tiered action suggestions for the Sales deal detail page.

High-confidence: applied automatically during sync (local DB field updates).
Mid-confidence: shown as 1-click approve cards; executes a HubSpot write on approval.
Low-confidence: shown as informational nudges with an optional HubSpot deep-link.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sales_support_agent.models.entities import HubSpotDeal, MailboxSignal


@dataclass
class ContactInfo:
    """Minimal contact data passed to compute_pending_actions for per-contact checks."""
    contact_id: str
    email: str
    hubspot_url: str = ""


@dataclass
class SalesAction:
    action_id: str                   # unique per deal; matched on approve POST
    action_type: str                 # "update_deal" | "update_contact" | "note"
    confidence: str                  # "mid" | "low"
    label: str                       # card headline / button text
    description: str                 # one-line explanation of what will happen
    hubspot_object_type: str         # "deals" | "contacts"
    hubspot_object_id: str
    properties: dict[str, str] = field(default_factory=dict)
    link_url: str = ""               # low-confidence: shown as "Fix in HubSpot →" button


def compute_pending_actions(
    deal: HubSpotDeal,
    recent_signals: list[MailboxSignal],
    *,
    line_item_total_cents: int = 0,
    contacts: list[ContactInfo] | None = None,
    portal_id: str = "",
    as_of: datetime | None = None,
) -> list[SalesAction]:
    """Return actions to surface as approval cards on the deal detail page."""
    if deal.is_closed:
        return []
    as_of = as_of or datetime.now(timezone.utc)
    actions: list[SalesAction] = []

    def _aware(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def _deal_url() -> str:
        if portal_id:
            return f"https://app.hubspot.com/contacts/{portal_id}/deal/{deal.hubspot_deal_id}"
        return ""

    # Mid: overdue close date → push 30 days
    close = deal.close_date
    if close is not None and _aware(close) < as_of:
        new_date = as_of + timedelta(days=30)
        new_ts = str(int(
            new_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
        ))
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:push_close_date",
            action_type="update_deal",
            confidence="mid",
            label=f"Push close date → {new_date.strftime('%b %-d, %Y')}",
            description="Close date has passed. Extend 30 days so this deal stays in the active pipeline.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"closedate": new_ts},
        ))

    # Mid: deal amount is $0 but line items have a real total → sync it
    if (deal.amount_cents or 0) <= 0 and line_item_total_cents > 0:
        amount_str = f"{line_item_total_cents / 100:.2f}"
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:sync_amount",
            action_type="update_deal",
            confidence="mid",
            label=f"Set amount from line items (${int(line_item_total_cents / 100):,})",
            description="Deal amount is $0 but line items total a real value. Sync it to HubSpot.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"amount": amount_str},
        ))

    # Mid: no close date at all → propose 30 days from now
    if deal.close_date is None:
        new_date = as_of + timedelta(days=30)
        new_ts = str(int(
            new_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
        ))
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:set_close_date",
            action_type="update_deal",
            confidence="mid",
            label=f"Set close date → {new_date.strftime('%b %-d, %Y')}",
            description="No close date set. Adding one keeps this deal prioritised on the board.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            properties={"closedate": new_ts},
        ))

    # Low: no contacts on deal
    if contacts is not None and len(contacts) == 0:
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:no_contacts",
            action_type="note",
            confidence="low",
            label="No contacts on this deal",
            description="Add the buyer contact in HubSpot so you can track who to follow up with.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=_deal_url(),
        ))

    # Low: no company linked
    if not (getattr(deal, "hubspot_company_id", "") or "").strip():
        actions.append(SalesAction(
            action_id=f"{deal.hubspot_deal_id}:no_company",
            action_type="note",
            confidence="low",
            label="No company linked to this deal",
            description="Associate a company in HubSpot so you can see company-level context and match inbound mail.",
            hubspot_object_type="deals",
            hubspot_object_id=deal.hubspot_deal_id,
            link_url=_deal_url(),
        ))

    # Low: contacts missing email (one card per contact)
    for ci in (contacts or []):
        if not (ci.email or "").strip():
            hs_url = ci.hubspot_url or (
                f"https://app.hubspot.com/contacts/{portal_id}/contact/{ci.contact_id}"
                if portal_id else ""
            )
            actions.append(SalesAction(
                action_id=f"{deal.hubspot_deal_id}:contact_no_email_{ci.contact_id}",
                action_type="note",
                confidence="low",
                label="Contact missing email address",
                description="Add an email address for this contact in HubSpot so you can follow up directly.",
                hubspot_object_type="contacts",
                hubspot_object_id=ci.contact_id,
                link_url=hs_url,
            ))

    # Recent inbound reply → propose stage move (mid) or nudge (low).
    cutoff = as_of - timedelta(days=14)
    inbound = [s for s in recent_signals if _aware(s.received_at) > cutoff]
    if inbound:
        latest = max(inbound, key=lambda s: s.received_at)
        late_stages = {"contractsent", "closedwon", "closedlost"}
        if deal.deal_stage not in late_stages:
            next_stage = _try_get_next_stage(
                getattr(deal, "pipeline", "") or "",
                deal.deal_stage or "",
            )
            if next_stage:
                next_id, next_label = next_stage
                actions.append(SalesAction(
                    action_id=f"{deal.hubspot_deal_id}:stage_move",
                    action_type="update_deal",
                    confidence="mid",
                    label=f"Move stage → {next_label}",
                    description=(
                        f"Prospect replied {latest.received_at.strftime('%b %-d')}. "
                        f"Move deal to '{next_label}' to reflect progress."
                    ),
                    hubspot_object_type="deals",
                    hubspot_object_id=deal.hubspot_deal_id,
                    properties={"dealstage": next_id},
                ))
            else:
                actions.append(SalesAction(
                    action_id=f"{deal.hubspot_deal_id}:replied_note",
                    action_type="note",
                    confidence="low",
                    label=f"Prospect replied {latest.received_at.strftime('%b %-d')} — update stage?",
                    description=(latest.subject or "(no subject)")[:100],
                    hubspot_object_type="deals",
                    hubspot_object_id=deal.hubspot_deal_id,
                    link_url=_deal_url(),
                ))

    return actions


def _try_get_next_stage(pipeline_id: str, stage_id: str):
    """Wrapper around pipeline.get_next_stage that swallows all errors."""
    try:
        from sales_support_agent.services.sales.pipeline import get_next_stage
        return get_next_stage(pipeline_id, stage_id)
    except Exception:
        return None
