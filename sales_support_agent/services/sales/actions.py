"""Confidence-tiered action suggestions for the Sales deal detail page.

High-confidence: applied automatically during sync (local DB field updates).
Mid-confidence: shown as 1-click approve cards; executes a HubSpot write on approval.
Low-confidence: shown as informational nudges with a HubSpot deep-link to act.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sales_support_agent.models.entities import HubSpotDeal, MailboxSignal


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


def compute_pending_actions(
    deal: HubSpotDeal,
    recent_signals: list[MailboxSignal],
    *,
    line_item_total_cents: int = 0,
    as_of: datetime | None = None,
) -> list[SalesAction]:
    """Return actions to surface as approval cards on the deal detail page."""
    if deal.is_closed:
        return []
    as_of = as_of or datetime.now(timezone.utc)
    actions: list[SalesAction] = []

    def _aware(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

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

    # Low: recent inbound reply → remind rep to move stage (stage IDs are pipeline-specific,
    # so we surface a nudge rather than auto-writing)
    cutoff = as_of - timedelta(days=14)
    inbound = [s for s in recent_signals if _aware(s.received_at) > cutoff]
    if inbound:
        latest = max(inbound, key=lambda s: s.received_at)
        late_stages = {"contractsent", "closedwon", "closedlost"}
        if deal.deal_stage not in late_stages:
            actions.append(SalesAction(
                action_id=f"{deal.hubspot_deal_id}:replied_note",
                action_type="note",
                confidence="low",
                label=f"Prospect replied {latest.received_at.strftime('%b %-d')} — update stage?",
                description=(latest.subject or "(no subject)")[:100],
                hubspot_object_type="deals",
                hubspot_object_id=deal.hubspot_deal_id,
                properties={},
            ))

    return actions
