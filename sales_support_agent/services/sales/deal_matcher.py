"""Match inbound email addresses to open HubSpot deals via the contact mirror.

Called during mailbox sync to populate MailboxSignal.matched_deal_id without
needing a live HubSpot API call — uses the already-synced contact mirror.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
)

logger = logging.getLogger(__name__)


def match_deal_by_email(session: Session, sender_email: str) -> Optional[str]:
    """Return the hubspot_deal_id of the best open deal for sender_email.

    Looks the email up in the HubSpotContact mirror (case-insensitive), finds
    deal associations via the link table, and returns the soonest-closing open
    deal. Returns None when no match is found.
    """
    if not (sender_email or "").strip():
        return None
    email_lower = sender_email.strip().lower()

    contacts = session.scalars(
        select(HubSpotContact).where(
            HubSpotContact.email.ilike(email_lower)
        )
    ).all()
    if not contacts:
        return None

    contact_ids = [c.hubspot_contact_id for c in contacts]
    links = session.scalars(
        select(HubSpotDealContact).where(
            HubSpotDealContact.hubspot_contact_id.in_(contact_ids)
        )
    ).all()

    deal_ids = list({lnk.hubspot_deal_id for lnk in links})
    if not deal_ids:
        return None

    deal = session.scalars(
        select(HubSpotDeal)
        .where(
            HubSpotDeal.hubspot_deal_id.in_(deal_ids),
            HubSpotDeal.is_closed.is_(False),
        )
        .order_by(HubSpotDeal.close_date.asc().nulls_last())
        .limit(1)
    ).first()

    if deal:
        logger.debug(
            "[deal_matcher] matched %s → deal %s (%s)",
            sender_email, deal.hubspot_deal_id, deal.deal_name,
        )
        return deal.hubspot_deal_id
    return None
