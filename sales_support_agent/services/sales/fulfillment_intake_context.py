"""Build fulfillment deck intake context from the HubSpot sales mirror."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    CommunicationEvent,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    MailboxSignal,
    SalesDealAsset,
)


def _fmt(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d")


@dataclass
class FulfillmentIntakeContext:
    deal_id: str = ""
    deal_name: str = ""
    company_id: str = ""
    company_name: str = ""
    company_domain: str = ""
    owner_email: str = ""
    contact_ids: list[str] = field(default_factory=list)
    contact_lines: list[str] = field(default_factory=list)
    asset_lines: list[str] = field(default_factory=list)
    mailbox_lines: list[str] = field(default_factory=list)
    conversation_lines: list[str] = field(default_factory=list)
    last_inbound: str = ""
    last_outbound: str = ""
    last_touch: str = ""
    recommended_next_action: str = ""
    communication_summary: str = ""

    @property
    def website_url(self) -> str:
        return self.company_domain

    def to_notes_block(self) -> str:
        lines = [
            "=== HUBSPOT DEAL CONTEXT ===",
            f"Deal: {self.deal_name or self.deal_id}",
            f"Company: {self.company_name}",
            f"Domain: {self.company_domain}",
            f"Owner: {self.owner_email}",
        ]
        if self.contact_lines:
            lines.append("Contacts: " + "; ".join(self.contact_lines))
        if self.last_touch or self.last_inbound or self.last_outbound:
            lines.append(
                "Touch history: "
                + ", ".join(
                    p for p in (
                        f"last inbound {self.last_inbound}" if self.last_inbound else "",
                        f"last outbound {self.last_outbound}" if self.last_outbound else "",
                        f"last meaningful touch {self.last_touch}" if self.last_touch else "",
                    )
                    if p
                )
            )
        if self.communication_summary:
            lines.append(f"Previous conversation summary: {self.communication_summary}")
        if self.recommended_next_action:
            lines.append(f"Recommended next action: {self.recommended_next_action}")
        if self.mailbox_lines:
            lines.append("Recent email context:")
            lines.extend(f"- {line}" for line in self.mailbox_lines[:5])
        if self.conversation_lines:
            lines.append("Recent logged activity:")
            lines.extend(f"- {line}" for line in self.conversation_lines[:5])
        if self.asset_lines:
            lines.append("Existing linked sales assets:")
            lines.extend(f"- {line}" for line in self.asset_lines)
        return "\n".join(line for line in lines if line).strip()


def build_fulfillment_intake_context(session: Session, deal_id: str) -> FulfillmentIntakeContext:
    deal = session.get(HubSpotDeal, deal_id)
    if deal is None:
        return FulfillmentIntakeContext(deal_id=deal_id)

    company = session.get(HubSpotCompany, deal.hubspot_company_id) if deal.hubspot_company_id else None
    ctx = FulfillmentIntakeContext(
        deal_id=deal.hubspot_deal_id,
        deal_name=deal.deal_name,
        company_id=deal.hubspot_company_id,
        company_name=company.name if company else "",
        company_domain=company.domain if company else "",
        owner_email=deal.owner_email,
        last_inbound=_fmt(deal.last_inbound_at),
        last_outbound=_fmt(deal.last_outbound_at),
        last_touch=_fmt(deal.last_meaningful_touch_at),
        recommended_next_action=deal.recommended_next_action,
        communication_summary=deal.communication_summary,
    )

    links = session.scalars(
        select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal.hubspot_deal_id)
    ).all()
    for link in links:
        contact = session.get(HubSpotContact, link.hubspot_contact_id)
        if not contact:
            continue
        name = " ".join(p for p in (contact.first_name, contact.last_name) if p).strip()
        ctx.contact_ids.append(contact.hubspot_contact_id)
        ctx.contact_lines.append(
            " | ".join(p for p in (name, contact.email, contact.job_title) if p)
        )

    for asset in session.scalars(
        select(SalesDealAsset).where(SalesDealAsset.hubspot_deal_id == deal.hubspot_deal_id)
    ).all():
        ctx.asset_lines.append(f"{asset.label or asset.asset_type}: {asset.url}")

    contact_emails = []
    for line in ctx.contact_lines:
        for part in line.split(" | "):
            if "@" in part:
                contact_emails.append(part.strip())
    criteria = [MailboxSignal.matched_deal_id == deal.hubspot_deal_id]
    if contact_emails:
        criteria.append(MailboxSignal.sender_email.in_(contact_emails))
    if ctx.company_domain:
        criteria.append(MailboxSignal.sender_domain == ctx.company_domain)
    signals = session.scalars(select(MailboxSignal).where(or_(*criteria))).all()
    signals = sorted(signals, key=lambda s: s.received_at, reverse=True)
    for sig in signals[:5]:
        ctx.mailbox_lines.append(
            f"{_fmt(sig.received_at)} {sig.sender_email}: {sig.subject or sig.action_summary or sig.snippet}"
        )

    events = session.scalars(
        select(CommunicationEvent).where(CommunicationEvent.hubspot_deal_id == deal.hubspot_deal_id)
    ).all()
    events = sorted(events, key=lambda e: e.occurred_at, reverse=True)
    for ev in events[:5]:
        ctx.conversation_lines.append(
            f"{_fmt(ev.occurred_at)} {ev.event_type}: {ev.summary or ev.recommended_next_action or ev.outcome}"
        )

    return ctx
