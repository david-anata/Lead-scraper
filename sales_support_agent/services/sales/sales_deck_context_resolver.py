"""Resolve the best HubSpot/current-lead context for a generated sales deck."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    Company,
    Contact,
    HubSpotCompany,
    HubSpotContact,
    HubSpotDeal,
    HubSpotDealContact,
    LeadMirror,
    LeadRecord,
    MailboxSignal,
)


_GENERIC_TOKENS = {
    "a",
    "an",
    "and",
    "anata",
    "brand",
    "co",
    "company",
    "deck",
    "for",
    "inc",
    "llc",
    "organic",
    "sales",
    "strategy",
    "the",
    "with",
    "x",
}
_NON_WORD = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class SalesDeckContextInput:
    hubspot_deal_id: str = ""
    hubspot_company_id: str = ""
    hubspot_contact_ids: tuple[str, ...] = ()
    company_name: str = ""
    company_domain: str = ""
    contact_email: str = ""
    brand_name: str = ""
    product_title: str = ""
    target_product_input: str = ""
    deck_title: str = ""
    notes: str = ""

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "SalesDeckContextInput":
        contact_ids_raw = str(
            payload.get("hubspot_contact_ids")
            or payload.get("hubspot_contact_id")
            or payload.get("contact_id")
            or ""
        )
        return cls(
            hubspot_deal_id=str(payload.get("hubspot_deal_id") or "").strip(),
            hubspot_company_id=str(payload.get("hubspot_company_id") or payload.get("company_id") or "").strip(),
            hubspot_contact_ids=tuple(p.strip() for p in contact_ids_raw.split(",") if p.strip()),
            company_name=str(payload.get("company_name") or payload.get("prospect") or "").strip(),
            company_domain=_domain(str(payload.get("company_domain") or payload.get("website") or payload.get("website_url") or "")),
            contact_email=str(payload.get("contact_email") or "").strip().lower(),
            brand_name=str(payload.get("brand_name") or payload.get("brand") or "").strip(),
            product_title=str(payload.get("product_title") or "").strip(),
            target_product_input=str(payload.get("target_product_input") or "").strip(),
            deck_title=str(payload.get("deck_title") or payload.get("design_title") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
        )


@dataclass(frozen=True)
class SalesDeckCandidate:
    kind: str
    source: str
    score: int
    label: str
    hubspot_deal_id: str = ""
    hubspot_company_id: str = ""
    hubspot_contact_ids: tuple[str, ...] = ()
    lead_id: str = ""
    company_name: str = ""
    company_domain: str = ""
    contact_email: str = ""
    audit: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "source": self.source,
            "score": self.score,
            "confidence": round(self.score / 100, 2),
            "label": self.label,
            "hubspot_deal_id": self.hubspot_deal_id,
            "hubspot_company_id": self.hubspot_company_id,
            "hubspot_contact_ids": list(self.hubspot_contact_ids),
            "lead_id": self.lead_id,
            "company_name": self.company_name,
            "company_domain": self.company_domain,
            "contact_email": self.contact_email,
            "audit": list(self.audit),
        }


@dataclass(frozen=True)
class SalesDeckResolution:
    action: str
    confidence: float
    matched_source: str = ""
    selected: SalesDeckCandidate | None = None
    candidates: tuple[SalesDeckCandidate, ...] = ()
    audit_lines: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "confidence": self.confidence,
            "matched_source": self.matched_source,
            "selected": self.selected.to_dict() if self.selected else None,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "audit_lines": list(self.audit_lines),
        }


def resolve_sales_deck_context(
    session: Session,
    context: SalesDeckContextInput | Mapping[str, Any],
    *,
    live_mailbox_messages: list[Mapping[str, Any]] | None = None,
) -> SalesDeckResolution:
    ctx = context if isinstance(context, SalesDeckContextInput) else SalesDeckContextInput.from_mapping(context)
    audit: list[str] = []
    candidates: list[SalesDeckCandidate] = []

    if ctx.hubspot_deal_id:
        deal = session.get(HubSpotDeal, ctx.hubspot_deal_id)
        if deal is not None and not deal.is_closed:
            company = _company_for_deal(session, deal)
            contacts = _contacts_for_deal(session, deal.hubspot_deal_id)
            candidates.append(_deal_candidate(
                deal,
                company,
                contacts,
                source="explicit_deal_id",
                score=100,
                audit=("Explicit HubSpot deal id supplied.",),
            ))
        elif deal is not None:
            audit.append(f"Explicit HubSpot deal {ctx.hubspot_deal_id} is closed; not attaching automatically.")
        else:
            audit.append(f"Explicit HubSpot deal {ctx.hubspot_deal_id} was not found in the local mirror.")

    tokens = _context_tokens(ctx)
    domains = {d for d in (ctx.company_domain, _domain(ctx.target_product_input)) if d}
    emails = {ctx.contact_email} if ctx.contact_email else set()

    for deal in session.scalars(select(HubSpotDeal).where(HubSpotDeal.is_closed.is_(False))).all():
        company = _company_for_deal(session, deal)
        contacts = _contacts_for_deal(session, deal.hubspot_deal_id)
        best_score, reasons = _score_deal(ctx, deal, company, contacts, tokens, domains, emails)
        if best_score >= 55:
            candidates.append(_deal_candidate(deal, company, contacts, source="hubspot_mirror", score=best_score, audit=tuple(reasons)))

    if emails:
        contact_rows = session.scalars(select(HubSpotContact).where(HubSpotContact.email.in_(emails))).all()
        contact_ids = {c.hubspot_contact_id for c in contact_rows}
        if contact_ids:
            links = session.scalars(select(HubSpotDealContact).where(HubSpotDealContact.hubspot_contact_id.in_(contact_ids))).all()
            for link in links:
                deal = session.get(HubSpotDeal, link.hubspot_deal_id)
                if deal is None or deal.is_closed:
                    continue
                company = _company_for_deal(session, deal)
                contacts = _contacts_for_deal(session, deal.hubspot_deal_id)
                candidates.append(_deal_candidate(
                    deal,
                    company,
                    contacts,
                    source="hubspot_contact_email",
                    score=96,
                    audit=(f"Contact email matched HubSpot contact {link.hubspot_contact_id}.",),
                ))

    mailbox_filters = []
    if emails:
        mailbox_filters.append(MailboxSignal.sender_email.in_(emails))
    if domains:
        mailbox_filters.append(MailboxSignal.sender_domain.in_(domains))
    if ctx.hubspot_deal_id:
        mailbox_filters.append(MailboxSignal.matched_deal_id == ctx.hubspot_deal_id)
    if mailbox_filters:
        for signal in session.scalars(select(MailboxSignal).where(or_(*mailbox_filters)).order_by(MailboxSignal.received_at.desc()).limit(20)).all():
            if not signal.matched_deal_id:
                continue
            deal = session.get(HubSpotDeal, signal.matched_deal_id)
            if deal is None or deal.is_closed:
                continue
            company = _company_for_deal(session, deal)
            contacts = _contacts_for_deal(session, deal.hubspot_deal_id)
            candidates.append(_deal_candidate(
                deal,
                company,
                contacts,
                source="mailbox_signal",
                score=88,
                audit=(f"Recent mailbox signal matched {signal.sender_email or signal.sender_domain}.",),
            ))

    for raw in live_mailbox_messages or []:
        sender_email = str(raw.get("senderEmail") or raw.get("sender_email") or "").strip().lower()
        sender_domain = _domain(sender_email)
        if sender_email:
            emails.add(sender_email)
        if sender_domain:
            domains.add(sender_domain)
            audit.append(f"Live Gmail search found recent message from {sender_email or sender_domain}.")

    candidates.extend(_lead_candidates(session, ctx, tokens, domains, emails))
    candidates = _dedupe_candidates(candidates)
    candidates = tuple(sorted(candidates, key=lambda c: c.score, reverse=True)[:8])
    if not candidates:
        return SalesDeckResolution(
            action="unmatched",
            confidence=0.0,
            candidates=(),
            audit_lines=tuple(audit + ["No matching open HubSpot deal or current lead found."]),
        )

    top = candidates[0]
    second_score = candidates[1].score if len(candidates) > 1 else 0
    confidence = round(top.score / 100, 2)
    audit.extend(top.audit)

    if top.kind == "deal" and top.score >= 85 and top.score - second_score >= 8:
        action = "attach_existing"
    elif top.kind == "lead" and top.score >= 85 and not any(c.kind == "deal" and c.score >= 85 for c in candidates):
        action = "create_then_attach"
    elif top.score >= 65:
        action = "needs_review"
    else:
        action = "unmatched"

    return SalesDeckResolution(
        action=action,
        confidence=confidence,
        matched_source=top.source,
        selected=top if action in {"attach_existing", "create_then_attach"} else None,
        candidates=tuple(candidates),
        audit_lines=tuple(audit),
    )


def _deal_candidate(
    deal: HubSpotDeal,
    company: HubSpotCompany | None,
    contacts: list[HubSpotContact],
    *,
    source: str,
    score: int,
    audit: tuple[str, ...],
) -> SalesDeckCandidate:
    contact_ids = tuple(c.hubspot_contact_id for c in contacts if c.hubspot_contact_id)
    contact_email = next((c.email for c in contacts if c.email), "")
    return SalesDeckCandidate(
        kind="deal",
        source=source,
        score=min(100, score),
        label=deal.deal_name or deal.hubspot_deal_id,
        hubspot_deal_id=deal.hubspot_deal_id,
        hubspot_company_id=deal.hubspot_company_id,
        hubspot_contact_ids=contact_ids,
        company_name=company.name if company else "",
        company_domain=company.domain if company else "",
        contact_email=contact_email,
        audit=audit,
    )


def _lead_candidates(
    session: Session,
    ctx: SalesDeckContextInput,
    tokens: set[str],
    domains: set[str],
    emails: set[str],
) -> list[SalesDeckCandidate]:
    candidates: list[SalesDeckCandidate] = []
    for lead in session.scalars(select(LeadMirror).where(LeadMirror.is_closed.is_(False))).all():
        score, reasons = _score_text_source(
            label=lead.task_name,
            tokens=tokens,
            extra_text=" ".join(str(v or "") for v in (lead.product, lead.source, lead.communication_summary, lead.recommended_next_action)),
        )
        lead_email = str(lead.email or "").strip().lower()
        if lead_email and lead_email in emails:
            score = max(score, 88)
            reasons.append(f"LeadMirror email matched {lead_email}.")
        if score >= 55:
            candidates.append(SalesDeckCandidate(
                kind="lead",
                source="lead_mirror",
                score=score,
                label=lead.task_name,
                lead_id=lead.clickup_task_id,
                company_name=ctx.company_name or lead.task_name,
                company_domain=next(iter(domains), ""),
                contact_email=lead_email or ctx.contact_email,
                audit=tuple(reasons),
            ))

    lead_rows = session.scalars(select(LeadRecord).where(LeadRecord.status != "rejected")).all()
    for lead in lead_rows:
        company = session.get(Company, lead.company_id)
        contact = session.get(Contact, lead.contact_id) if lead.contact_id else None
        company_text = " ".join(str(v or "") for v in (company.company_name if company else "", company.domain if company else "", company.website if company else ""))
        contact_email = str(contact.email or "").strip().lower() if contact else ""
        score, reasons = _score_text_source(label=company_text, tokens=tokens, extra_text=str((lead.metadata_json or {})))
        company_domain = _domain(company.domain if company else "") or _domain(company.website if company else "")
        if company_domain and company_domain in domains:
            score = max(score, 90)
            reasons.append(f"Current lead company domain matched {company_domain}.")
        if contact_email and contact_email in emails:
            score = max(score, 90)
            reasons.append(f"Current lead contact email matched {contact_email}.")
        if score >= 55:
            candidates.append(SalesDeckCandidate(
                kind="lead",
                source="lead_record",
                score=score,
                label=(company.company_name if company else "") or lead.lead_key,
                lead_id=str(lead.id),
                company_name=(company.company_name if company else "") or ctx.company_name,
                company_domain=company_domain or next(iter(domains), ""),
                contact_email=contact_email or ctx.contact_email,
                audit=tuple(reasons),
            ))
    return candidates


def _score_deal(
    ctx: SalesDeckContextInput,
    deal: HubSpotDeal,
    company: HubSpotCompany | None,
    contacts: list[HubSpotContact],
    tokens: set[str],
    domains: set[str],
    emails: set[str],
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    company_domain = _domain(company.domain if company else "")
    if company_domain and company_domain in domains:
        score = max(score, 92)
        reasons.append(f"Company domain matched {company_domain}.")
    contact_emails = {str(c.email or "").strip().lower() for c in contacts if str(c.email or "").strip()}
    if contact_emails & emails:
        score = max(score, 95)
        reasons.append("Contact email matched an associated HubSpot contact.")
    text_score, text_reasons = _score_text_source(
        label=" ".join(str(v or "") for v in (deal.deal_name, company.name if company else "")),
        tokens=tokens,
        extra_text=deal.description,
    )
    if text_score:
        score = max(score, text_score)
        reasons.extend(text_reasons)
    if ctx.hubspot_company_id and ctx.hubspot_company_id == deal.hubspot_company_id:
        score = max(score, 94)
        reasons.append("HubSpot company id matched.")
    return score, reasons


def _score_text_source(*, label: str, tokens: set[str], extra_text: str = "") -> tuple[int, list[str]]:
    if not tokens:
        return 0, []
    source_tokens = _tokens(f"{label} {extra_text}")
    if not source_tokens:
        return 0, []
    overlap = tokens & source_tokens
    if len(tokens) >= 2 and tokens.issubset(source_tokens):
        return 84, [f"Brand/company tokens matched: {', '.join(sorted(tokens))}."]
    if len(overlap) >= max(2, min(len(tokens), 3)):
        return 76, [f"Brand/company token overlap: {', '.join(sorted(overlap))}."]
    if len(overlap) == 1 and len(next(iter(overlap))) >= 6:
        return 62, [f"Distinctive brand token matched: {next(iter(overlap))}."]
    return 0, []


def _context_tokens(ctx: SalesDeckContextInput) -> set[str]:
    return _tokens(" ".join([
        ctx.company_name,
        ctx.brand_name,
        ctx.product_title,
        ctx.deck_title,
        ctx.target_product_input,
        ctx.notes,
    ]))


def _tokens(value: str) -> set[str]:
    return {
        part
        for part in _NON_WORD.sub(" ", str(value or "").lower()).split()
        if len(part) >= 3 and part not in _GENERIC_TOKENS and not part.isdigit()
    }


def _domain(value: str) -> str:
    raw = str(value or "").lower().strip()
    if "@" in raw and not raw.startswith("http"):
        raw = raw.rsplit("@", 1)[-1]
    raw = raw.removeprefix("https://").removeprefix("http://").removeprefix("www.")
    raw = raw.split("/", 1)[0].split("?", 1)[0].strip()
    return raw.removeprefix("www.")


def _company_for_deal(session: Session, deal: HubSpotDeal) -> HubSpotCompany | None:
    return session.get(HubSpotCompany, deal.hubspot_company_id) if deal.hubspot_company_id else None


def _contacts_for_deal(session: Session, deal_id: str) -> list[HubSpotContact]:
    links = session.scalars(select(HubSpotDealContact).where(HubSpotDealContact.hubspot_deal_id == deal_id)).all()
    contacts: list[HubSpotContact] = []
    for link in links:
        contact = session.get(HubSpotContact, link.hubspot_contact_id)
        if contact is not None:
            contacts.append(contact)
    return contacts


def _dedupe_candidates(candidates: list[SalesDeckCandidate]) -> list[SalesDeckCandidate]:
    best: dict[tuple[str, str], SalesDeckCandidate] = {}
    for candidate in candidates:
        key = (candidate.kind, candidate.hubspot_deal_id or candidate.lead_id or candidate.label)
        existing = best.get(key)
        if existing is None or candidate.score > existing.score:
            best[key] = candidate
    return list(best.values())
