"""Read models for the Building contract workspace.

This module only reads. Every contract mutation keeps its existing router,
permission, typed confirmation, idempotency, and audit contract. Nothing here
creates a document, signature request, invoice, or provider object.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingContact,
    BuildingPaymentRequestReadiness,
    BuildingProposal,
    BuildingReservation,
    BuildingSignatureRequestReadiness,
    BuildingSpace,
)
from sales_support_agent.services.building_contract_templates import (
    document_checksum,
    render_document_text,
)


#: Shared operator state vocabulary from DESIGN.md, mapped to ``app-status--*``.
_PREPARATION_STATES: dict[str, tuple[str, str]] = {
    "approved": ("Ready", "ready"),
    "in_review": ("Needs review", "review"),
    "prepared": ("Queued", "queued"),
    "expired": ("Blocked", "blocked"),
    "cancelled": ("Blocked", "blocked"),
}
_PROVIDER_STATES: dict[str, tuple[str, str]] = {
    "signed": ("Confirmed", "confirmed"),
    "sent": ("Delivered", "delivered"),
    "voided": ("Failed", "failed"),
}
_PAYMENT_STATES: dict[str, tuple[str, str]] = {
    "approved": ("Ready", "ready"),
    "in_review": ("Needs review", "review"),
    "prepared": ("Queued", "queued"),
    "expired": ("Blocked", "blocked"),
    "cancelled": ("Blocked", "blocked"),
}
UNVERIFIED_STATE = ("Unverified", "stale")
CONTRACT_STATE_FILTERS = (
    "Ready",
    "Needs review",
    "Queued",
    "Confirmed",
    "Delivered",
    "Failed",
    "Blocked",
    "Unverified",
)
CONTRACT_TYPE_LABELS = {"event": "Event", "workspace": "Workspace"}


def _discount_terms(quote: BuildingProposal) -> dict[str, Any]:
    """Read the discount off the frozen quote so the contract states it.

    The quote flow already records a discount as its own negative line item with
    a required business reason. Deriving from that rather than from a separate
    field means the contract, the quote and the invoice cannot disagree: there
    is only one number.

    A booking with no discount returns zero and an empty reason, which the
    document renders as a plain total with no discount language.
    """

    discount_cents = 0
    reason = ""
    for item in quote.line_items_json or []:
        if not isinstance(item, dict) or item.get("type") != "discount":
            continue
        discount_cents += abs(int(item.get("amount_cents") or 0))
        if not reason:
            reason = str(item.get("description") or "").strip()
    return {
        "subtotal_before_discount": quote.amount_cents + discount_cents,
        "discount_amount": discount_cents,
        "discount_reason": reason,
    }


def compute_event_merge_values(
    *,
    reservation: BuildingReservation,
    contact: BuildingContact,
    space: BuildingSpace,
    quote: BuildingProposal,
) -> tuple[dict[str, Any], int, str]:
    """Derive the allow-listed merge values, required amount, and request type.

    Package preparation and the template preview both call this, so an operator
    previews exactly the values a prepared package would freeze. Returns a
    deposit amount of zero when the frozen terms cannot produce a valid request;
    the caller decides whether that is fatal.
    """

    rate = dict(quote.rate_plan_snapshot_json or {})
    deposit_type = str(rate.get("deposit_type") or "none")
    deposit_cents = {
        "fixed": min(int(rate.get("deposit_amount_cents") or 0), quote.amount_cents),
        "percent": min(
            (quote.amount_cents * int(rate.get("deposit_percent_bps") or 0) + 5000)
            // 10000,
            quote.amount_cents,
        ),
        "none": quote.amount_cents,
    }.get(deposit_type)
    if deposit_cents is None:
        deposit_cents = 0
    request_type = "full_amount" if deposit_type == "none" else "deposit"
    merge_values = {
        "customer_name": contact.full_name,
        "customer_email": contact.email,
        "event_space": space.name,
        "setup_starts_at": instant_iso(reservation.starts_at),
        "guest_starts_at": instant_iso(reservation.guest_starts_at),
        "guest_ends_at": instant_iso(reservation.guest_ends_at),
        "teardown_ends_at": instant_iso(reservation.ends_at),
        "attendance": reservation.attendance,
        **_discount_terms(quote),
        "quote_total": quote.amount_cents,
        "currency": quote.currency,
        "deposit_amount": deposit_cents,
        "deposit_type": deposit_type,
        "cancellation_policy": str(rate.get("cancellation_policy") or ""),
        "tax_terms": {
            "status": str(rate.get("tax_status") or "review_required"),
            "rate_bps": int(rate.get("tax_rate_bps") or 0),
            "note": str(rate.get("tax_note") or ""),
        },
        "included": list(rate.get("included") or []),
        "addons": list(rate.get("addons") or []),
    }
    return merge_values, deposit_cents, request_type


def load_preview_merge_values(
    session: Any, reservation_id: str
) -> Optional[dict[str, Any]]:
    """Merge values for a template preview, or None when the records are missing."""

    reservation = session.get(BuildingReservation, reservation_id)
    if reservation is None:
        return None
    contact = (
        session.get(BuildingContact, reservation.contact_id)
        if reservation.contact_id
        else None
    )
    space = session.get(BuildingSpace, reservation.space_id) if reservation.space_id else None
    quote = session.execute(
        select(BuildingProposal)
        .where(
            BuildingProposal.reservation_id == reservation.id,
            BuildingProposal.proposal_type == "quote",
        )
        .order_by(BuildingProposal.version.desc())
    ).scalars().first()
    if contact is None or space is None or quote is None:
        return None
    merge_values, _deposit, _request_type = compute_event_merge_values(
        reservation=reservation, contact=contact, space=space, quote=quote
    )
    return merge_values


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def instant_iso(value: Optional[datetime]) -> Optional[str]:
    """Serialise a stored instant with its offset attached.

    SQLite returns these naive. Without the offset the contract renderer reads a
    UTC instant as local time, which printed a 5 PM event as 9 PM on the page
    and in the agreement the customer signs.
    """

    aware = _aware(value)
    return aware.isoformat() if aware is not None else None


def contract_state(agreement: BuildingAgreement) -> tuple[str, str]:
    """Reconcile provider status and preparation status into one honest state."""

    if not agreement.package_checksum:
        return UNVERIFIED_STATE
    provider = _PROVIDER_STATES.get(str(agreement.status or ""))
    if provider:
        return provider
    return _PREPARATION_STATES.get(
        str(agreement.preparation_status or ""), ("Stale", "stale")
    )


def payment_state(
    payment: Optional[BuildingPaymentRequestReadiness],
) -> tuple[str, str]:
    if payment is None:
        return ("Missing", "blocked")
    return _PAYMENT_STATES.get(str(payment.status or ""), ("Stale", "stale"))


def has_approved_template(session: Any) -> bool:
    """True when at least one approved template exists to prepare against."""

    return session.execute(
        select(BuildingAgreementTemplate.id).where(
            BuildingAgreementTemplate.status == "approved"
        ).limit(1)
    ).first() is not None


def _row_value_cents(
    agreement: BuildingAgreement,
    payment: Optional[BuildingPaymentRequestReadiness],
) -> tuple[int, str]:
    snapshot = dict(agreement.package_snapshot_json or {})
    quote = dict(snapshot.get("quote") or {})
    amount = int(quote.get("amount_cents") or 0)
    currency = str(quote.get("currency") or "")
    if not amount and payment is not None:
        amount = int(payment.amount_cents or 0)
    if not currency:
        currency = str(payment.currency if payment is not None else "USD") or "USD"
    return amount, currency


def load_contract_rows(session: Any) -> list[dict[str, Any]]:
    """Return every agreement joined to the records an operator needs to judge it."""

    agreements = session.execute(
        select(BuildingAgreement).order_by(BuildingAgreement.updated_at.desc())
    ).scalars().all()
    if not agreements:
        return []

    reservation_ids = {item.reservation_id for item in agreements if item.reservation_id}
    reservations = {
        item.id: item
        for item in session.execute(
            select(BuildingReservation).where(
                BuildingReservation.id.in_(reservation_ids)
            )
        ).scalars().all()
    } if reservation_ids else {}
    contact_ids = {
        item.contact_id for item in reservations.values() if item.contact_id
    }
    contacts = {
        item.id: item
        for item in session.execute(
            select(BuildingContact).where(BuildingContact.id.in_(contact_ids))
        ).scalars().all()
    } if contact_ids else {}
    space_ids = {item.space_id for item in reservations.values() if item.space_id}
    spaces = {
        item.id: item
        for item in session.execute(
            select(BuildingSpace).where(BuildingSpace.id.in_(space_ids))
        ).scalars().all()
    } if space_ids else {}
    payments = {
        item.agreement_id: item
        for item in session.execute(
            select(BuildingPaymentRequestReadiness).where(
                BuildingPaymentRequestReadiness.agreement_id.in_(
                    [item.id for item in agreements]
                )
            )
        ).scalars().all()
    }

    rows: list[dict[str, Any]] = []
    for agreement in agreements:
        reservation = reservations.get(agreement.reservation_id)
        contact = contacts.get(reservation.contact_id) if reservation else None
        space = spaces.get(reservation.space_id) if reservation else None
        payment = payments.get(agreement.id)
        state_label, state_modifier = contract_state(agreement)
        payment_label, payment_modifier = payment_state(payment)
        amount_cents, currency = _row_value_cents(agreement, payment)
        kind = str(reservation.kind if reservation else "") or "event"
        rows.append({
            "id": agreement.id,
            "version": agreement.version,
            "verified": bool(agreement.package_checksum),
            "reservation_id": str(agreement.reservation_id or ""),
            "customer_name": str(contact.full_name if contact else "") or "Unlinked contact",
            "customer_email": str(contact.email if contact else ""),
            "space_name": str(space.name if space else "") or "Unlinked space",
            "contract_type": kind,
            "contract_type_label": CONTRACT_TYPE_LABELS.get(kind, kind.title()),
            "starts_at": _aware(
                (reservation.guest_starts_at or reservation.starts_at)
                if reservation
                else None
            ),
            "ends_at": _aware(
                (reservation.guest_ends_at or reservation.ends_at)
                if reservation
                else None
            ),
            "amount_cents": amount_cents,
            "currency": currency,
            "deposit_required": bool(reservation.deposit_required) if reservation else False,
            "state_label": state_label,
            "state_modifier": state_modifier,
            "payment_label": payment_label,
            "payment_modifier": payment_modifier,
            "owner": str(reservation.assigned_owner if reservation else ""),
            "template_name": str(agreement.template_name or ""),
            "checksum": str(agreement.package_checksum or ""),
            "updated_at": _aware(agreement.updated_at),
        })
    return rows


def filter_contract_rows(
    rows: list[dict[str, Any]],
    *,
    search: str = "",
    state: str = "",
    contract_type: str = "",
) -> list[dict[str, Any]]:
    """Apply the command-bar scope. Filters combine; empty values mean no filter."""

    needle = search.strip().lower()
    result = rows
    if needle:
        result = [
            row for row in result
            if needle in row["customer_name"].lower()
            or needle in row["customer_email"].lower()
            or needle in row["space_name"].lower()
            or needle in row["reservation_id"].lower()
            or needle in row["id"].lower()
        ]
    if state:
        result = [row for row in result if row["state_label"] == state]
    if contract_type:
        result = [row for row in result if row["contract_type"] == contract_type]
    return result


def load_contract_detail(session: Any, agreement_id: str) -> Optional[dict[str, Any]]:
    """Return one contract with its linked records, snapshot, and audit history."""

    agreement = session.get(BuildingAgreement, agreement_id)
    if agreement is None:
        return None
    reservation = (
        session.get(BuildingReservation, agreement.reservation_id)
        if agreement.reservation_id
        else None
    )
    contact = (
        session.get(BuildingContact, reservation.contact_id)
        if reservation and reservation.contact_id
        else None
    )
    space = (
        session.get(BuildingSpace, reservation.space_id)
        if reservation and reservation.space_id
        else None
    )
    payment = session.execute(
        select(BuildingPaymentRequestReadiness).where(
            BuildingPaymentRequestReadiness.agreement_id == agreement.id
        )
    ).scalars().first()
    signature = session.execute(
        select(BuildingSignatureRequestReadiness).where(
            BuildingSignatureRequestReadiness.agreement_id == agreement.id
        )
    ).scalars().first()
    template = (
        session.get(BuildingAgreementTemplate, agreement.template_id)
        if agreement.template_id
        else None
    )
    snapshot = dict(agreement.package_snapshot_json or {})
    frozen_template = dict(snapshot.get("template") or {})
    frozen_document = dict(snapshot.get("document") or {})
    template_differences: list[str] = []
    current_document_checksum = ""
    if template is None:
        template_differences.append("The frozen template version no longer exists.")
    else:
        if template.status != "approved":
            template_differences.append(
                f"The template is now {template.status}, not approved."
            )
        for key, current in (
            ("id", template.id),
            ("version", template.version),
            ("reference", template.template_reference),
        ):
            if frozen_template.get(key) != current:
                template_differences.append(
                    f"Template {key.replace('_', ' ')} differs from the frozen package."
                )
        if frozen_document.get("text"):
            current_text = render_document_text(
                name=template.name,
                body_markdown=template.body_markdown or "",
                clauses=template.clauses_json or [],
                merge_values=dict(snapshot.get("merge_values") or {}),
            )
            current_document_checksum = document_checksum(current_text)
            if current_document_checksum != frozen_document.get("checksum"):
                template_differences.append(
                    "Rendered contract text differs from the frozen package."
                )
    quote_id = str((snapshot.get("quote") or {}).get("id") or "")
    quote = session.get(BuildingProposal, quote_id) if quote_id else None
    audit_ids = (
        [agreement.id]
        + ([payment.id] if payment else [])
        + ([signature.id] if signature else [])
    )
    audit = session.execute(
        select(BuildingAuditEvent)
        .where(BuildingAuditEvent.entity_id.in_(audit_ids))
        .order_by(BuildingAuditEvent.created_at.desc(), BuildingAuditEvent.id.desc())
        .limit(100)
    ).scalars().all()
    state_label, state_modifier = contract_state(agreement)
    payment_label, payment_modifier = payment_state(payment)
    amount_cents, currency = _row_value_cents(agreement, payment)
    kind = str(reservation.kind if reservation else "") or "event"
    hold_expires_at = _aware(reservation.hold_expires_at) if reservation else None
    hold_active = bool(
        reservation
        and reservation.status == "soft_hold"
        and hold_expires_at is not None
        and hold_expires_at > datetime.now(timezone.utc)
    )
    return {
        "id": agreement.id,
        "version": agreement.version,
        "verified": bool(agreement.package_checksum),
        "checksum": str(agreement.package_checksum or ""),
        "provider_status": str(agreement.status or ""),
        "preparation_status": str(agreement.preparation_status or ""),
        "state_label": state_label,
        "state_modifier": state_modifier,
        "provider": str(agreement.provider or ""),
        "provider_reference": str(agreement.provider_reference or ""),
        "document_url": str(agreement.document_url or ""),
        "reviewed_by": str(agreement.reviewed_by or ""),
        "reviewed_at": _aware(agreement.reviewed_at),
        "approved_by": str(agreement.approved_by or ""),
        "approved_at": _aware(agreement.approved_at),
        "created_at": _aware(agreement.created_at),
        "updated_at": _aware(agreement.updated_at),
        "contract_type": kind,
        "contract_type_label": CONTRACT_TYPE_LABELS.get(kind, kind.title()),
        "customer_name": str(contact.full_name if contact else "") or "Unlinked contact",
        "customer_email": str(contact.email if contact else ""),
        "space_name": str(space.name if space else "") or "Unlinked space",
        "reservation_id": str(agreement.reservation_id or ""),
        "reservation_status": str(reservation.status if reservation else ""),
        # The lead this contract came from, so the two are one click apart.
        "inquiry_id": str(reservation.inquiry_id if reservation else "") or "",
        "hold_expires_at": hold_expires_at,
        "hold_active": hold_active,
        "owner": str(reservation.assigned_owner if reservation else ""),
        "starts_at": _aware(
            (reservation.guest_starts_at or reservation.starts_at)
            if reservation
            else None
        ),
        "ends_at": _aware(
            (reservation.guest_ends_at or reservation.ends_at) if reservation else None
        ),
        "amount_cents": amount_cents,
        "currency": currency,
        "snapshot": snapshot,
        "template": {
            "id": str(template.id if template else agreement.template_id or ""),
            "name": str(template.name if template else agreement.template_name or ""),
            "version": template.version if template else None,
            "status": str(template.status if template else ""),
            "reference": str(template.template_reference if template else ""),
        },
        "template_comparison": {
            "matches": not template_differences,
            "differences": template_differences,
            "frozen_document_checksum": str(frozen_document.get("checksum") or ""),
            "current_document_checksum": current_document_checksum,
        },
        "quote": {
            "id": quote_id,
            "version": quote.version if quote else None,
            "status": str(quote.status if quote else ""),
        },
        "payment": None if payment is None else {
            "id": payment.id,
            "status": str(payment.status or ""),
            "state_label": payment_label,
            "state_modifier": payment_modifier,
            "request_type": str(payment.request_type or ""),
            "amount_cents": int(payment.amount_cents or 0),
            "currency": str(payment.currency or "USD"),
            "checksum": str(payment.checksum or ""),
            "metadata": dict(payment.metadata_json or {}),
        },
        "signature": None if signature is None else {
            "id": signature.id,
            "version": signature.version,
            "status": str(signature.status or ""),
            "signer_name": str(signature.signer_name or ""),
            "signer_email": str(signature.signer_email or ""),
            "agreement_checksum": str(signature.agreement_checksum or ""),
            "checksum": str(signature.checksum or ""),
            "provider": str(signature.provider or ""),
            "provider_reference": str(signature.provider_reference or ""),
            "delivery_status": str(signature.delivery_status or "not_sent"),
            "reviewed_by": str(signature.reviewed_by or ""),
            "reviewed_at": _aware(signature.reviewed_at),
            "approved_by": str(signature.approved_by or ""),
            "approved_at": _aware(signature.approved_at),
        },
        "payment_label": payment_label,
        "payment_modifier": payment_modifier,
        "audit": [
            {
                "action": item.action,
                "actor": item.actor,
                "entity_type": item.entity_type,
                "created_at": _aware(item.created_at),
                "before": dict(item.before_json or {}),
                "after": dict(item.after_json or {}),
            }
            for item in audit
        ],
    }


def load_preparation_options(session: Any) -> dict[str, Any]:
    """Eligible reservations, frozen quote drafts, and approved templates.

    Preparation fails closed on the same preconditions the internal API enforces,
    so the picker only offers records that can actually succeed.
    """

    now = datetime.now(timezone.utc)
    reservations = session.execute(
        select(BuildingReservation)
        .where(
            BuildingReservation.kind == "event",
            BuildingReservation.status == "soft_hold",
        )
        .order_by(BuildingReservation.starts_at)
    ).scalars().all()
    active = [
        item for item in reservations
        if item.hold_expires_at is not None and _aware(item.hold_expires_at) > now
    ]
    contacts = {
        item.id: item
        for item in session.execute(
            select(BuildingContact).where(
                BuildingContact.id.in_(
                    [item.contact_id for item in active if item.contact_id]
                )
            )
        ).scalars().all()
    } if active else {}
    spaces = {
        item.id: item
        for item in session.execute(
            select(BuildingSpace).where(
                BuildingSpace.id.in_([item.space_id for item in active if item.space_id])
            )
        ).scalars().all()
    } if active else {}
    quotes = session.execute(
        select(BuildingProposal)
        .where(
            BuildingProposal.proposal_type == "quote",
            BuildingProposal.status == "draft",
            BuildingProposal.reservation_id.in_([item.id for item in active]),
        )
        .order_by(BuildingProposal.version.desc())
    ).scalars().all() if active else []
    templates = session.execute(
        select(BuildingAgreementTemplate)
        .where(BuildingAgreementTemplate.status == "approved")
        .order_by(
            BuildingAgreementTemplate.template_key,
            BuildingAgreementTemplate.version.desc(),
        )
    ).scalars().all()
    return {
        "reservations": [
            {
                "id": item.id,
                "label": " · ".join(filter(None, (
                    str(contacts[item.contact_id].full_name)
                    if item.contact_id in contacts else "",
                    str(spaces[item.space_id].name) if item.space_id in spaces else "",
                    _aware(item.starts_at).strftime("%b %d, %Y")
                    if item.starts_at else "",
                ))) or item.id,
            }
            for item in active
        ],
        "quotes": [
            {
                "id": item.id,
                "reservation_id": item.reservation_id,
                "label": f"v{item.version} · {item.currency} {item.amount_cents / 100:,.2f}",
            }
            for item in quotes
        ],
        "templates": [
            {
                "id": item.id,
                "label": f"{item.name} · v{item.version}",
            }
            for item in templates
        ],
    }
