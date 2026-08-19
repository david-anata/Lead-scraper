"""Can this lead become a contract, and if not, why not.

One function answers that, and both the page and the route ask it. They used to
decide separately: the page drew a live button for every lead, while the route
sent non-event leads to a section the page never renders. The click reloaded,
landed at the top, and said nothing. A single verdict removes that whole class
of failure, because a button can only exist where the verdict says it can.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingInquiry,
    BuildingOffering,
    BuildingPaymentRequestReadiness,
    BuildingRelationship,
    BuildingReservation,
)

#: A prepared package is still only a draft, so it may be undone. Anything the
#: customer could already have seen is not ours to take back.
UNDOABLE_AGREEMENT_STATUSES = frozenset({"prepared"})


@dataclass(frozen=True)
class ContractReadiness:
    """Whether the press can succeed, and what to say when it cannot."""

    ready: bool
    #: Machine-readable cause, for tests and audit. Empty when ready.
    reason: str = ""
    #: One sentence an operator can act on. Empty when ready.
    message: str = ""
    #: Where to go to fix it, when there is somewhere to go.
    fix_url: str = ""
    fix_label: str = ""
    #: Set when this lead already has a contract, so the page can link to it.
    agreement_id: str = ""
    #: Interview answers the contract itself needs, still blank.
    missing_answers: tuple[str, ...] = field(default_factory=tuple)

    @property
    def blocked(self) -> bool:
        return not self.ready


#: A hold that was released is not a booking any more. Treating one as if it
#: were made an undone lead look permanently spent: the page kept offering
#: "Open the contract" for a contract that had been cancelled.
INACTIVE_RESERVATION_STATUSES = frozenset({"cancelled", "expired"})


def active_reservation_for(session: Any, inquiry_id: str) -> Any:
    """The booking this lead currently has, ignoring released ones."""

    return session.execute(
        select(BuildingReservation)
        .where(
            BuildingReservation.inquiry_id == inquiry_id,
            BuildingReservation.status.notin_(INACTIVE_RESERVATION_STATUSES),
        )
        .order_by(BuildingReservation.created_at.desc())
    ).scalars().first()


def _package_amount_cents(agreement: BuildingAgreement) -> int:
    """What the frozen package says this contract is worth."""

    snapshot = dict(agreement.package_snapshot_json or {})
    quote = dict(snapshot.get("quote") or {})
    return int(quote.get("amount_cents") or 0)


def _lead_url(inquiry_id: str) -> str:
    return f"/admin/building/inquiries/{inquiry_id}"


def contract_readiness(
    session: Any,
    inquiry: BuildingInquiry,
    *,
    current_total_cents: int | None = None,
) -> ContractReadiness:
    """Decide once whether "Create the contract" can work on this lead.

    Ordered by what an operator would fix first, so the message names the next
    real step rather than the deepest technical precondition.
    """

    inquiry_id = str(inquiry.id)

    # How intake happened to label a lead is not a reason to refuse a contract.
    # Arena enquiries arrive from Eventective filed as workspace requests, with
    # an event date and Arena pricing already on them, and refusing those left
    # the operator with a correct message and no way to do their job. Pressing
    # the button is a deliberate act, and it can be undone.

    reservation = active_reservation_for(session, inquiry_id)

    if reservation is not None:
        agreement = session.execute(
            select(BuildingAgreement)
            .where(BuildingAgreement.reservation_id == reservation.id)
            .order_by(BuildingAgreement.version.desc())
        ).scalars().first()
        live = (
            agreement is not None
            and str(agreement.preparation_status or "") != "cancelled"
        )
        # Repricing a lead is allowed to produce a new agreement version, so a
        # contract that no longer matches the pricing on screen is not a reason
        # to refuse. Blocking that turned an intended revision into a dead end.
        stale = live and current_total_cents is not None and (
            _package_amount_cents(agreement) != int(current_total_cents)
        )
        if live and not stale:
            return ContractReadiness(
                ready=False,
                reason="already_has_contract",
                message="This lead already has a contract.",
                fix_url=f"/admin/building/contracts/{agreement.id}",
                fix_label="Open the contract",
                agreement_id=str(agreement.id),
            )

    relationship = session.execute(
        select(BuildingRelationship).where(
            BuildingRelationship.source_reference == f"inquiry:{inquiry_id}"
        )
    ).scalars().first()
    if relationship is None:
        return ContractReadiness(
            ready=False,
            reason="no_customer",
            message="No customer is linked to this lead yet.",
            fix_url=f"{_lead_url(inquiry_id)}#customer",
            fix_label="Link the customer",
        )

    offering = session.execute(
        select(BuildingOffering).where(BuildingOffering.offering_type == "event")
    ).scalars().first()
    if offering is None:
        return ContractReadiness(
            ready=False,
            reason="no_event_offering",
            message="No event offering is set up to book against.",
            fix_url="/admin/building/settings",
            fix_label="Open building settings",
        )

    template = session.execute(
        select(BuildingAgreementTemplate.id).where(
            BuildingAgreementTemplate.status == "approved"
        )
    ).scalars().first()
    if not template:
        return ContractReadiness(
            ready=False,
            reason="no_approved_template",
            message="No contract template is approved yet.",
            fix_url="/admin/building/contracts/templates",
            fix_label="Open templates",
        )

    return ContractReadiness(ready=True)


def undo_refusal(
    session: Any,
    agreement: BuildingAgreement | None,
    reservation: BuildingReservation | None = None,
) -> str:
    """Why this contract cannot be undone, or an empty string when it can.

    Undo is gated on state rather than a clock. A timer either runs out while
    the operator is still looking at the screen, or lets them retract something
    a customer has already been shown; neither is honest.
    """

    if agreement is None:
        return "There is no contract on this lead to undo."
    status = str(agreement.preparation_status or "")
    if status == "cancelled":
        return "This contract was already undone."
    if status not in UNDOABLE_AGREEMENT_STATUSES:
        return (
            f"This contract has moved to {status.replace('_', ' ')}, so it can "
            "no longer be undone. Cancel it from the contract instead."
        )
    if str(agreement.status or "") in {"sent", "signed"}:
        return (
            "This contract has already gone to the customer, so it cannot be "
            "undone here."
        )
    if agreement.sent_at is not None or agreement.signed_at is not None:
        return (
            "This contract has already gone to the customer, so it cannot be "
            "undone here."
        )
    if str(agreement.document_url or "").strip():
        return (
            "A signing document exists for this contract, so it cannot be "
            "undone here."
        )
    if reservation is not None:
        # Releasing a date the customer has already paid to hold is not an
        # undo, it is a cancellation, and it belongs on the booking with a
        # refund decision attached.
        if str(reservation.deposit_status or "") in {"paid", "partially_paid"}:
            return (
                "A payment has been recorded against this booking, so the "
                "contract cannot be undone here."
            )
        readiness = session.execute(
            select(BuildingPaymentRequestReadiness).where(
                BuildingPaymentRequestReadiness.reservation_id == reservation.id
            )
        ).scalars().all()
        if any(
            str(row.status or "") not in {"prepared", "cancelled"}
            for row in readiness
        ):
            return (
                "A payment request has been issued for this booking, so the "
                "contract cannot be undone here."
            )
    return ""
