"""Shared terminal-state propagation for prepared agreement/payment records."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingPaymentRequestReadiness,
    BuildingReservation,
)


def propagate_event_readiness_terminal_state(
    session,
    reservation: BuildingReservation,
    *,
    terminal_status: str,
    actor: str,
) -> None:
    """Expire or cancel unsent preparation records without provider writes."""

    if terminal_status not in {"expired", "cancelled"}:
        return
    now = datetime.now(timezone.utc)
    package_status = "expired" if terminal_status == "expired" else "cancelled"
    agreements = session.execute(
        select(BuildingAgreement).where(
            BuildingAgreement.reservation_id == reservation.id,
            BuildingAgreement.preparation_status.in_(
                {"prepared", "in_review", "approved"}
            ),
            BuildingAgreement.status == "draft",
        )
    ).scalars().all()
    for agreement in agreements:
        before = agreement.preparation_status
        agreement.preparation_status = package_status
        agreement.updated_at = now
        session.add(BuildingAuditEvent(
            entity_type="agreement",
            entity_id=agreement.id,
            action=f"agreement_package_{package_status}",
            actor=actor,
            before_json={"preparation_status": before},
            after_json={
                "preparation_status": package_status,
                "reservation_status": terminal_status,
                "provider_write": False,
            },
        ))
    payments = session.execute(
        select(BuildingPaymentRequestReadiness).where(
            BuildingPaymentRequestReadiness.reservation_id == reservation.id,
            BuildingPaymentRequestReadiness.status.in_(
                {"prepared", "in_review", "approved"}
            ),
        )
    ).scalars().all()
    for payment in payments:
        before = payment.status
        payment.status = package_status
        payment.updated_at = now
        session.add(BuildingAuditEvent(
            entity_type="payment_request_readiness",
            entity_id=payment.id,
            action=f"payment_request_{package_status}",
            actor=actor,
            before_json={"status": before},
            after_json={
                "status": package_status,
                "reservation_status": terminal_status,
                "provider_write": False,
            },
        ))
