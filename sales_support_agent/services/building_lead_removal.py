"""Take a lead off the board, without taking a contract with it.

Two different things get called "delete". A lead nobody acted on is junk and
should genuinely go. A lead that produced a booking, a contract or a payment is
the top of a paper trail the rest of the system reads, and destroying it would
strand an invoice or an agreement that still has to be honoured. So this picks
per lead: throw it away when nothing hangs off it, hide it when something does.

Archived state lives in the lead's own payload rather than a new column, so this
needs no schema change and cannot fail a deploy.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import delete, select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingEventLifecycleCommand,
    BuildingInquiry,
    BuildingInquiryReceipt,
    BuildingRelationship,
    BuildingReservation,
)
from sales_support_agent.services.building_inquiry_workspace import is_test_inquiry

ARCHIVE_KEY = "_archived"


@dataclass(frozen=True)
class RemovalOutcome:
    """What happened, in words an operator can read back."""

    action: str  # "deleted" | "archived" | "refused"
    message: str


def is_archived(inquiry: BuildingInquiry) -> bool:
    return bool(dict(inquiry.payload_json or {}).get(ARCHIVE_KEY))


def archived_ids(session: Any) -> set[str]:
    """Every archived lead id, for list filtering in one query."""

    rows = session.execute(
        select(BuildingInquiry.id, BuildingInquiry.payload_json)
    ).all()
    return {
        str(row[0])
        for row in rows
        if isinstance(row[1], dict) and row[1].get(ARCHIVE_KEY)
    }


def lead_attachments(session: Any, inquiry_id: str) -> list[str]:
    """Plain-language names for whatever would be destroyed with this lead."""

    found: list[str] = []
    reservations = session.execute(
        select(BuildingReservation).where(
            BuildingReservation.inquiry_id == inquiry_id
        )
    ).scalars().all()
    if reservations:
        found.append(
            f"{len(reservations)} booking{'s' if len(reservations) != 1 else ''}"
        )
        agreements = session.execute(
            select(BuildingAgreement).where(
                BuildingAgreement.reservation_id.in_(
                    [row.id for row in reservations]
                )
            )
        ).scalars().all()
        if agreements:
            found.append(
                f"{len(agreements)} contract{'s' if len(agreements) != 1 else ''}"
            )
        if any(
            str(row.deposit_status or "") in {"paid", "partially_paid"}
            for row in reservations
        ):
            found.append("a recorded payment")
    return found


def remove_lead(
    session: Any, inquiry: BuildingInquiry, *, actor: str
) -> RemovalOutcome:
    """Delete a lead that produced nothing; archive one that produced something."""

    inquiry_id = str(inquiry.id)
    label = str(inquiry.name or inquiry.email or inquiry_id)
    attachments = lead_attachments(session, inquiry_id)
    now = datetime.now(timezone.utc)

    if attachments:
        if is_archived(inquiry):
            return RemovalOutcome("refused", f"{label} is already removed.")
        payload = dict(inquiry.payload_json or {})
        payload[ARCHIVE_KEY] = {"at": now.isoformat(), "by": actor}
        inquiry.payload_json = payload
        inquiry.updated_at = now
        session.add(inquiry)
        session.add(BuildingAuditEvent(
            entity_type="inquiry",
            entity_id=inquiry_id,
            action="lead_archived",
            actor=actor,
            after_json={"kept": attachments, "customer_contacted": False},
        ))
        return RemovalOutcome(
            "archived",
            f"{label} is off your list. Its {' and '.join(attachments)} "
            "stayed, because other records still read them.",
        )

    # Nothing hangs off this one, so it can actually go. The audit line is
    # written against the id and outlives the row on purpose.
    session.add(BuildingAuditEvent(
        entity_type="inquiry",
        entity_id=inquiry_id,
        action="lead_deleted",
        actor=actor,
        before_json={
            "name": label,
            "email": str(inquiry.email or ""),
            "kind": str(inquiry.kind or ""),
            "source": str(inquiry.source or ""),
        },
        after_json={"customer_contacted": False},
    ))
    session.execute(
        delete(BuildingInquiryReceipt).where(
            BuildingInquiryReceipt.inquiry_id == inquiry_id
        )
    )
    session.execute(
        delete(BuildingEventLifecycleCommand).where(
            BuildingEventLifecycleCommand.inquiry_id == inquiry_id
        )
    )
    session.execute(
        delete(BuildingRelationship).where(
            BuildingRelationship.source_reference == f"inquiry:{inquiry_id}"
        )
    )
    session.delete(inquiry)
    return RemovalOutcome("deleted", f"{label} is deleted.")


def restore_lead(
    session: Any, inquiry: BuildingInquiry, *, actor: str
) -> RemovalOutcome:
    """Put an archived lead back on the board."""

    label = str(inquiry.name or inquiry.email or inquiry.id)
    if not is_archived(inquiry):
        return RemovalOutcome("refused", f"{label} is not removed.")
    payload = dict(inquiry.payload_json or {})
    payload.pop(ARCHIVE_KEY, None)
    inquiry.payload_json = payload
    inquiry.updated_at = datetime.now(timezone.utc)
    session.add(inquiry)
    session.add(BuildingAuditEvent(
        entity_type="inquiry",
        entity_id=str(inquiry.id),
        action="lead_restored",
        actor=actor,
        after_json={"customer_contacted": False},
    ))
    return RemovalOutcome("archived", f"{label} is back on your list.")


def remove_test_leads(session: Any, *, actor: str) -> dict[str, Any]:
    """Clear out every internal QA record in one action.

    Uses the same test-record rule the lists already use to grey them out, so
    nothing a real prospect sent can be caught by this.
    """

    inquiries = session.execute(select(BuildingInquiry)).scalars().all()
    targets = [
        row for row in inquiries
        if is_test_inquiry(
            name=str(row.name or ""),
            email=str(row.email or ""),
            source=str(row.source or ""),
        )
        and not is_archived(row)
    ]
    deleted = 0
    archived = 0
    for row in targets:
        outcome = remove_lead(session, row, actor=actor)
        if outcome.action == "deleted":
            deleted += 1
        elif outcome.action == "archived":
            archived += 1
    return {"deleted": deleted, "archived": archived, "total": len(targets)}
