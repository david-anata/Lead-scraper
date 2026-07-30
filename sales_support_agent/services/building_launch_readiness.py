"""Shared Arena launch-readiness policy and deterministic identifiers."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreementTemplate,
    BuildingAuditEvent,
    BuildingLaunchDecision,
    BuildingOffering,
    BuildingRatePlan,
    BuildingSpace,
)

ARENA_LAUNCH_DECISIONS = {
    "cancellation_policy": ("Cancellation policy", "accepted_policy"),
    "tax_treatment": ("Tax treatment and rate", "accepted_policy"),
    "setup_price": ("Setup add-on price", "accepted_policy"),
    "teardown_price": ("Teardown add-on price", "accepted_policy"),
    "overtime_rate": ("Overtime hourly rate", "accepted_policy"),
    "payment_workflow": ("Venue payment workflow", "accepted_policy"),
    "agreement_template": ("Reusable agreement template", "approved_reference"),
    "event_calendar": ("Dedicated event calendar", "provider_verified"),
    "transactional_sender": ("Transactional sender and owner", "owner_confirmed"),
    "effective_date": ("Launch effective date", "accepted_policy"),
}
ARENA_RATE_PLAN_DECISION_KEYS = {
    "cancellation_policy",
    "tax_treatment",
    "setup_price",
    "teardown_price",
    "overtime_rate",
}
ARENA_AGREEMENT_TEMPLATE_KEY = "arena-event-agreement"


def launch_decision_id(offering_id: str, decision_key: str) -> str:
    """Return a stable ID that always fits the 64-character database column."""

    digest = hashlib.sha256(f"{offering_id}:{decision_key}".encode()).hexdigest()[:32]
    return f"launch-{digest}"


def arena_rate_plan_decision_blockers(session, offering_id: str) -> list[str]:
    """Return required commercial decisions for every Arena-linked offering."""

    offering = session.get(BuildingOffering, offering_id)
    space = (
        session.get(BuildingSpace, offering.space_id)
        if offering is not None and offering.space_id
        else None
    )
    if space is None or space.name.strip().casefold() != "the arena":
        return []
    rows = {
        item.decision_key: item.status
        for item in session.execute(
            select(BuildingLaunchDecision).where(
                BuildingLaunchDecision.offering_id == offering_id
            )
        ).scalars()
    }
    return [
        key
        for key in ARENA_RATE_PLAN_DECISION_KEYS
        if rows.get(key) != ARENA_LAUNCH_DECISIONS[key][1]
    ]


def _arena_offering(
    session,
    offering_id: str = "arena-events",
) -> BuildingOffering | None:
    offering = session.get(BuildingOffering, offering_id)
    space = (
        session.get(BuildingSpace, offering.space_id)
        if offering is not None and offering.space_id
        else None
    )
    if (
        offering is None
        or offering.offering_type != "event"
        or space is None
        or space.name.strip().casefold() != "the arena"
    ):
        return None
    return offering


def _record_derived_decision(
    session,
    *,
    offering: BuildingOffering,
    decision_key: str,
    status: str,
    value: str,
    evidence: str,
    actor: str,
    source_entity_type: str,
    source_entity_id: str,
) -> BuildingLaunchDecision:
    decision_id = launch_decision_id(offering.id, decision_key)
    row = session.get(BuildingLaunchDecision, decision_id)
    if row is None:
        row = session.execute(
            select(BuildingLaunchDecision).where(
                BuildingLaunchDecision.offering_id == offering.id,
                BuildingLaunchDecision.decision_key == decision_key,
            )
        ).scalar_one_or_none()
    before = (
        {
            "status": row.status,
            "value": row.value,
            "evidence": row.evidence,
        }
        if row is not None
        else {"status": "unresolved"}
    )
    if row is None:
        row = BuildingLaunchDecision(
            id=decision_id,
            offering_id=offering.id,
            decision_key=decision_key,
        )
    now = datetime.now(timezone.utc)
    row.status = status
    row.value = value
    row.evidence = evidence
    row.decided_by = actor
    row.decided_at = now if status != "unresolved" else None
    row.updated_at = now
    session.add(row)
    after = {
        "decision_key": decision_key,
        "status": status,
        "value": value,
        "evidence": evidence,
        "derived": True,
        "source_entity_type": source_entity_type,
        "source_entity_id": source_entity_id,
        "external_write": False,
    }
    if before != {
        "status": row.status,
        "value": row.value,
        "evidence": row.evidence,
    }:
        session.add(
            BuildingAuditEvent(
                entity_type="launch_decision",
                entity_id=row.id,
                action="arena_launch_decision_derived",
                actor=actor,
                before_json=before,
                after_json=after,
            )
        )
    return row


def sync_arena_agreement_template_decision(
    session,
    *,
    template: BuildingAgreementTemplate,
    actor: str,
) -> BuildingLaunchDecision | None:
    """Derive the Arena agreement decision from the approved template record."""

    if template.template_key != ARENA_AGREEMENT_TEMPLATE_KEY:
        return None
    offering = _arena_offering(session)
    if offering is None:
        return None

    approved = template if template.status == "approved" else None
    if approved is None:
        session.flush()
        approved = session.execute(
            select(BuildingAgreementTemplate)
            .where(
                BuildingAgreementTemplate.template_key
                == ARENA_AGREEMENT_TEMPLATE_KEY,
                BuildingAgreementTemplate.status == "approved",
            )
            .order_by(BuildingAgreementTemplate.version.desc())
        ).scalars().first()
    if approved is None:
        return _record_derived_decision(
            session,
            offering=offering,
            decision_key="agreement_template",
            status="unresolved",
            value="",
            evidence=(
                "No approved reusable Arena agreement template currently exists."
            ),
            actor=actor,
            source_entity_type="agreement_template",
            source_entity_id=template.id,
        )
    return _record_derived_decision(
        session,
        offering=offering,
        decision_key="agreement_template",
        status="approved_reference",
        value=f"{approved.name} · version {approved.version}",
        evidence=(
            f"{approved.approval_evidence} "
            f"(template {approved.id}, approved by {approved.approved_by})"
        ).strip(),
        actor=actor,
        source_entity_type="agreement_template",
        source_entity_id=approved.id,
    )


def sync_arena_effective_date_decision(
    session,
    *,
    rate_plan: BuildingRatePlan,
    actor: str,
) -> BuildingLaunchDecision | None:
    """Derive launch effective date from the approved Arena rate plan."""

    if rate_plan.status != "approved":
        return None
    offering = _arena_offering(session, rate_plan.offering_id)
    if offering is None:
        return None
    return _record_derived_decision(
        session,
        offering=offering,
        decision_key="effective_date",
        status="accepted_policy",
        value=(
            f"Arena commercial terms version {rate_plan.version} become "
            f"effective {rate_plan.effective_from.isoformat()}."
        ),
        evidence=(
            f"{rate_plan.approval_evidence} "
            f"(rate plan {rate_plan.id}, approved by {rate_plan.approved_by})"
        ).strip(),
        actor=actor,
        source_entity_type="rate_plan",
        source_entity_id=rate_plan.id,
    )
