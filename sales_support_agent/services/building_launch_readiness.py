"""Shared Arena launch-readiness policy and deterministic identifiers."""

from __future__ import annotations

import hashlib

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingLaunchDecision,
    BuildingOffering,
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
    "effective_date",
}


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
