"""Prepare the private Arena commercial draft from recorded owner decisions.

The result is deliberately a draft. It never approves or publishes pricing,
claims availability, creates a hold, sends a message, calls a provider, or
charges a customer.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingLaunchDecision,
    BuildingOffering,
    BuildingRatePlan,
    BuildingSpace,
)
from sales_support_agent.services.building_launch_readiness import (
    ARENA_LAUNCH_DECISIONS,
    ARENA_RATE_PLAN_DECISION_KEYS,
    launch_decision_id,
)


ARENA_RATE_PLAN_NAME = "Arena owner-reconciled commercial draft"
ARENA_TAX_EVIDENCE = (
    "Owner/accountant approval recorded 2026-07-30; Utah State Tax Commission "
    "2026 Q3 combined sales and use tax rate chart, Lehi 25-066, effective "
    "2026-07-01."
)
ARENA_TIMEZONE = ZoneInfo("America/Denver")
ARENA_CANCELLATION_POLICY = (
    "All payments are non-refundable. One transfer may be approved when "
    "requested at least 14 days before the event, may be used only once, and "
    "must move the event to an available date within six months. Paid amounts "
    "carry forward to the approved transferred date; otherwise cancellation "
    "forfeits paid amounts."
)


def proposed_effective_date() -> date:
    """Return today's Denver date for a visibly unapproved draft proposal."""

    return datetime.now(ARENA_TIMEZONE).date()


def build_arena_commercial_draft(
    *,
    offering_id: str,
    version: int,
    effective_from: date,
    actor: str,
    rate_plan_id: str | None = None,
) -> BuildingRatePlan:
    """Construct the current owner-reconciled, provider-neutral draft."""

    return BuildingRatePlan(
        id=rate_plan_id or f"{offering_id}-owner-commercial-v{version}",
        offering_id=offering_id,
        version=version,
        name=ARENA_RATE_PLAN_NAME,
        status="draft",
        currency="USD",
        unit_amount_cents=17_500,
        public_price_display="$175/hour · 6-hour minimum",
        booking_unit="hour",
        minimum_units=6,
        deposit_type="percent",
        deposit_percent_bps=5_000,
        cancellation_policy=ARENA_CANCELLATION_POLICY,
        included_json=[
            "Two hours of self-service setup access before the event",
            "Two hours of self-service teardown access after the event",
            "Up to 30 tables and 200 chairs",
            "Built-in stage and available venue A/V equipment",
        ],
        addons_json=[
            {
                "id": "cleaning",
                "name": "Required routine cleaning",
                "pricing_mode": "flat",
                "amount_cents": 25_000,
            },
            {
                "id": "setup-reset-75",
                "name": "Table and chair setup/reset · up to 75 guests",
                "pricing_mode": "flat",
                "amount_cents": 25_000,
            },
            {
                "id": "setup-reset-150",
                "name": "Table and chair setup/reset · 76–150 guests",
                "pricing_mode": "flat",
                "amount_cents": 40_000,
            },
            {
                "id": "setup-reset-200",
                "name": "Table and chair setup/reset · 151–200 guests",
                "pricing_mode": "flat",
                "amount_cents": 55_000,
            },
            {
                "id": "av-technician",
                "name": (
                    "A/V technician · 2-hour minimum; additional time "
                    "quoted at $75/hour"
                ),
                "pricing_mode": "flat",
                "amount_cents": 15_000,
            },
            {
                "id": "anata-event-labor",
                "name": (
                    "Premium Anata event labor · 2-hour minimum per staff "
                    "member; additional time quoted at $125/staff hour"
                ),
                "pricing_mode": "flat",
                "amount_cents": 25_000,
            },
        ],
        commercial_terms_json={
            "venue_square_feet": 6_000,
            "maximum_public_capacity": 200,
            "minimum_base_amount_cents": 105_000,
            "booking_deposit": {
                "percent_bps": 5_000,
                "non_refundable": True,
                "holds_date_only_after_cleared_funds": True,
            },
            "security_deposit": {
                "amount_cents": 50_000,
                "refundable": True,
                "damage_and_extraordinary_cleaning_deductible": True,
            },
            "balance_due_days_before_event": 7,
            "ach_check_clearing_days": 7,
            "setup_teardown": {
                "included_setup_hours": 2,
                "included_teardown_hours": 2,
                "labor_included": False,
                "additional_time_requires_approval": True,
            },
            "overtime": {
                "booking_unit": "full_hour",
                "amount_cents": 17_500,
                "requires_approval": True,
            },
            "rush_addon_fee_percent_bps": 2_000,
            "rush_window_days": 7,
            "agreement_template_status": "legal_review_required",
            "effective_date_status": "draft_proposal_only",
        },
        source_evidence_json=[
            {
                "source": "Anata Event Center Listing Copy Pack",
                "classification": "verified_commercial_baseline",
                "terms": [
                    "6000_square_feet",
                    "capacity_200",
                    "175_per_hour",
                    "6_hour_minimum",
                    "250_cleaning",
                    "50_percent_deposit",
                ],
            },
            {
                "source": "Owner event-policy interview",
                "classification": "owner_approved_business_rules",
                "terms": [
                    "balance_due_7_days_before",
                    "500_security_deposit",
                    "two_setup_hours_included",
                    "two_teardown_hours_included",
                    "175_full_hour_overtime",
                    "setup_reset_tiers",
                    "av_technician_75_hour",
                    "event_labor_125_staff_hour",
                    "one_transfer_14_days_six_months",
                ],
            },
        ],
        conflicts_json=[
            {
                "id": "tidycal-deposit",
                "summary": (
                    "TidyCal says 70% deposit; the owner-approved policy is 50%."
                ),
                "status": "provider_remediation_required",
                "blocks_rate_plan_approval": True,
                "allowed_resolution_statuses": [
                    "reconciled_in_agent",
                    "accepted_exception",
                    "provider_remediated",
                ],
                "approval_resolution_statuses": ["provider_remediated"],
            },
            {
                "id": "tidycal-balance",
                "summary": (
                    "TidyCal says the balance is due 48 hours before; the "
                    "owner-approved policy is seven days before."
                ),
                "status": "provider_remediation_required",
                "blocks_rate_plan_approval": True,
                "allowed_resolution_statuses": [
                    "reconciled_in_agent",
                    "accepted_exception",
                    "provider_remediated",
                ],
                "approval_resolution_statuses": ["provider_remediated"],
            },
            {
                "id": "tidycal-payment-link",
                "summary": "TidyCal contains a placeholder payment link.",
                "status": "provider_remediation_required",
                "blocks_rate_plan_approval": True,
                "allowed_resolution_statuses": [
                    "reconciled_in_agent",
                    "provider_remediated",
                ],
                "approval_resolution_statuses": ["provider_remediated"],
            },
            {
                "id": "tax-review",
                "summary": (
                    "Use the legally applicable combined Lehi sales-tax rate at "
                    "the transaction date. The July 1, 2026 Lehi rate is 7.45%."
                ),
                "status": "accountant_verified",
                "blocks_rate_plan_approval": False,
                "allowed_resolution_statuses": ["accountant_verified"],
                "approval_resolution_statuses": ["accountant_verified"],
                "evidence": (
                    "Owner/accountant approval recorded 2026-07-30; Utah State "
                    "Tax Commission 2026 Q3 rate chart, Lehi 25-066."
                ),
            },
        ],
        tax_status="taxable",
        tax_rate_bps=745,
        tax_note=(
            "Apply the legally applicable combined rate for 1657 N. State "
            "Street, Lehi, Utah at the transaction date. The rate effective "
            "2026-07-01 is 7.45%. Refundable security deposits are not taxable "
            "unless retained or applied to taxable charges. Re-verify the "
            "official Utah rate chart before a later effective period."
        ),
        effective_from=effective_from,
        created_by=actor,
        updated_at=datetime.now(timezone.utc),
    )


def ensure_arena_commercial_draft(
    session_factory,
    *,
    actor: str = "agent-predeploy",
    effective_from: date | None = None,
) -> str:
    """Create the draft once when the Arena catalog exists and has no plan."""

    with session_scope(session_factory) as session:
        offering = session.get(BuildingOffering, "arena-events")
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
            return "catalog_missing"

        existing = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == offering.id
            )
        ).scalars().first()
        if existing is not None:
            return "existing_plan_preserved"

        row = build_arena_commercial_draft(
            offering_id=offering.id,
            version=1,
            effective_from=effective_from or proposed_effective_date(),
            actor=actor,
        )
        session.add(row)
        session.add(
            BuildingAuditEvent(
                entity_type="rate_plan",
                entity_id=row.id,
                action="owner_reconciled_draft_prepared",
                actor=actor,
                after_json={
                    "offering_id": offering.id,
                    "version": row.version,
                    "status": "draft",
                    "effective_date_status": "draft_proposal_only",
                    "source_count": len(row.source_evidence_json),
                    "conflict_count": len(row.conflicts_json),
                    "provider_write": False,
                    "published": False,
                },
            )
        )
        return "created"


def reconcile_approved_arena_tax(
    session_factory,
    *,
    actor: str,
) -> str:
    """Apply the dated owner/accountant tax decision to the canonical draft.

    The operation is deliberately narrow: it will not alter an approved plan,
    a noncanonical plan, or a plan with conflicting tax evidence. It approves
    the rate plan only when every saved commercial decision and every blocking
    provider conflict is already resolved.
    """

    with session_scope(session_factory) as session:
        plan = session.execute(
            select(BuildingRatePlan).where(
                BuildingRatePlan.offering_id == "arena-events",
                BuildingRatePlan.name == ARENA_RATE_PLAN_NAME,
            )
        ).scalars().first()
        if plan is None:
            return "catalog_missing"
        if plan.status == "approved":
            return "already_approved"
        if plan.status != "draft":
            return "non_draft_preserved"
        if (plan.tax_status, plan.tax_rate_bps) not in {
            ("review_required", 0),
            ("taxable", 745),
        }:
            raise RuntimeError(
                "Arena tax evidence conflicts with the approved 7.45% Lehi "
                "decision; operator review is required."
            )

        conflicts = [dict(item) for item in list(plan.conflicts_json or [])]
        tax_found = False
        for item in conflicts:
            if str(item.get("id") or "") == "tax-review":
                item.update(
                    {
                        "status": "accountant_verified",
                        "summary": (
                            "Use the legally applicable combined Lehi sales-tax "
                            "rate at the transaction date. The July 1, 2026 rate "
                            "is 7.45%."
                        ),
                        "evidence": ARENA_TAX_EVIDENCE,
                    }
                )
                tax_found = True
        if not tax_found:
            raise RuntimeError(
                "Canonical Arena draft is missing its tax-review evidence row."
            )

        now = datetime.now(timezone.utc)
        plan.tax_status = "taxable"
        plan.tax_rate_bps = 745
        plan.tax_note = (
            "Apply the legally applicable combined rate for 1657 N. State "
            "Street, Lehi, Utah at the transaction date. The rate effective "
            "2026-07-01 is 7.45%. Refundable security deposits are not taxable "
            "unless retained or applied to taxable charges. Re-verify the "
            "official Utah rate chart before a later effective period."
        )
        plan.conflicts_json = conflicts
        plan.updated_at = now

        decision = session.get(
            BuildingLaunchDecision,
            launch_decision_id("arena-events", "tax_treatment"),
        )
        if decision is None:
            decision = BuildingLaunchDecision(
                id=launch_decision_id("arena-events", "tax_treatment"),
                offering_id="arena-events",
                decision_key="tax_treatment",
            )
        decision.status = "accepted_policy"
        decision.value = (
            "Tax all quoted event charges at the legally applicable combined "
            "Lehi rate; 7.45% is effective 2026-07-01. Exclude refundable "
            "security deposits unless retained or applied to taxable charges."
        )
        decision.evidence = ARENA_TAX_EVIDENCE
        decision.decided_by = actor
        decision.decided_at = now
        decision.updated_at = now
        session.add(decision)
        session.flush()

        decision_rows = {
            item.decision_key: item.status
            for item in session.execute(
                select(BuildingLaunchDecision).where(
                    BuildingLaunchDecision.offering_id == "arena-events"
                )
            ).scalars()
        }
        decision_blockers = [
            key
            for key in ARENA_RATE_PLAN_DECISION_KEYS
            if decision_rows.get(key) != ARENA_LAUNCH_DECISIONS[key][1]
        ]
        conflict_blockers = [
            str(item.get("id") or "unknown")
            for item in conflicts
            if bool(item.get("blocks_rate_plan_approval"))
            and str(item.get("status") or "unresolved")
            not in set(item.get("approval_resolution_statuses") or [])
        ]
        outcome = "tax_reconciled"
        if not decision_blockers and not conflict_blockers:
            plan.status = "approved"
            plan.approved_by = actor
            plan.approved_at = now
            plan.approval_evidence = (
                "Owner, legal, accounting, TidyCal administration, and IT "
                "approval recorded in the Agent production-readiness task on "
                f"2026-07-30. {ARENA_TAX_EVIDENCE}"
            )
            outcome = "approved"

        session.add(
            BuildingAuditEvent(
                entity_type="rate_plan",
                entity_id=plan.id,
                action=f"arena_tax_{outcome}",
                actor=actor,
                after_json={
                    "status": plan.status,
                    "tax_status": plan.tax_status,
                    "tax_rate_bps": plan.tax_rate_bps,
                    "tax_evidence": ARENA_TAX_EVIDENCE,
                    "decision_blockers": decision_blockers,
                    "conflict_blockers": conflict_blockers,
                    "published": False,
                    "provider_write": False,
                },
            )
        )
        return outcome
