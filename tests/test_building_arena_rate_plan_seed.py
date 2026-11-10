from __future__ import annotations

import tempfile
import unittest
import uuid
from datetime import date

try:
    from sqlalchemy import select

    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingAgreementTemplate,
        BuildingLaunchDecision,
        BuildingOffering,
        BuildingRatePlan,
        BuildingSpace,
    )
    from sales_support_agent.services.building_arena_rate_plan_seed import (
        ARENA_CANCELLATION_POLICY,
        ensure_arena_commercial_draft,
        reconcile_approved_arena_tax,
        refresh_canonical_arena_event_policy,
    )
    from sales_support_agent.services.building_launch_readiness import (
        launch_decision_id,
        sync_arena_agreement_template_decision,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "sqlalchemy required")
class ArenaRatePlanSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        path = tempfile.gettempdir() + f"/arena-rate-plan-seed-{uuid.uuid4()}.db"
        self.factory = create_session_factory("sqlite:///" + path)
        init_database(self.factory)

    def _catalog(self) -> None:
        with self.factory() as session:
            session.add(
                BuildingSpace(
                    id="arena",
                    slug="arena",
                    name="The Arena",
                    space_type="event",
                    capacity=200,
                    status="unavailable",
                    is_public=False,
                )
            )
            session.add(
                BuildingOffering(
                    id="arena-events",
                    slug="arena-events",
                    space_id="arena",
                    name="The Arena events",
                    offering_type="event",
                    booking_unit="hour",
                    is_published=False,
                )
            )
            session.commit()

    def test_creates_current_private_review_draft_and_is_idempotent(self) -> None:
        self._catalog()
        result = ensure_arena_commercial_draft(
            self.factory,
            actor="david@anatainc.com",
            effective_from=date(2026, 8, 1),
        )
        self.assertEqual(result, "created")
        self.assertEqual(
            ensure_arena_commercial_draft(self.factory),
            "existing_plan_preserved",
        )

        with self.factory() as session:
            plans = session.execute(select(BuildingRatePlan)).scalars().all()
            self.assertEqual(len(plans), 1)
            row = plans[0]
            self.assertEqual(row.status, "draft")
            self.assertEqual(row.booking_unit, "day_portion")
            self.assertEqual(row.unit_amount_cents, 0)
            self.assertEqual(row.minimum_units, 1)
            self.assertEqual(row.deposit_percent_bps, 5_000)
            self.assertEqual(row.cancellation_policy, ARENA_CANCELLATION_POLICY)
            self.assertIsNone(
                row.commercial_terms_json["balance_due_days_before_event"]
            )
            self.assertEqual(
                row.commercial_terms_json["half_day_amount_cents"], 150_000
            )
            self.assertEqual(
                row.commercial_terms_json["full_day_amount_cents"], 200_000
            )
            self.assertEqual(
                row.commercial_terms_json["full_plus_half_amount_cents"], 300_000
            )
            self.assertEqual(
                row.commercial_terms_json["two_full_days_amount_cents"], 350_000
            )
            self.assertEqual(
                row.commercial_terms_json["security_deposit"]["amount_cents"],
                0,
            )
            self.assertIsNone(row.commercial_terms_json["overtime"]["amount_cents"])
            self.assertEqual(row.tax_status, "taxable")
            self.assertEqual(row.tax_rate_bps, 745)
            self.assertIn("transaction date", row.tax_note)
            self.assertIn("no security deposit", row.tax_note.lower())
            self.assertTrue(
                all(
                    item["status"] == "provider_remediation_required"
                    for item in row.conflicts_json
                    if item["id"].startswith("tidycal-")
                )
            )
            audit = session.execute(
                select(BuildingAuditEvent).where(
                    BuildingAuditEvent.entity_id == row.id
                )
            ).scalar_one()
            self.assertFalse(audit.after_json["provider_write"])
            self.assertFalse(audit.after_json["published"])

    def test_refresh_updates_the_named_plan_and_leaves_other_plans(self) -> None:
        self._catalog()
        with self.factory() as session:
            session.add(
                BuildingRatePlan(
                    id="arena-events-owner-commercial-v1",
                    offering_id="arena-events",
                    version=1,
                    name="Arena owner-reconciled commercial draft",
                    status="approved",
                    currency="USD",
                    unit_amount_cents=17_500,
                    public_price_display="$175/hour · 6-hour minimum",
                    booking_unit="hour",
                    minimum_units=6,
                    deposit_type="percent",
                    deposit_percent_bps=5_000,
                    cancellation_policy="All payments are non-refundable.",
                    included_json=["Two hours of self-service setup access"],
                    addons_json=[{
                        "id": "cleaning",
                        "name": "Required routine cleaning",
                        "pricing_mode": "flat",
                        "amount_cents": 25_000,
                    }],
                    commercial_terms_json={
                        "security_deposit": {"amount_cents": 50_000},
                    },
                    source_evidence_json=[],
                    conflicts_json=[{
                        "id": "tidycal-deposit",
                        "status": "provider_remediated",
                    }],
                    tax_status="taxable",
                    tax_rate_bps=745,
                    tax_note="old",
                    effective_from=date(2026, 8, 1),
                    approved_by="david@anatainc.com",
                    created_by="seed",
                )
            )
            session.add(
                BuildingRatePlan(
                    id="operator-plan",
                    offering_id="arena-events",
                    version=2,
                    name="Operator draft",
                    status="draft",
                    currency="USD",
                    unit_amount_cents=99,
                    public_price_display="Custom",
                    booking_unit="hour",
                    minimum_units=1,
                    deposit_type="percent",
                    deposit_percent_bps=0,
                    cancellation_policy="",
                    included_json=[],
                    addons_json=[],
                    commercial_terms_json={},
                    source_evidence_json=[],
                    conflicts_json=[],
                    tax_status="review_required",
                    tax_rate_bps=0,
                    tax_note="",
                    effective_from=date(2026, 9, 1),
                    created_by="operator",
                )
            )
            session.commit()

        self.assertEqual(
            refresh_canonical_arena_event_policy(
                self.factory, actor="david@anatainc.com"
            ),
            "updated",
        )
        self.assertEqual(
            refresh_canonical_arena_event_policy(self.factory),
            "already_current",
        )
        with self.factory() as session:
            plan = session.get(
                BuildingRatePlan, "arena-events-owner-commercial-v1"
            )
            other = session.get(BuildingRatePlan, "operator-plan")
            self.assertEqual(plan.status, "approved")
            self.assertEqual(plan.approved_by, "david@anatainc.com")
            self.assertEqual(plan.booking_unit, "day_portion")
            self.assertEqual(
                plan.commercial_terms_json["half_day_amount_cents"], 150_000
            )
            self.assertEqual(
                plan.commercial_terms_json["security_deposit"]["amount_cents"], 0
            )
            self.assertEqual(
                plan.conflicts_json[0]["status"], "provider_remediated"
            )
            self.assertEqual(other.unit_amount_cents, 99)
            self.assertEqual(other.public_price_display, "Custom")

    def test_fails_closed_without_verified_arena_catalog(self) -> None:
        self.assertEqual(
            ensure_arena_commercial_draft(self.factory),
            "catalog_missing",
        )
        with self.factory() as session:
            self.assertEqual(
                session.execute(select(BuildingRatePlan)).scalars().all(),
                [],
            )

    def test_preserves_any_existing_plan(self) -> None:
        self._catalog()
        with self.factory() as session:
            session.add(
                BuildingRatePlan(
                    id="existing-plan",
                    offering_id="arena-events",
                    version=7,
                    name="Operator draft",
                    status="draft",
                    currency="USD",
                    unit_amount_cents=99,
                    public_price_display="",
                    booking_unit="hour",
                    minimum_units=1,
                    deposit_type="flat",
                    deposit_amount_cents=0,
                    deposit_percent_bps=0,
                    cancellation_policy="",
                    included_json=[],
                    addons_json=[],
                    commercial_terms_json={},
                    source_evidence_json=[],
                    conflicts_json=[],
                    tax_status="review_required",
                    tax_rate_bps=0,
                    tax_note="",
                    effective_from=date(2027, 1, 1),
                    created_by="operator",
                )
            )
            session.commit()

        self.assertEqual(
            ensure_arena_commercial_draft(self.factory),
            "existing_plan_preserved",
        )
        with self.factory() as session:
            rows = session.execute(select(BuildingRatePlan)).scalars().all()
            self.assertEqual([(row.id, row.unit_amount_cents) for row in rows], [
                ("existing-plan", 99)
            ])

    def test_reconciles_tax_and_approves_only_after_saved_blockers_clear(self) -> None:
        self._catalog()
        ensure_arena_commercial_draft(
            self.factory,
            actor="david@anatainc.com",
            effective_from=date(2026, 7, 30),
        )
        with self.factory() as session:
            plan = session.execute(select(BuildingRatePlan)).scalar_one()
            conflicts = [dict(item) for item in plan.conflicts_json]
            for item in conflicts:
                if item["id"].startswith("tidycal-"):
                    item["status"] = "provider_remediated"
            plan.conflicts_json = conflicts
            for key in (
                "cancellation_policy",
                "setup_price",
                "teardown_price",
                "overtime_rate",
            ):
                session.add(
                    BuildingLaunchDecision(
                        id=launch_decision_id("arena-events", key),
                        offering_id="arena-events",
                        decision_key=key,
                        status="accepted_policy",
                        value="Owner-approved value",
                        evidence="Owner decision evidence",
                        decided_by="david@anatainc.com",
                    )
                )
            session.commit()

        result = reconcile_approved_arena_tax(
            self.factory,
            actor="david@anatainc.com",
        )
        self.assertEqual(result, "approved")
        with self.factory() as session:
            plan = session.execute(select(BuildingRatePlan)).scalar_one()
            self.assertEqual(plan.status, "approved")
            self.assertEqual(plan.tax_status, "taxable")
            self.assertEqual(plan.tax_rate_bps, 745)
            self.assertEqual(plan.approved_by, "david@anatainc.com")
            decision = session.get(
                BuildingLaunchDecision,
                launch_decision_id("arena-events", "tax_treatment"),
            )
            self.assertEqual(decision.status, "accepted_policy")
            audit = session.execute(
                select(BuildingAuditEvent).where(
                    BuildingAuditEvent.entity_id == plan.id,
                    BuildingAuditEvent.action == "arena_tax_approved",
                )
            ).scalar_one()
            self.assertFalse(audit.after_json["published"])
            self.assertFalse(audit.after_json["provider_write"])

    def test_agreement_readiness_is_derived_from_template_approval(self) -> None:
        self._catalog()
        with self.factory() as session:
            template = BuildingAgreementTemplate(
                id="arena-event-agreement-v1",
                template_key="arena-event-agreement",
                version=1,
                name="Arena event agreement",
                status="approved",
                contract_type="event",
                body_markdown="# Agreement",
                clauses_json=[],
                merge_fields_json=[],
                approval_evidence="Counsel approval record 2026-08",
                approved_by="legal@anatainc.com",
            )
            session.add(template)
            sync_arena_agreement_template_decision(
                session,
                template=template,
                actor="david@anatainc.com",
            )
            session.commit()

        decision_id = launch_decision_id("arena-events", "agreement_template")
        with self.factory() as session:
            decision = session.get(BuildingLaunchDecision, decision_id)
            self.assertEqual(decision.status, "approved_reference")
            self.assertIn("version 1", decision.value)
            self.assertIn("Counsel approval record", decision.evidence)

            template = session.get(
                BuildingAgreementTemplate,
                "arena-event-agreement-v1",
            )
            template.status = "retired"
            sync_arena_agreement_template_decision(
                session,
                template=template,
                actor="david@anatainc.com",
            )
            session.commit()

        with self.factory() as session:
            decision = session.get(BuildingLaunchDecision, decision_id)
            self.assertEqual(decision.status, "unresolved")
            self.assertEqual(decision.value, "")


if __name__ == "__main__":
    unittest.main()
