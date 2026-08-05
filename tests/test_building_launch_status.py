from __future__ import annotations

import unittest

from sales_support_agent.services.building_launch_status import (
    OWNER_DECISION_KEYS,
    build_arena_launch_status,
)


def _decisions(*, include_calendar: bool = False) -> list[dict]:
    rows = [
        {
            "decision_key": key,
            "status": (
                "owner_confirmed"
                if key == "transactional_sender"
                else "accepted_policy"
            ),
        }
        for key in OWNER_DECISION_KEYS
    ]
    if include_calendar:
        rows.append({
            "decision_key": "event_calendar",
            "status": "provider_verified",
        })
    return rows


def _plan(
    *,
    status: str = "draft",
    tax_status: str = "review_required",
    tax_rate_bps: int = 0,
    conflicts: list[dict] | None = None,
) -> dict:
    return {
        "offering_id": "arena-events",
        "version": 1,
        "status": status,
        "tax_status": tax_status,
        "tax_rate_bps": tax_rate_bps,
        "conflicts": conflicts or [],
    }


class ArenaLaunchStatusTests(unittest.TestCase):
    def test_owner_answers_are_separate_from_external_setup(self) -> None:
        status = build_arena_launch_status(
            launch_decisions=_decisions(),
            rate_plans=[_plan()],
            agreement_templates=[{
                "template_key": "arena-event-agreement",
                "version": 1,
                "status": "in_review",
            }],
            provider_readiness={},
        )

        self.assertEqual(status["owner_complete"], 7)
        self.assertEqual(status["owner_total"], 7)
        by_key = {row["key"]: row for row in status["items"]}
        self.assertEqual(by_key["business_rules"]["state"], "complete")
        self.assertEqual(by_key["pricing"]["state"], "needs_review")
        self.assertEqual(by_key["tax"]["state"], "external")
        self.assertEqual(by_key["agreement"]["state"], "external")
        self.assertEqual(by_key["customer_launch"]["state"], "automatic")
        self.assertFalse(status["customer_launch_ready"])
        self.assertIn("outside approvals", status["headline"])

    def test_conflicts_require_actual_provider_remediation(self) -> None:
        conflict = {
            "id": "tidycal-deposit",
            "status": "accepted_exception",
            "blocks_rate_plan_approval": True,
            "approval_resolution_statuses": ["provider_remediated"],
        }
        status = build_arena_launch_status(
            launch_decisions=_decisions(),
            rate_plans=[_plan(conflicts=[conflict])],
            agreement_templates=[],
            provider_readiness={},
        )
        copy_item = next(
            row for row in status["items"] if row["key"] == "booking_copy"
        )
        self.assertEqual(copy_item["state"], "external")
        self.assertIn("1 TidyCal conflict", copy_item["summary"])

        conflict["status"] = "provider_remediated"
        resolved = build_arena_launch_status(
            launch_decisions=_decisions(),
            rate_plans=[_plan(conflicts=[conflict])],
            agreement_templates=[],
            provider_readiness={},
        )
        copy_item = next(
            row for row in resolved["items"] if row["key"] == "booking_copy"
        )
        self.assertEqual(copy_item["state"], "complete")

    def test_launch_only_unlocks_from_all_authoritative_evidence(self) -> None:
        status = build_arena_launch_status(
            launch_decisions=_decisions(include_calendar=True),
            rate_plans=[
                _plan(
                    status="approved",
                    tax_status="taxable",
                    tax_rate_bps=725,
                )
            ],
            agreement_templates=[{
                "template_key": "arena-event-agreement",
                "version": 1,
                "status": "approved",
            }],
            provider_readiness={
                "esign_verified": True,
                "payment_credentials": True,
                "payment_webhook": True,
                "calendar_configured": True,
                "calendar_writes_enabled": True,
                "sender_credentials": True,
                "sender_webhook": True,
                "sender_matches_owner_choice": True,
            },
        )

        self.assertTrue(status["customer_launch_ready"])
        self.assertEqual(status["external_count"], 0)
        self.assertEqual(status["blocked_count"], 0)
        by_key = {row["key"]: row for row in status["items"]}
        self.assertEqual(by_key["customer_launch"]["state"], "complete")
        self.assertEqual(
            status["headline"],
            "Ready for a controlled launch rehearsal",
        )

    def test_effective_date_is_not_a_separate_owner_question(self) -> None:
        status = build_arena_launch_status(
            launch_decisions=[
                *_decisions(),
                {"decision_key": "effective_date", "status": "unresolved"},
            ],
            rate_plans=[_plan()],
            agreement_templates=[],
            provider_readiness={},
        )
        self.assertEqual(status["owner_total"], 7)
        self.assertNotIn(
            "effective_date",
            {row["key"] for row in status["items"]},
        )

    def test_private_catalog_keeps_customer_launch_closed(self) -> None:
        status = build_arena_launch_status(
            launch_decisions=_decisions(include_calendar=True),
            rate_plans=[_plan(status="approved", tax_status="taxable", tax_rate_bps=725)],
            agreement_templates=[{
                "template_key": "arena-event-agreement",
                "version": 1,
                "status": "approved",
            }],
            provider_readiness={
                "esign_verified": True,
                "payment_credentials": True,
                "payment_webhook": True,
                "calendar_configured": True,
                "calendar_writes_enabled": True,
                "sender_credentials": True,
                "sender_webhook": True,
                "sender_matches_owner_choice": True,
                "arena_space_public_available": False,
                "arena_offering_published": False,
            },
        )
        by_key = {row["key"]: row for row in status["items"]}
        self.assertEqual(by_key["public_catalog"]["state"], "blocked")
        self.assertFalse(status["customer_launch_ready"])
        self.assertEqual(by_key["customer_launch"]["state"], "automatic")


if __name__ == "__main__":
    unittest.main()
