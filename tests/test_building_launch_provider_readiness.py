"""Launch rows must be able to reach green.

Found 2026-08-01: `esign_verified` was hardcoded False, so the electronic
signatures row could never complete however much setup was done, and the
customer-launch gate that depends on it could never open. A checklist row that
cannot be satisfied is worse than no row, because it reads as work outstanding
forever.
"""

import unittest

from sales_support_agent.services.building_launch_status import (
    build_arena_launch_status,
)


def _row(status: dict, key: str) -> dict:
    return next(item for item in status["items"] if item["key"] == key)


READY_PROVIDERS = {
    "quickbooks_connected": True,
    "esign_verified": True,
    "payment_credentials": True,
    "payment_webhook": True,
    "calendar_configured": True,
    "calendar_writes_enabled": True,
    "sender_credentials": True,
    "sender_webhook": True,
    "sender_matches_owner_choice": True,
}


class EsignRowTests(unittest.TestCase):
    def test_the_esign_row_can_complete(self) -> None:
        """The regression: with everything verified, this row must go green."""
        status = build_arena_launch_status(
            launch_decisions=[],
            rate_plans=[],
            agreement_templates=[],
            provider_readiness=READY_PROVIDERS,
        )
        self.assertEqual(_row(status, "esign")["state"], "complete")

    def test_the_esign_row_is_outstanding_when_unverified(self) -> None:
        status = build_arena_launch_status(
            launch_decisions=[],
            rate_plans=[],
            agreement_templates=[],
            provider_readiness={**READY_PROVIDERS, "esign_verified": False},
        )
        self.assertEqual(_row(status, "esign")["state"], "external")


class CalendarRowTests(unittest.TestCase):
    def test_all_three_calendar_conditions_are_required(self) -> None:
        """Configured alone is not ready. Writes and the verified decision count."""
        verified = [{"decision_key": "event_calendar", "status": "provider_verified"}]
        for providers, decisions, expected in (
            ({**READY_PROVIDERS}, [], "external"),  # no verified decision
            (
                {**READY_PROVIDERS, "calendar_writes_enabled": False},
                verified,
                "external",
            ),
            ({**READY_PROVIDERS, "calendar_configured": False}, verified, "external"),
            ({**READY_PROVIDERS}, verified, "complete"),
        ):
            status = build_arena_launch_status(
                launch_decisions=decisions,
                rate_plans=[],
                agreement_templates=[],
                provider_readiness=providers,
            )
            self.assertEqual(_row(status, "calendar")["state"], expected)


class PaymentRailTests(unittest.TestCase):
    """Anata bills on QuickBooks. Stripe is only the optional auto-confirm path."""

    def _payment(self, **providers) -> dict:
        status = build_arena_launch_status(
            launch_decisions=[],
            rate_plans=[],
            agreement_templates=[],
            provider_readiness=providers,
        )
        return _row(status, "payment")

    def test_quickbooks_alone_satisfies_the_payment_row(self) -> None:
        row = self._payment(
            quickbooks_connected=True,
            payment_credentials=False,
            payment_webhook=False,
        )
        self.assertEqual(row["state"], "complete")
        self.assertIn("QuickBooks issues the invoice", row["summary"])
        self.assertIn("by hand", row["summary"])

    def test_stripe_alone_also_satisfies_it(self) -> None:
        row = self._payment(
            quickbooks_connected=False,
            payment_credentials=True,
            payment_webhook=True,
        )
        self.assertEqual(row["state"], "complete")

    def test_neither_leaves_it_outstanding(self) -> None:
        row = self._payment(
            quickbooks_connected=False,
            payment_credentials=False,
            payment_webhook=False,
        )
        self.assertEqual(row["state"], "external")
        self.assertIn("Connect QuickBooks", row["next_action"])

    def test_half_configured_stripe_is_not_enough_on_its_own(self) -> None:
        row = self._payment(
            quickbooks_connected=False,
            payment_credentials=True,
            payment_webhook=False,
        )
        self.assertEqual(row["state"], "external")


class LaunchGateTests(unittest.TestCase):
    def test_the_gate_opens_when_every_condition_is_met(self) -> None:
        """Proves the end state is reachable, which the hardcoded False prevented."""
        status = build_arena_launch_status(
            launch_decisions=[
                {"decision_key": key, "status": "accepted_policy"}
                for key in (
                    "cancellation_policy",
                    "tax_treatment",
                    "setup_price",
                    "teardown_price",
                    "overtime_rate",
                    "payment_workflow",
                )
            ]
            + [
                {"decision_key": "transactional_sender", "status": "owner_confirmed"},
                {"decision_key": "event_calendar", "status": "provider_verified"},
                {"decision_key": "agreement_template", "status": "approved_reference"},
            ],
            # These are matched by offering and template key, so a fixture
            # without them is invisible to the checklist.
            rate_plans=[
                {
                    "offering_id": "arena-events",
                    "version": 1,
                    "status": "approved",
                    "tax_status": "non_taxable",
                    "tax_rate_bps": 0,
                    "conflicts": [],
                }
            ],
            agreement_templates=[
                {
                    "id": "arena-event-agreement-business-terms-v2",
                    "template_key": "arena-event-agreement",
                    "version": 2,
                    "status": "approved",
                }
            ],
            provider_readiness=READY_PROVIDERS,
        )
        self.assertTrue(
            status["customer_launch_ready"],
            f"gate still closed: {[(i['key'], i['state']) for i in status['items']]}",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
