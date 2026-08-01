"""A discount on an accepted quote must reach the invoice without retyping.

Before this, quotes and invoices were unconnected: a quote carried line items
and a discount, while the invoice was built from a billing schedule holding one
hand-entered amount. Someone had to copy the number across, and nothing checked
that the two agreed.
"""

from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_quote_invoice_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingBillingSchedule,
        BuildingProposal,
        BuildingReservation,
        BuildingSpace,
    )

    DEPS = True
except ModuleNotFoundError as exc:  # pragma: no cover
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


SCHEDULES = "/api/internal/building/billing/schedules"
FROM_PROPOSAL = f"{SCHEDULES}/from-proposal"
INVOICES = "/api/internal/building/billing/invoices"


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class QuoteToInvoiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_quote_invoice_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings, internal_api_key="internal-test-key"
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-test-key"}
        cls._seed()

    @classmethod
    def _seed(cls) -> None:
        starts = datetime.now(timezone.utc) + timedelta(days=30)
        with cls.factory() as session:
            session.add(
                BuildingSpace(
                    id="arena", slug="arena", name="The Arena", space_type="event"
                )
            )
            # A proposal version is unique per reservation, so each quote in
            # these tests needs its own booking.
            for index in (1, 2, 3):
                session.add(
                    BuildingReservation(
                        id=f"res-{index}",
                        kind="event",
                        status="quote_sent",
                        space_id="arena",
                        starts_at=starts + timedelta(days=index),
                        ends_at=starts + timedelta(days=index, hours=6),
                    )
                )
            session.commit()
        cls.client.put(
            "/api/internal/building/billing/accounts/ferro",
            headers=cls.headers,
            json={
                "id": "ferro",
                "account_name": "Ferro Events",
                "billing_email": "billing@ferro.example",
                "actor": "operator@example.com",
            },
        )

    def _quote(
        self,
        proposal_id: str,
        *,
        amount_cents: int,
        status: str = "accepted",
        version: int = 1,
        reservation_id: str = "res-1",
    ) -> None:
        """A quote of $6,200 discounted by $700, exactly as the quote flow records it."""
        with self.factory() as session:
            row = session.get(BuildingProposal, proposal_id)
            if row is None:
                row = BuildingProposal(id=proposal_id, reservation_id=reservation_id)
                session.add(row)
            row.version = version
            row.proposal_type = "quote"
            row.status = status
            row.currency = "USD"
            row.amount_cents = amount_cents
            row.line_items_json = [
                {
                    "type": "package",
                    "description": "Event package before discount and tax",
                    "amount_cents": 620000,
                },
                {
                    "type": "discount",
                    "description": "Repeat customer, third booking this year",
                    "amount_cents": -70000,
                },
            ]
            row.terms_summary = "Arena, six hour block, Sep 30"
            session.commit()

    # ------------------------------------------------------------------

    def test_01_discount_reaches_the_schedule_without_being_retyped(self) -> None:
        self._quote("quote-1", amount_cents=550000)
        created = self.client.post(
            FROM_PROPOSAL,
            headers=self.headers,
            json={
                "id": "sched-1",
                "proposal_id": "quote-1",
                "billing_account_id": "ferro",
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(created.status_code, 201, created.text)
        body = created.json()
        # $6,200 less the $700 discount. The caller never sent an amount.
        self.assertEqual(body["amount_cents"], 550000)
        self.assertEqual(body["proposal_id"], "quote-1")

        with self.factory() as session:
            row = session.get(BuildingBillingSchedule, "sched-1")
            self.assertEqual(row.amount_cents, 550000)
            self.assertEqual(row.source_amount_cents, 550000)
            self.assertEqual(row.reservation_id, "res-1")
            self.assertEqual(row.description, "Arena, six hour block, Sep 30")

    def test_02_the_same_accepted_quote_never_bills_twice(self) -> None:
        repeat = self.client.post(
            FROM_PROPOSAL,
            headers=self.headers,
            json={
                "id": "sched-1-again",
                "proposal_id": "quote-1",
                "billing_account_id": "ferro",
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(repeat.status_code, 201, repeat.text)
        self.assertTrue(repeat.json()["duplicate"])
        self.assertEqual(repeat.json()["schedule_id"], "sched-1")

    def test_03_an_unaccepted_quote_cannot_be_billed(self) -> None:
        self._quote("quote-draft", amount_cents=400000, status="sent", reservation_id="res-2")
        refused = self.client.post(
            FROM_PROPOSAL,
            headers=self.headers,
            json={
                "id": "sched-draft",
                "proposal_id": "quote-draft",
                "billing_account_id": "ferro",
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(refused.status_code, 409, refused.text)
        self.assertIn("accepted", refused.json()["detail"])

    def test_04_a_revised_quote_will_not_silently_bill_the_old_number(self) -> None:
        """The customer renegotiates. The stale schedule must not invoice."""
        approved = self.client.post(
            f"{SCHEDULES}/sched-1/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        self.assertEqual(approved.status_code, 200, approved.text)

        # Version 2 accepted at a lower total after further negotiation.
        self._quote("quote-1", amount_cents=500000, version=2)

        blocked = self.client.post(
            INVOICES,
            headers=self.headers,
            json={
                "schedule_id": "sched-1",
                "idempotency_key": "ferro-stale-quote",
                "execute": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(blocked.status_code, 409, blocked.text)
        self.assertIn("no longer matches", blocked.json()["detail"])

    def test_05_a_matching_quote_still_previews_normally(self) -> None:
        self._quote("quote-2", amount_cents=310000, reservation_id="res-3")
        self.client.post(
            FROM_PROPOSAL,
            headers=self.headers,
            json={
                "id": "sched-2",
                "proposal_id": "quote-2",
                "billing_account_id": "ferro",
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.client.post(
            f"{SCHEDULES}/sched-2/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        preview = self.client.post(
            INVOICES,
            headers=self.headers,
            json={
                "schedule_id": "sched-2",
                "idempotency_key": "ferro-good-quote",
                "execute": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["proposal"]["amount_cents"], 310000)

    def test_06_hand_entered_schedules_are_unaffected(self) -> None:
        """Schedules with no quote behind them keep working exactly as before."""
        self.client.put(
            f"{SCHEDULES}/manual-1",
            headers=self.headers,
            json={
                "id": "manual-1",
                "billing_account_id": "ferro",
                "schedule_type": "one_time",
                "description": "Ad hoc room hire",
                "amount_cents": 90000,
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.client.post(
            f"{SCHEDULES}/manual-1/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        preview = self.client.post(
            INVOICES,
            headers=self.headers,
            json={
                "schedule_id": "manual-1",
                "idempotency_key": "ferro-manual",
                "execute": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["proposal"]["amount_cents"], 90000)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
