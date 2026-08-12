from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_quote_adjustments_boot.db",
)

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingOffering,
    BuildingProposal,
    BuildingRatePlan,
    BuildingReservation,
    BuildingSpace,
)


class BuildingQuoteAdjustmentTests(unittest.TestCase):
    def setUp(self) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"building_quote_adjustments_{uuid.uuid4().hex}.db"
        )
        self.factory = create_session_factory("sqlite:///" + path)
        init_database(self.factory)
        app.state.session_factory = self.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="quote-adjustment-key",
        )
        self.client = TestClient(app)
        self.headers = {"X-Internal-Api-Key": "quote-adjustment-key"}
        starts_at = datetime.now(timezone.utc) + timedelta(days=30)
        with self.factory() as session:
            session.add(
                BuildingSpace(
                    id="quote-arena",
                    slug="quote-arena",
                    name="The Arena",
                    space_type="event",
                    capacity=200,
                    status="available",
                )
            )
            session.add(
                BuildingOffering(
                    id="quote-arena-events",
                    space_id="quote-arena",
                    slug="quote-arena-events",
                    name="The Arena",
                    offering_type="event",
                )
            )
            session.add(
                BuildingRatePlan(
                    id="quote-arena-rate",
                    offering_id="quote-arena-events",
                    version=1,
                    name="Approved Arena rate",
                    status="approved",
                    currency="USD",
                    unit_amount_cents=17_500,
                    booking_unit="hour",
                    minimum_units=6,
                    tax_status="taxable",
                    tax_rate_bps=745,
                    approval_evidence="Owner/accountant approval and Utah 2026 Q3 rate chart.",
                    approved_by="owner@example.com",
                    approved_at=datetime.now(timezone.utc),
                    effective_from=date.today(),
                )
            )
            session.add(
                BuildingReservation(
                    id="quote-adjustment-event",
                    kind="event",
                    status="soft_hold",
                    offering_id="quote-arena-events",
                    space_id="quote-arena",
                    starts_at=starts_at,
                    ends_at=starts_at + timedelta(hours=8),
                    attendance=120,
                )
            )
            session.commit()

    def _payload(self) -> dict:
        return {
            "version": 1,
            "status": "draft",
            "proposal_type": "quote",
            "amount_cents": 0,
            "pricing_subtotal_cents": 200_000,
            "discount_cents": 25_000,
            "discount_reason": "Community partner event",
            "rate_plan_id": "quote-arena-rate",
            "actor": "operator@example.com",
        }

    def test_calculates_discount_tax_and_final_total_with_audit_detail(self) -> None:
        response = self.client.post(
            "/api/internal/building/bookings/quote-adjustment-event/proposals",
            headers=self.headers,
            json=self._payload(),
        )
        self.assertEqual(response.status_code, 201, response.text)
        with self.factory() as session:
            quote = session.query(BuildingProposal).one()
            self.assertEqual(quote.amount_cents, 188_038)
            pricing_adjustment = dict(
                quote.rate_plan_snapshot_json["pricing_adjustment"]
            )
            self.assertEqual(
                pricing_adjustment.pop("transaction_date"),
                quote.rate_plan_snapshot_json["transaction_date"],
            )
            self.assertEqual(
                pricing_adjustment,
                {
                    "pricing_subtotal_cents": 200_000,
                    "discount_cents": 25_000,
                    "discount_reason": "Community partner event",
                    "tax_status": "taxable",
                    "tax_rate_bps": 745,
                    "tax_cents": 13_038,
                    "final_amount_cents": 188_038,
                },
            )
            self.assertEqual(
                [item["type"] for item in quote.line_items_json],
                ["pricing_subtotal", "discount", "tax"],
            )

    def test_rejects_unexplained_or_excessive_discount(self) -> None:
        missing_reason = self._payload()
        missing_reason["discount_reason"] = ""
        response = self.client.post(
            "/api/internal/building/bookings/quote-adjustment-event/proposals",
            headers=self.headers,
            json=missing_reason,
        )
        self.assertEqual(response.status_code, 422, response.text)
        self.assertIn("business reason", response.text)

        excessive = self._payload()
        excessive["discount_cents"] = 200_001
        response = self.client.post(
            "/api/internal/building/bookings/quote-adjustment-event/proposals",
            headers=self.headers,
            json=excessive,
        )
        self.assertEqual(response.status_code, 422, response.text)
        self.assertIn("cannot exceed", response.text)

    def test_terminal_booking_rejects_quote_mutation(self) -> None:
        with self.factory() as session:
            reservation = session.get(BuildingReservation, "quote-adjustment-event")
            reservation.status = "cancelled"
            session.add(reservation)
            session.commit()
        try:
            response = self.client.post(
                "/api/internal/building/bookings/quote-adjustment-event/proposals",
                headers=self.headers,
                json=self._payload(),
            )
            self.assertEqual(response.status_code, 409, response.text)
            self.assertIn("read-only", response.text)
        finally:
            with self.factory() as session:
                reservation = session.get(BuildingReservation, "quote-adjustment-event")
                reservation.status = "soft_hold"
                session.add(reservation)
                session.commit()


if __name__ == "__main__":
    unittest.main()
