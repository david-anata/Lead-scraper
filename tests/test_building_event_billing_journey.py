from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_event_billing_boot.db",
)

from fastapi.testclient import TestClient

from sales_support_agent.integrations.building_quickbooks import BuildingQuickBooksClient
from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingBillingSchedule,
    BuildingContact,
    BuildingDepositEvidence,
    BuildingInvoice,
    BuildingPaymentRequestReadiness,
    BuildingProposal,
    BuildingReservation,
    BuildingSpace,
)


class BuildingEventBillingJourneyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), f"building_event_billing_{uuid.uuid4().hex}.db")
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="event-billing-key",
        )
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "event-billing-key"}
        now = datetime.now(timezone.utc)
        quote_snapshot = {
            "deposit_type": "percent",
            "deposit_percent_bps": 5000,
            "security_deposit_cents": 50000,
            "commercial_terms": {
                "balance_due_days_before_event": 7,
            },
        }
        with cls.factory() as session:
            session.add(BuildingSpace(
                id="billing-arena",
                slug="billing-arena",
                name="The Arena",
                space_type="event",
            ))
            session.add(BuildingContact(
                id="billing-contact",
                full_name="Jordan Customer",
                email="jordan@example.com",
                status="active",
            ))
            session.add(BuildingReservation(
                id="billing-reservation",
                kind="event",
                status="soft_hold",
                contact_id="billing-contact",
                space_id="billing-arena",
                starts_at=now + timedelta(days=30),
                ends_at=now + timedelta(days=30, hours=8),
                agreement_status="sent",
                deposit_required=True,
            ))
            session.add(BuildingProposal(
                id="billing-quote",
                reservation_id="billing-reservation",
                version=1,
                proposal_type="quote",
                status="draft",
                amount_cents=112000,
                currency="USD",
                line_items_json=[
                    {"type": "pricing_subtotal", "amount_cents": 104235},
                    {"type": "tax", "amount_cents": 7765},
                ],
                rate_plan_id="arena-rate",
                rate_plan_snapshot_json=quote_snapshot,
            ))
            session.add(BuildingAgreement(
                id="billing-agreement",
                reservation_id="billing-reservation",
                version=1,
                status="draft",
                preparation_status="approved",
                provider="google_docs",
                provider_reference="",
                package_checksum="a" * 64,
                package_snapshot_json={
                    "quote": {"id": "billing-quote", "version": 1, "amount_cents": 112000}
                },
            ))
            session.add(BuildingPaymentRequestReadiness(
                id="billing-payment-ready",
                reservation_id="billing-reservation",
                agreement_id="billing-agreement",
                version=1,
                status="approved",
                request_type="deposit",
                amount_cents=56000,
                currency="USD",
                checksum="b" * 64,
            ))
            session.commit()

    def test_01_approved_unsigned_booking_prepares_exact_idempotent_components(self) -> None:
        endpoint = "/api/internal/building/billing/reservations/billing-reservation/prepare"
        first = self.client.post(endpoint, headers=self.headers, json={"actor": "operator@example.com"})
        self.assertEqual(first.status_code, 201, first.text)
        self.assertEqual(first.json()["component_count"], 1)
        self.assertFalse(first.json()["provider_write"])
        replay = self.client.post(endpoint, headers=self.headers, json={"actor": "operator@example.com"})
        self.assertTrue(replay.json()["duplicate"])
        with self.factory() as session:
            schedules = session.query(BuildingBillingSchedule).all()
            self.assertEqual(len(schedules), 1)
            amounts = {row.billing_component: row.amount_cents for row in schedules}
            self.assertEqual(amounts, {"full_amount": 162000})
            invoice = schedules[0]
            self.assertEqual(invoice.source_quote_total_cents, 112000)
            self.assertTrue(invoice.source_quote_checksum)
            self.assertIn("Booking deposit 560.00 due now", invoice.description)

    def test_02_quickbooks_paid_refresh_is_authoritative_and_idempotent(self) -> None:
        with self.factory() as session:
            schedule = session.query(BuildingBillingSchedule).filter_by(
                billing_component="full_amount"
            ).one()
            schedule.status = "approved"
            schedule_id = schedule.id
            session.commit()
        provider_invoice = {"Id": "QB-INV-88", "TotalAmt": 1620.00, "DueDate": datetime.now().date().isoformat()}
        with (
            patch.object(BuildingQuickBooksClient, "is_configured", new_callable=lambda: property(lambda _self: True)),
            patch.object(BuildingQuickBooksClient, "ensure_customer", return_value={"Id": "QB-CUSTOMER-88"}),
            patch.object(BuildingQuickBooksClient, "create_draft_invoice", return_value=provider_invoice),
        ):
            created = self.client.post(
                "/api/internal/building/billing/invoices",
                headers=self.headers,
                json={
                    "schedule_id": schedule_id,
                    "idempotency_key": "billing-deposit-88",
                    "execute": True,
                    "actor": "operator@example.com",
                },
            )
        self.assertEqual(created.status_code, 200, created.text)
        invoice_id = created.json()["invoice"]["id"]
        qbo_evidence = {
            "Id": "QB-INV-88",
            "TotalAmt": 1620.00,
            "Balance": 0,
            "SyncToken": "3",
            "EmailStatus": "EmailSent",
        }
        with (
            patch.object(BuildingQuickBooksClient, "is_configured", new_callable=lambda: property(lambda _self: True)),
            patch.object(BuildingQuickBooksClient, "get_invoice", return_value=qbo_evidence),
        ):
            synced = self.client.post(
                f"/api/internal/building/billing/invoices/{invoice_id}/sync-qbo",
                headers=self.headers,
                json={"actor": "operator@example.com"},
            )
            replay = self.client.post(
                f"/api/internal/building/billing/invoices/{invoice_id}/sync-qbo",
                headers=self.headers,
                json={"actor": "operator@example.com"},
            )
        self.assertEqual(synced.status_code, 200, synced.text)
        self.assertEqual(synced.json()["invoice"]["status"], "paid")
        self.assertEqual(replay.json()["invoice"]["status"], "paid")
        with self.factory() as session:
            invoice = session.get(BuildingInvoice, invoice_id)
            reservation = session.get(BuildingReservation, "billing-reservation")
            self.assertEqual(invoice.amount_paid_cents, 162000)
            self.assertEqual(reservation.deposit_status, "paid")
            evidence = session.query(BuildingDepositEvidence).one()
            self.assertEqual(evidence.provider, "quickbooks")
            self.assertEqual(evidence.provider_reference, "QB-INV-88")
            self.assertEqual(session.query(BuildingDepositEvidence).count(), 1)

    def test_03_refundable_security_deposit_is_explicitly_non_taxable(self) -> None:
        client = BuildingQuickBooksClient()
        captured: dict = {}

        def fake_request(method, path, *, params=None, payload=None):
            captured.update(payload or {})
            return {"Invoice": {"Id": "QB-SECURITY-1"}}

        with patch.object(client, "_request", side_effect=fake_request):
            client.create_draft_invoice(
                customer_id="QB-CUSTOMER-88",
                description="Refundable security deposit",
                amount_cents=50000,
                schedule_type="security_deposit",
                due_date=datetime.now().date(),
                idempotency_key="security-deposit-88",
            )
        detail = captured["Line"][0]["SalesItemLineDetail"]
        self.assertEqual(detail["ItemRef"]["value"], "79")
        self.assertEqual(detail["TaxCodeRef"]["value"], "NON")


if __name__ == "__main__":
    unittest.main()
