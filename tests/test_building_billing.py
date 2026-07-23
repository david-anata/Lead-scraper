from __future__ import annotations

import dataclasses
import hashlib
import hmac
import json
import os
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_billing_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.integrations.stripe_billing import (
        StripeBillingClient,
        StripeBillingError,
    )
    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingBillingSchedule,
        BuildingCollectionCase,
        BuildingInvoice,
        BuildingPayment,
        BuildingStripeEvent,
        BuildingSuppression,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingBillingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_billing_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-test-key",
            stripe_secret_key="sk_test_building",
            stripe_webhook_secret="whsec_building",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-test-key"}

    def test_01_preview_approval_execution_and_idempotency(self) -> None:
        account = self.client.put(
            "/api/internal/building/billing/accounts/acme",
            headers=self.headers,
            json={
                "id": "acme",
                "account_name": "Acme Studio",
                "billing_email": "billing@acme.example",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(account.status_code, 200, account.text)
        schedule = self.client.put(
            "/api/internal/building/billing/schedules/acme-monthly",
            headers=self.headers,
            json={
                "id": "acme-monthly",
                "billing_account_id": "acme",
                "schedule_type": "monthly",
                "description": "Private office membership",
                "amount_cents": 125000,
                "starts_on": date.today().isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(schedule.status_code, 200, schedule.text)
        before_approval = self.client.post(
            "/api/internal/building/billing/invoices",
            headers=self.headers,
            json={
                "schedule_id": "acme-monthly",
                "idempotency_key": "acme-2026-07",
                "execute": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(before_approval.status_code, 409)
        approved = self.client.post(
            "/api/internal/building/billing/schedules/acme-monthly/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        preview = self.client.post(
            "/api/internal/building/billing/invoices",
            headers=self.headers,
            json={
                "schedule_id": "acme-monthly",
                "idempotency_key": "acme-2026-07",
                "execute": False,
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertFalse(preview.json()["execute"])
        self.assertEqual(preview.json()["proposal"]["accounting_destination"], "quickbooks")

        customer = {"id": "cus_acme"}
        provider_invoice = {
            "id": "in_acme_2026_07",
            "status": "open",
            "amount_due": 125000,
            "amount_paid": 0,
            "currency": "usd",
            "hosted_invoice_url": "https://invoice.example/acme",
        }
        with (
            patch.object(StripeBillingClient, "create_customer", return_value=customer) as create_customer,
            patch.object(StripeBillingClient, "create_invoice", return_value=provider_invoice) as create_invoice,
        ):
            executed = self.client.post(
                "/api/internal/building/billing/invoices",
                headers=self.headers,
                json={
                    "schedule_id": "acme-monthly",
                    "idempotency_key": "acme-2026-07",
                    "execute": True,
                    "actor": "operator@example.com",
                },
            )
            self.assertEqual(executed.status_code, 200, executed.text)
            self.assertEqual(executed.json()["invoice"]["accounting_status"], "pending_qbo")
            duplicate = self.client.post(
                "/api/internal/building/billing/invoices",
                headers=self.headers,
                json={
                    "schedule_id": "acme-monthly",
                    "idempotency_key": "acme-2026-07",
                    "execute": True,
                    "actor": "operator@example.com",
                },
            )
            self.assertTrue(duplicate.json()["duplicate"])
            create_customer.assert_called_once()
            create_invoice.assert_called_once()

        with self.factory() as session:
            invoice = session.query(BuildingInvoice).one()
            billing_schedule = session.get(BuildingBillingSchedule, "acme-monthly")
            self.assertEqual(invoice.provider_invoice_id, "in_acme_2026_07")
            self.assertEqual(billing_schedule.status, "approved")
            self.assertIsNotNone(billing_schedule.next_invoice_on)

    def test_02_paid_webhook_is_verified_and_idempotent(self) -> None:
        with self.factory() as session:
            invoice = session.query(BuildingInvoice).one()
            invoice_id = invoice.id
        event = {
            "id": "evt_invoice_paid",
            "type": "invoice.paid",
            "data": {
                "object": {
                    "id": "in_acme_2026_07",
                    "status": "paid",
                    "amount_due": 125000,
                    "amount_paid": 125000,
                    "currency": "usd",
                    "payment_intent": "pi_acme_2026_07",
                    "metadata": {"building_invoice_id": invoice_id},
                }
            },
        }
        payload = json.dumps(event, separators=(",", ":")).encode()
        timestamp = int(time.time())
        signature = hmac.new(
            b"whsec_building",
            str(timestamp).encode() + b"." + payload,
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Stripe-Signature": f"t={timestamp},v1={signature}",
        }
        response = self.client.post(
            "/api/integrations/stripe/webhook",
            headers=headers,
            content=payload,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()["duplicate"])
        duplicate = self.client.post(
            "/api/integrations/stripe/webhook",
            headers=headers,
            content=payload,
        )
        self.assertEqual(duplicate.status_code, 200, duplicate.text)
        self.assertTrue(duplicate.json()["duplicate"])
        with self.factory() as session:
            invoice = session.query(BuildingInvoice).one()
            payment = session.query(BuildingPayment).one()
            event_row = session.get(BuildingStripeEvent, "evt_invoice_paid")
            self.assertEqual(invoice.status, "paid")
            self.assertEqual(invoice.amount_paid_cents, 125000)
            self.assertEqual(payment.evidence_class, "provider_confirmed")
            self.assertEqual(event_row.status, "processed")

    def test_03_webhook_signature_rejects_tampering_and_stale_events(self) -> None:
        client = StripeBillingClient(app.state.settings)
        payload = b'{"id":"evt_test","type":"invoice.paid"}'
        with self.assertRaises(StripeBillingError):
            client.verify_webhook(
                payload=payload,
                signature_header="t=100,v1=not-valid",
                now=100,
            )
        valid = hmac.new(
            b"whsec_building",
            b"100." + payload,
            hashlib.sha256,
        ).hexdigest()
        with self.assertRaises(StripeBillingError):
            client.verify_webhook(
                payload=payload,
                signature_header=f"t=100,v1={valid}",
                now=1000,
            )

    def test_04_qbo_export_and_reviewed_accounting_link(self) -> None:
        export = self.client.get(
            "/api/internal/building/billing/qbo-export",
            headers=self.headers,
        )
        self.assertEqual(export.status_code, 200, export.text)
        self.assertEqual(export.json()["destination"], "quickbooks")
        self.assertEqual(len(export.json()["invoices"]), 1)
        invoice_id = export.json()["invoices"][0]["id"]
        missing_reference = self.client.put(
            f"/api/internal/building/billing/invoices/{invoice_id}/accounting-link",
            headers=self.headers,
            json={
                "accounting_status": "reconciled",
                "actor": "bookkeeper@example.com",
            },
        )
        self.assertEqual(missing_reference.status_code, 422)
        linked = self.client.put(
            f"/api/internal/building/billing/invoices/{invoice_id}/accounting-link",
            headers=self.headers,
            json={
                "accounting_status": "reconciled",
                "qbo_invoice_id": "9137",
                "note": "Matched during parallel close.",
                "actor": "bookkeeper@example.com",
            },
        )
        self.assertEqual(linked.status_code, 200, linked.text)
        self.assertEqual(linked.json()["invoice"]["qbo_invoice_id"], "9137")
        after = self.client.get(
            "/api/internal/building/billing/qbo-export",
            headers=self.headers,
        )
        self.assertEqual(after.json()["invoices"], [])

    def test_05_future_schedule_cannot_bill_early(self) -> None:
        future_date = date.today() + timedelta(days=30)
        schedule = self.client.put(
            "/api/internal/building/billing/schedules/acme-future",
            headers=self.headers,
            json={
                "id": "acme-future",
                "billing_account_id": "acme",
                "schedule_type": "one_time",
                "description": "Future reviewed charge",
                "amount_cents": 25000,
                "starts_on": future_date.isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(schedule.status_code, 200, schedule.text)
        approved = self.client.post(
            "/api/internal/building/billing/schedules/acme-future/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        with patch.object(StripeBillingClient, "create_invoice") as create_invoice:
            response = self.client.post(
                "/api/internal/building/billing/invoices",
                headers=self.headers,
                json={
                    "schedule_id": "acme-future",
                    "idempotency_key": "acme-future-charge",
                    "execute": True,
                    "actor": "operator@example.com",
                },
            )
            self.assertEqual(response.status_code, 409)
            create_invoice.assert_not_called()

    def test_06_overdue_collection_queue_and_reminder_are_audited(self) -> None:
        with self.factory() as session:
            session.add(BuildingInvoice(
                id="collection-invoice",
                billing_account_id="acme",
                idempotency_key="collection-invoice-key",
                provider="stripe",
                provider_invoice_id="in_collection",
                description="Past-due office membership",
                status="open",
                amount_due_cents=125000,
                amount_paid_cents=25000,
                currency="usd",
                due_at=datetime.now(timezone.utc) - timedelta(days=5),
                hosted_invoice_url="https://invoice.example/collection",
                created_by="test",
            ))
            session.commit()
        preview = self.client.post(
            "/api/internal/building/billing/collections/refresh",
            headers=self.headers,
            json={
                "execute": False,
                "default_owner": "finance@example.com",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["would_create_count"], 1)
        executed = self.client.post(
            "/api/internal/building/billing/collections/refresh",
            headers=self.headers,
            json={
                "execute": True,
                "default_owner": "finance@example.com",
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(executed.status_code, 200, executed.text)
        self.assertEqual(executed.json()["created_count"], 1)
        queue = self.client.get(
            "/api/internal/building/billing/collections",
            headers=self.headers,
        )
        self.assertEqual(queue.status_code, 200, queue.text)
        case = next(
            item
            for item in queue.json()["collections"]
            if item["invoice_id"] == "collection-invoice"
        )
        self.assertEqual(case["outstanding_cents"], 100000)
        self.assertGreaterEqual(case["days_overdue"], 5)
        wrong = self.client.post(
            f"/api/internal/building/billing/collections/{case['id']}/remind",
            headers=self.headers,
            json={"confirmation": "send", "actor": "operator@example.com"},
        )
        self.assertEqual(wrong.status_code, 409)
        with patch(
            "sales_support_agent.api.building_billing_router.ResendClient"
        ) as resend:
            resend.return_value.is_configured.return_value = True
            resend.return_value.send_message.return_value = "email-collection-1"
            reminded = self.client.post(
                f"/api/internal/building/billing/collections/{case['id']}/remind",
                headers=self.headers,
                json={
                    "confirmation": f"REMIND {case['id']}",
                    "next_action_at": (
                        datetime.now(timezone.utc) + timedelta(days=3)
                    ).isoformat(),
                    "actor": "operator@example.com",
                },
            )
        self.assertEqual(reminded.status_code, 200, reminded.text)
        self.assertEqual(reminded.json()["collection"]["reminder_count"], 1)
        self.assertEqual(reminded.json()["collection"]["status"], "contacted")
        resend.return_value.send_message.assert_called_once()
        with self.factory() as session:
            session.add(BuildingSuppression(
                email="billing@acme.example",
                scope="all",
                reason="privacy_request",
            ))
            session.commit()
        suppressed = self.client.post(
            f"/api/internal/building/billing/collections/{case['id']}/remind",
            headers=self.headers,
            json={
                "confirmation": f"REMIND {case['id']}",
                "next_action_at": (
                    datetime.now(timezone.utc) + timedelta(days=3)
                ).isoformat(),
                "actor": "operator@example.com",
            },
        )
        self.assertEqual(suppressed.status_code, 409, suppressed.text)
        closed = self.client.put(
            f"/api/internal/building/billing/collections/{case['id']}",
            headers=self.headers,
            json={
                "status": "resolved",
                "resolution": "Payment received outside Stripe and reconciled.",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(closed.status_code, 200, closed.text)
        with self.factory() as session:
            saved = session.get(BuildingCollectionCase, case["id"])
            self.assertEqual(saved.status, "resolved")
            self.assertEqual(saved.reminder_count, 1)


if __name__ == "__main__":
    unittest.main()
