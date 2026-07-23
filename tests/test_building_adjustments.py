from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_adjustments_boot.db",
)

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.models.database import create_session_factory, init_database
    from sales_support_agent.models.entities import (
        BuildingAuditEvent,
        BuildingBillingAccount,
        BuildingBillingAdjustment,
        BuildingInvoice,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingAdjustmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_adjustments_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="internal-adjustment-key",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        cls.headers = {"X-Internal-Api-Key": "internal-adjustment-key"}
        with factory.begin() as session:
            session.add(
                BuildingBillingAccount(
                    id="adjustment-account",
                    account_name="Adjustment Account",
                    billing_email="billing@example.com",
                )
            )
            session.add(
                BuildingInvoice(
                    id="adjustment-invoice",
                    billing_account_id="adjustment-account",
                    idempotency_key="adjustment-invoice-key",
                    provider_invoice_id="in_adjustment",
                    description="Reviewed invoice",
                    status="paid",
                    amount_due_cents=10000,
                    amount_paid_cents=6000,
                    currency="usd",
                    created_by="billing@example.com",
                )
            )

    def _request(self, adjustment_type: str, amount_cents: int, actor: str = "requester@example.com"):
        return self.client.post(
            "/api/internal/building/billing/adjustments",
            headers=self.headers,
            json={
                "invoice_id": "adjustment-invoice",
                "adjustment_type": adjustment_type,
                "amount_cents": amount_cents,
                "reason": "Reviewed customer billing exception",
                "actor": actor,
            },
        )

    def test_00_adjustments_cannot_exceed_evidence(self) -> None:
        refund = self._request("refund", 6001)
        self.assertEqual(refund.status_code, 409, refund.text)
        credit = self._request("credit", 4001)
        self.assertEqual(credit.status_code, 409, credit.text)
        write_off = self._request("write_off", 4001)
        self.assertEqual(write_off.status_code, 409, write_off.text)

    def test_01_refund_requires_separate_approval_and_provider_evidence(self) -> None:
        requested = self._request("refund", 3000)
        self.assertEqual(requested.status_code, 201, requested.text)
        adjustment_id = requested.json()["adjustment"]["id"]
        self_approval = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/approve",
            headers=self.headers,
            json={"actor": "requester@example.com"},
        )
        self.assertEqual(self_approval.status_code, 409)
        approved = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        wrong_evidence = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/evidence",
            headers=self.headers,
            json={
                "status": "accounting_confirmed",
                "qbo_reference": "credit-note-1",
                "note": "Recorded in QBO",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(wrong_evidence.status_code, 409)
        missing_provider = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/evidence",
            headers=self.headers,
            json={
                "status": "provider_confirmed",
                "note": "Stripe refund completed",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(missing_provider.status_code, 422)
        confirmed = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/evidence",
            headers=self.headers,
            json={
                "status": "provider_confirmed",
                "provider_reference": "re_test_123",
                "note": "Stripe refund completed",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        self.assertEqual(
            confirmed.json()["adjustment"]["status"],
            "provider_confirmed",
        )

    def test_02_write_off_requires_accounting_evidence(self) -> None:
        requested = self._request("write_off", 4000, "writer@example.com")
        self.assertEqual(requested.status_code, 201, requested.text)
        adjustment_id = requested.json()["adjustment"]["id"]
        approved = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/approve",
            headers=self.headers,
            json={"actor": "approver@example.com"},
        )
        self.assertEqual(approved.status_code, 200, approved.text)
        wrong_evidence = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/evidence",
            headers=self.headers,
            json={
                "status": "provider_confirmed",
                "provider_reference": "re_not_valid",
                "note": "Wrong evidence class",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(wrong_evidence.status_code, 409)
        confirmed = self.client.post(
            f"/api/internal/building/billing/adjustments/{adjustment_id}/evidence",
            headers=self.headers,
            json={
                "status": "accounting_confirmed",
                "qbo_reference": "qbo-writeoff-123",
                "note": "Reviewed write-off recorded in formal accounting",
                "actor": "finance@example.com",
            },
        )
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        with self.factory() as session:
            rows = session.query(BuildingBillingAdjustment).all()
            self.assertEqual(len(rows), 2)
            self.assertGreaterEqual(
                session.query(BuildingAuditEvent)
                .filter(BuildingAuditEvent.entity_type == "billing_adjustment")
                .count(),
                6,
            )


if __name__ == "__main__":
    unittest.main()
