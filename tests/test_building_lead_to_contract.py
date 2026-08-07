from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/lead_to_contract_boot.db",
)
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "lead-to-contract-secret")

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAvailabilityBlock,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingProposal,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class LeadToContractTests(unittest.TestCase):
    """The whole path, from a lead's own pricing to a prepared contract.

    Pricing is edited on the lead and feeds the frozen quote, so billing,
    invoicing, and the QuickBooks handoff keep reading the record they always
    have.
    """

    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"lead_to_contract_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_factory = app.state.session_factory
        cls.original_settings = app.state.settings
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="lead-contract-key",
            building_campaign_token_secret="lead-contract-csrf",
        )
        cls.client = TestClient(app)
        cls.client.cookies.set(
            app.state.agent_settings.admin_cookie_name,
            create_user_session_token(
                app.state.agent_settings,
                email="david@anatainc.com",
                name="David",
                role="admin",
            ),
        )
        cls.headers = {"Origin": "http://testserver", "Sec-Fetch-Mode": "navigate"}
        now = datetime.now(timezone.utc)
        starts = now + timedelta(days=40)
        with cls.factory() as session:
            session.add_all([
                BuildingSpace(
                    id="sp", slug="sp", name="The Arena", space_type="event",
                    capacity=200, status="available",
                ),
                BuildingOffering(
                    id="off", slug="off", name="Event", offering_type="event",
                    space_id="sp",
                ),
                BuildingRatePlan(
                    id="plan-v1", offering_id="off", version=1, name="Standard",
                    status="approved", currency="USD", unit_amount_cents=17_500,
                    minimum_units=6, deposit_type="percent",
                    deposit_percent_bps=5_000,
                    cancellation_policy="Non-refundable inside 30 days.",
                    tax_status="non_taxable", effective_from=date.today(),
                    approval_evidence="owner-approved 2026-07-31",
                ),
                BuildingContact(
                    id="c1", email="rosa@example.com", full_name="Rosa Delgado",
                    status="active",
                ),
                BuildingAgreementTemplate(
                    id="tpl-v1", template_key="event-agreement", version=1,
                    name="Event agreement", status="approved", contract_type="event",
                    body_markdown=(
                        "{{customer_name}} at {{event_space}} for "
                        "{{quote_total}} {{currency}}. Deposit {{deposit_amount}}."
                    ),
                    clauses_json=[],
                    merge_fields_json=[
                        "customer_name", "event_space", "quote_total",
                        "currency", "deposit_amount",
                    ],
                    approved_by="legal@anatainc.com", approved_at=now,
                ),
            ])
            session.add(BuildingInquiry(
                id="lead-1", idempotency_key="lead-1-key", kind="event",
                name="Rosa Delgado", email="rosa@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            session.add(BuildingRelationship(
                id="rel-1", contact_id="c1", relationship_type="prospect",
                status="active", source_reference="inquiry:lead-1",
            ))
            reservation = BuildingReservation(
                id="res-1", kind="event", status="quote_sent", inquiry_id="lead-1",
                contact_id="c1", space_id="sp", starts_at=starts,
                guest_starts_at=starts + timedelta(hours=2),
                guest_ends_at=starts + timedelta(hours=7),
                ends_at=starts + timedelta(hours=9), attendance=120,
                deposit_required=True, created_by="test",
            )
            session.add(reservation)
            session.add(BuildingAvailabilityBlock(
                id="blk-1", space_id="sp", state="soft_hold",
                starts_at=reservation.starts_at, ends_at=reservation.ends_at,
                source="agent", source_reference="reservation:res-1",
            ))
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.session_factory = cls.original_factory
        app.state.settings = cls.original_settings

    def _csrf(self, lead: str = "lead-1") -> str:
        page = self.client.get(f"/admin/building/inquiries/{lead}")
        match = re.search(r'name="_csrf_token" value="([^"]+)"', page.text)
        self.assertIsNotNone(match, page.text[:400])
        return match.group(1)

    def _price(self, **fields: str):
        body = {
            "_csrf_token": self._csrf(), "hourly_rate": "175", "hours": "6",
            "cleaning_fee": "250", "security_deposit": "500", "discount": "0",
            "discount_reason": "", "deposit_percent": "50",
        }
        body.update(fields)
        return self.client.post(
            "/admin/building/inquiries/lead-1/pricing",
            headers=self.headers, follow_redirects=False, data=body,
        )

    def test_01_lead_pricing_reaches_the_contract(self) -> None:
        priced = self._price(
            hourly_rate="200", hours="8", discount="100",
            discount_reason="Repeat customer",
        )
        self.assertEqual(priced.status_code, 303, priced.text)

        created = self.client.post(
            "/admin/building/inquiries/lead-1/contract",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf()},
        )
        self.assertEqual(created.status_code, 303, created.text)
        self.assertIn("/admin/building/contracts/", created.headers["location"])
        self.assertIn("notice=", created.headers["location"])

        with self.factory() as session:
            quote = session.query(BuildingProposal).filter_by(
                reservation_id="res-1"
            ).one()
            # 8 hours x 200, plus 250 cleaning, less the 100 discount.
            self.assertEqual(quote.amount_cents, 175_000)
            self.assertEqual(
                {item["type"] for item in quote.line_items_json},
                {"base", "fee", "discount"},
            )
            snapshot = quote.rate_plan_snapshot_json
            self.assertEqual(snapshot["deposit_percent_bps"], 5_000)
            self.assertEqual(snapshot["security_deposit_cents"], 50_000)

            agreement = session.query(BuildingAgreement).filter(
                BuildingAgreement.package_checksum != ""
            ).one()
            merged = agreement.package_snapshot_json["merge_values"]
            self.assertEqual(merged["quote_total"], 175_000)
            self.assertEqual(merged["deposit_amount"], 87_500)
            document = agreement.package_snapshot_json["document"]["text"]
            self.assertIn("Rosa Delgado", document)
            self.assertIn("The Arena", document)
            self.assertNotIn("[not provided]", document)

    def test_02_repricing_leaves_the_earlier_contract_intact(self) -> None:
        self._price()
        again = self.client.post(
            "/admin/building/inquiries/lead-1/contract",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf()},
        )
        self.assertEqual(again.status_code, 303, again.text)
        with self.factory() as session:
            agreements = session.query(BuildingAgreement).filter(
                BuildingAgreement.package_checksum != ""
            ).all()
            self.assertEqual(len(agreements), 2)
            # Editing pricing is always allowed; each contract keeps its own
            # numbers, so what a customer was sent stays readable.
            totals = sorted(
                item.package_snapshot_json["merge_values"]["quote_total"]
                for item in agreements
            )
            self.assertEqual(totals, [130_000, 175_000])

    def test_03_a_lead_with_no_booking_is_told_what_to_do(self) -> None:
        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-2", idempotency_key="lead-2-key", kind="event",
                name="No Booking", email="none@example.com",
                payload_json={"_lifecycle": {"stage": "new"}},
            ))
            session.commit()
        blocked = self.client.post(
            "/admin/building/inquiries/lead-2/contract",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-2")},
        )
        self.assertEqual(blocked.status_code, 303, blocked.text)
        self.assertIn("error=", blocked.headers["location"])
        self.assertIn("event", blocked.headers["location"].lower())

    def test_04_a_qualified_lead_can_take_its_own_date(self) -> None:
        """The whole point: no detour to a booking screen to hold a date."""
        starts = datetime.now(timezone.utc) + timedelta(days=90)

        def stamp(hours: int) -> str:
            return (starts + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M")

        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-3", idempotency_key="lead-3-key", kind="event",
                name="Priya Nair", email="priya@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            session.add(BuildingContact(
                id="c3", email="priya@example.com", full_name="Priya Nair",
                status="active",
            ))
            session.add(BuildingRelationship(
                id="rel-3", contact_id="c3", relationship_type="prospect",
                status="active", source_reference="inquiry:lead-3",
            ))
            session.commit()

        page = self.client.get("/admin/building/inquiries/lead-3")
        self.assertIn("Take this date", page.text)
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text).group(1)

        held = self.client.post(
            "/admin/building/inquiries/lead-3/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": token, "setup_starts_at": stamp(0),
                  "guest_starts_at": stamp(2), "guest_ends_at": stamp(7),
                  "teardown_ends_at": stamp(9), "attendance": "80"},
        )
        self.assertEqual(held.status_code, 303, held.text)
        self.assertIn("notice=", held.headers["location"])

        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="lead-3"
            ).one()
            self.assertEqual(reservation.status, "soft_hold")
            self.assertEqual(reservation.attendance, 80)
            session.query(BuildingProposal).filter_by(
                reservation_id=reservation.id
            ).one()

    def test_05_an_out_of_order_window_is_refused(self) -> None:
        starts = datetime.now(timezone.utc) + timedelta(days=120)

        def stamp(hours: int) -> str:
            return (starts + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M")

        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-4", idempotency_key="lead-4-key", kind="event",
                name="Out Of Order", email="ooo@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            session.add(BuildingContact(id="c4", email="ooo@example.com",
                                        full_name="Out Of Order", status="active"))
            session.add(BuildingRelationship(
                id="rel-4", contact_id="c4", relationship_type="prospect",
                status="active", source_reference="inquiry:lead-4",
            ))
            session.commit()
        token = self._csrf("lead-4")
        refused = self.client.post(
            "/admin/building/inquiries/lead-4/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": token, "setup_starts_at": stamp(9),
                  "guest_starts_at": stamp(2), "guest_ends_at": stamp(7),
                  "teardown_ends_at": stamp(0), "attendance": "40"},
        )
        self.assertEqual(refused.status_code, 303, refused.text)
        self.assertIn("error=", refused.headers["location"])


if __name__ == "__main__":
    unittest.main()
