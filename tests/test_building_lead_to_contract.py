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
from sales_support_agent.services.building_event_calendar import MOUNTAIN


def _mountain(value: datetime) -> datetime:
    """Read a stored instant the way an operator in Denver reads it."""

    aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(MOUNTAIN)


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
        self.assertIn(
            "/admin/building/inquiries/lead-1",
            created.headers["location"],
        )
        self.assertTrue(created.headers["location"].endswith("#agreement"))
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

    def test_99_stale_agreement_is_blocked_and_offers_a_revision(self) -> None:
        self._price(hourly_rate="225", hours="8")
        page = self.client.get("/admin/building/inquiries/lead-1")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("This agreement is out of date", page.text)
        self.assertIn("Create revised agreement", page.text)
        self.assertNotIn("Approve and create the signing copy", page.text)

    def test_03_a_lead_with_no_customer_is_told_why_not(self) -> None:
        """A lead nobody is linked to cannot become a contract, and saying so
        beats a confirmation screen that would fail one press later.

        This replaces the earlier ask-first behaviour: the press no longer asks
        which date to take, it takes the date the lead already carries, so the
        only thing left to say is what is genuinely missing."""
        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-2", idempotency_key="lead-2-key", kind="event",
                name="No Booking", email="none@example.com",
                payload_json={"_lifecycle": {"stage": "new"}},
            ))
            session.commit()
        refused = self.client.post(
            "/admin/building/inquiries/lead-2/contract",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-2")},
        )
        self.assertEqual(refused.status_code, 303, refused.text)
        self.assertIn("error=", refused.headers["location"])
        self.assertIn("No+customer+is+linked", refused.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="lead-2"
                ).count(),
                0,
                "a refusal must not take a date",
            )
        # And the page says the same thing in place of a button that cannot work.
        page = self.client.get("/admin/building/inquiries/lead-2")
        self.assertIn("Cannot create a contract from this lead yet", page.text)
        self.assertIn("No customer is linked to this lead yet.", page.text)
        self.assertNotIn(
            ">Create the contract</button>", page.text,
            "a lead that cannot contract must not offer the press",
        )

    def test_02b_the_lead_and_its_contract_link_to_each_other(self) -> None:
        """A contract is an output of a lead. Reaching one from the other should
        not mean going out to a separate section and searching."""
        page = self.client.get("/admin/building/inquiries/lead-1")
        self.assertEqual(page.status_code, 200, page.text)
        for section_id in (
            "agreement",
            "billing",
            "confirmation",
            "communications",
            "operations",
        ):
            self.assertIn(f'id="{section_id}"', page.text)
        self.assertIn("QuickBooks invoice and payment", page.text)
        self.assertIn("Agreement and signature", page.text)
        self.assertIn("creates a Google Doc", page.text)
        self.assertEqual(page.text.count("Do this next"), 1)
        match = re.search(r'href="/admin/building/contracts/([a-z0-9-]+)"', page.text)
        self.assertIsNotNone(match, "the lead must link to the contract it produced")
        self.assertIn("Advanced contract record", page.text)

        contract = self.client.get(f"/admin/building/contracts/{match.group(1)}")
        self.assertEqual(contract.status_code, 200, contract.text)
        self.assertIn("/admin/building/inquiries/lead-1", contract.text)

    def test_03a_the_confirmation_names_the_window_it_will_take(self) -> None:
        """The panel has to say which date and hours it is about to hold, or
        agreeing to it means nothing."""
        preferred = (datetime.now(MOUNTAIN) + timedelta(days=210)).date()
        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-9", idempotency_key="lead-9-key", kind="event",
                name="Confirm Me", email="confirm@example.com",
                preferred_date=preferred,
                payload_json={
                    "_lifecycle": {"stage": "qualified"},
                    "guestStartTime": "5:00 PM",
                    "guestEndTime": "10:00 PM",
                    "_event_interview": {
                        "event_purpose": "Party", "event_format": "Dinner",
                        "candidate_dates": preferred.isoformat(),
                        "guest_schedule": "5pm to 10pm", "attendance": "80",
                        "agreed_next_step": "Send the agreement",
                        "access_schedule": "3pm", "alcohol": "Licensed bar",
                    },
                },
            ))
            session.commit()
        page = self.client.get(
            f"/admin/building/inquiries/lead-9?confirm=contract"
            f"&month={preferred.strftime('%Y-%m')}"
        )
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("and create the contract?", page.text)
        self.assertIn("2:00 PM", page.text)      # setup, three hours before 5pm
        self.assertIn("1:00 AM", page.text)      # teardown, three hours after 10pm
        self.assertIn("Pick a different date", page.text)

    def test_03b_a_clash_is_refused_unless_the_owner_authorises_it(self) -> None:
        """Double booking is allowed, but only as a named decision, and the
        booking has to carry what it was booked over."""
        from sales_support_agent.models.entities import BuildingAuditEvent

        event_day = (datetime.now(MOUNTAIN) + timedelta(days=200)).date()
        for suffix in ("7", "8"):
            with self.factory() as session:
                session.add(BuildingInquiry(
                    id=f"lead-{suffix}", idempotency_key=f"lead-{suffix}-key",
                    kind="event", name=f"Clash {suffix}",
                    email=f"clash{suffix}@example.com",
                    payload_json={"_lifecycle": {"stage": "qualified"}},
                ))
                session.add(BuildingContact(
                    id=f"c{suffix}", email=f"clash{suffix}@example.com",
                    full_name=f"Clash {suffix}", status="active",
                ))
                session.add(BuildingRelationship(
                    id=f"rel-{suffix}", contact_id=f"c{suffix}",
                    relationship_type="prospect", status="active",
                    source_reference=f"inquiry:lead-{suffix}",
                ))
                session.commit()

        booked = {"event_date": event_day.isoformat(),
                  "guest_start_time": "17:00", "guest_end_time": "22:00",
                  "attendance": "50"}
        first = self.client.post(
            "/admin/building/inquiries/lead-7/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-7"), **booked},
        )
        self.assertIn("notice=", first.headers["location"], first.headers["location"])

        refused = self.client.post(
            "/admin/building/inquiries/lead-8/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-8"), **booked},
        )
        self.assertIn("error=", refused.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="lead-8"
                ).count(),
                0,
            )

        authorised = self.client.post(
            "/admin/building/inquiries/lead-8/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-8"), **booked,
                  "override_conflicts": "yes"},
        )
        self.assertIn("notice=", authorised.headers["location"],
                      authorised.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="lead-8"
            ).one()
            requirements = dict(reservation.requirements_json or {})
            self.assertTrue(requirements.get("double_booked"))
            self.assertEqual(requirements.get("double_booked_by"), "david@anatainc.com")
            self.assertTrue(requirements.get("double_booked_over"))
            trail = session.query(BuildingAuditEvent).filter_by(
                entity_id=reservation.id, action="double_booking_authorised"
            ).one()
            self.assertEqual(trail.actor, "david@anatainc.com")

    def test_03c_a_double_bookable_day_can_actually_be_submitted(self) -> None:
        """Every hour of an occupied day is taken. Marking those options
        disabled made the form unsubmittable, so the one button offering to
        double-book could never do it."""
        event_day = (datetime.now(MOUNTAIN) + timedelta(days=230)).date()
        for suffix, ident in (("10", "c10"), ("11", "c11")):
            with self.factory() as session:
                session.add(BuildingInquiry(
                    id=f"lead-{suffix}", idempotency_key=f"lead-{suffix}-key",
                    kind="event", name=f"Rival {suffix}",
                    email=f"rival{suffix}@example.com",
                    payload_json={
                        "_lifecycle": {"stage": "qualified"},
                        "_event_interview": {
                            "event_purpose": "Party", "event_format": "Dinner",
                            "candidate_dates": event_day.isoformat(),
                            "guest_schedule": "5pm to 10pm", "attendance": "60",
                            "agreed_next_step": "Send it",
                            "access_schedule": "2pm", "alcohol": "Licensed",
                        },
                    },
                ))
                session.add(BuildingContact(
                    id=ident, email=f"rival{suffix}@example.com",
                    full_name=f"Rival {suffix}", status="active",
                ))
                session.add(BuildingRelationship(
                    id=f"rel-{ident}", contact_id=ident,
                    relationship_type="prospect", status="active",
                    source_reference=f"inquiry:lead-{suffix}",
                ))
                session.commit()
        booked = {"event_date": event_day.isoformat(),
                  "guest_start_time": "17:00", "guest_end_time": "22:00",
                  "attendance": "60"}
        first = self.client.post(
            "/admin/building/inquiries/lead-10/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-10"), **booked},
        )
        self.assertIn("notice=", first.headers["location"], first.headers["location"])

        page = self.client.get(
            f"/admin/building/inquiries/lead-11?date={event_day.isoformat()}"
            f"&month={event_day.strftime('%Y-%m')}"
        )
        self.assertEqual(page.status_code, 200, page.text)
        form = page.text.split(
            '<form class="lead-availability lead-availability--hold"'
        )[1].split("</form>")[0]
        self.assertIn("already taken", form)
        self.assertIn("override_conflicts", form)
        self.assertNotIn(
            "disabled", form,
            "an hour the operator is being invited to double-book must submit",
        )

    def test_04_a_qualified_lead_can_take_its_own_date(self) -> None:
        """The whole point: no detour to a booking screen to hold a date."""
        event_day = (datetime.now(MOUNTAIN) + timedelta(days=90)).date()

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
        self.assertIn("Pick the date", page.text)
        token = re.search(r'name="_csrf_token" value="([^"]+)"', page.text).group(1)

        held = self.client.post(
            "/admin/building/inquiries/lead-3/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": token, "event_date": event_day.isoformat(),
                  "guest_start_time": "17:00", "guest_end_time": "22:00",
                  "attendance": "80"},
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
            # Setup and teardown are the owner's three-hour buffers, derived
            # rather than retyped, so guests at 5pm means doors open at 2pm.
            local_setup = _mountain(reservation.starts_at)
            local_guests = _mountain(reservation.guest_starts_at)
            local_end = _mountain(reservation.ends_at)
            self.assertEqual(local_guests.hour, 17)
            self.assertEqual(local_setup.hour, 14)
            self.assertEqual(local_end.hour, 1)
            self.assertEqual(local_setup.date(), event_day)

    def test_05_an_out_of_order_window_is_refused(self) -> None:
        """An operator picks hours, not a window, so the only way to describe an
        impossible event is to have guests arrive and leave at the same time."""
        event_day = (datetime.now(MOUNTAIN) + timedelta(days=120)).date()

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
            data={"_csrf_token": token, "event_date": event_day.isoformat(),
                  "guest_start_time": "18:00", "guest_end_time": "18:00",
                  "attendance": "40"},
        )
        self.assertEqual(refused.status_code, 303, refused.text)
        self.assertIn("error=", refused.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="lead-4"
                ).count(),
                0,
            )

    def test_05b_an_evening_event_carries_past_midnight(self) -> None:
        """Guests leaving at 1am belong to the night they arrived, not to the
        morning of the same calendar day."""
        event_day = (datetime.now(MOUNTAIN) + timedelta(days=150)).date()
        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-6", idempotency_key="lead-6-key", kind="event",
                name="Late Night", email="late@example.com",
                payload_json={"_lifecycle": {"stage": "qualified"}},
            ))
            session.add(BuildingContact(id="c6", email="late@example.com",
                                        full_name="Late Night", status="active"))
            session.add(BuildingRelationship(
                id="rel-6", contact_id="c6", relationship_type="prospect",
                status="active", source_reference="inquiry:lead-6",
            ))
            session.commit()
        held = self.client.post(
            "/admin/building/inquiries/lead-6/hold-date",
            headers=self.headers, follow_redirects=False,
            data={"_csrf_token": self._csrf("lead-6"),
                  "event_date": event_day.isoformat(),
                  "guest_start_time": "20:00", "guest_end_time": "01:00",
                  "attendance": "60"},
        )
        self.assertEqual(held.status_code, 303, held.text)
        self.assertIn("notice=", held.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="lead-6"
            ).one()
            self.assertEqual(_mountain(reservation.guest_starts_at).date(), event_day)
            self.assertEqual(
                _mountain(reservation.guest_ends_at).date(),
                event_day + timedelta(days=1),
            )
            self.assertEqual(_mountain(reservation.ends_at).hour, 4)

    def test_06_an_unqualified_lead_can_still_take_its_date(self) -> None:
        """The date panel used to render only for qualified leads, so the one
        control that unblocks a contract was invisible on most leads."""
        with self.factory() as session:
            session.add(BuildingInquiry(
                id="lead-5", idempotency_key="lead-5-key", kind="event",
                name="Not Yet Qualified", email="new@example.com",
                payload_json={"_lifecycle": {"stage": "new"}},
            ))
            session.commit()
        page = self.client.get("/admin/building/inquiries/lead-5")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Pick the date", page.text)
        self.assertIn("lead-cal__grid", page.text)


if __name__ == "__main__":
    unittest.main()
