"""One press creates the contract, one press takes it back.

The old flow reloaded the page, jumped to a section that is not drawn on every
lead, and said nothing at all. These tests hold the two properties that stop
that returning: the press either works or explains itself, and whatever it did
can be undone while the contract is still nobody's but ours.
"""

from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
import uuid
from unittest import mock
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/one_press_boot.db",
)
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "one-press-secret")

from fastapi.testclient import TestClient

from sales_support_agent.main import app
from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAgreementTemplate,
    BuildingAvailabilityBlock,
    BuildingCalendarProjection,
    BuildingContact,
    BuildingInquiry,
    BuildingOffering,
    BuildingRatePlan,
    BuildingRelationship,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.admin_auth import create_user_session_token


class OnePressContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"one_press_{uuid.uuid4().hex}.db"
        )
        cls.factory = create_session_factory("sqlite:///" + path)
        init_database(cls.factory)
        cls.original_factory = app.state.session_factory
        cls.original_settings = app.state.settings
        app.state.session_factory = cls.factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            internal_api_key="one-press-key",
            building_campaign_token_secret="one-press-csrf",
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
                BuildingAgreementTemplate(
                    id="tpl-v1", template_key="event-agreement", version=1,
                    name="Event agreement", status="approved",
                    contract_type="event",
                    body_markdown="{{customer_name}} at {{event_space}}.",
                    clauses_json=[],
                    merge_fields_json=["customer_name", "event_space"],
                    approved_by="david@anatainc.com", approved_at=now,
                ),
            ])
            session.commit()

    @classmethod
    def tearDownClass(cls) -> None:
        app.state.session_factory = cls.original_factory
        app.state.settings = cls.original_settings

    def _csrf(self, lead: str) -> str:
        page = self.client.get(f"/admin/building/inquiries/{lead}")
        self.assertEqual(page.status_code, 200, page.text)
        return re.search(r'name="_csrf_token" value="([^"]+)"', page.text).group(1)

    def _lead(
        self,
        lead_id: str,
        *,
        kind: str = "event",
        with_customer: bool = True,
        days_out: int = 0,
        hours: tuple[str, str] | None = ("16:00", "21:00"),
        interview_schedule: str = "",
    ) -> None:
        """A lead shaped the way the website form leaves one."""

        # Distinct per lead unless a test deliberately wants a clash: two leads
        # on one day is exactly what the clash stop is for.
        if not days_out:
            days_out = 30 + (abs(hash(lead_id)) % 200)
        with self.factory() as session:
            if with_customer and session.get(BuildingContact, "c1") is None:
                session.add(BuildingContact(
                    id="c1", email="rosa@example.com", full_name="Rosa Delgado",
                    status="active",
                ))
            payload = {
                "_lifecycle": {"stage": "qualified"},
            }
            if hours is not None:
                payload.update({
                    "guestStartTime": hours[0],
                    "guestEndTime": hours[1],
                })
            if interview_schedule:
                payload["_event_interview"] = {
                    "guest_schedule": interview_schedule,
                    "attendance": "80 expected",
                }
            session.add(BuildingInquiry(
                id=lead_id, idempotency_key=f"{lead_id}-key", kind=kind,
                name="Rosa Delgado", email="rosa@example.com",
                preferred_date=date.today() + timedelta(days=days_out),
                payload_json=payload,
            ))
            if with_customer:
                session.add(BuildingRelationship(
                    id=f"rel-{lead_id}", contact_id="c1",
                    relationship_type="prospect", status="active",
                    source_reference=f"inquiry:{lead_id}",
                ))
            session.commit()

    def _press(self, lead: str, **extra: str):
        data = {"_csrf_token": self._csrf(lead), **extra}
        return self.client.post(
            f"/admin/building/inquiries/{lead}/contract",
            headers=self.headers, data=data, follow_redirects=False,
        )

    def _undo(self, lead: str):
        return self.client.post(
            f"/admin/building/inquiries/{lead}/contract/undo",
            headers=self.headers,
            data={"_csrf_token": self._csrf(lead)},
            follow_redirects=False,
        )

    # ---- the press ----------------------------------------------------

    def test_01_one_press_takes_the_date_and_prepares_the_contract(self) -> None:
        """No calendar click, no confirmation screen, no second form."""
        self._lead("press-1")
        pressed = self._press("press-1")
        self.assertEqual(pressed.status_code, 303, pressed.text)
        location = pressed.headers["location"]
        self.assertNotIn("error=", location)
        self.assertNotIn("confirm=contract", location)
        self.assertIn("notice=", location)
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="press-1"
            ).one()
            self.assertEqual(reservation.status, "soft_hold")
            agreement = session.query(BuildingAgreement).filter_by(
                reservation_id=reservation.id
            ).one()
            self.assertEqual(agreement.preparation_status, "prepared")
            self.assertNotEqual(agreement.package_checksum, "")

    def test_02_the_button_names_the_date_before_it_is_pressed(self) -> None:
        """One press is only safe if the decision is readable first."""
        self._lead("press-2", days_out=45)
        page = self.client.get("/admin/building/inquiries/press-2")
        target = (date.today() + timedelta(days=45)).strftime("%A, %B %d, %Y")
        self.assertIn(f"Holds {target}", page.text)
        self.assertIn("guests", page.text)
        self.assertIn(">Create the contract</button>", page.text)

    def test_02b_saved_interview_hours_drive_preview_and_contract(self) -> None:
        """The validated interview must not be replaced by all-day defaults."""

        self._lead(
            "press-interview-hours",
            days_out=246,
            hours=None,
            interview_schedule="Guests 09:00–15:00",
        )

    def _change_date(self, lead: str):
        return self.client.post(
            f"/admin/building/inquiries/{lead}/contract/undo?intent=change_date",
            headers=self.headers,
            data={"_csrf_token": self._csrf(lead)},
            follow_redirects=False,
        )
        page = self.client.get("/admin/building/inquiries/press-interview-hours")
        self.assertIn("guests 9:00 AM to 3:00 PM", page.text)

        pressed = self._press("press-interview-hours")
        self.assertEqual(pressed.status_code, 303, pressed.text)
        self.assertNotIn("error=", pressed.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="press-interview-hours"
            ).one()
            starts = reservation.guest_starts_at.replace(
                tzinfo=timezone.utc
            ).astimezone(ZoneInfo("America/Denver"))
            ends = reservation.guest_ends_at.replace(
                tzinfo=timezone.utc
            ).astimezone(ZoneInfo("America/Denver"))
            self.assertEqual(starts.hour, 9)
            self.assertEqual(ends.hour, 15)

    def test_02c_structured_hours_override_interview_prose(self) -> None:
        self._lead(
            "press-structured-hours",
            days_out=247,
            hours=("10:00", "14:00"),
            interview_schedule="Guests 09:00–15:00",
        )
        page = self.client.get("/admin/building/inquiries/press-structured-hours")
        self.assertIn("guests 10:00 AM to 2:00 PM", page.text)

    def test_02d_ambiguous_interview_hours_are_not_guessed(self) -> None:
        from sales_support_agent.api.building_inquiry_workspace_router import (
            _guest_schedule_window,
        )

        self.assertEqual(
            _guest_schedule_window("Doors at 9, lunch at 12, finish at 3"),
            ("", ""),
        )

    def test_03_pressing_twice_does_not_make_two_contracts(self) -> None:
        self._lead("press-3")
        self.assertEqual(self._press("press-3").status_code, 303)
        again = self._press("press-3")
        self.assertIn("error=", again.headers["location"])
        self.assertIn("already+has+a+contract", again.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="press-3"
            ).one()
            self.assertEqual(
                session.query(BuildingAgreement).filter_by(
                    reservation_id=reservation.id
                ).count(),
                1,
            )

    # ---- refusals, in writing -----------------------------------------

    def test_04_a_workspace_filed_lead_still_makes_its_contract(self) -> None:
        """The exact lead David was stuck on.

        Arena enquiries arrive from Eventective filed as workspace requests,
        carrying an event date and Arena pricing. Refusing those because of the
        intake label was correct and useless: the operator could read why and
        still not do their job."""
        self._lead("press-4", kind="workspace")
        page = self.client.get("/admin/building/inquiries/press-4")
        self.assertIn(">Create the contract</button>", page.text)
        self.assertNotIn("Cannot create a contract from this lead yet", page.text)
        pressed = self._press("press-4")
        self.assertEqual(pressed.status_code, 303, pressed.text)
        self.assertNotIn("error=", pressed.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="press-4"
            ).one()
            agreement = session.query(BuildingAgreement).filter_by(
                reservation_id=reservation.id
            ).one()
            self.assertEqual(agreement.preparation_status, "prepared")

    def test_04b_a_workspace_lead_names_its_date_on_the_button(self) -> None:
        self._lead("press-4b", kind="workspace", days_out=52)
        page = self.client.get("/admin/building/inquiries/press-4b")
        target = (date.today() + timedelta(days=52)).strftime("%A, %B %d, %Y")
        self.assertIn(f"Holds {target}", page.text)

    def test_05_every_refusal_reaches_the_page_as_words(self) -> None:
        """A redirect that says nothing is the bug. Each cause must carry text."""
        self._lead("press-5", with_customer=False)
        refused = self._press("press-5")
        location = refused.headers["location"]
        self.assertIn("error=", location)
        followed = self.client.get(location)
        self.assertIn("No customer is linked to this lead yet.", followed.text)

    def test_06_a_refusal_never_points_at_a_section_that_is_not_drawn(self) -> None:
        """The original failure was an anchor with nothing to anchor to."""
        self._lead("press-6", with_customer=False)
        refused = self._press("press-6")
        location = refused.headers["location"]
        if "#" in location:
            anchor = location.split("#", 1)[1]
            page = self.client.get(location.split("#", 1)[0])
            self.assertIn(f'id="{anchor}"', page.text)

    def test_06b_text_in_attendance_moves_correction_into_contract_confirmation(self) -> None:
        """Regression for Elisa's production answer, which began with Event."""

        self._lead("press-attendance-text")
        with self.factory() as session:
            lead = session.get(BuildingInquiry, "press-attendance-text")
            payload = dict(lead.payload_json or {})
            payload["_event_interview"] = {"attendance": "Event reception"}
            lead.payload_json = payload
            session.commit()
        correction = self._press("press-attendance-text")
        self.assertEqual(correction.status_code, 303, correction.text)
        self.assertIn("confirm=contract", correction.headers["location"])
        self.assertNotIn("error=", correction.headers["location"])
        page = self.client.get(correction.headers["location"].split("#")[0])
        self.assertIn("Expected attendance", page.text)
        self.assertIn('name="attendance"', page.text)
        self.assertIn('type="number"', page.text)
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="press-attendance-text"
                ).count(),
                0,
            )

    def test_06c_human_attendance_text_can_still_create_the_contract(self) -> None:
        self._lead("press-attendance-number")
        with self.factory() as session:
            lead = session.get(BuildingInquiry, "press-attendance-number")
            payload = dict(lead.payload_json or {})
            payload["_event_interview"] = {
                "attendance": "Event reception: 80 expected, 100 maximum"
            }
            lead.payload_json = payload
            session.commit()
        created = self._press("press-attendance-number")
        self.assertEqual(created.status_code, 303, created.text)
        self.assertNotIn("error=", created.headers["location"])
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="press-attendance-number"
            ).one()
            self.assertEqual(reservation.attendance, 80)

    def test_06d_holding_a_date_with_bad_saved_attendance_never_500s(self) -> None:
        self._lead("hold-attendance-text", days_out=380)
        with self.factory() as session:
            lead = session.get(BuildingInquiry, "hold-attendance-text")
            payload = dict(lead.payload_json or {})
            payload["_event_interview"] = {"attendance": "Event reception"}
            lead.payload_json = payload
            session.commit()
        held = self.client.post(
            "/admin/building/inquiries/hold-attendance-text/hold-date",
            headers=self.headers,
            follow_redirects=False,
            data={
                "_csrf_token": self._csrf("hold-attendance-text"),
                "event_date": (date.today() + timedelta(days=380)).isoformat(),
                "guest_start_time": "16:00",
                "guest_end_time": "21:00",
                "attendance": "",
            },
        )
        self.assertEqual(held.status_code, 303, held.text)
        self.assertIn("error=", held.headers["location"])
        self.assertIn("attendance+needs+a+number", held.headers["location"])

    # ---- the clash, the one place it still asks ------------------------

    def test_07_a_taken_date_stops_and_asks_before_double_booking(self) -> None:
        """One Arena. Booking it twice is a decision, not a side effect."""
        self._lead("press-7a", days_out=400)
        self.assertEqual(self._press("press-7a").status_code, 303)
        self._lead("press-7b", days_out=400)
        clashed = self._press("press-7b")
        self.assertIn("confirm=contract", clashed.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="press-7b"
                ).count(),
                0,
                "a clash must not take the date by itself",
            )

    def test_07b_the_clash_screen_names_what_it_would_book_over(self) -> None:
        """The one remaining stop has to be readable, or it is just friction."""
        self._lead("press-7c", days_out=460)
        self.assertEqual(self._press("press-7c").status_code, 303)
        self._lead("press-7d", days_out=460)
        clashed = self._press("press-7d")
        page = self.client.get(clashed.headers["location"].split("#")[0])
        self.assertIn("already taken", page.text)
        self.assertIn("Double-book it and create the contract", page.text)
        self.assertNotIn("A few answers first", page.text)

    def test_08_a_clash_proceeds_once_the_owner_authorises_it(self) -> None:
        self._lead("press-8a", days_out=430)
        self.assertEqual(self._press("press-8a").status_code, 303)
        self._lead("press-8b", days_out=430)
        forced = self._press("press-8b", override_conflicts="yes")
        self.assertNotIn("error=", forced.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.query(BuildingReservation).filter_by(
                    inquiry_id="press-8b"
                ).count(),
                1,
            )

    # ---- undo ----------------------------------------------------------

    def test_09_undo_releases_the_date_and_cancels_the_contract(self) -> None:
        self._lead("undo-1")
        self.assertEqual(self._press("undo-1").status_code, 303)
        with self.factory() as session:
            reservation_id = session.query(BuildingReservation).filter_by(
                inquiry_id="undo-1"
            ).one().id
            self.assertEqual(
                session.query(BuildingAvailabilityBlock).filter_by(
                    source_reference=f"reservation:{reservation_id}"
                ).count(),
                1,
            )
        undone = self._undo("undo-1")
        self.assertEqual(undone.status_code, 303, undone.text)
        self.assertNotIn("error=", undone.headers["location"])
        with self.factory() as session:
            reservation = session.get(BuildingReservation, reservation_id)
            self.assertEqual(reservation.status, "cancelled")
            self.assertIsNone(reservation.hold_expires_at)
            self.assertEqual(
                session.query(BuildingAvailabilityBlock).filter_by(
                    source_reference=f"reservation:{reservation_id}"
                ).count(),
                0,
                "the date must actually be free again",
            )
            agreement = session.query(BuildingAgreement).filter_by(
                reservation_id=reservation_id
            ).one()
            self.assertEqual(agreement.preparation_status, "cancelled")
            # Cancelled, never deleted: billing reads these rows.
            self.assertNotEqual(agreement.package_checksum, "")

    def test_09b_undo_delivers_a_live_calendar_deletion(self) -> None:
        self._lead("undo-calendar", days_out=248)
        self.assertEqual(self._press("undo-calendar").status_code, 303)
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="undo-calendar"
            ).one()
            reservation_id = reservation.id
            projection = session.query(BuildingCalendarProjection).filter_by(
                reservation_id=reservation.id
            ).one()
            projection.provider_event_id = "google-event-1"
            projection.status = "synced"
            reservation.calendar_event_id = "google-event-1"
            session.commit()

        with mock.patch(
            "sales_support_agent.api.building_inquiry_workspace_router.sync_calendar_projections",
            return_value={"synced_count": 1, "failed_count": 0},
        ) as delivered:
            undone = self._undo("undo-calendar")

        self.assertNotIn("error=", undone.headers["location"])
        delivered.assert_called_once()
        sync_input = delivered.call_args.args[0]
        self.assertTrue(sync_input.execute)
        self.assertFalse(sync_input.dry_run)
        self.assertEqual(sync_input.reservation_id, reservation_id)

    def test_09c_undo_reports_when_google_does_not_confirm_deletion(self) -> None:
        self._lead("undo-calendar-fail", days_out=249)
        self.assertEqual(self._press("undo-calendar-fail").status_code, 303)
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="undo-calendar-fail"
            ).one()
            projection = session.query(BuildingCalendarProjection).filter_by(
                reservation_id=reservation.id
            ).one()
            projection.provider_event_id = "google-event-2"
            projection.status = "synced"
            session.commit()

        with mock.patch(
            "sales_support_agent.api.building_inquiry_workspace_router.sync_calendar_projections",
            return_value={"synced_count": 0, "failed_count": 1},
        ):
            undone = self._undo("undo-calendar-fail")

        self.assertIn("error=", undone.headers["location"])
        self.assertIn("did+not+confirm+the+deletion", undone.headers["location"])

    def test_10_undo_is_offered_then_withdrawn(self) -> None:
        self._lead("undo-2")
        self._press("undo-2")
        offered = self.client.get("/admin/building/inquiries/undo-2")
        self.assertIn("/contract/undo?intent=change_date", offered.text)
        self.assertIn(">Change date</button>", offered.text)
        self._undo("undo-2")
        after = self.client.get("/admin/building/inquiries/undo-2")
        self.assertNotIn(">Change date</button>", after.text)

    def test_10b_change_date_returns_to_review_without_losing_lead_work(self) -> None:
        self._lead("change-date", days_out=250)
        self.assertEqual(self._press("change-date").status_code, 303)
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "change-date")
            payload = dict(inquiry.payload_json or {})
            payload["_event_interview"] = {
                "guest_schedule": "9:00 AM to 3:00 PM",
                "attendance": "80",
            }
            payload["_pricing"] = {"hourly_rate_cents": 17500, "hours": 6}
            inquiry.payload_json = payload
            session.commit()

        changed = self._change_date("change-date")

        self.assertNotIn("error=", changed.headers["location"])
        self.assertIn("Old+date+released", changed.headers["location"])
        self.assertTrue(changed.headers["location"].endswith("#date-review"))
        with self.factory() as session:
            inquiry = session.get(BuildingInquiry, "change-date")
            payload = dict(inquiry.payload_json or {})
            self.assertEqual(payload["_event_interview"]["attendance"], "80")
            self.assertEqual(payload["_pricing"]["hourly_rate_cents"], 17500)
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="change-date"
            ).one()
            self.assertEqual(reservation.status, "cancelled")

    def test_10c_sent_contract_keeps_date_change_guidance_visible(self) -> None:
        self._lead("change-date-sent", days_out=251)
        self._press("change-date-sent")
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="change-date-sent"
            ).one()
            agreement = session.query(BuildingAgreement).filter_by(
                reservation_id=reservation.id
            ).one()
            agreement.sent_at = datetime.now(timezone.utc)
            session.commit()

        page = self.client.get("/admin/building/inquiries/change-date-sent")

        self.assertIn("Need to change the date?", page.text)
        self.assertIn("already gone to the customer", page.text)
        self.assertNotIn(">Change date</button>", page.text)

    def test_11_undoing_twice_is_refused_the_second_time(self) -> None:
        self._lead("undo-3")
        self._press("undo-3")
        self.assertNotIn("error=", self._undo("undo-3").headers["location"])
        again = self._undo("undo-3")
        self.assertIn("error=", again.headers["location"])

    def test_12_undo_is_refused_once_the_contract_has_been_sent(self) -> None:
        """Undo is state-gated, not timed: what a customer has seen stays."""
        self._lead("undo-4")
        self._press("undo-4")
        with self.factory() as session:
            reservation_id = session.query(BuildingReservation).filter_by(
                inquiry_id="undo-4"
            ).one().id
            agreement = session.query(BuildingAgreement).filter_by(
                reservation_id=reservation_id
            ).one()
            agreement.sent_at = datetime.now(timezone.utc)
            session.add(agreement)
            session.commit()
        refused = self._undo("undo-4")
        self.assertIn("error=", refused.headers["location"])
        self.assertIn("already+gone+to+the+customer", refused.headers["location"])
        with self.factory() as session:
            self.assertEqual(
                session.get(BuildingReservation, reservation_id).status,
                "soft_hold",
                "a refused undo must change nothing",
            )

    def test_13_undo_is_refused_once_a_payment_is_recorded(self) -> None:
        self._lead("undo-5")
        self._press("undo-5")
        with self.factory() as session:
            reservation = session.query(BuildingReservation).filter_by(
                inquiry_id="undo-5"
            ).one()
            reservation.deposit_status = "paid"
            session.add(reservation)
            session.commit()
            reservation_id = reservation.id
        refused = self._undo("undo-5")
        self.assertIn("error=", refused.headers["location"])
        self.assertIn("payment", refused.headers["location"].lower())
        with self.factory() as session:
            self.assertEqual(
                session.get(BuildingReservation, reservation_id).status, "soft_hold"
            )

    def test_14_an_undone_lead_can_be_contracted_again(self) -> None:
        """Undo puts the lead back, so the next press must work like the first."""
        self._lead("undo-6")
        self._press("undo-6")
        self._undo("undo-6")
        page = self.client.get("/admin/building/inquiries/undo-6")
        self.assertIn(">Create the contract</button>", page.text)


if __name__ == "__main__":
    unittest.main()
