"""The month calendar must agree with the records that decide availability.

A calendar that says a date is open when the hold would refuse it is worse than
no calendar, so these check the states an operator actually reads.
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from datetime import date, datetime, time, timedelta, timezone
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/event_calendar_boot.db",
)

from sales_support_agent.models.database import create_session_factory, init_database
from sales_support_agent.models.entities import (
    BuildingOffering,
    BuildingReservation,
    BuildingSpace,
)
from sales_support_agent.services.building_event_calendar import (
    FULL_DAY_HOURS,
    MOUNTAIN,
    access_window,
    guest_hour_options,
    month_availability,
)


def _cell(view: dict, day: date) -> dict:
    return next(item for item in view["cells"] if item["date"] == day)


class AccessWindowTests(unittest.TestCase):
    def test_setup_and_teardown_are_three_hours_either_side(self) -> None:
        day = date(2026, 9, 28)
        setup, guests_in, guests_out, teardown = access_window(
            day, time(17), time(22)
        )
        self.assertEqual(setup.hour, 14)
        self.assertEqual(guests_in.hour, 17)
        self.assertEqual(guests_out.hour, 22)
        self.assertEqual(teardown.hour, 1)
        self.assertEqual(setup.date(), day)
        self.assertEqual(teardown.date(), day + timedelta(days=1))

    def test_an_event_ending_after_midnight_runs_into_the_next_day(self) -> None:
        day = date(2026, 9, 28)
        _setup, guests_in, guests_out, teardown = access_window(
            day, time(20), time(1)
        )
        self.assertEqual(guests_in.date(), day)
        self.assertEqual(guests_out.date(), day + timedelta(days=1))
        self.assertGreater(teardown, guests_out)


class MonthAvailabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        path = os.path.join(
            tempfile.gettempdir(), f"event_calendar_{uuid.uuid4().hex}.db"
        )
        self.factory = create_session_factory("sqlite:///" + path)
        init_database(self.factory)
        with self.factory() as session:
            session.add(BuildingSpace(
                id="arena", slug="arena", name="The Arena", space_type="event",
                capacity=200, status="available",
            ))
            session.add(BuildingOffering(
                id="off", slug="off", name="Event", offering_type="event",
                space_id="arena",
            ))
            session.commit()
        # Far enough out that "past" never interferes with the assertions.
        self.month = (datetime.now(MOUNTAIN).date() + timedelta(days=120)).replace(
            day=1
        )

    def _reserve(
        self,
        *,
        day: date,
        status: str,
        guests: tuple[int, int] = (17, 21),
        inquiry_id: str = "",
        ident: str = "",
    ) -> None:
        setup, _in, _out, teardown = access_window(
            day, time(guests[0]), time(guests[1])
        )
        with self.factory() as session:
            session.add(BuildingReservation(
                id=ident or f"res-{uuid.uuid4().hex[:8]}",
                kind="event", status=status, space_id="arena", offering_id="off",
                inquiry_id=inquiry_id or None,
                starts_at=setup.astimezone(timezone.utc),
                ends_at=teardown.astimezone(timezone.utc),
                guest_starts_at=_in.astimezone(timezone.utc),
                guest_ends_at=_out.astimezone(timezone.utc),
                attendance=50, created_by="test",
            ))
            session.commit()

    def _view(self, **kwargs) -> dict:
        with self.factory() as session:
            return month_availability(
                session,
                calendar=mock.Mock(configured=False),
                month=self.month,
                space_id="arena",
                **kwargs,
            )

    def test_an_untouched_day_is_open_and_selectable(self) -> None:
        cell = _cell(self._view(), self.month.replace(day=10))
        self.assertEqual(cell["state"], "open")
        self.assertTrue(cell["selectable"])

    def test_a_soft_hold_reads_as_held_not_booked(self) -> None:
        day = self.month.replace(day=10)
        self._reserve(day=day, status="soft_hold")
        cell = _cell(self._view(), day)
        self.assertEqual(cell["state"], "pending")
        self.assertIn("not yet signed", cell["note"])
        self.assertTrue(cell["occupied"])
        # Still selectable: double-booking is the owner's call, and the picker
        # names the clash before it can be taken.
        self.assertTrue(cell["selectable"])

    def test_a_confirmed_booking_reads_as_booked(self) -> None:
        day = self.month.replace(day=11)
        self._reserve(day=day, status="confirmed")
        self.assertEqual(_cell(self._view(), day)["state"], "booked")

    def test_a_cancelled_reservation_frees_the_day(self) -> None:
        day = self.month.replace(day=12)
        self._reserve(day=day, status="cancelled")
        self.assertEqual(_cell(self._view(), day)["state"], "open")

    def test_a_booking_outranks_a_hold_on_the_same_day(self) -> None:
        day = self.month.replace(day=13)
        self._reserve(day=day, status="soft_hold")
        self._reserve(day=day, status="confirmed")
        self.assertEqual(_cell(self._view(), day)["state"], "booked")

    def test_a_full_day_warns_its_neighbours_without_closing_them(self) -> None:
        """The owner chose a warning, not a wall: the day stays sellable."""
        day = self.month.replace(day=15)
        # Guests 09:00 to 18:00 plus three-hour buffers is fifteen hours in the
        # building, and it starts and ends on the one day.
        self._reserve(day=day, status="confirmed", guests=(9, 18))
        view = self._view()
        self.assertEqual(_cell(view, day)["state"], "booked")
        for neighbour in (day - timedelta(days=1), day + timedelta(days=1)):
            cell = _cell(view, neighbour)
            self.assertEqual(cell["state"], "heads_up")
            self.assertTrue(
                cell["selectable"],
                "a heads-up must stay bookable, or it is a block",
            )

    def test_a_short_event_leaves_its_neighbours_alone(self) -> None:
        day = self.month.replace(day=18)
        self._reserve(day=day, status="confirmed", guests=(17, 20))
        view = self._view()
        for neighbour in (day - timedelta(days=1), day + timedelta(days=1)):
            self.assertEqual(_cell(view, neighbour)["state"], "open")

    def test_an_evening_event_does_not_close_the_following_day(self) -> None:
        """Teardown finishing at midnight is the end of one day, not the loss
        of the next."""
        day = self.month.replace(day=26)
        self._reserve(day=day, status="confirmed", guests=(18, 21))
        view = self._view()
        self.assertEqual(_cell(view, day)["state"], "booked")
        self.assertEqual(_cell(view, day + timedelta(days=1))["state"], "open")

    def test_the_full_day_threshold_matches_the_owner_rule(self) -> None:
        self.assertEqual(FULL_DAY_HOURS, 12)

    def test_this_lead_s_own_hold_does_not_block_its_own_calendar(self) -> None:
        """Re-picking a date on the lead that holds it must not look taken."""
        day = self.month.replace(day=20)
        self._reserve(day=day, status="soft_hold", inquiry_id="lead-1")
        self.assertEqual(
            _cell(self._view(exclude_inquiry_id="lead-1"), day)["state"], "open"
        )
        self.assertEqual(_cell(self._view(), day)["state"], "pending")

    def test_requested_dates_stay_visible(self) -> None:
        day = self.month.replace(day=22)
        view = self._view(requested=[day])
        self.assertTrue(_cell(view, day)["requested"])
        self.assertFalse(_cell(view, day + timedelta(days=1))["requested"])

    def test_an_unreachable_calendar_is_reported_not_assumed_open(self) -> None:
        failing = mock.Mock(configured=True)
        failing.find_conflicts.side_effect = RuntimeError("network down")
        with self.factory() as session:
            view = month_availability(
                session, calendar=failing, month=self.month, space_id="arena"
            )
        self.assertEqual(view["calendar_status"], "unknown")

    def test_an_outside_calendar_event_occupies_the_day(self) -> None:
        day = self.month.replace(day=24)
        connected = mock.Mock(configured=True)
        connected.find_conflicts.return_value = [
            {"start": {"date": day.isoformat()},
             "end": {"date": (day + timedelta(days=1)).isoformat()}}
        ]
        with self.factory() as session:
            view = month_availability(
                session, calendar=connected, month=self.month, space_id="arena"
            )
        cell = _cell(view, day)
        self.assertEqual(cell["state"], "external")
        self.assertTrue(cell["occupied"])

    def test_past_days_cannot_be_picked(self) -> None:
        today = datetime.now(MOUNTAIN).date()
        with self.factory() as session:
            view = month_availability(
                session,
                calendar=mock.Mock(configured=False),
                month=today.replace(day=1),
                space_id="arena",
            )
        past = [item for item in view["cells"] if item["is_past"]]
        self.assertTrue(past, "a current-month grid should contain past days")
        self.assertTrue(all(not item["selectable"] for item in past))


class GuestHourOptionTests(unittest.TestCase):
    def test_hours_inside_an_existing_booking_are_marked_taken(self) -> None:
        day = date(2026, 9, 28)
        busy = [(
            datetime.combine(day, time(14), tzinfo=MOUNTAIN),
            datetime.combine(day, time(18), tzinfo=MOUNTAIN),
        )]
        options = {item["value"]: item["taken"] for item in guest_hour_options(busy, day)}
        self.assertTrue(options["15:00"])
        self.assertTrue(options["17:00"])
        self.assertFalse(options["19:00"])

    def test_hours_read_as_a_clock_not_a_number(self) -> None:
        labels = {
            item["value"]: item["label"]
            for item in guest_hour_options([], date(2026, 9, 28))
        }
        self.assertEqual(labels["13:00"], "1:00 PM")
        self.assertEqual(labels["09:00"], "9:00 AM")


if __name__ == "__main__":
    unittest.main()
