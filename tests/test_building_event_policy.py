"""November 10, 2026 Arena card: day rates, no deposit, weekend party hours."""

from __future__ import annotations

import unittest
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from sales_support_agent.services.building_event_policy import (
    FULL_DAY_CENTS,
    FULL_PLUS_HALF_CENTS,
    HALF_DAY_CENTS,
    TWO_FULL_DAYS_CENTS,
    evaluate_event_request,
    quote_duration,
    remaining_balance_due,
    review_base_charge,
)
from sales_support_agent.services.building_inquiry_workspace import (
    _day_portion_options,
)


MOUNTAIN = ZoneInfo("America/Denver")


class PublishedCardTests(unittest.TestCase):
    def test_hour_counts_match_the_card(self) -> None:
        self.assertEqual(quote_duration(hours=6), ("half", HALF_DAY_CENTS))
        self.assertEqual(quote_duration(hours=8), ("full", FULL_DAY_CENTS))
        self.assertEqual(
            quote_duration(hours=18), ("full_plus_half", FULL_PLUS_HALF_CENTS)
        )
        self.assertEqual(quote_duration(hours=24), ("two_full", TWO_FULL_DAYS_CENTS))

    def test_day_counts_match_the_card(self) -> None:
        self.assertEqual(quote_duration(days=0.5), ("half", 150_000))
        self.assertEqual(quote_duration(days=1), ("full", 200_000))
        self.assertEqual(quote_duration(days=1.5), ("full_plus_half", 300_000))
        self.assertEqual(quote_duration(days=2), ("two_full", 350_000))

    def test_one_calendar_day_longer_than_six_hours_is_a_full_day(self) -> None:
        """Saturday 8:00 AM to midnight is one day, not a day and a half."""

        decision = evaluate_event_request(
            event_date=date(2026, 11, 14),  # Saturday
            guest_start="08:00",
            guest_end="24:00",
            event_type="Company party",
            attendance=120,
        )
        self.assertTrue(decision.quotable)
        self.assertEqual(decision.day_portion, "full")
        self.assertEqual(decision.venue_cents, FULL_DAY_CENTS)
        self.assertTrue(decision.review_required)
        self.assertFalse(decision.auto_confirm)

    def test_a_span_across_a_full_day_and_a_half_day(self) -> None:
        start = datetime(2026, 11, 14, 8, 0, tzinfo=MOUNTAIN)
        end = datetime(2026, 11, 15, 6, 0, tzinfo=MOUNTAIN)
        from sales_support_agent.services.building_event_policy import portion_for_span

        self.assertEqual(portion_for_span(start, end), "full_plus_half")


class ScheduleRuleTests(unittest.TestCase):
    def test_weekday_business_requests_produce_no_price(self) -> None:
        decision = evaluate_event_request(
            event_date=date(2026, 11, 10),  # Tuesday
            guest_start="09:00",
            guest_end="15:00",
            event_type="Board meeting",
            attendance=12,
            hours=6,
        )
        self.assertFalse(decision.quotable)
        self.assertFalse(decision.rejected)
        self.assertIsNone(decision.venue_cents)
        self.assertIn("put together pricing", decision.message)
        self.assertIn("tour", decision.message)
        self.assertEqual(decision.questions, ())

    def test_weekend_party_outside_hours_is_rejected(self) -> None:
        early = evaluate_event_request(
            event_date=date(2026, 11, 14),  # Saturday
            guest_start="06:00",
            guest_end="11:00",
            event_type="Birthday party",
            attendance=40,
        )
        late = evaluate_event_request(
            event_date=date(2026, 11, 15),  # Sunday
            guest_start="18:00",
            guest_end="23:00",
            event_type="Open bar party",
            attendance=80,
        )
        weekday = evaluate_event_request(
            event_date=date(2026, 11, 12),  # Thursday
            guest_start="18:00",
            guest_end="23:00",
            event_type="Rager",
            attendance=150,
            alcohol="yes",
        )
        for decision in (early, late, weekday):
            self.assertTrue(decision.rejected, decision.message)
            self.assertFalse(decision.quotable)
            self.assertIsNone(decision.venue_cents)

    def test_new_years_eve_is_allowed_on_a_weekday(self) -> None:
        decision = evaluate_event_request(
            event_date=date(2026, 12, 31),  # Thursday
            guest_start="16:00",
            guest_end="24:00",
            event_type="New Year's Eve party",
            attendance=180,
            alcohol="open bar",
        )
        self.assertFalse(decision.rejected)
        self.assertTrue(decision.quotable)
        self.assertEqual(decision.venue_cents, FULL_DAY_CENTS)
        self.assertTrue(decision.review_required)
        self.assertGreaterEqual(len(decision.questions), 4)

    def test_a_small_weekend_gathering_is_priced_without_party_questions(self) -> None:
        decision = evaluate_event_request(
            event_date=date(2026, 11, 15),  # Sunday
            guest_start="10:00",
            guest_end="14:00",
            event_type="Family lunch",
            attendance=20,
        )
        self.assertTrue(decision.quotable)
        self.assertEqual(decision.venue_cents, HALF_DAY_CENTS)
        self.assertFalse(decision.review_required)
        self.assertEqual(decision.questions, ())

    def test_a_weekend_party_inside_hours_uses_the_same_card(self) -> None:
        decision = evaluate_event_request(
            event_date=date(2026, 11, 14),
            guest_start="17:00",
            guest_end="23:00",
            event_type="Wedding reception",
            attendance=140,
            alcohol="yes",
            hours=6,
        )
        self.assertTrue(decision.quotable)
        self.assertFalse(decision.rejected)
        self.assertEqual(decision.venue_cents, HALF_DAY_CENTS)
        self.assertTrue(decision.review_required)


class ReviewAndBillingFixesTests(unittest.TestCase):
    def test_a_zero_unit_day_card_still_prices_the_hold(self) -> None:
        start = datetime(2026, 11, 14, 17, 0, tzinfo=MOUNTAIN)
        end = datetime(2026, 11, 14, 23, 0, tzinfo=MOUNTAIN)
        quantity, amount, name, addon_units = review_base_charge(
            booking_unit="day_portion",
            unit_amount_cents=0,
            minimum_units=1,
            commercial_terms={"half_day_amount_cents": 150_000},
            guest_starts_at=start,
            guest_ends_at=end,
            units=6,
        )
        self.assertEqual(quantity, 1)
        self.assertEqual(amount, HALF_DAY_CENTS)
        self.assertIn("Half day", name)
        self.assertEqual(addon_units, 6)

    def test_an_eight_hour_window_on_the_retired_hourly_plan_is_a_full_day(self) -> None:
        start = datetime(2026, 11, 14, 10, 0, tzinfo=MOUNTAIN)
        end = datetime(2026, 11, 14, 18, 0, tzinfo=MOUNTAIN)
        _quantity, amount, _name, _units = review_base_charge(
            booking_unit="hour",
            unit_amount_cents=17_500,
            minimum_units=6,
            commercial_terms=None,
            guest_starts_at=start,
            guest_ends_at=end,
            units=8,
        )
        self.assertEqual(amount, FULL_DAY_CENTS)

    def test_a_custom_plan_still_multiplies_its_own_unit_price(self) -> None:
        start = datetime(2026, 11, 14, 17, 0, tzinfo=MOUNTAIN)
        end = datetime(2026, 11, 14, 21, 0, tzinfo=MOUNTAIN)
        quantity, amount, _name, _units = review_base_charge(
            booking_unit="event",
            unit_amount_cents=250_000,
            minimum_units=1,
            commercial_terms=None,
            guest_starts_at=start,
            guest_ends_at=end,
            units=1,
        )
        self.assertEqual((quantity, amount), (1, 250_000))

    def test_the_new_card_is_due_before_the_event_not_seven_days_prior(self) -> None:
        event_day = date(2026, 11, 20)
        self.assertEqual(
            remaining_balance_due(
                event_day, {"balance_due": "before_the_event", "balance_due_days_before_event": None}
            ),
            event_day,
        )
        self.assertEqual(
            remaining_balance_due(event_day, {"balance_due_days_before_event": 7}),
            date(2026, 11, 13),
        )
        self.assertEqual(remaining_balance_due(event_day, {}), date(2026, 11, 13))

    def test_a_saved_unquoted_price_stays_selected(self) -> None:
        html = _day_portion_options(
            {"pricing_model": "unquoted", "day_portion": "", "updated_by": "d@a.com"},
            show_unquoted=False,
        )
        self.assertIn('value="unquoted" selected', html)
        self.assertNotIn('value="" selected', html)


if __name__ == "__main__":
    unittest.main()
