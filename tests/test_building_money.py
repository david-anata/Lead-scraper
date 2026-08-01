"""Money must read the way an operator types it, or refuse clearly."""

import unittest

from sales_support_agent.services.building_money import (
    MoneyError,
    cents_to_dollars,
    dollars_to_cents,
    parse_lines,
    suggested_rate_plan_id,
)


class DollarsToCentsTests(unittest.TestCase):
    def test_the_arena_hourly_rate(self):
        """The case that started this: 175 must mean $175, not $1.75."""
        self.assertEqual(dollars_to_cents("175"), 17500)
        self.assertEqual(dollars_to_cents("175.00"), 17500)
        self.assertEqual(dollars_to_cents("$175"), 17500)

    def test_operators_type_symbols_and_separators(self):
        self.assertEqual(dollars_to_cents("$1,050.50"), 105050)
        self.assertEqual(dollars_to_cents(" 1,050 "), 105000)
        self.assertEqual(dollars_to_cents("$2,500.00"), 250000)

    def test_blank_is_zero(self):
        for blank in ("", "   ", None, "$"):
            self.assertEqual(dollars_to_cents(blank), 0, repr(blank))

    def test_cents_are_exact(self):
        """Float arithmetic rounds 17.55 the wrong way. Decimal does not."""
        self.assertEqual(dollars_to_cents("17.55"), 1755)
        self.assertEqual(dollars_to_cents("0.01"), 1)
        self.assertEqual(dollars_to_cents("0.10"), 10)

    def test_rejects_rather_than_guesses(self):
        for bad in ("abc", "175.005", "-5", "1/2"):
            with self.assertRaises(MoneyError, msg=bad):
                dollars_to_cents(bad)

    def test_catches_cents_pasted_into_a_dollars_field(self):
        """The old bug in reverse: 17500 dollars for an hourly room is a paste."""
        with self.assertRaises(MoneyError):
            dollars_to_cents("175000000")

    def test_error_text_tells_the_operator_what_to_do(self):
        with self.assertRaises(MoneyError) as caught:
            dollars_to_cents("abc")
        self.assertIn("175", str(caught.exception))


class CentsToDollarsTests(unittest.TestCase):
    def test_round_trips(self):
        for typed in ("175", "1,050.50", "0.01", "2500"):
            self.assertEqual(
                dollars_to_cents(cents_to_dollars(dollars_to_cents(typed))),
                dollars_to_cents(typed),
            )

    def test_renders_for_a_form_field(self):
        self.assertEqual(cents_to_dollars(17500), "175.00")
        self.assertEqual(cents_to_dollars(0), "0.00")
        self.assertEqual(cents_to_dollars(None), "0.00")


class ParseLinesTests(unittest.TestCase):
    def test_one_per_line_replaces_json(self):
        self.assertEqual(
            parse_lines("Setup and reset\nA/V technician\n\nExtra labour"),
            ["Setup and reset", "A/V technician", "Extra labour"],
        )

    def test_pasted_bullets_survive(self):
        self.assertEqual(parse_lines("- Tables\n- Chairs\n• Stage"), ["Tables", "Chairs", "Stage"])

    def test_blank_is_empty(self):
        self.assertEqual(parse_lines(""), [])
        self.assertEqual(parse_lines(None), [])


class SuggestedIdTests(unittest.TestCase):
    def test_operator_never_invents_an_id(self):
        self.assertEqual(suggested_rate_plan_id("arena-events", 1), "arena-events-v1")
        self.assertEqual(suggested_rate_plan_id("arena-events", 2), "arena-events-v2")

    def test_messy_offering_ids_still_produce_a_clean_id(self):
        self.assertEqual(suggested_rate_plan_id("Private Office 201", 1), "private-office-201-v1")
        self.assertEqual(suggested_rate_plan_id("", 1), "plan-v1")
        self.assertEqual(suggested_rate_plan_id("a//b", 1), "a-b-v1")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
