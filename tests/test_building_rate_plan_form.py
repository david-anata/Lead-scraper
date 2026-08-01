"""The rate-plan form and its handler must not drift apart.

The form was rewritten in plain English on 2026-07-31: dollars instead of
cents, one-per-line instead of JSON, and a generated reference instead of an
invented id. A renamed input that the handler does not accept fails silently,
saving a default instead of what was typed, which is exactly the class of bug
that nearly priced The Arena at four cents.
"""

from __future__ import annotations

import inspect
import re
import unittest
from pathlib import Path

from sales_support_agent.api.building_crm_router import save_rate_plan_from_control_room
from sales_support_agent.services.building_money import dollars_to_cents

PAGE = Path(__file__).resolve().parents[1] / "sales_support_agent" / "services" / "building_page.py"


def _rate_plan_form_fields() -> set[str]:
    source = PAGE.read_text(encoding="utf-8")
    start = source.index('action="/admin/building/rate-plans"')
    end = source.index("</form>", start)
    block = source[start:end]
    return {
        name
        for name in re.findall(r'name="([a-z0-9_]+)"', block)
        if name != "_csrf_token"
    }


class RatePlanFormContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fields = _rate_plan_form_fields()
        self.accepted = set(inspect.signature(save_rate_plan_from_control_room).parameters)

    def test_every_form_field_is_accepted_by_the_handler(self) -> None:
        orphans = sorted(self.fields - self.accepted)
        self.assertEqual(orphans, [], f"form posts fields the handler ignores: {orphans}")

    def test_the_price_is_asked_for_in_dollars(self) -> None:
        self.assertIn("unit_amount", self.fields)
        self.assertNotIn(
            "unit_amount_cents",
            self.fields,
            "the visible form must never ask an operator for cents",
        )

    def test_the_deposit_is_asked_for_in_dollars(self) -> None:
        self.assertIn("deposit_amount", self.fields)
        self.assertNotIn("deposit_amount_cents", self.fields)

    def test_addons_are_not_asked_for_as_json(self) -> None:
        self.assertIn("addons", self.fields)
        self.assertNotIn("addons_json", self.fields)

    def test_the_handler_still_accepts_the_old_field_names(self) -> None:
        """Existing callers and tests post cents and JSON. They must keep working."""
        for legacy in ("unit_amount_cents", "deposit_amount_cents", "addons_json"):
            self.assertIn(legacy, self.accepted)

    def test_the_operator_is_not_required_to_invent_a_reference(self) -> None:
        param = inspect.signature(save_rate_plan_from_control_room).parameters["rate_plan_id"]
        self.assertEqual(
            param.default.default,
            "",
            "rate_plan_id must default to blank so it can be generated",
        )


class ArenaPricingTests(unittest.TestCase):
    """The figures David would actually type for The Arena."""

    def test_the_arena_price_list(self) -> None:
        self.assertEqual(dollars_to_cents("175"), 17500)  # per hour
        self.assertEqual(dollars_to_cents("250"), 25000)  # cleaning
        self.assertEqual(dollars_to_cents("500"), 50000)  # security deposit
        self.assertEqual(dollars_to_cents("1,050"), 105000)  # six hour minimum

    def test_the_four_cent_mistake_is_now_four_dollars(self) -> None:
        """Typing 4 in the price box means $4.00, which is visibly wrong, not silently wrong."""
        self.assertEqual(dollars_to_cents("4"), 400)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
