"""The Arena agreement's dynamic portions must resolve, and state the discount.

David approved the agreement on 2026-07-31 on two conditions: that it follows
the policies published on anatabuilding.com, and that its dynamic portions are
viable. These tests hold the second condition: every merge token the document
uses must be supported and must render a real value, never a gap.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from sales_support_agent.services.building_contract_templates import (
    EVENT_MERGE_FIELDS,
    MERGE_FIELD_HELP,
    format_merge_value,
)

DOCUMENT = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "building"
    / "agreements"
    / "arena-event-agreement-business-terms-v2.md"
)
TOKEN_RE = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")


class ArenaAgreementDynamicFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.body = DOCUMENT.read_text(encoding="utf-8")
        self.tokens = sorted(set(TOKEN_RE.findall(self.body)))

    def test_every_token_in_the_document_is_a_supported_field(self) -> None:
        """A token with no field behind it renders as literal braces to a customer."""
        unsupported = [t for t in self.tokens if t not in set(EVENT_MERGE_FIELDS)]
        self.assertEqual(unsupported, [], f"unsupported merge tokens: {unsupported}")

    def test_every_supported_field_is_documented(self) -> None:
        missing = [f for f in EVENT_MERGE_FIELDS if f not in MERGE_FIELD_HELP]
        self.assertEqual(missing, [], f"merge fields with no help text: {missing}")

    def test_the_agreement_states_the_discount(self) -> None:
        """David's requirement: a discount must appear on the contract, not only the quote."""
        for token in ("subtotal_before_discount", "discount_amount", "discount_reason"):
            self.assertIn(token, self.tokens, f"{token} missing from the agreement")

    def test_a_discounted_booking_renders_every_field(self) -> None:
        """No token may render as a gap on a complete booking."""
        values = {
            "customer_name": "Rosalind Ferro",
            "customer_email": "rosalind@ferro.example",
            "event_space": "The Arena",
            "setup_starts_at": "2026-09-30T15:00:00+00:00",
            "guest_starts_at": "2026-09-30T17:00:00+00:00",
            "guest_ends_at": "2026-09-30T23:00:00+00:00",
            "teardown_ends_at": "2026-10-01T01:00:00+00:00",
            "attendance": 120,
            "subtotal_before_discount": 620000,
            "discount_amount": 70000,
            "discount_reason": "Repeat customer, third booking this year",
            "quote_total": 550000,
            "currency": "USD",
            "deposit_amount": 275000,
            "deposit_type": "percent",
            "cancellation_policy": "Non-refundable inside 14 days.",
            "tax_terms": {"status": "non_taxable", "rate_bps": 0, "note": "Owner decision"},
            "included": ["Tables", "Chairs", "Stage"],
            "addons": ["Setup and reset"],
        }
        rendered = self.body
        for token in self.tokens:
            text = format_merge_value(token, values[token])
            self.assertNotEqual(text, "[not provided]", f"{token} rendered as a gap")
            rendered = rendered.replace("{{" + token + "}}", text)

        self.assertNotIn("{{", rendered, "a token survived the merge")
        self.assertIn("Rosalind Ferro", rendered)
        self.assertIn("6,200.00", rendered)  # subtotal
        self.assertIn("700.00", rendered)  # discount
        self.assertIn("5,500.00", rendered)  # total after discount
        self.assertIn("Repeat customer, third booking this year", rendered)

    def test_an_undiscounted_booking_reads_cleanly(self) -> None:
        """Zero must render as 0.00, not as a gap, so the clause still makes sense."""
        self.assertEqual(format_merge_value("discount_amount", 0), "0.00")
        self.assertEqual(format_merge_value("subtotal_before_discount", 550000), "5,500.00")
        # An empty reason is the one acceptable gap: there is no discount to explain.
        self.assertEqual(format_merge_value("discount_reason", ""), "[not provided]")

    def test_money_fields_render_as_money(self) -> None:
        self.assertEqual(format_merge_value("discount_amount", 70000), "700.00")
        self.assertEqual(format_merge_value("quote_total", 550000), "5,500.00")


CONTRACT = DOCUMENT.parent / "arena-event-agreement-v1.md"


class CustomerContractTests(unittest.TestCase):
    """The document a customer actually signs.

    The schedule beside it is written for a reviewer. This one is written for
    the person paying, and David approves it as owner.
    """

    def setUp(self) -> None:
        self.body = CONTRACT.read_text(encoding="utf-8")
        self.flat = re.sub(r"\s+", " ", self.body)
        self.tokens = sorted(set(TOKEN_RE.findall(self.body)))

    def test_every_token_is_a_supported_field(self) -> None:
        unsupported = [t for t in self.tokens if t not in set(EVENT_MERGE_FIELDS)]
        self.assertEqual(unsupported, [], f"unsupported merge tokens: {unsupported}")

    def test_it_states_the_discount(self) -> None:
        for token in ("subtotal_before_discount", "discount_amount", "discount_reason"):
            self.assertIn(token, self.tokens)

    def test_nothing_renders_as_a_gap_on_a_real_booking(self) -> None:
        values = {
            "customer_name": "Rosalind Ferro",
            "customer_email": "rosalind@ferro.example",
            "event_space": "The Arena",
            "setup_starts_at": "2026-09-30T15:00:00+00:00",
            "guest_starts_at": "2026-09-30T17:00:00+00:00",
            "guest_ends_at": "2026-09-30T23:00:00+00:00",
            "teardown_ends_at": "2026-10-01T01:00:00+00:00",
            "attendance": 120,
            "subtotal_before_discount": 620000,
            "discount_amount": 70000,
            "discount_reason": "Repeat customer, third booking this year",
            "quote_total": 550000,
            "currency": "USD",
            "deposit_amount": 275000,
            "deposit_type": "percent",
            "cancellation_policy": "Non-refundable inside 14 days.",
            "tax_terms": {"status": "non_taxable", "rate_bps": 0, "note": "Owner decision"},
            "included": ["Tables", "Chairs", "Stage"],
            "addons": ["Setup and reset"],
        }
        rendered = self.body
        for token in self.tokens:
            text = format_merge_value(token, values[token])
            self.assertNotEqual(text, "[not provided]", f"{token} rendered as a gap")
            rendered = rendered.replace("{{" + token + "}}", text)
        self.assertNotIn("{{", rendered)
        self.assertIn("5,500.00", rendered)

    def test_it_matches_the_published_policies(self) -> None:
        for needle in (
            "$175 per full hour",
            "$250 cleaning fee",
            "$500 refundable security deposit",
            "seven days before your event",
            "$250 up to 75 guests",
            "$75 per hour",
            "$125 per hour",
            "20% rush fee",
            "$1 million per occurrence",
            "at least 14 days before your event",
            "Cooking and kitchen use are not allowed",
        ):
            self.assertIn(needle, self.flat, f"published term missing: {needle}")

    def test_cancellation_terms_are_strict_without_a_graduated_refund_schedule(self) -> None:
        self.assertIn("all payments are non-refundable", self.flat.lower())
        self.assertIn("sole discretion", self.flat.lower())
        self.assertNotIn("30 or more days before", self.flat.lower())
        self.assertNotIn("14 to 29 days before", self.flat.lower())
        self.assertNotIn("half the remaining balance", self.flat.lower())

    def test_it_reads_as_a_contract_not_a_review_schedule(self) -> None:
        self.assertIn("Signature:", self.body)
        self.assertIn("governed by the laws of the State of Utah", self.flat)
        self.assertNotIn("prepared for legal review", self.flat.lower())
        self.assertNotIn("must not be sent to a customer", self.flat.lower())

    def test_no_em_dashes(self) -> None:
        """David's standing rule for anything customer-facing."""
        self.assertNotIn("—", self.body)


class DiscountResolverTests(unittest.TestCase):
    """The contract's discount must come off the quote, not a second source."""

    class _Quote:
        def __init__(self, amount_cents: int, line_items: list) -> None:
            self.amount_cents = amount_cents
            self.line_items_json = line_items

    def test_discount_is_read_from_the_quote_line_item(self) -> None:
        from sales_support_agent.services.building_contracts import _discount_terms

        terms = _discount_terms(
            self._Quote(
                550000,
                [
                    {"type": "package", "description": "Event package", "amount_cents": 620000},
                    {
                        "type": "discount",
                        "description": "Repeat customer, third booking this year",
                        "amount_cents": -70000,
                    },
                ],
            )
        )
        self.assertEqual(terms["subtotal_before_discount"], 620000)
        self.assertEqual(terms["discount_amount"], 70000)
        self.assertEqual(terms["discount_reason"], "Repeat customer, third booking this year")

    def test_a_booking_with_no_discount_reads_zero(self) -> None:
        from sales_support_agent.services.building_contracts import _discount_terms

        terms = _discount_terms(
            self._Quote(620000, [{"type": "package", "amount_cents": 620000}])
        )
        self.assertEqual(terms["subtotal_before_discount"], 620000)
        self.assertEqual(terms["discount_amount"], 0)
        self.assertEqual(terms["discount_reason"], "")

    def test_sales_tax_is_not_mislabeled_as_pre_discount_subtotal(self) -> None:
        from sales_support_agent.services.building_contracts import _discount_terms

        terms = _discount_terms(
            self._Quote(
                139685,
                [
                    {"type": "base", "amount_cents": 105000},
                    {"type": "fee", "amount_cents": 25000},
                    {"type": "tax", "amount_cents": 9685},
                ],
            )
        )
        self.assertEqual(terms["subtotal_before_discount"], 130000)

    def test_the_subtotal_always_reconciles(self) -> None:
        """subtotal - discount must equal the quote total, or the contract lies."""
        from sales_support_agent.services.building_contracts import _discount_terms

        for total, discount in ((550000, 70000), (620000, 0), (100, 99900)):
            quote = self._Quote(
                total,
                [{"type": "discount", "description": "x", "amount_cents": -discount}],
            )
            terms = _discount_terms(quote)
            self.assertEqual(
                terms["subtotal_before_discount"] - terms["discount_amount"],
                total,
            )


class ArenaAgreementWebsiteAlignmentTests(unittest.TestCase):
    """David's first condition: the agreement must follow the published policies.

    These pin the commercial figures that appear on
    anatabuilding.com/events/the-arena/policies. If someone changes one side
    without the other, this fails and names the number.
    """

    PUBLISHED = {
        "$175 per paid venue hour": "$175 per paid venue hour",
        "six-hour minimum": "six-hour minimum",
        "$250 routine cleaning": "$250 routine cleaning",
        "50% booking deposit": "50% of venue rental",
        "$500 refundable security deposit": "$500 refundable security deposit",
        "balance due seven days before": "seven days before",
        "setup and reset tiers": "$250 for up to 75 guests",
        "A/V technician rate": "$75 per hour",
        "event labor rate": "$125 per hour",
        "rush fee": "20% rush fee",
        "insurance floor": "$1 million per occurrence",
        "transfer window": "at least 14 days",
        "cooking prohibited": "cooking and kitchen use are prohibited",
    }

    def setUp(self) -> None:
        # Markdown wraps at 80 columns, so a term can straddle a line break.
        # Compare on normalised whitespace or these checks fail on formatting.
        self.body = re.sub(r"\s+", " ", DOCUMENT.read_text(encoding="utf-8"))

    def test_published_commercial_terms_appear_in_the_agreement(self) -> None:
        for label, needle in self.PUBLISHED.items():
            self.assertIn(needle, self.body, f"published term missing: {label}")

    def test_the_agreement_does_not_permit_onsite_cooking(self) -> None:
        """The website allows assembly only. 'Prepared' read wider than that."""
        self.assertNotIn("assembled or prepared onsite", self.body)
        self.assertIn("assembled onsite", self.body)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
