"""Tests for the bottleneck/capacity math (docs/outbound/09 B1)."""

from __future__ import annotations

import unittest

import outbound_bottlenecks as bn


class ComputeTests(unittest.TestCase):
    def test_under_capacity_is_flagged(self):
        b = bn.compute_bottlenecks(
            emails_have=300, emails_need=600,
            members_have=2, members_need=5,
            clay_have=1000, clay_need=4000,
        )
        for row in b.rows:
            self.assertFalse(row.ok, row.stage)

    def test_biggest_is_worst_ratio(self):
        b = bn.compute_bottlenecks(
            emails_have=300, emails_need=600,     # 2x
            members_have=2, members_need=5,       # 2.5x
            clay_have=1000, clay_need=4000,       # 4x  <-- worst
        )
        self.assertEqual(b.biggest.stage, "Clay enrichment")
        self.assertIn("Clay enrichment", b.headline)

    def test_no_bottleneck_when_all_ok(self):
        b = bn.compute_bottlenecks(
            emails_have=1000, emails_need=600,
            members_have=5, members_need=2,
            clay_have=5000, clay_need=4000,
        )
        self.assertIsNone(b.biggest)
        self.assertIn("keeping up", b.headline)

    def test_unknown_never_wins_biggest(self):
        b = bn.compute_bottlenecks(
            emails_have=None, emails_need=600,     # unknown
            members_have=2, members_need=5,        # under, ratio 2.5
            clay_have=None, clay_need=4000,        # unknown
        )
        self.assertEqual(b.biggest.stage, "Reply capacity")

    def test_zero_have_but_need_is_infinite_shortfall(self):
        row = bn.BottleneckRow("x", 0, 10, "u")
        self.assertEqual(row.shortfall_ratio, float("inf"))
        self.assertFalse(row.ok)

    def test_headline_asks_for_numbers_when_missing(self):
        b = bn.compute_bottlenecks(
            emails_have=None, emails_need=600,
            members_have=5, members_need=2,
            clay_have=5000, clay_need=4000,
        )
        self.assertIn("Add your capacity numbers", b.headline)


class RenderTests(unittest.TestCase):
    def test_render_contains_rows_and_headline(self):
        b = bn.compute_bottlenecks(
            emails_have=300, emails_need=600,
            members_have=2, members_need=5,
            clay_have=1000, clay_need=4000,
        )
        out = bn.render_bottlenecks_html(b)
        self.assertIn("Capacity and bottlenecks", out)
        self.assertIn("Emails per day", out)
        self.assertIn("Clay enrichment", out)
        self.assertIn("Under target", out)


class EnvDerivationTests(unittest.TestCase):
    def setUp(self):
        # Clear any outbound env so defaults apply deterministically.
        import os
        self._saved = {k: v for k, v in os.environ.items() if k.startswith("OUTBOUND_")}
        for k in list(self._saved):
            del os.environ[k]

    def tearDown(self):
        import os
        for k in [k for k in os.environ if k.startswith("OUTBOUND_")]:
            del os.environ[k]
        os.environ.update(self._saved)

    def test_defaults_give_sensible_email_need(self):
        # 15 calls * 2000 epc / 5 days = 6000 emails/day need
        b = bn.get_bottlenecks(reply_rate_pct=0.4, emails_per_booked_call=None)
        emails = next(r for r in b.rows if r.stage == "Emails per day")
        self.assertEqual(emails.need, 6000)
        self.assertIsNone(emails.have)  # capacity not set -> unknown

    def test_live_epc_overrides_assumption(self):
        b = bn.get_bottlenecks(reply_rate_pct=0.4, emails_per_booked_call=1000)
        emails = next(r for r in b.rows if r.stage == "Emails per day")
        self.assertEqual(emails.need, 3000)  # 15*1000/5

    def test_members_default_to_two(self):
        b = bn.get_bottlenecks(reply_rate_pct=0.0, emails_per_booked_call=None)
        members = next(r for r in b.rows if r.stage == "Reply capacity")
        self.assertEqual(members.have, 2.0)


if __name__ == "__main__":
    unittest.main()
