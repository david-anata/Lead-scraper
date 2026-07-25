"""Tests for per-signal efficacy join + render (docs/outbound/08 Part 3)."""

from __future__ import annotations

import unittest

import outbound_efficacy as ef


def _lead(domain, *signals):
    return {"domain": domain, "signals": list(signals)}


class ComputeTests(unittest.TestCase):
    def test_counts_sent_per_signal_without_outcomes(self):
        pushed = [
            _lead("a.com", "Runs Meta and Google ads"),
            _lead("b.com", "Runs Meta and Google ads", "Trending on social right now"),
        ]
        e = ef.compute_signal_efficacy(pushed, outcomes={})
        self.assertFalse(e.has_outcomes)
        meta = next(s for s in e.stats if s.signal == "Runs Meta and Google ads")
        self.assertEqual(meta.sent, 2)
        self.assertEqual(meta.positive, 0)
        self.assertIsNone(e.baseline_rate)

    def test_positive_rate_and_lift_with_outcomes(self):
        pushed = [
            _lead("a.com", "CRO app"),
            _lead("b.com", "CRO app"),
            _lead("c.com", "Ads"),
            _lead("d.com", "Ads"),
        ]
        # CRO converts 1/2 = 50%; Ads 0/2 = 0%; baseline 1/4 = 25%
        outcomes = {"a.com": {"positive": True}}
        e = ef.compute_signal_efficacy(pushed, outcomes)
        self.assertTrue(e.has_outcomes)
        self.assertEqual(e.baseline_rate, 25.0)
        cro = next(s for s in e.stats if s.signal == "CRO app")
        self.assertEqual(cro.positive_rate, 50.0)
        self.assertEqual(e.lift(cro), 2.0)  # 50 / 25

    def test_sorted_by_sent_desc(self):
        pushed = [_lead("a.com", "X"), _lead("b.com", "X"), _lead("c.com", "Y")]
        e = ef.compute_signal_efficacy(pushed, {})
        self.assertEqual(e.stats[0].signal, "X")

    def test_empty_pushed(self):
        e = ef.compute_signal_efficacy([], {})
        self.assertEqual(e.stats, [])


class RenderTests(unittest.TestCase):
    def test_empty_state(self):
        out = ef.render_efficacy_html(ef.compute_signal_efficacy([], {}))
        self.assertIn("fill in once", out.lower())

    def test_counts_render_with_waiting_note(self):
        pushed = [_lead("a.com", "Runs Meta and Google ads")]
        out = ef.render_efficacy_html(ef.compute_signal_efficacy(pushed, {}))
        self.assertIn("Runs Meta and Google ads", out)
        self.assertIn("waiting on replies", out)


if __name__ == "__main__":
    unittest.main()
