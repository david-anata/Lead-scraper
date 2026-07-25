"""Tests for system compliance against the outbound briefs."""

from __future__ import annotations

import os
import unittest

import outbound_compliance as cp


class EnvIsolated(unittest.TestCase):
    def setUp(self):
        self._saved = {k: v for k, v in os.environ.items() if k.startswith("OUTBOUND_")}
        for k in list(self._saved):
            del os.environ[k]

    def tearDown(self):
        for k in [k for k in os.environ if k.startswith("OUTBOUND_")]:
            del os.environ[k]
        os.environ.update(self._saved)

    def _by_name(self, checks, name):
        return next(c for c in checks if c.name == name)


class KpiTests(EnvIsolated):
    def test_positive_rate_below_target_fails(self):
        checks = cp.compute_compliance(positive_rate=0.1, bounce_rate=1.0, connected=True)
        c = self._by_name(checks, "Positive reply rate (our #1 KPI)")
        self.assertEqual(c.status, cp.FAIL)
        self.assertIn("0.1%", c.detail)

    def test_positive_rate_at_target_passes(self):
        checks = cp.compute_compliance(positive_rate=1.5, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Positive reply rate (our #1 KPI)").status, cp.PASS)

    def test_target_is_tunable(self):
        os.environ["OUTBOUND_POSITIVE_REPLY_TARGET_PCT"] = "0.05"
        checks = cp.compute_compliance(positive_rate=0.1, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Positive reply rate (our #1 KPI)").status, cp.PASS)

    def test_not_connected_is_confirm_not_pass(self):
        checks = cp.compute_compliance(positive_rate=None, bounce_rate=None, connected=False)
        self.assertEqual(self._by_name(checks, "Positive reply rate (our #1 KPI)").status, cp.CONFIRM)


class GuardrailTests(EnvIsolated):
    def test_bounce_over_ceiling_fails(self):
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=5.0, connected=True)
        self.assertEqual(self._by_name(checks, "Bounce rate under control").status, cp.FAIL)

    def test_bounce_under_ceiling_passes(self):
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.9, connected=True)
        self.assertEqual(self._by_name(checks, "Bounce rate under control").status, cp.PASS)

    def test_mailbox_volume_over_safe_rate_fails(self):
        os.environ["OUTBOUND_EMAILS_PER_MAILBOX_PER_DAY"] = "60"
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Per-mailbox daily volume").status, cp.FAIL)

    def test_mailbox_volume_default_passes(self):
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Per-mailbox daily volume").status, cp.PASS)

    def test_sequence_over_three_fails(self):
        os.environ["OUTBOUND_SEQUENCE_EMAILS"] = "5"
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Sequence length").status, cp.FAIL)

    def test_sequence_two_passes(self):
        os.environ["OUTBOUND_SEQUENCE_EMAILS"] = "2"
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Sequence length").status, cp.PASS)


class ConfirmFlagTests(EnvIsolated):
    def test_unset_setting_is_confirm_never_pass(self):
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        for name in ("Open and click tracking off", "Warmup on for every mailbox",
                     "Copy reviewed against the playbook"):
            self.assertEqual(self._by_name(checks, name).status, cp.CONFIRM, name)

    def test_confirmed_setting_passes(self):
        os.environ["OUTBOUND_TRACKING_DISABLED"] = "true"
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Open and click tracking off").status, cp.PASS)

    def test_explicitly_false_setting_fails(self):
        os.environ["OUTBOUND_TRACKING_DISABLED"] = "false"
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        self.assertEqual(self._by_name(checks, "Open and click tracking off").status, cp.FAIL)


class CodeEnforcedTests(EnvIsolated):
    def test_code_enforced_rules_pass(self):
        checks = cp.compute_compliance(positive_rate=2.0, bounce_rate=1.0, connected=True)
        for name in ("Dropshippers and print-on-demand excluded",
                     "Never email the same brand twice", "List recycling window"):
            self.assertEqual(self._by_name(checks, name).status, cp.PASS, name)


class RenderTests(EnvIsolated):
    def test_render_lists_rules_and_verdict(self):
        checks = cp.compute_compliance(positive_rate=0.1, bounce_rate=1.0, connected=True)
        out = cp.render_compliance_html(checks)
        self.assertIn("System compliance", out)
        self.assertIn("Positive reply rate", out)
        self.assertIn("Fix this", out)

    def test_summary_counts(self):
        checks = cp.compute_compliance(positive_rate=0.1, bounce_rate=1.0, connected=True)
        s = cp.summarize(checks)
        self.assertGreaterEqual(s[cp.FAIL], 1)
        self.assertGreaterEqual(s[cp.CONFIRM], 1)
        self.assertGreaterEqual(s[cp.PASS], 1)


if __name__ == "__main__":
    unittest.main()
