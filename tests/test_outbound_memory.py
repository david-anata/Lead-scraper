"""Tests for the never-email-twice outbound memory (SQLite-backed here)."""

from __future__ import annotations

import unittest

from sqlalchemy import create_engine

from sales_support_agent.services import outbound_memory as m


class OutboundMemoryTests(unittest.TestCase):
    def _engine(self):
        return create_engine("sqlite://", future=True)  # in-memory, per-test

    def test_starts_empty(self):
        self.assertEqual(m.load_contacted(self._engine()), set())

    def test_record_normalizes_and_dedups(self):
        e = self._engine()
        n = m.record_contacted(e, ["A.com", "b.com ", " b.com", "", "x.com"])
        self.assertEqual(n, 3)
        self.assertEqual(m.load_contacted(e), {"a.com", "b.com", "x.com"})

    def test_record_is_idempotent(self):
        e = self._engine()
        m.record_contacted(e, ["a.com", "b.com"])
        m.record_contacted(e, ["a.com", "new.com"])  # overlap must not error/dupe
        self.assertEqual(m.load_contacted(e), {"a.com", "b.com", "new.com"})

    def test_exported_domains_excluded_on_next_pull(self):
        """The set load_contacted returns is exactly what the pipeline treats as
        already-contacted, so a re-export skips them."""
        e = self._engine()
        m.record_contacted(e, ["seen.com"])
        already = m.load_contacted(e)
        self.assertIn("seen.com", already)

    def test_load_fails_open_on_bad_engine(self):
        self.assertEqual(m.load_contacted(object()), set())  # no crash, empty set

    def test_record_fails_open_on_bad_engine(self):
        self.assertEqual(m.record_contacted(object(), ["a.com"]), 0)  # no crash


if __name__ == "__main__":
    unittest.main()
