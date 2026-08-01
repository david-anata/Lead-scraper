"""The Tape link: stored against the brand, host-checked, and it reaches the CSV.

The host check is the point of most of these. This is the only field in the
whole pipeline where a person types a URL that later goes out inside an email,
so a link to somewhere that is not ours must not be storable at all.
"""
from __future__ import annotations

import unittest

from sqlalchemy import create_engine

import outbound_pipeline as op
from sales_support_agent.services import outbound_memory as om


def _engine():
    eng = create_engine("sqlite://")
    om.ensure_table(eng)
    om.record_leads(eng, [{"domain": "rho.co", "brand": "Rho", "tier": "A", "score": 90}],
                    source="test")
    return eng


class VideoUrlStoreTests(unittest.TestCase):
    def test_saves_a_tape_link(self):
        eng = _engine()
        self.assertTrue(om.set_video_url(eng, "rho.co", "https://tape.anatainc.com/share/abc123"))
        lead = om.load_leads(eng)[0]
        self.assertEqual(lead["video_url"], "https://tape.anatainc.com/share/abc123")

    def test_rejects_a_link_that_is_not_ours(self):
        eng = _engine()
        for bad in ("https://loom.com/share/abc",
                    "https://tape.anatainc.com.evil.test/share/abc",
                    "https://evil.test/?u=https://tape.anatainc.com/share/abc",
                    "javascript:alert(1)"):
            with self.subTest(bad=bad):
                self.assertFalse(om.set_video_url(eng, "rho.co", bad))
                self.assertEqual(om.load_leads(eng)[0]["video_url"], "")

    def test_empty_clears_it(self):
        eng = _engine()
        om.set_video_url(eng, "rho.co", "https://tape.anatainc.com/share/abc123")
        self.assertTrue(om.set_video_url(eng, "rho.co", ""))
        self.assertEqual(om.load_leads(eng)[0]["video_url"], "")

    def test_unknown_brand_is_not_created(self):
        self.assertFalse(om.set_video_url(_engine(), "nobody.example",
                                          "https://tape.anatainc.com/share/x"))

    def test_blank_domain_is_refused(self):
        self.assertFalse(om.set_video_url(_engine(), "  ", "https://tape.anatainc.com/share/x"))

    def test_survives_a_re_record_of_the_same_brand(self):
        """record_leads must not wipe a link that was pasted after sourcing."""
        eng = _engine()
        om.set_video_url(eng, "rho.co", "https://tape.anatainc.com/share/abc123")
        om.record_leads(eng, [{"domain": "rho.co", "brand": "Rho"}], source="test")
        self.assertEqual(om.load_leads(eng)[0]["video_url"],
                         "https://tape.anatainc.com/share/abc123")


class VideoUrlReachesClayTests(unittest.TestCase):
    def test_the_column_exists_and_carries_the_link(self):
        self.assertIn("video_url", op.CLAY_CSV_COLUMNS)
        csv_text = op.leads_to_csv([{"domain": "rho.co", "brand": "Rho",
                                     "video_url": "https://tape.anatainc.com/share/abc123"}])
        self.assertIn("https://tape.anatainc.com/share/abc123", csv_text)

    def test_a_brand_with_no_recording_exports_blank_not_missing(self):
        """Instantly drops an empty liquid field silently; a missing column breaks
        the Clay import mapping for every row."""
        csv_text = op.leads_to_csv([{"domain": "rho.co", "brand": "Rho"}])
        self.assertIn("video_url", csv_text.splitlines()[0])


if __name__ == "__main__":
    unittest.main()
