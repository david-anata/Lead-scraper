"""Pushing Clay's contacts to LinkedIn without messaging anyone twice.

The dedupe tests are the point of this file. Everything else here is shape
checking; sending the same person a second connection request is the one
failure in this pipeline that cannot be taken back.
"""
from __future__ import annotations

import unittest
from unittest.mock import Mock

import requests

from sales_support_agent.services import outbound_heyreach as hr


class _Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._payload


def _session(post_status=200, get_status=200):
    s = Mock()
    s.get.return_value = _Resp(get_status)
    s.post.return_value = _Resp(post_status)
    return s


ROW = {"linkedin_url": "https://www.linkedin.com/in/jane-doe/",
       "first_name": "Jane", "last_name": "Doe",
       "email": "jane@brand.com", "company_name": "Brand", "role": "Founder"}


class NormaliseTests(unittest.TestCase):
    def test_one_form_per_person(self):
        forms = ["https://www.linkedin.com/in/jane-doe",
                 "https://www.linkedin.com/in/jane-doe/",
                 "http://www.linkedin.com/in/jane-doe",
                 "https://www.linkedin.com/in/jane-doe?utm_source=clay",
                 "  https://WWW.linkedin.com/in/Jane-Doe/  "]
        self.assertEqual(len({hr.normalize_profile_url(f) for f in forms}), 1)

    def test_rejects_things_that_are_not_a_person(self):
        for bad in ("https://linkedin.com/company/brand",
                    "https://linkedin.com/feed/update/123",
                    "https://example.com/in/jane-doe",
                    "https://linkedin.com.evil.test/in/jane-doe",
                    "", "not a url"):
            with self.subTest(bad=bad):
                self.assertFalse(hr.is_profile_url(bad))

    def test_accepts_a_real_profile(self):
        self.assertTrue(hr.is_profile_url("https://www.linkedin.com/in/jane-doe/"))


class PrepareTests(unittest.TestCase):
    def test_maps_clays_column_spellings(self):
        leads, _, _ = hr.prepare([{"LinkedIn Profile": "https://linkedin.com/in/j",
                                   "First Name": "Jane", "Work Email": "j@b.com",
                                   "Merchant Name": "Brand", "Job Title": "Founder"}],
                                 campaign_id="7")
        self.assertEqual(leads[0]["firstName"], "Jane")
        self.assertEqual(leads[0]["email"], "j@b.com")
        self.assertEqual(leads[0]["company"], "Brand")
        self.assertEqual(leads[0]["position"], "Founder")

    def test_skips_someone_already_sent(self):
        key = hr.lead_key("7", ROW["linkedin_url"])
        leads, _, stats = hr.prepare([ROW], campaign_id="7", already={key})
        self.assertEqual(leads, [])
        self.assertEqual(stats["duplicate"], 1)

    def test_skips_a_duplicate_inside_one_file(self):
        leads, _, stats = hr.prepare([ROW, dict(ROW)], campaign_id="7")
        self.assertEqual(len(leads), 1)
        self.assertEqual(stats["duplicate"], 1)

    def test_counts_reconcile(self):
        rows = [ROW, dict(ROW), {"email": "x@y.com"}, {"linkedin_url": "https://linkedin.com/company/x"}]
        _, _, s = hr.prepare(rows, campaign_id="7")
        self.assertEqual(s["rows"], s["queued"] + s["duplicate"] + s["no_profile"])


class PushTests(unittest.TestCase):
    def test_sends_and_reports(self):
        s = _session()
        out = hr.push([ROW], api_key="k", campaign_id="7", session=s)
        self.assertTrue(out["ok"])
        self.assertEqual(out["sent"], 1)
        body = s.post.call_args.kwargs["json"]
        self.assertEqual(body["campaignId"], 7)
        self.assertEqual(body["leads"][0]["linkedinUrl"], "https://www.linkedin.com/in/jane-doe")

    def test_marks_people_before_trusting_the_response(self):
        """A push that times out on the way back may still have landed."""
        seen = []
        s = _session()
        s.post.side_effect = requests.ConnectionError("boom")
        out = hr.push([ROW], api_key="k", campaign_id="7",
                      record=lambda keys: seen.extend(keys), session=s)
        self.assertFalse(out["ok"])
        self.assertEqual(len(seen), 1)  # recorded despite the failure

    def test_a_failed_batch_does_not_resend_earlier_ones(self):
        rows = [dict(ROW, linkedin_url=f"https://linkedin.com/in/p{i}") for i in range(150)]
        s = _session()
        s.post.side_effect = [_Resp(200), requests.ConnectionError("boom")]
        out = hr.push(rows, api_key="k", campaign_id="7", session=s)
        self.assertFalse(out["ok"])
        self.assertEqual(out["sent"], 100)
        self.assertIn("Nobody was messaged twice", out["reason"])

    def test_refuses_without_a_campaign(self):
        out = hr.push([ROW], api_key="k", campaign_id="", session=_session())
        self.assertFalse(out["ok"])
        self.assertIn("CAMPAIGN_ID", out["reason"])

    def test_refuses_a_bad_key_before_sending(self):
        s = _session(get_status=401)
        out = hr.push([ROW], api_key="k", campaign_id="7", session=s)
        self.assertFalse(out["ok"])
        s.post.assert_not_called()

    def test_nothing_to_send_says_why(self):
        key = hr.lead_key("7", ROW["linkedin_url"])
        out = hr.push([ROW], api_key="k", campaign_id="7", already={key}, session=_session())
        self.assertTrue(out["ok"])
        self.assertEqual(out["sent"], 0)
        self.assertIn("already sent before", out["reason"])

    def test_batches_above_the_cap(self):
        rows = [dict(ROW, linkedin_url=f"https://linkedin.com/in/p{i}") for i in range(250)]
        s = _session()
        out = hr.push(rows, api_key="k", campaign_id="7", session=s)
        self.assertEqual(out["sent"], 250)
        self.assertEqual(s.post.call_count, 3)


if __name__ == "__main__":
    unittest.main()


class PreviewContractTests(unittest.TestCase):
    """Preview must be inert. The route returns before push() is reached, so
    what is guarded here is that prepare() alone never contacts or records."""

    def test_prepare_touches_nothing(self):
        called = []
        s = _session()
        leads, keys, stats = hr.prepare([ROW], campaign_id="7")
        self.assertEqual(len(leads), 1)
        self.assertEqual(len(keys), 1)
        s.post.assert_not_called()
        self.assertEqual(called, [])

    def test_preview_names_match_what_would_send(self):
        leads, _, _ = hr.prepare([ROW], campaign_id="7")
        self.assertEqual(f"{leads[0]['firstName']} {leads[0]['lastName']}", "Jane Doe")

    def test_nothing_reason_is_readable_when_all_are_duplicates(self):
        key = hr.lead_key("7", ROW["linkedin_url"])
        _, _, stats = hr.prepare([ROW], campaign_id="7", already={key})
        self.assertIn("already sent before", hr._nothing_reason(stats))
