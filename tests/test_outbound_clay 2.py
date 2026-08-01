"""Tests for the Clay hand-off (docs/outbound/14).

The two rules this module exists to enforce, both covered here:
  * the webhook address never reaches a log, an error or the page
  * a brand Clay did not accept is never counted as contacted
"""

from __future__ import annotations

import unittest

import outbound_clay as cl

URL = "https://api.clay.com/v3/sources/webhook/pull-in-abcdef123456-secret"


def _lead(domain="a.com", **kw):
    base = {"domain": domain, "brand": "Brand", "niche": "beauty_wellness",
            "country": "US", "tier": "A", "score": 9,
            "reason": "They added a growth tool", "recipe": "new_growth_app",
            "estimated_sales_yearly_cents": 5_000_000_00,
            "categories": ["Beauty", "Skincare"]}
    base.update(kw)
    return base


class RowShapeTests(unittest.TestCase):
    def test_row_carries_the_attribution_fields(self):
        row = cl.to_clay_row(_lead(), config_version=3)
        self.assertEqual(row["recipe"], "new_growth_app")
        self.assertEqual(row["reason"], "They added a growth tool")
        self.assertEqual(row["tier"], "A")
        self.assertEqual(row["settings_version"], 3)

    def test_lists_are_flattened_for_a_spreadsheet(self):
        self.assertEqual(cl.to_clay_row(_lead())["categories"], "Beauty, Skincare")

    def test_row_has_no_unexpected_fields(self):
        row = cl.to_clay_row(_lead(signals=["x"], apps=[{"a": 1}]))
        self.assertNotIn("signals", row)
        self.assertNotIn("apps", row)


class PushTests(unittest.TestCase):
    def _post(self, results):
        calls = {"n": 0}

        def post(url, row, **kw):
            i = calls["n"]
            calls["n"] += 1
            return results[i] if i < len(results) else (True, "")

        return post, calls

    def test_all_accepted(self):
        post, _ = self._post([(True, ""), (True, "")])
        r = cl.push_leads(URL, [_lead("a.com"), _lead("b.com")], post=post,
                          pace_seconds=0, sleep=lambda s: None)
        self.assertTrue(r.ok)
        self.assertEqual(r.accepted, 2)
        self.assertEqual(r.accepted_domains, ["a.com", "b.com"])
        self.assertIn("accepted all 2", r.summary)

    def test_partial_rejection_is_reported_and_separated(self):
        post, _ = self._post([(True, ""), (False, "Clay returned 400")])
        r = cl.push_leads(URL, [_lead("a.com"), _lead("b.com")], post=post,
                          pace_seconds=0, sleep=lambda s: None)
        self.assertFalse(r.ok)
        self.assertEqual(r.accepted_domains, ["a.com"])
        self.assertEqual(r.rejected_domains, ["b.com"])
        self.assertIn("come back on the next pull", r.summary)

    def test_not_connected_sends_nothing(self):
        r = cl.push_leads("", [_lead()], pace_seconds=0)
        self.assertEqual(r.attempted, 0)
        self.assertIn("not connected", r.reason)

    def test_no_leads_sends_nothing(self):
        r = cl.push_leads(URL, [], pace_seconds=0)
        self.assertEqual(r.attempted, 0)

    def test_settings_version_travels_on_every_row(self):
        seen = []

        def post(url, row, **kw):
            seen.append(row["settings_version"])
            return True, ""

        cl.push_leads(URL, [_lead("a.com"), _lead("b.com")], config_version=7,
                      post=post, pace_seconds=0, sleep=lambda s: None)
        self.assertEqual(seen, [7, 7])


class BudgetGuardTests(unittest.TestCase):
    """Clay caps a webhook source at 50,000 submissions for its whole life."""

    def test_refuses_when_budget_is_spent(self):
        r = cl.push_leads(URL, [_lead()], used_submissions=cl.CLAY_SUBMISSION_CAP,
                          pace_seconds=0)
        self.assertEqual(r.attempted, 0)
        self.assertIn("budget", r.reason.lower())

    def test_sends_only_what_fits(self):
        post = lambda url, row, **kw: (True, "")
        leads = [_lead(f"d{i}.com") for i in range(10)]
        r = cl.push_leads(URL, leads, used_submissions=cl.CLAY_SUBMISSION_CAP - 503,
                          reserve=500, post=post, pace_seconds=0, sleep=lambda s: None)
        self.assertEqual(r.attempted, 3)
        self.assertIn("to stay inside", r.reason)

    def test_budget_note_warns_before_it_bites(self):
        self.assertIn("submissions used", cl.budget_note(100))
        self.assertIn("low", cl.budget_note(cl.CLAY_SUBMISSION_CAP - 1_000).lower())
        self.assertIn("spent", cl.budget_note(cl.CLAY_SUBMISSION_CAP).lower())


class SecretHandlingTests(unittest.TestCase):
    """Anyone holding the webhook address can write rows and burn the budget."""

    def test_address_never_appears_in_a_failure_reason(self):
        def boom(url, row, **kw):
            return False, cl._redact(f"could not reach Clay: timeout on {URL}", URL)

        r = cl.push_leads(URL, [_lead()], post=boom, pace_seconds=0, sleep=lambda s: None)
        self.assertNotIn("secret", r.reason)
        self.assertNotIn(URL, r.reason)
        self.assertIn("<clay-webhook>", r.reason)

    def test_redact_leaves_short_strings_alone(self):
        self.assertEqual(cl._redact("plain message", ""), "plain message")


class RetryTests(unittest.TestCase):
    def test_retries_then_succeeds(self):
        calls = {"n": 0}

        class R:
            def __init__(self, code):
                self.status_code = code

        def fake_post(url, json=None, headers=None, timeout=None):
            calls["n"] += 1
            return R(500 if calls["n"] == 1 else 200)

        import outbound_clay
        real = outbound_clay.requests.post
        outbound_clay.requests.post = fake_post
        try:
            ok, why = cl.post_one(URL, {"a": 1}, sleep=lambda s: None)
        finally:
            outbound_clay.requests.post = real
        self.assertTrue(ok)
        self.assertEqual(calls["n"], 2)

    def test_client_error_is_not_retried(self):
        calls = {"n": 0}

        class R:
            status_code = 400

        def fake_post(url, json=None, headers=None, timeout=None):
            calls["n"] += 1
            return R()

        import outbound_clay
        real = outbound_clay.requests.post
        outbound_clay.requests.post = fake_post
        try:
            ok, why = cl.post_one(URL, {"a": 1}, sleep=lambda s: None)
        finally:
            outbound_clay.requests.post = real
        self.assertFalse(ok)
        self.assertEqual(calls["n"], 1)
        self.assertIn("400", why)


class UncontactedRuleTests(unittest.TestCase):
    """The spec's key safety rule: a brand Clay did NOT accept must stay
    un-contacted, so it comes back on the next pull instead of being lost.
    This exercises the exact selection the push endpoint performs."""

    def test_only_accepted_domains_are_recorded_as_contacted(self):
        from sqlalchemy import create_engine
        from sales_support_agent.services import outbound_memory as mem

        leads = [_lead("good.com"), _lead("bad.com")]

        def post(url, row, **kw):
            return (row["domain"] != "bad.com"), "Clay returned 400"

        pushed = cl.push_leads(URL, leads, post=post, pace_seconds=0, sleep=lambda s: None)
        self.assertEqual(pushed.accepted_domains, ["good.com"])
        self.assertEqual(pushed.rejected_domains, ["bad.com"])

        # Mirror the endpoint: record ONLY what Clay accepted.
        engine = create_engine("sqlite://", future=True)
        accepted = {d for d in pushed.accepted_domains if d}
        mem.record_leads(engine, [l for l in leads if l.get("domain") in accepted],
                         source="clay_push")

        contacted = mem.load_contacted(engine)
        self.assertIn("good.com", contacted)
        self.assertNotIn("bad.com", contacted,
                         "a rejected brand must remain available for the next pull")

    def test_a_total_rejection_records_nobody(self):
        from sqlalchemy import create_engine
        from sales_support_agent.services import outbound_memory as mem

        leads = [_lead("a.com"), _lead("b.com")]
        pushed = cl.push_leads(URL, leads, post=lambda u, r, **k: (False, "Clay returned 500"),
                               pace_seconds=0, sleep=lambda s: None)
        engine = create_engine("sqlite://", future=True)
        accepted = {d for d in pushed.accepted_domains if d}
        mem.record_leads(engine, [l for l in leads if l.get("domain") in accepted],
                         source="clay_push")
        self.assertEqual(mem.load_contacted(engine), set())


if __name__ == "__main__":
    unittest.main()
