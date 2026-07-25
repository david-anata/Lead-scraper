"""Tests for HubSpot nurture enrollment (docs/outbound/09 B3), fake client."""

from __future__ import annotations

import unittest

from sales_support_agent.services import outbound_nurture as nur


class FakeClient:
    def __init__(self, *, configured=True, existing=None, raise_on=None):
        self._configured = configured
        self._existing = existing
        self._raise_on = raise_on
        self.created = []
        self.updated = []

    def is_configured(self):
        return self._configured

    def find_contact_by_email(self, email):
        return self._existing

    def create_contact(self, props):
        if self._raise_on == "create":
            raise RuntimeError("boom")
        self.created.append(props)
        return {"id": "new1", "properties": props}

    def update_contact(self, cid, props):
        if self._raise_on == "update":
            raise RuntimeError("boom")
        self.updated.append((cid, props))
        return {"id": cid, "properties": props}


class EnrollTests(unittest.TestCase):
    def test_creates_when_no_existing_contact(self):
        c = FakeClient(existing=None)
        r = nur.enroll_contact(c, email="Jane@Acme.com", outcome="follow_up", brand="Acme")
        self.assertTrue(r["ok"])
        self.assertEqual(r["action"], "created")
        self.assertEqual(len(c.created), 1)
        self.assertEqual(c.created[0]["email"], "jane@acme.com")
        self.assertEqual(c.created[0]["outbound_nurture_status"], "follow_up")
        self.assertEqual(c.created[0]["company"], "Acme")

    def test_updates_when_contact_exists(self):
        c = FakeClient(existing={"id": "c99"})
        r = nur.enroll_contact(c, email="jane@acme.com", outcome="no_show")
        self.assertTrue(r["ok"])
        self.assertEqual(r["action"], "updated")
        self.assertEqual(c.updated[0][0], "c99")

    def test_rejects_bad_outcome(self):
        c = FakeClient()
        r = nur.enroll_contact(c, email="a@b.com", outcome="booked")
        self.assertFalse(r["ok"])
        self.assertIn("Outcome", r["reason"])

    def test_rejects_bad_email(self):
        c = FakeClient()
        r = nur.enroll_contact(c, email="notanemail", outcome="follow_up")
        self.assertFalse(r["ok"])

    def test_not_connected(self):
        c = FakeClient(configured=False)
        r = nur.enroll_contact(c, email="a@b.com", outcome="follow_up")
        self.assertFalse(r["ok"])
        self.assertIn("not connected", r["reason"])

    def test_api_error_is_caught(self):
        c = FakeClient(existing=None, raise_on="create")
        r = nur.enroll_contact(c, email="a@b.com", outcome="follow_up")
        self.assertFalse(r["ok"])
        self.assertIn("HubSpot rejected", r["reason"])


if __name__ == "__main__":
    unittest.main()
