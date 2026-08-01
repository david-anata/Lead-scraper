"""Building mail sends from the building inbox and copies David and Val."""

import unittest
from unittest import mock

from sales_support_agent.integrations.resend import ResendClient
from sales_support_agent.services.building_sender import (
    BUILDING_ALWAYS_CC,
    building_cc,
    building_from_address,
)


class BuildingSenderTests(unittest.TestCase):
    def test_the_building_inbox_is_the_sender(self):
        self.assertEqual(building_from_address(), "building@anatainc.com")

    def test_david_and_val_are_always_copied(self):
        self.assertEqual(building_cc(), ["david@anatainc.com", "val@anatainc.com"])
        self.assertEqual(len(BUILDING_ALWAYS_CC), 2)

    def test_a_recipient_is_not_also_copied(self):
        """Mail to David should not copy David as well."""
        self.assertEqual(building_cc(exclude=["David@anatainc.com"]), ["val@anatainc.com"])
        self.assertEqual(building_cc(exclude=BUILDING_ALWAYS_CC), [])


class ResendCcTests(unittest.TestCase):
    def _client(self):
        settings = mock.Mock(resend_api_key="key", resend_from="building@anatainc.com")
        return ResendClient(settings)

    def _payload(self, **kwargs):
        client = self._client()
        with mock.patch("sales_support_agent.integrations.resend.requests.post") as post:
            post.return_value = mock.Mock(
                status_code=200, json=lambda: {"id": "sent"}, raise_for_status=lambda: None
            )
            client.send_message(subject="s", text="t", **kwargs)
            return post.call_args.kwargs["json"]

    def test_copies_are_sent(self):
        payload = self._payload(to="customer@example.com", cc=building_cc())
        self.assertEqual(payload["cc"], ["david@anatainc.com", "val@anatainc.com"])

    def test_nobody_receives_the_same_message_twice(self):
        payload = self._payload(
            to="david@anatainc.com", cc=["david@anatainc.com", "val@anatainc.com"]
        )
        self.assertEqual(payload["cc"], ["val@anatainc.com"])

    def test_no_cc_key_when_there_is_nothing_to_copy(self):
        payload = self._payload(to="customer@example.com", cc=[])
        self.assertNotIn("cc", payload)

    def test_a_single_address_works_as_well_as_a_list(self):
        payload = self._payload(to="customer@example.com", cc="val@anatainc.com")
        self.assertEqual(payload["cc"], ["val@anatainc.com"])

    def test_blank_entries_are_dropped(self):
        payload = self._payload(to="customer@example.com", cc=["", "  ", "val@anatainc.com"])
        self.assertEqual(payload["cc"], ["val@anatainc.com"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
