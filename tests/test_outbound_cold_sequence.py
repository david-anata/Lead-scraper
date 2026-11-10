"""Cold sequences cannot contain a URL. Replies still can."""

import pytest

import abby
import outbound_cold_sequence as cold


def test_every_generated_cold_sequence_has_zero_urls():
    sequences = cold.known_cold_sequences()
    assert {name for name, _steps in sequences} >= {"reengage", "sales_org"}
    for name, steps in sequences:
        assert cold.check_cold_steps(steps) == [], name
        for step in steps:
            for chunk in (
                step.get("subject"),
                step.get("body"),
                *[
                    variant.get(field)
                    for variant in (step.get("variants") or [])
                    for field in ("subject", "body")
                ],
            ):
                text = str(chunk or "")
                assert "http://" not in text and "https://" not in text and "www." not in text.lower()


def test_a_url_in_a_cold_step_is_refused():
    bad = [{"subject": "hello", "body": "See https://example.com/book"}]
    assert cold.check_cold_steps(bad)
    with pytest.raises(ValueError):
        cold.require_no_cold_urls(bad)
    www = [{"subject": "www.example.com", "body": "Hi"}]
    assert cold.cold_urls(www[0]["subject"])
    assert cold.check_cold_steps([{"subject": "Hi", "body": "Write ada@brand.com"}]) == []


def test_abby_replies_can_still_include_the_booking_link():
    assert abby.BOOKING_LINK.startswith("https://")
    assert "http" not in "\n".join(
        step["body"] for _name, steps in cold.known_cold_sequences() for step in steps if "body" in step
    )
