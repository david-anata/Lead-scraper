"""The Resend webhook must accept every endpoint that signs for this URL.

In production 99 of 112 deliveries in six hours were refused while 13 were
accepted, which is the signature of two configured Resend endpoints pointing at
one URL: each signs with its own secret, and the app knew only one of them.
Refusing them all as a bare 401 with nothing in the log made a configuration
problem look like background noise for weeks.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging

import pytest
from fastapi import HTTPException

from sales_support_agent.api.building_email_webhook_router import (
    verify_resend_webhook,
)

BODY = b'{"type":"email.delivered","data":{"email_id":"abc"}}'
EVENT_ID = "msg_2abc"
NOW = 1_760_000_000
OLD_SECRET = "whsec_" + base64.b64encode(b"the-render-endpoint").decode()
NEW_SECRET = "whsec_" + base64.b64encode(b"the-vercel-endpoint").decode()


def _sign(secret: str, *, timestamp: int = NOW) -> str:
    signed = b".".join((EVENT_ID.encode(), str(timestamp).encode(), BODY))
    digest = hmac.new(
        base64.b64decode(secret.removeprefix("whsec_")), signed, hashlib.sha256
    ).digest()
    return "v1," + base64.b64encode(digest).decode()


def _verify(secret, *, signed_by: str, timestamp: int = NOW):
    verify_resend_webhook(
        raw_body=BODY,
        event_id=EVENT_ID,
        timestamp=str(timestamp),
        signature_header=_sign(signed_by, timestamp=timestamp),
        secret=secret,
        now_seconds=NOW,
    )


def test_a_single_secret_still_works() -> None:
    """The ordinary case must not regress."""
    _verify(OLD_SECRET, signed_by=OLD_SECRET)


def test_either_configured_endpoint_is_accepted() -> None:
    """The production failure: one URL, two endpoints, two signing secrets."""
    both = (OLD_SECRET, NEW_SECRET)

    _verify(both, signed_by=OLD_SECRET)
    _verify(both, signed_by=NEW_SECRET)


def test_a_secret_nobody_configured_is_still_refused() -> None:
    """Accepting a list must not become accepting anything."""
    stranger = "whsec_" + base64.b64encode(b"not-ours").decode()

    with pytest.raises(HTTPException) as raised:
        _verify((OLD_SECRET, NEW_SECRET), signed_by=stranger)

    assert raised.value.status_code == 401


def test_a_replayed_delivery_is_still_refused() -> None:
    with pytest.raises(HTTPException) as raised:
        _verify(OLD_SECRET, signed_by=OLD_SECRET, timestamp=NOW - 601)

    assert raised.value.status_code == 401


def test_nothing_configured_says_so_rather_than_blaming_the_sender() -> None:
    """503 is the honest answer: the caller did nothing wrong."""
    with pytest.raises(HTTPException) as raised:
        _verify((), signed_by=OLD_SECRET)

    assert raised.value.status_code == 503


@pytest.mark.parametrize(
    "case, expected",
    [
        ("wrong_secret", "no_configured_secret_matched"),
        ("stale", "outside_five_minute_window"),
    ],
)
def test_every_refusal_says_why_in_the_log(caplog, case, expected) -> None:
    """Without this the two causes are indistinguishable, which is how a
    misconfigured endpoint went unnoticed."""
    stranger = "whsec_" + base64.b64encode(b"not-ours").decode()

    with caplog.at_level(logging.WARNING):
        with pytest.raises(HTTPException):
            if case == "wrong_secret":
                _verify(OLD_SECRET, signed_by=stranger)
            else:
                _verify(OLD_SECRET, signed_by=OLD_SECRET, timestamp=NOW - 601)

    assert expected in caplog.text
    assert EVENT_ID in caplog.text, "the log must name the delivery it refused"


def test_the_log_never_writes_down_the_secret_or_the_signature(caplog) -> None:
    stranger = "whsec_" + base64.b64encode(b"not-ours").decode()

    with caplog.at_level(logging.WARNING):
        with pytest.raises(HTTPException):
            _verify(OLD_SECRET, signed_by=stranger)

    assert OLD_SECRET not in caplog.text
    assert _sign(stranger) not in caplog.text


# --- reading the setting -----------------------------------------------------

def _request_with(configured: str):
    from types import SimpleNamespace

    return SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                settings=SimpleNamespace(resend_webhook_secret=configured)
            )
        )
    )


@pytest.mark.parametrize(
    "configured, expected",
    [
        (f"{OLD_SECRET}", (OLD_SECRET,)),
        (f"{OLD_SECRET},{NEW_SECRET}", (OLD_SECRET, NEW_SECRET)),
        (f" {OLD_SECRET} , {NEW_SECRET} ", (OLD_SECRET, NEW_SECRET)),
        (f"{OLD_SECRET} {NEW_SECRET}", (OLD_SECRET, NEW_SECRET)),
        (f"{OLD_SECRET},,{NEW_SECRET},", (OLD_SECRET, NEW_SECRET)),
    ],
)
def test_the_setting_can_hold_more_than_one_secret(configured, expected) -> None:
    """Pasting a second secret in should not need a code change, and a stray
    comma or space should not silently produce an empty secret that matches
    nothing."""
    from sales_support_agent.api.building_email_webhook_router import _webhook_secrets

    assert _webhook_secrets(_request_with(configured)) == expected


def test_an_unset_setting_reports_that_it_is_unconfigured() -> None:
    from sales_support_agent.api.building_email_webhook_router import _webhook_secrets

    with pytest.raises(HTTPException) as raised:
        _webhook_secrets(_request_with("   "))

    assert raised.value.status_code == 503
