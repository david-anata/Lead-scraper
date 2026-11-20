"""Twilio's published signature example, plus the media-stream token."""

import time

from voice.security import compute_signature, mint_stream_token, valid_signature, valid_stream_token


def test_twilio_documented_signature() -> None:
    url = "https://mycompany.com/myapp.php?foo=1&bar=2"
    params = [
        ("To", "+18005551212"),
        ("CallSid", "CA1234567890ABCDE"),
        ("From", "+14158675309"),
        ("Digits", "1234"),
        ("Caller", "+14158675309"),
    ]
    signature = compute_signature(url, params, "12345")
    assert signature == "RSOYDt4T1cUTdK1PDd93/VVr8B8="
    assert valid_signature(url, params, signature, "12345")
    assert not valid_signature(url, params, signature, "wrong")
    assert not valid_signature(url, params, "nope", "12345")


def test_stream_token_is_bound_to_the_call_and_expires() -> None:
    token = mint_stream_token("CA123", "message", "secret", ttl_seconds=100)
    assert valid_stream_token(token, "secret", now=time.time() + 10000)[0] is False
    ok, call_sid, resume = valid_stream_token(token, "secret")
    assert ok is True
    assert call_sid == "CA123"
    assert resume == "message"
    assert valid_stream_token(token, "other-secret")[0] is False
    assert valid_stream_token(token + "x", "secret")[0] is False
