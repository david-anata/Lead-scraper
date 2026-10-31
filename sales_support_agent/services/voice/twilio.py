"""Minimal Twilio helpers: webhook signature check, TwiML, stream token, call update."""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Mapping
from xml.sax.saxutils import escape, quoteattr

import requests

from sales_support_agent.services.voice.config import VoiceConfig


def compute_signature(auth_token: str, url: str, params: Mapping[str, str]) -> str:
    payload = url + "".join(f"{key}{params[key]}" for key in sorted(params))
    digest = hmac.new(auth_token.encode(), payload.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def valid_signature(auth_token: str, url: str, params: Mapping[str, str], signature: str) -> bool:
    if not auth_token or not signature:
        return False
    return hmac.compare_digest(compute_signature(auth_token, url, params), signature)


def stream_token(auth_token: str, call_sid: str, *, ttl_seconds: int = 120, now: float | None = None) -> str:
    expires = int((now or time.time()) + ttl_seconds)
    mac = hmac.new(auth_token.encode(), f"voice-stream:{call_sid}:{expires}".encode(), hashlib.sha256)
    return f"{expires}.{mac.hexdigest()}"


def valid_stream_token(auth_token: str, call_sid: str, token: str, *, now: float | None = None) -> bool:
    try:
        expires_raw, _ = token.split(".", 1)
        expires = int(expires_raw)
    except (AttributeError, ValueError):
        return False
    if expires < (now or time.time()):
        return False
    mac = hmac.new(auth_token.encode(), f"voice-stream:{call_sid}:{expires}".encode(), hashlib.sha256)
    return hmac.compare_digest(f"{expires}.{mac.hexdigest()}", token)


def connect_stream_twiml(stream_url: str, parameters: Mapping[str, str]) -> str:
    params = "".join(
        f"<Parameter name={quoteattr(name)} value={quoteattr(value)} />"
        for name, value in parameters.items()
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><Response><Connect>'
        f"<Stream url={quoteattr(stream_url)}>{params}</Stream>"
        "</Connect></Response>"
    )


def unavailable_twiml(message: str) -> str:
    return f'<?xml version="1.0" encoding="UTF-8"?><Response><Say>{escape(message)}</Say><Hangup/></Response>'


def transfer_twiml(number: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?><Response>'
        "<Say>Connecting you now.</Say>"
        f"<Dial>{escape(number)}</Dial></Response>"
    )


def transfer_call(config: VoiceConfig, call_sid: str) -> dict:
    """Redirect a live call to the human transfer number via the REST API."""
    if not config.transfer_number:
        return {"ok": False, "error": "transfer_number_not_configured"}
    if not (config.twilio_account_sid and config.twilio_auth_token):
        return {"ok": False, "error": "twilio_not_configured"}
    response = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{config.twilio_account_sid}/Calls/{call_sid}.json",
        data={"Twiml": transfer_twiml(config.transfer_number)},
        auth=(config.twilio_account_sid, config.twilio_auth_token),
        timeout=10,
    )
    if response.status_code >= 400:
        return {"ok": False, "error": f"twilio_http_{response.status_code}"}
    return {"ok": True}
