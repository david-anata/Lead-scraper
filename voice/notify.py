"""Twilio REST calls: live-call update and the after-call text.

SMS to the notify number is attempted for every non-spam call. Delivery can
still fail until the Twilio number has A2P 10DLC registration. The failure is
logged as a status code and the call record keeps `sms_notified` false.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Protocol

from voice.session import CallSession
from voice.settings import Settings

logger = logging.getLogger(__name__)


class HttpClient(Protocol):
    def post(self, url: str, *, data: dict[str, str], auth: tuple[str, str], timeout: int) -> Any: ...


class TwilioRest:
    def __init__(self, settings: Settings, http: HttpClient | None = None) -> None:
        self.settings = settings
        self.http = http
        self._requests: Any = None

    @property
    def configured(self) -> bool:
        return bool(self.settings.twilio_account_sid and self.settings.twilio_auth_token)

    def update_call(self, call_sid: str, twiml: str) -> bool:
        if not self.configured or not call_sid:
            return False
        url = (
            "https://api.twilio.com/2010-04-01/Accounts/"
            f"{self.settings.twilio_account_sid}/Calls/{call_sid}.json"
        )
        response = self._post(url, {"Twiml": twiml})
        return response is not None and int(getattr(response, "status_code", 0)) < 300

    def send_sms(
        self,
        *,
        from_number: str,
        to_number: str,
        body: str,
        status_callback: str = "",
        pin_from: bool = False,
    ) -> str:
        """Return the MessageSid when Twilio accepts the message.

        A 2xx only means the message was queued. It is not delivery.
        The status callback is what may later set `sms_notified`.
        """

        if not self.configured or not to_number or not body.strip():
            return ""
        url = (
            "https://api.twilio.com/2010-04-01/Accounts/"
            f"{self.settings.twilio_account_sid}/Messages.json"
        )
        data: dict[str, str] = {"To": to_number, "Body": body[:1400]}
        if status_callback:
            data["StatusCallback"] = status_callback
            data["StatusCallbackMethod"] = "POST"
        service = self.settings.twilio_messaging_service_sid
        if pin_from and from_number and service:
            # Keep the reply on the number they texted, and on the A2P service
            # that is allowed to send. Omitting From lets the service pick.
            data["MessagingServiceSid"] = service
            data["From"] = from_number
        elif service and not pin_from:
            data["MessagingServiceSid"] = service
        elif from_number:
            data["From"] = from_number
        else:
            return ""
        response = self._post(url, data)
        if response is None or int(getattr(response, "status_code", 0)) >= 300:
            return ""
        payload = _json_dict(response)
        return str(payload.get("sid") or payload.get("Sid") or "accepted")

    def create_call(
        self,
        *,
        to_number: str,
        from_number: str,
        url: str,
        status_callback: str,
        amd_status_callback: str,
        timeout: int,
    ) -> str:
        """Dial the on-call phone. The caller is already waiting in a conference."""

        if not self.configured or not to_number or not from_number or not url:
            return ""
        endpoint = (
            "https://api.twilio.com/2010-04-01/Accounts/"
            f"{self.settings.twilio_account_sid}/Calls.json"
        )
        response = self._post(
            endpoint,
            {
                "To": to_number,
                "From": from_number,
                "Url": url,
                "Method": "POST",
                "Timeout": str(timeout),
                "StatusCallback": status_callback,
                "StatusCallbackMethod": "POST",
                "MachineDetection": "Enable",
                "AsyncAmd": "true",
                "AsyncAmdStatusCallback": amd_status_callback,
                "AsyncAmdStatusCallbackMethod": "POST",
            },
        )
        if response is None or int(getattr(response, "status_code", 0)) >= 300:
            return ""
        payload = _json_dict(response)
        return str(payload.get("sid") or payload.get("Sid") or "")

    def complete_call(self, call_sid: str) -> bool:
        """Hang up one leg. Used when voicemail answers or the caller gives up."""

        if not self.configured or not call_sid:
            return False
        url = (
            "https://api.twilio.com/2010-04-01/Accounts/"
            f"{self.settings.twilio_account_sid}/Calls/{call_sid}.json"
        )
        response = self._post(url, {"Status": "completed"})
        return response is not None and int(getattr(response, "status_code", 0)) < 300

    def _post(self, url: str, data: dict[str, str]) -> Any:
        client = self.http or self._client()
        try:
            response = client.post(
                url,
                data=data,
                auth=(self.settings.twilio_account_sid, self.settings.twilio_auth_token),
                timeout=15,
            )
        except Exception:
            logger.exception("Twilio request failed")
            return None
        status = int(getattr(response, "status_code", 0))
        if status >= 300:
            code = str(_json_dict(response).get("code") or "")
            logger.warning("Twilio returned status %s error %s", status, code)
        return response

    def _client(self) -> Any:
        if self._requests is None:
            import requests

            self._requests = requests
        return self._requests


def _json_dict(response: Any) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def should_text_about_call(category: str) -> bool:
    return category != "spam"


# One GSM segment. Longer texts get cut so David is not sent a second bubble.
SMS_SEGMENT = 160
_PLACEHOLDER_REASON = re.compile(r"^known [a-z0-9_ ]+ caller$")
_LABEL_PREFIX = re.compile(r"^(message|notes|reason)\s*:\s*", re.IGNORECASE)


def format_phone(value: str) -> str:
    """Format a US number as 801-319-3675. Other numbers stay readable."""

    digits = "".join(character for character in (value or "") if character.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    cleaned = " ".join((value or "").split())
    return cleaned or "unknown number"


def _who(name: str, number: str) -> str:
    display = format_phone(number)
    cleaned = " ".join((name or "").split())
    if cleaned:
        return f"{cleaned} ({display})"
    return display


def _sentences(text: str) -> list[str]:
    parts = re.split(r"[\n;]+|(?<=[.!?])\s+", text or "")
    found: list[str] = []
    for part in parts:
        cleaned = _LABEL_PREFIX.sub("", " ".join(part.split())).strip(" .")
        if cleaned:
            found.append(cleaned)
    return found


def _noise(sentence: str, name: str) -> bool:
    low = sentence.casefold().strip(" .")
    if not low or _PLACEHOLDER_REASON.match(low):
        return True
    if low in {"vip caller", "vip", "callback", "personal message"}:
        return True
    if "callback number" in low:
        return True
    if name and low in {name.casefold(), f"vip caller, {name.casefold()}"}:
        return True
    digits = "".join(character for character in sentence if character.isdigit())
    if digits and len(digits) >= 10 and len(sentence) <= len(digits) + 4:
        return True
    return False


def plain_reason(session: CallSession) -> str:
    """One spoken reason, without labels, ids, or the caller's name repeated."""

    name = session.known_caller_name or session.caller_name
    for raw in (session.summary, session.notes, session.reason):
        for sentence in _sentences(raw):
            if not _noise(sentence, name):
                return sentence[:1].upper() + sentence[1:]
    return "No details were captured"


def _needs_callback(session: CallSession) -> bool:
    if session.transfer_accepted or session.action == "transferred" or session.transfer_outcome == "answered":
        return bool(session.callback_needed)
    return session.callback_needed or session.action in {"needs_callback", "message", "transfer_no_answer_message"}


def _fit(who_part: str, reason: str, tail: str, limit: int = SMS_SEGMENT) -> str:
    reason = reason.rstrip(".")
    text = f"{who_part}{reason}. {tail}"
    if len(text) <= limit:
        return text
    overhead = len(who_part) + len(tail) + 2
    room = limit - overhead
    if room < 12:
        clipped = text[: limit - 1].rstrip()
        return clipped + "…"
    short = reason[:room].rstrip(" ,;:-")
    if " " in short:
        short = short.rsplit(" ", 1)[0]
    fitted = f"{who_part}{short}. {tail}"
    if len(fitted) <= limit:
        return fitted
    return fitted[: limit - 1].rstrip() + "…"


def call_summary_text(session: CallSession) -> str:
    """One plain sentence for David. No labels, source codes, or repeated facts."""

    name = session.known_caller_name or session.caller_name
    number = session.caller_number or session.phone
    who = _who(name, number)
    reason = plain_reason(session)
    if _needs_callback(session):
        return _fit(f"Missed call: {who}. ", reason, "Call back.")
    return _fit(f"{who} called about ", reason[:1].lower() + reason[1:] if reason else reason, "Handled.")


def sms_attention_text(session: CallSession) -> str:
    """One plain sentence for David about a text. Who, their number, and why."""

    name = session.known_caller_name or session.caller_name
    number = session.caller_number or session.phone
    reason = plain_reason(session).rstrip(".")
    text = f"Text from {_who(name, number)}: {reason}."
    if len(text) <= SMS_SEGMENT:
        return text
    return text[: SMS_SEGMENT - 1].rstrip() + "…"


def sms_forward_text(
    *,
    from_number: str,
    to_number: str,
    body: str,
    reply_sent: bool,
    category: str,
    source: str = "",
) -> str:
    """One plain sentence for an inbound text. The source code is not included."""

    del to_number, reply_sent, source
    number = format_phone(from_number)
    if category == "spam":
        return f"Sales text from {number}."
    message = " ".join((body or "").split()) or "Empty text"
    text = f"Text from {number}: {message}"
    if len(text) <= SMS_SEGMENT:
        return text
    return text[: SMS_SEGMENT - 1].rstrip() + "…"
