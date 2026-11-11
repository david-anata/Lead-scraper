"""Twilio REST calls: live-call update and the after-call text.

SMS to the notify number is attempted for every non-spam call. Delivery can
still fail until the Twilio number has A2P 10DLC registration. The failure is
logged as a status code and the call record keeps `sms_notified` false.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from voice.screening import one_line
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
        if self.settings.twilio_messaging_service_sid:
            data["MessagingServiceSid"] = self.settings.twilio_messaging_service_sid
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


def call_summary_text(session: CallSession) -> str:
    lead = session.lead_fields()
    bits = [
        value
        for value in (
            lead["name"],
            lead["company"],
            lead["business_line"],
            lead["marketplaces"],
            lead["revenue_range"],
            lead["email"],
            lead["event_date"],
            f"{lead['guest_count']} guests" if lead["guest_count"] else "",
            lead["type"],
        )
        if value
    ]
    callback = "yes" if session.callback_needed else "no"
    lines = [
        f"Anata {session.line.label} call",
        f"From: {session.caller_number or 'unknown'}",
    ]
    if lead.get("source"):
        lines.append(f"Source: {one_line(lead['source'], 80)}")
    lines.extend([
        f"Reason: {one_line(session.reason or session.summary or session.category, 160)}",
        f"Callback: {callback}",
        f"Action: {session.action or 'message'}",
    ])
    if session.security_preference:
        lines.append(f"Security preference: {session.security_preference}")
    if bits:
        lines.append("Lead: " + one_line(" / ".join(bits), 300))
    if session.notes:
        lines.append("Notes: " + one_line(session.notes, 240))
    return "\n".join(lines)[:1000]


def sms_forward_text(
    *,
    from_number: str,
    to_number: str,
    body: str,
    reply_sent: bool,
    category: str,
    source: str = "",
) -> str:
    prefix = "Possible solicitation.\n" if category == "spam" else ""
    reply = "yes" if reply_sent else "no"
    source_line = f"Source: {one_line(source, 80)}\n" if source.strip() else ""
    return (
        f"{prefix}Anata SMS\n"
        f"From: {from_number or 'unknown'}\n"
        f"To: {to_number or 'unknown'}\n"
        f"{source_line}"
        f"Reply sent: {reply}\n"
        f"They wrote: {one_line(body, 300)}"
    )[:1000]
