"""One-shot Twilio texting setup for the receptionist numbers.

Runs only when SMS_SETUP_RUN=1, using the Twilio credentials already on the
service. It does not print those credentials or the notify number. The report
is a single log line the operator can read after boot.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from voice.settings import Settings

logger = logging.getLogger(__name__)

SMS_WEBHOOK_PATH = "/voice/sms"
TEST_BODY = "Anata receptionist texting is live."
SERVICE_PREFIX = "MG9ef20a"
SERVICE_NAME = "Anata AI Receptionist"

# The 12 tracking numbers plus the test line. E.164.
RECEPTIONIST_NUMBERS = (
    "+13853363455",
    "+13853361778",
    "+13853363629",
    "+13853361379",
    "+13853363562",
    "+13853363972",
    "+13853363127",
    "+13853362440",
    "+13853362974",
    "+13853365070",
    "+13854551673",
    "+13854557391",
    "+13853343936",
)


def _digits(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _last10(value: str) -> str:
    digits = _digits(value)
    return digits[-10:]


def schedule_sms_setup(settings: Settings) -> None:
    """Configure texting after the process is listening. Failures stay in the log."""

    thread = threading.Thread(
        target=_run_and_log,
        args=(settings,),
        name="sms-setup",
        daemon=True,
    )
    thread.start()


def _run_and_log(settings: Settings) -> None:
    try:
        report = configure_texting(settings)
    except Exception:
        logger.exception("SMS setup failed")
        return
    logger.info("sms setup report %s", json.dumps(report, sort_keys=True))


def configure_texting(settings: Settings, http: Any | None = None) -> dict[str, Any]:
    """Add the receptionist numbers to the verified Messaging Service and text David once."""

    client = _Twilio(settings.twilio_account_sid, settings.twilio_auth_token, http)
    webhook = _webhook(settings)
    service = _find_service(client)
    owned = _incoming_numbers(client)
    by_last10 = {_last10(item["phone_number"]): item for item in owned}
    numbers: list[dict[str, Any]] = []
    missing: list[str] = []
    for e164 in RECEPTIONIST_NUMBERS:
        found = by_last10.get(_last10(e164))
        if found is None:
            missing.append(e164)
            numbers.append({"e164": e164, "found": False, "webhook": "", "pool": "missing"})
            continue
        webhook_result = _set_sms_webhook(client, found["sid"], webhook)
        pool_result = _add_to_service(client, service["sid"], found["sid"])
        numbers.append(
            {
                "e164": e164,
                "found": True,
                "webhook": webhook_result,
                "pool": pool_result["pool"],
                "error_code": pool_result.get("error_code", ""),
            }
        )
    test = _send_test(client, service["sid"], settings.notify_sms_number)
    return {
        "messaging_service_sid": service["sid"],
        "messaging_service_name": service["friendly_name"],
        "webhook": webhook,
        "numbers": numbers,
        "missing": missing,
        "test_sms": test,
    }


def _webhook(settings: Settings) -> str:
    base = settings.http_base().rstrip("/")
    if not base:
        raise RuntimeError("PUBLIC_BASE_URL is not set")
    return base + SMS_WEBHOOK_PATH


class _Twilio:
    def __init__(self, account_sid: str, token: str, http: Any | None) -> None:
        self.account_sid = account_sid
        self.token = token
        self.http = http

    def get(self, url: str) -> Any:
        if not url.startswith("https://"):
            url = "https://api.twilio.com" + url
        client = self.http or _requests()
        response = client.get(url, auth=(self.account_sid, self.token), timeout=20)
        return _json(response)

    def post(self, url: str, data: dict[str, str]) -> Any:
        client = self.http or _requests()
        response = client.post(url, data=data, auth=(self.account_sid, self.token), timeout=20)
        return response


def _requests() -> Any:
    import requests

    return requests


def _json(response: Any) -> dict[str, Any]:
    status = int(getattr(response, "status_code", 0))
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    if status >= 300:
        code = str(payload.get("code") or payload.get("error_code") or status)
        message = str(payload.get("message") or "")[:180]
        raise RuntimeError(f"twilio {code}: {message}")
    return payload


def _find_service(client: _Twilio) -> dict[str, str]:
    url = "https://messaging.twilio.com/v1/Services?PageSize=50"
    while url:
        payload = client.get(url)
        for service in payload.get("services") or []:
            sid = str(service.get("sid") or "")
            name = str(service.get("friendly_name") or "")
            if sid.startswith(SERVICE_PREFIX) or name == SERVICE_NAME:
                return {"sid": sid, "friendly_name": name}
        url = str((payload.get("meta") or {}).get("next_page_url") or "")
    raise RuntimeError("Anata AI Receptionist messaging service was not found")


def _incoming_numbers(client: _Twilio) -> list[dict[str, str]]:
    url = (
        "https://api.twilio.com/2010-04-01/Accounts/"
        f"{client.account_sid}/IncomingPhoneNumbers.json?PageSize=200"
    )
    found: list[dict[str, str]] = []
    while url:
        payload = client.get(url)
        for item in payload.get("incoming_phone_numbers") or []:
            found.append(
                {
                    "sid": str(item.get("sid") or ""),
                    "phone_number": str(item.get("phone_number") or ""),
                }
            )
        url = str(payload.get("next_page_uri") or "")
    return found


def _set_sms_webhook(client: _Twilio, number_sid: str, webhook: str) -> str:
    url = (
        "https://api.twilio.com/2010-04-01/Accounts/"
        f"{client.account_sid}/IncomingPhoneNumbers/{number_sid}.json"
    )
    response = client.post(url, {"SmsUrl": webhook, "SmsMethod": "POST"})
    payload = _json(response)
    return str(payload.get("sms_url") or webhook)


def _add_to_service(client: _Twilio, service_sid: str, number_sid: str) -> dict[str, str]:
    url = f"https://messaging.twilio.com/v1/Services/{service_sid}/PhoneNumbers"
    response = client.post(url, {"PhoneNumberSid": number_sid})
    status = int(getattr(response, "status_code", 0))
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    if status < 300:
        return {"pool": "added", "error_code": ""}
    code = str(payload.get("code") or status)
    message = str(payload.get("message") or "").casefold()
    if "already" in message:
        return {"pool": "already", "error_code": code}
    return {"pool": "error", "error_code": code}


def _send_test(client: _Twilio, service_sid: str, to_number: str) -> dict[str, str]:
    if not to_number:
        return {"sid": "", "status": "skipped", "error_code": "no_notify_number"}
    url = (
        "https://api.twilio.com/2010-04-01/Accounts/"
        f"{client.account_sid}/Messages.json"
    )
    response = client.post(
        url,
        {
            "MessagingServiceSid": service_sid,
            "To": to_number,
            "Body": TEST_BODY,
        },
    )
    try:
        payload = _json(response)
    except RuntimeError as exc:
        text = str(exc)
        code = text.split(":", 1)[0].replace("twilio ", "")
        return {"sid": "", "status": "failed", "error_code": code}
    message_sid = str(payload.get("sid") or "")
    status = str(payload.get("status") or "")
    error_code = str(payload.get("error_code") or "")
    for _ in range(8):
        if status in {"delivered", "undelivered", "failed"}:
            break
        time.sleep(3)
        fetched = client.get(
            "https://api.twilio.com/2010-04-01/Accounts/"
            f"{client.account_sid}/Messages/{message_sid}.json"
        )
        status = str(fetched.get("status") or status)
        error_code = str(fetched.get("error_code") or error_code or "")
    return {"sid": message_sid, "status": status, "error_code": error_code}
