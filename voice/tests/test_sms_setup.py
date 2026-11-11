"""The one-shot texting setup targets the receptionist numbers and the verified service."""

from __future__ import annotations

from voice.sms_setup import RECEPTIONIST_NUMBERS, TEST_BODY, configure_texting
from voice.tests.test_store_and_app import _settings


class _Response:
    def __init__(self, status: int, payload: dict) -> None:
        self.status_code = status
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class _Http:
    def __init__(self, numbers: list[str], pool_status: int = 201, pool_message: str = "") -> None:
        self.numbers = numbers
        self.pool_status = pool_status
        self.pool_message = pool_message
        self.posts: list[tuple[str, dict]] = []

    def get(self, url: str, *, auth: tuple[str, str], timeout: int) -> _Response:
        assert auth[0] == "AC123"
        if url.startswith("https://messaging.twilio.com/v1/Services"):
            return _Response(
                200,
                {
                    "services": [
                        {"sid": "MG9ef20a00000000000000000000000000", "friendly_name": "Anata AI Receptionist"}
                    ],
                    "meta": {},
                },
            )
        if "IncomingPhoneNumbers.json" in url:
            return _Response(
                200,
                {
                    "incoming_phone_numbers": [
                        {"sid": f"PN{index:032d}", "phone_number": number}
                        for index, number in enumerate(self.numbers, start=1)
                    ]
                },
            )
        if "/Messages/" in url:
            return _Response(200, {"sid": "SM1", "status": "delivered", "error_code": None})
        raise AssertionError(url)

    def post(self, url: str, *, data: dict, auth: tuple[str, str], timeout: int) -> _Response:
        self.posts.append((url, data))
        if url.endswith("/PhoneNumbers"):
            return _Response(self.pool_status, {"code": 21710, "message": self.pool_message, "sid": "PN"})
        if url.endswith("/Messages.json"):
            assert data["Body"] == TEST_BODY
            assert data["MessagingServiceSid"].startswith("MG9ef20a")
            assert data["To"] == "+18015550100"
            return _Response(201, {"sid": "SM1", "status": "delivered", "error_code": None})
        assert data["SmsMethod"] == "POST"
        assert data["SmsUrl"].endswith("/voice/sms")
        return _Response(200, {"sms_url": data["SmsUrl"]})


def test_setup_adds_every_receptionist_number_and_points_sms_at_the_service() -> None:
    http = _Http(list(RECEPTIONIST_NUMBERS))
    report = configure_texting(
        _settings(notify_sms_number="+18015550100", public_base_url="https://anata-voice-receptionist.onrender.com"),
        http=http,
    )
    assert len(RECEPTIONIST_NUMBERS) == 13
    assert report["missing"] == []
    assert report["webhook"].endswith("/voice/sms")
    assert report["messaging_service_sid"].startswith("MG9ef20a")
    assert report["test_sms"]["status"] == "delivered"
    assert report["test_sms"]["error_code"] == ""
    pool = [item for item in report["numbers"] if item["pool"] == "added"]
    assert len(pool) == 13
    sms_posts = [data for url, data in http.posts if "SmsUrl" in data]
    assert len(sms_posts) == 13
    assert all(data["SmsUrl"].endswith("/voice/sms") for data in sms_posts)


def test_a_number_already_in_the_pool_is_not_a_failure() -> None:
    http = _Http(list(RECEPTIONIST_NUMBERS), pool_status=409, pool_message="already in the Messaging Service")
    report = configure_texting(
        _settings(notify_sms_number="+18015550100"),
        http=http,
    )
    assert {item["pool"] for item in report["numbers"]} == {"already"}
    assert report["missing"] == []


def test_a_missing_tracking_number_is_reported() -> None:
    http = _Http(["+13853363562"])
    report = configure_texting(_settings(notify_sms_number="+18015550100"), http=http)
    assert "+13853363562" not in report["missing"]
    assert len(report["missing"]) == 12
