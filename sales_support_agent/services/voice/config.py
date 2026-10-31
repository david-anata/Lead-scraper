"""Voice receptionist configuration, read from the environment only."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except ValueError:
        return default


DEFAULT_DISCLOSURE = (
    "Hi, thanks for calling {business}. I'm an AI assistant, and this call is "
    "recorded and transcribed so our team can follow up. You can ask for a "
    "person at any time. How can I help?"
)


@dataclass(frozen=True)
class VoiceConfig:
    enabled: bool
    business_name: str
    xai_api_key: str
    xai_realtime_url: str
    xai_voice_model: str
    xai_voice: str
    xai_summary_model: str
    twilio_account_sid: str
    twilio_auth_token: str
    twilio_phone_number: str
    transfer_number: str
    public_base_url: str
    google_client_id: str
    google_client_secret: str
    google_refresh_token: str
    tour_calendar_id: str
    tour_timezone: str
    tour_start_hour: int
    tour_end_hour: int
    tour_minutes: int
    tour_location: str
    summary_email_to: str
    building_facts: str
    disclosure_template: str

    @property
    def disclosure(self) -> str:
        return self.disclosure_template.format(business=self.business_name)

    @property
    def calendar_ready(self) -> bool:
        return bool(
            self.google_client_id
            and self.google_client_secret
            and self.google_refresh_token
            and self.tour_calendar_id
        )

    def missing_for_calls(self) -> list[str]:
        required = {
            "XAI_API_KEY": self.xai_api_key,
            "TWILIO_ACCOUNT_SID": self.twilio_account_sid,
            "TWILIO_AUTH_TOKEN": self.twilio_auth_token,
        }
        return [name for name, value in required.items() if not value]


def load_voice_config() -> VoiceConfig:
    env = os.getenv
    return VoiceConfig(
        enabled=_flag("VOICE_AGENT_ENABLED"),
        business_name=env("VOICE_BUSINESS_NAME", "").strip() or "the Anata Building",
        xai_api_key=env("XAI_API_KEY", "").strip(),
        xai_realtime_url=env("XAI_REALTIME_URL", "").strip() or "wss://api.x.ai/v1/realtime",
        xai_voice_model=env("XAI_VOICE_MODEL", "").strip() or "grok-voice-latest",
        xai_voice=env("XAI_VOICE_NAME", "").strip() or "eve",
        xai_summary_model=env("VOICE_SUMMARY_MODEL", "").strip() or "grok-4-latest",
        twilio_account_sid=env("TWILIO_ACCOUNT_SID", "").strip(),
        twilio_auth_token=env("TWILIO_AUTH_TOKEN", "").strip(),
        twilio_phone_number=env("TWILIO_PHONE_NUMBER", "").strip(),
        transfer_number=env("VOICE_TRANSFER_NUMBER", "").strip(),
        public_base_url=env("VOICE_PUBLIC_BASE_URL", "").strip().rstrip("/"),
        google_client_id=env("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
        google_client_secret=env("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
        google_refresh_token=env("VOICE_GOOGLE_REFRESH_TOKEN", "").strip(),
        tour_calendar_id=env("VOICE_TOUR_CALENDAR_ID", "").strip(),
        tour_timezone=env("VOICE_TOUR_TIMEZONE", "").strip() or "America/Denver",
        tour_start_hour=_int("VOICE_TOUR_START_HOUR", 10),
        tour_end_hour=_int("VOICE_TOUR_END_HOUR", 16),
        tour_minutes=_int("VOICE_TOUR_MINUTES", 30),
        tour_location=env("VOICE_TOUR_LOCATION", "").strip() or "Anata Building",
        summary_email_to=env("VOICE_SUMMARY_EMAIL_TO", "").strip(),
        building_facts=env("VOICE_BUILDING_FACTS", "").strip(),
        disclosure_template=env("VOICE_DISCLOSURE_TEXT", "").strip() or DEFAULT_DISCLOSURE,
    )
