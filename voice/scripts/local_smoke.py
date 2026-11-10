"""Exercise the receptionist without placing a phone call.

From the repo root, with signature checks off:

    VOICE_SIGNATURE_VALIDATION=false PUBLIC_BASE_URL=http://localhost:8090 \
      XAI_API_KEY=local TWILIO_ACCOUNT_SID=ACLOCAL TWILIO_AUTH_TOKEN=local \
      python voice/scripts/local_smoke.py

A real call still needs a public URL and the Twilio webhooks in voice/README.md.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("VOICE_SIGNATURE_VALIDATION", "false")
os.environ.setdefault("PUBLIC_BASE_URL", "http://localhost:8090")
os.environ.setdefault("XAI_API_KEY", "local")
os.environ.setdefault("TWILIO_ACCOUNT_SID", "ACLOCAL")
os.environ.setdefault("TWILIO_AUTH_TOKEN", "local")
os.environ.setdefault("SMS_AUTOREPLY", "false")
os.environ.setdefault("RECORDING_ENABLED", "false")

from voice.main import create_app  # noqa: E402
from voice.screening import ScreenInput, decide  # noqa: E402
from voice.settings import Settings  # noqa: E402


def main() -> None:
    try:
        from fastapi.testclient import TestClient
    except ImportError as exc:
        raise SystemExit("Install httpx to run the local smoke check: pip install httpx") from exc

    app = create_app(Settings.from_env())
    client = TestClient(app)
    health = client.get("/health")
    inbound = client.post(
        "/voice/inbound",
        data={
            "CallSid": "CA-local",
            "From": "+14155550100",
            "To": "+15550001111",
            "CallStatus": "ringing",
        },
    )
    spam = decide(
        ScreenInput(
            category="lead",
            reason="SEO services to rank your website",
            wants_live_person=True,
            qualified_opportunity=True,
            caller_name="Pat",
            company="RankFirst",
        )
    )
    emergency = decide(
        ScreenInput(
            category="urgent",
            reason="Water leak in the lobby",
            is_building_emergency=True,
        )
    )
    print(f"health {health.status_code} calls_ready={health.json().get('calls_ready')}")
    print(f"inbound {inbound.status_code}")
    print(inbound.text)
    print(f"spam transfer={spam.allow_transfer} category={spam.category}")
    print(f"emergency transfer={emergency.allow_transfer} category={emergency.category}")
    if health.status_code != 200 or inbound.status_code != 200 or "/voice/media?token=" not in inbound.text:
        raise SystemExit(1)
    if spam.allow_transfer or not emergency.allow_transfer:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
