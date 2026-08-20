"""Deterministic ``Settings`` construction for tests.

``Settings`` is a frozen dataclass with 130+ fields and no defaults, so any test
that built one by hand broke every time a field was added. Three test files did
exactly that and had been failing at construction — silently removing coverage
from the stale-lead job, the mailbox sync, and the admin preload.

Build from :func:`load_settings` and override, so new fields are picked up
automatically and can never break a test again. Ambient environment is
neutralised for anything that would let a test reach a real provider.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from sales_support_agent.config import Settings, load_settings


#: Credentials and endpoints forced to inert values so a populated developer
#: environment can never make a test talk to a live provider or change results.
_NEUTRALISED: dict[str, Any] = {
    "clickup_api_token": "",
    "slack_bot_token": "",
    "slack_channel_id": "",
    "hubspot_api_token": "",
    "resend_api_key": "",
    "stripe_secret_key": "",
    "stripe_webhook_secret": "",
    "gmail_access_token": "",
    "gmail_refresh_token": "",
    "gmail_client_id": "",
    "gmail_client_secret": "",
    "gmail_mailbox_accounts": (),
    "internal_api_key": "",
    "google_oauth_client_id": "",
    "google_oauth_client_secret": "",
    "google_service_account_json": "",
    "admin_password": "",
}


def make_settings(**overrides: Any) -> Settings:
    """Return a ``Settings`` with inert credentials plus the given overrides.

    Every field not named in ``overrides`` keeps whatever :func:`load_settings`
    resolves, so adding a field to ``Settings`` never breaks a caller.
    """

    base = dataclasses.replace(load_settings(), **_NEUTRALISED)
    if not overrides:
        return base
    known = {field.name for field in dataclasses.fields(Settings)}
    unknown = sorted(set(overrides) - known)
    if unknown:
        raise TypeError(
            f"Unknown Settings field(s): {', '.join(unknown)}. "
            "Check the name against sales_support_agent.config.Settings."
        )
    return dataclasses.replace(base, **overrides)
