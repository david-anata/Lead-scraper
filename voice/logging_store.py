"""Choose the call log writer.

`VOICE_DATABASE_URL` uses the limited Postgres role. Otherwise the service-role
REST client is used. Neither path runs on the audio loop: the callers hand
the work to a thread.
"""

from __future__ import annotations

from typing import Any

from voice.leads import HttpClient, SupabaseStore
from voice.postgres_log import PostgresLog
from voice.session import CallSession


class LoggingStore:
    """Postgres when configured, REST otherwise."""

    def __init__(
        self,
        *,
        database_url: str = "",
        supabase_url: str = "",
        service_key: str = "",
        owner: str = "building-operator",
        http: HttpClient | None = None,
        connect: Any = None,
    ) -> None:
        self.postgres = PostgresLog(database_url, owner=owner, connect=connect) if database_url else None
        self.rest = (
            SupabaseStore(supabase_url, service_key, http=http, owner=owner)
            if supabase_url and service_key
            else None
        )

    @property
    def configured(self) -> bool:
        return self.postgres is not None or (self.rest is not None and self.rest.configured)

    @property
    def http(self) -> Any:
        return self.rest.http if self.rest is not None else None

    @http.setter
    def http(self, value: Any) -> None:
        if self.rest is not None:
            self.rest.http = value

    def save_call(self, session: CallSession, *, sms_notified: bool) -> dict[str, str]:
        if self.postgres is not None:
            return self.postgres.save_call(session, sms_notified=sms_notified)
        if self.rest is not None:
            return self.rest.save_call(session, sms_notified=sms_notified)
        return {"call_log": "skipped", "inquiry_id": ""}

    def save_sms(self, **kwargs: Any) -> None:
        if self.postgres is not None:
            self.postgres.save_sms(**kwargs)
            return
        if self.rest is not None:
            self.rest.save_sms(**kwargs)
