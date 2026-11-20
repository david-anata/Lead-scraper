"""The limited Postgres logger never interpolates values into SQL."""

from __future__ import annotations

from voice.leads import build_call_row
from voice.lines import LineContext
from voice.logging_store import LoggingStore
from voice.postgres_log import PostgresLog, ensure_sslmode_require
from voice.session import CallSession
from voice.settings import Settings


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self._row: tuple[object, ...] | None = None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.conn.statements.append((sql, params))
        if sql.startswith("INSERT INTO public.building_inquiries"):
            self._row = None if self.conn.conflict else ((params or ("",))[0],)
        elif sql.startswith("SELECT id"):
            self._row = ("existing-inquiry",)
        else:
            self._row = None

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


class _Conn:
    def __init__(self, *, conflict: bool = False) -> None:
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []
        self.conflict = conflict
        self.committed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def _session(**overrides: object) -> CallSession:
    session = CallSession(
        call_sid="CA123",
        caller_number="+14155550100",
        dialed_number="+15550001111",
        line=LineContext(id="anata", label="Anata"),
        category="lead",
        caller_name="Jamie Lee",
        event_date="2026-12-01",
        event_or_interest_type="workshop",
        summary="Asked about a workshop.",
        action="message",
    )
    for key, value in overrides.items():
        setattr(session, key, value)
    return session


def test_sslmode_require_is_forced() -> None:
    assert ensure_sslmode_require("postgresql://voice_logger@pooler/db").endswith("?sslmode=require")
    upgraded = ensure_sslmode_require("postgresql://voice_logger@pooler/db?sslmode=disable")
    assert "sslmode=require" in upgraded
    assert "sslmode=disable" not in upgraded
    assert ensure_sslmode_require("host=pooler user=voice_logger").endswith("sslmode=require")


def test_lead_is_inserted_with_parameters_and_then_upserted() -> None:
    conn = _Conn()
    writer = PostgresLog(
        "postgresql://voice_logger:secret@pooler.example/postgres",
        connect=lambda dsn: conn if "sslmode=require" in dsn else (_ for _ in ()).throw(AssertionError(dsn)),
    )
    result = writer.save_call(_session(), sms_notified=True)
    assert conn.committed
    assert result["call_log"] == "saved"
    assert result["inquiry_id"]
    sql = " ".join(statement for statement, _ in conn.statements)
    assert "INSERT INTO public.building_inquiries" in sql
    assert "INSERT INTO public.call_log" in sql
    assert "ON CONFLICT (call_sid) DO UPDATE" in sql
    assert "+14155550100" not in sql
    assert "Jamie Lee" not in sql
    for _, params in conn.statements:
        assert params is not None
        assert all("%s" not in str(value) for value in params)
        joined = " ".join(str(value) for value in params)
        assert "+14155550100" in joined or "CA123" in joined


def test_a_repeated_inquiry_is_looked_up_instead_of_updated() -> None:
    conn = _Conn(conflict=True)
    writer = PostgresLog("postgresql://voice_logger@pooler/db", connect=lambda dsn: conn)
    result = writer.save_call(_session(), sms_notified=False)
    assert result["inquiry_id"] == "existing-inquiry"
    kinds = [sql.split()[0] for sql, _ in conn.statements]
    assert kinds == ["INSERT", "SELECT", "INSERT"]
    assert "DO UPDATE" not in conn.statements[0][0]
    assert conn.statements[1][1] == ("voice-call:CA123",)


def test_spam_does_not_insert_an_inquiry() -> None:
    conn = _Conn()
    writer = PostgresLog("postgresql://voice_logger@pooler/db", connect=lambda dsn: conn)
    session = _session(category="spam", action="logged_only")
    writer.save_call(session, sms_notified=False)
    tables = [sql for sql, _ in conn.statements]
    assert len(tables) == 1
    assert "call_log" in tables[0]
    assert "building_inquiries" not in tables[0]


def test_database_url_wins_over_the_rest_fallback() -> None:
    conn = _Conn()
    rest_calls: list[str] = []

    class _Rest:
        configured = True

        def save_call(self, session: CallSession, *, sms_notified: bool) -> dict[str, str]:
            rest_calls.append("call")
            return {"call_log": "saved", "inquiry_id": ""}

        def save_sms(self, **kwargs: object) -> None:
            rest_calls.append("sms")

    store = LoggingStore(
        database_url="postgresql://voice_logger@pooler/db",
        supabase_url="https://example.supabase.co",
        service_key="service-key",
        connect=lambda dsn: conn,
    )
    store.rest = _Rest()  # type: ignore[assignment]
    store.save_call(_session(category="spam"), sms_notified=False)
    store.save_sms(
        message_sid="SM1",
        caller_number="+14155550100",
        dialed_number="+15550001111",
        body="hello",
        category="other",
        summary="hello",
        sms_notified=True,
    )
    assert rest_calls == []
    assert any("call_log" in sql for sql, _ in conn.statements)


def test_full_transcript_and_summary_are_not_trimmed() -> None:
    session = _session()
    session.summary = "S" * 2500
    session.add_turn("Caller", "C" * 3000)
    session.add_turn("Anata", "The published half day is one thousand five hundred.")
    row = build_call_row(session, inquiry_id="", sms_notified=False)
    assert row["transcript_summary"] == "S" * 2500
    assert "C" * 3000 in row["transcript"]
    assert "Anata: The published half day" in row["transcript"]


def test_inquiry_failure_still_upserts_the_call_log(caplog) -> None:
    class _Cursor(_CursorBase):
        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            self.conn.statements.append((sql, params))
            if "building_inquiries" in sql and sql.startswith("INSERT"):
                raise RuntimeError("permission denied for table building_inquiries")
            self._row = None

    class _Conn(_ConnBase):
        def cursor(self) -> _Cursor:
            return _Cursor(self)

    conn = _Conn()
    writer = PostgresLog("postgresql://voice_logger@pooler/db", connect=lambda dsn: conn)
    with caplog.at_level("ERROR"):
        result = writer.save_call(_session(), sms_notified=False)
    assert result["call_log"] == "saved"
    assert result["inquiry_id"] == ""
    assert result["inquiry"] == "failed"
    assert any("call_log" in sql for sql, _ in conn.statements)
    assert any("Inquiry insert failed" in record.message for record in caplog.records)
    assert conn.committed


class _CursorBase:
    def __init__(self, conn: object) -> None:
        self.conn = conn
        self._row: tuple[object, ...] | None = None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        raise NotImplementedError

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def __enter__(self) -> "_CursorBase":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


class _ConnBase:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []
        self.committed = False

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_ConnBase":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def test_privilege_probe_requires_the_grants_not_just_the_url() -> None:
    class _Conn:
        def __init__(self, allowed: bool) -> None:
            self.allowed = allowed
            self.sql = ""

        def cursor(self) -> "_Conn":
            return self

        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            self.sql = sql
            assert params is None
            assert "has_table_privilege(current_user, 'public.call_log', 'SELECT')" in sql
            assert "has_column_privilege" in sql
            assert "transcript_summary" in sql
            assert "payload_json" in sql

        def fetchone(self) -> tuple[bool]:
            return (self.allowed,)

        def __enter__(self) -> "_Conn":
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    denied = _Conn(False)
    writer = PostgresLog("postgresql://voice_logger:secret@pooler/db", connect=lambda dsn: denied)
    assert writer.probe_privileges() is False
    assert writer.probe_status == "denied"
    allowed = _Conn(True)
    writer._connect = lambda dsn: allowed
    assert writer.probe_privileges() is True
    assert writer.probe_status == "ok"
    assert "secret" not in writer.probe_status


def test_sms_delivery_update_is_parameterized() -> None:
    conn = _Conn()
    writer = PostgresLog("postgresql://voice_logger@pooler/db", connect=lambda dsn: conn)
    writer.set_sms_notified("CA123", False)
    sql, params = conn.statements[-1]
    assert sql.startswith("UPDATE public.call_log")
    assert params is not None
    assert params[0] is False
    assert params[2] == "CA123"
    assert "CA123" not in sql


def test_logging_ready_accepts_either_path() -> None:
    base = dict(
        xai_api_key="k",
        xai_model="grok-voice-latest",
        xai_voice="eve",
        xai_realtime_url="wss://api.x.ai/v1/realtime",
        twilio_account_sid="AC",
        twilio_auth_token="token",
        twilio_messaging_service_sid="",
        forward_to_number="+14155550199",
        notify_sms_number="+14155550198",
        public_base_url="https://voice.example.com",
        render_external_url="",
        supabase_url="",
        supabase_service_key="",
        calendar_id="",
        google_service_account_json="",
        google_delegated_subject="",
        line_contexts_json="",
        known_caller_numbers="",
        lead_owner="building-operator",
        sms_autoreply=False,
        recording_enabled=False,
        validate_signatures=True,
        max_call_seconds=720,
    )
    assert Settings(**base).logging_ready is False
    assert "VOICE_DATABASE_URL" in Settings(**base).missing()
    direct = Settings(**base, voice_database_url="postgresql://voice_logger:secret@pooler/db")
    assert direct.logging_ready is True
    assert "VOICE_DATABASE_URL" not in direct.missing()
    assert "secret" not in " ".join(direct.missing())
    rest_values = dict(base)
    rest_values["supabase_url"] = "https://example.supabase.co"
    rest_values["supabase_service_key"] = "service-key"
    rest = Settings(**rest_values)
    assert rest.logging_ready is True
