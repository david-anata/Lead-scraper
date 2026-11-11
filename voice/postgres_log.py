"""Direct Postgres writes for the limited `voice_logger` role.

Used when `VOICE_DATABASE_URL` is set. Statements are parameterized.
Connections are opened per write and closed immediately. Callers must not
run these methods on the audio event loop; `finalize_call` and the SMS
route already use a worker thread.

The role needs no sequences. `call_log.id` defaults to `gen_random_uuid()`,
and inquiry ids are generated in the app. See `voice/README.md` for the
column grants. This path writes `call_log` and `building_inquiries` only.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable

from voice.leads import build_call_row, build_inquiry_row, should_create_inquiry
from voice.session import CallSession

logger = logging.getLogger(__name__)

_SSL_RE = re.compile(r"(^|[?&\s])sslmode=[^&\s]+")

_CALL_COLUMNS = (
    "call_sid",
    "channel",
    "caller_number",
    "dialed_number",
    "line_id",
    "started_at",
    "ended_at",
    "duration_seconds",
    "transcript_summary",
    "transcript",
    "category",
    "lead_fields",
    "action_taken",
    "transfer_attempted",
    "transfer_outcome",
    "sms_notified",
    "building_inquiry_id",
    "updated_at",
)

_INQUIRY_COLUMNS = (
    "id",
    "idempotency_key",
    "kind",
    "source",
    "source_reference",
    "offering_id",
    "name",
    "email",
    "phone",
    "preferred_date",
    "status",
    "assigned_owner",
    "response_due_at",
    "consent_to_contact",
    "consent_to_marketing",
    "hubspot_contact_id",
    "payload_json",
    "created_at",
    "updated_at",
)

_JSON_COLUMNS = frozenset({"lead_fields", "payload_json"})


def ensure_sslmode_require(dsn: str) -> str:
    """Force TLS. The Supabase pooler is on the public internet."""

    text = (dsn or "").strip()
    if not text:
        return text
    if _SSL_RE.search(text):
        return _SSL_RE.sub(lambda match: f"{match.group(1)}sslmode=require", text, count=1)
    if "://" in text:
        joiner = "&" if "?" in text else "?"
        return text + joiner + "sslmode=require"
    return text + " sslmode=require"


def _upsert_sql(table: str, columns: tuple[str, ...], conflict: str) -> str:
    names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    assignments = ", ".join(
        f"{column} = EXCLUDED.{column}" for column in columns if column != conflict
    )
    return (
        f"INSERT INTO public.{table} ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"
    )


_CALL_UPSERT = _upsert_sql("call_log", _CALL_COLUMNS, "call_sid")
_INQUIRY_INSERT = (
    f"INSERT INTO public.building_inquiries ({', '.join(_INQUIRY_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INQUIRY_COLUMNS))}) "
    "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id"
)
_INQUIRY_LOOKUP = (
    "SELECT id FROM public.building_inquiries WHERE idempotency_key = %s"
)
_SMS_UPDATE = (
    "UPDATE public.call_log SET sms_notified = %s, updated_at = %s WHERE call_sid = %s"
)
# Column write grants plus table-level SELECT on call_log. With RLS on,
# INSERT … ON CONFLICT DO UPDATE requires table SELECT. Column SELECT on
# call_sid still raises permission denied for the table, so a column-only
# grant must not turn logging_ready green. Table-level UPDATE and inquiry
# INSERT stay false under these column grants and are not probed.
_PRIVILEGE_PROBE = """
SELECT
  has_table_privilege(current_user, 'public.call_log', 'SELECT')
  AND has_column_privilege(current_user, 'public.call_log', 'call_sid', 'INSERT')
  AND has_column_privilege(current_user, 'public.call_log', 'transcript', 'INSERT')
  AND has_column_privilege(current_user, 'public.call_log', 'transcript', 'UPDATE')
  AND has_column_privilege(current_user, 'public.call_log', 'transcript_summary', 'INSERT')
  AND has_column_privilege(current_user, 'public.call_log', 'transcript_summary', 'UPDATE')
  AND has_column_privilege(current_user, 'public.call_log', 'sms_notified', 'UPDATE')
  AND has_column_privilege(current_user, 'public.building_inquiries', 'source', 'INSERT')
  AND has_column_privilege(current_user, 'public.building_inquiries', 'idempotency_key', 'INSERT')
  AND has_column_privilege(current_user, 'public.building_inquiries', 'payload_json', 'INSERT')
  AND has_column_privilege(current_user, 'public.building_inquiries', 'id', 'SELECT')
  AND has_column_privilege(current_user, 'public.building_inquiries', 'idempotency_key', 'SELECT')
"""
_PROBE_OK_SECONDS = 30


class PostgresLog:
    """One short connection per save. Safe to call from a worker thread."""

    def __init__(
        self,
        dsn: str,
        *,
        owner: str = "building-operator",
        connect: Callable[[str], Any] | None = None,
    ) -> None:
        self.dsn = ensure_sslmode_require(dsn)
        self.owner = owner
        self._connect = connect
        self.probe_status = "not_configured"
        self._probe_ok = False
        self._probe_at = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.dsn)

    def save_call(self, session: CallSession, *, sms_notified: bool) -> dict[str, str]:
        if not self.configured:
            return {"call_log": "skipped", "inquiry_id": "", "inquiry": "skipped"}
        inquiry_id = ""
        inquiry_state = "skipped"
        if should_create_inquiry(session):
            try:
                inquiry_id = self._write_inquiry(session)
            except Exception:
                logger.exception("Inquiry insert failed for call %s", session.call_sid)
                inquiry_id = ""
            else:
                if not inquiry_id:
                    logger.error("Inquiry insert failed for call %s", session.call_sid)
            inquiry_state = "saved" if inquiry_id else "failed"
            session.inquiry_id = inquiry_id
        with self._connection() as conn:
            with conn.cursor() as cur:
                row = build_call_row(session, inquiry_id=inquiry_id, sms_notified=sms_notified)
                cur.execute(_CALL_UPSERT, _values(row, _CALL_COLUMNS))
            conn.commit()
        return {"call_log": "saved", "inquiry_id": inquiry_id, "inquiry": inquiry_state}

    def set_sms_notified(self, call_sid: str, notified: bool) -> None:
        """Update delivery after the status callback. A 2xx from send is not enough."""

        if not self.configured or not call_sid:
            return
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_SMS_UPDATE, (notified, _now(), call_sid))
            conn.commit()

    def probe_privileges(self) -> bool:
        """True only when this role can write the log and the inquiry table."""

        if not self.configured:
            self.probe_status = "not_configured"
            return False
        now = time.monotonic()
        if self._probe_ok and now - self._probe_at < _PROBE_OK_SECONDS:
            return True
        try:
            with self._connection(timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute(_PRIVILEGE_PROBE)
                    row = cur.fetchone()
        except Exception as exc:
            logger.exception("Logging privilege probe failed")
            self._probe_ok = False
            self._probe_at = 0.0
            text = str(exc).casefold()
            self.probe_status = "denied" if "permission" in text or "privilege" in text else "unreachable"
            return False
        ok = bool(row and row[0])
        self._probe_ok = ok
        self._probe_at = now if ok else 0.0
        self.probe_status = "ok" if ok else "denied"
        return ok

    def save_sms(
        self,
        *,
        message_sid: str,
        caller_number: str,
        dialed_number: str,
        body: str,
        category: str,
        summary: str,
        sms_notified: bool,
        line_id: str = "",
        lead_fields: dict[str, Any] | None = None,
    ) -> None:
        if not self.configured or not message_sid:
            return
        from voice.leads import _now

        now = _now().isoformat()
        row = {
            "call_sid": message_sid,
            "channel": "sms",
            "caller_number": caller_number,
            "dialed_number": dialed_number,
            "line_id": line_id,
            "started_at": now,
            "ended_at": now,
            "duration_seconds": 0,
            "transcript_summary": summary,
            "transcript": body,
            "category": category or "other",
            "lead_fields": dict(lead_fields or {}),
            "action_taken": "sms_forwarded" if sms_notified else "sms_logged",
            "transfer_attempted": False,
            "transfer_outcome": "",
            "sms_notified": sms_notified,
            "building_inquiry_id": "",
            "updated_at": now,
        }
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_CALL_UPSERT, _values(row, _CALL_COLUMNS))
            conn.commit()

    def _write_inquiry(self, session: CallSession) -> str:
        """Own connection so a denied inquiry insert cannot abort the call log."""

        with self._connection() as conn:
            with conn.cursor() as cur:
                inquiry_id = self._insert_inquiry(cur, session)
            conn.commit()
        return inquiry_id

    def _insert_inquiry(self, cur: Any, session: CallSession) -> str:
        row = build_inquiry_row(session, owner=self.owner)
        cur.execute(_INQUIRY_INSERT, _values(row, _INQUIRY_COLUMNS))
        found = cur.fetchone()
        if found and found[0]:
            return str(found[0])
        cur.execute(_INQUIRY_LOOKUP, (row["idempotency_key"],))
        found = cur.fetchone()
        if found and found[0]:
            return str(found[0])
        logger.warning("Inquiry insert did not return an id")
        return ""

    def _connection(self, timeout: int = 10) -> Any:
        if self._connect is not None:
            return self._connect(self.dsn)
        import psycopg

        return psycopg.connect(self.dsn, connect_timeout=timeout)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _values(row: dict[str, Any], columns: tuple[str, ...]) -> tuple[Any, ...]:
    encoded: list[Any] = []
    for column in columns:
        value = row.get(column)
        if column in _JSON_COLUMNS:
            encoded.append(_json(value if value is not None else {}))
        else:
            encoded.append(value)
    return tuple(encoded)


def _json(value: Any) -> Any:
    try:
        from psycopg.types.json import Json
    except ModuleNotFoundError:
        return value
    return Json(value)
