"""Shape a phone lead like the building admin inquiry, and log every call.

The admin reads `building_inquiries`. A voice lead uses that table's columns
and the same payload keys the website form stores (`eventType`, `groupSize`,
`notes`). Spam stays in `call_log` only.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from urllib.parse import quote
from uuid import uuid4

from voice.session import CallSession

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
INQUIRY_CATEGORIES = frozenset({"lead"})
# anata Inc. leads (marketing, fulfillment, software) are not building
# inquiries. They stay in call_log and the after-call text.
NON_BUILDING_LINES = frozenset({"agency", "fulfillment", "software"})


class HttpClient(Protocol):
    def post(self, url: str, *, json: Any, headers: dict[str, str], timeout: int) -> Any: ...

    def get(self, url: str, *, headers: dict[str, str], timeout: int) -> Any: ...

    def patch(self, url: str, *, json: Any, headers: dict[str, str], timeout: int) -> Any: ...


def inquiry_kind(session: CallSession) -> str:
    blob = " ".join(
        (
            session.event_or_interest_type,
            session.notes,
            session.reason,
            session.summary,
        )
    ).casefold()
    eventish = bool(session.event_date or session.guest_count) or any(
        word in blob
        for word in (
            "wedding",
            "party",
            "event",
            "arena",
            "quince",
            "shower",
            "reception",
            "birthday",
            "corporate",
        )
    )
    if eventish:
        return "event"
    if any(word in blob for word in ("office", "lease", "cowork", "bullpen", "desk", "warehouse")):
        return "workspace"
    return "tour"


def should_create_inquiry(session: CallSession) -> bool:
    """Leads and real event or tour requests show up in the building admin.

    Vendor pitches stay out even when the notes happen to contain the word
    "event". Spam stays out too.
    """

    if session.category in {"spam", "vendor"}:
        return False
    if session.business_line.strip().casefold() in NON_BUILDING_LINES:
        return False
    if session.category in INQUIRY_CATEGORIES:
        return True
    blob = " ".join((session.notes, session.reason, session.summary)).casefold()
    asked_for_space = bool(
        session.event_date
        or session.guest_count
        or session.event_or_interest_type
        or "tour" in blob
    )
    return asked_for_space and session.category in {"other", "tenant", "urgent"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _valid_email(value: str) -> str:
    text = (value or "").strip()
    if EMAIL_RE.match(text):
        return text
    return ""


def build_inquiry_row(session: CallSession, *, owner: str) -> dict[str, Any]:
    """Return a `building_inquiries` insert matching the admin's columns."""

    received = _now()
    kind = inquiry_kind(session)
    email = _valid_email(session.email)
    name = session.caller_name.strip() or "Phone caller"
    details: dict[str, Any] = {
        "channel": "phone",
        "eventType": session.event_or_interest_type,
        "groupSize": session.guest_count,
        "notes": session.notes or session.reason,
        "company": session.company,
        "callerNumber": session.caller_number,
        "dialedNumber": session.dialed_number,
        "callSid": session.call_sid,
        "line": session.line.label,
        "_lifecycle": {"stage": "new"},
    }
    if session.security_preference:
        details["security_preference"] = session.security_preference
    if session.line.source.strip():
        details["source"] = session.line.source.strip()
    details["_follow_up_sequence"] = _follow_up(received)
    if kind == "event":
        interview = {
            "event_format": session.event_or_interest_type,
            "candidate_dates": session.event_date,
            "attendance": session.guest_count,
            "special_requests": session.notes,
        }
        if session.security_preference:
            interview["security_preference"] = session.security_preference
        details["_event_interview"] = interview
        details["_event_interview_meta"] = {
            "source": "voice_receptionist",
            "reviewed": False,
        }
    preferred = session.event_date[:10] if _iso_date(session.event_date) else None
    return {
        "id": str(uuid4()),
        "idempotency_key": f"voice-call:{session.call_sid}"[:128],
        "kind": kind,
        "source": "voice",
        "source_reference": session.call_sid[:255],
        "offering_id": None,
        "name": name[:255],
        "email": email,
        "phone": (session.phone or session.caller_number)[:128],
        "preferred_date": preferred,
        "status": "new",
        "assigned_owner": (owner or "building-operator")[:255],
        "response_due_at": (received + timedelta(hours=4)).isoformat(),
        "consent_to_contact": True,
        "consent_to_marketing": False,
        "hubspot_contact_id": "",
        "payload_json": details,
        "created_at": received.isoformat(),
        "updated_at": received.isoformat(),
    }


def build_call_row(session: CallSession, *, inquiry_id: str, sms_notified: bool) -> dict[str, Any]:
    started = datetime.fromtimestamp(session.started_at, tz=timezone.utc)
    ended = _now()
    summary = session.summary.strip() or _fallback_summary(session)
    return {
        "call_sid": session.call_sid,
        "channel": "voice",
        "caller_number": session.caller_number,
        "dialed_number": session.dialed_number,
        "line_id": session.line.id,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_seconds": session.duration_seconds(ended.timestamp()),
        "transcript_summary": summary,
        "transcript": session.transcript(),
        "category": session.category if session.category else "other",
        "lead_fields": session.lead_fields(),
        "action_taken": (session.action or "message")[:500],
        "transfer_attempted": bool(session.transfer_started or session.transfer_requested),
        "transfer_outcome": session.transfer_outcome[:200],
        "sms_notified": sms_notified,
        "building_inquiry_id": inquiry_id,
        "updated_at": ended.isoformat(),
    }


def _fallback_summary(session: CallSession) -> str:
    who = session.caller_name or session.caller_number or "Unknown caller"
    reason = session.reason or session.notes or session.category or "no reason captured"
    return f"{who}: {reason}"


def _iso_date(value: str) -> bool:
    if len(value) < 10:
        return False
    try:
        datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _follow_up(received: datetime) -> list[dict[str, str]]:
    """Same step keys the building lead workspace already understands."""

    def due(delta: timedelta) -> str:
        return (received + delta).isoformat()

    return [
        {"key": "review", "label": "Review the request and assign an owner", "due_at": due(timedelta(0)), "status": "due"},
        {"key": "first_response", "label": "Make the first personal response", "due_at": due(timedelta(hours=4)), "status": "queued"},
        {"key": "interview", "label": "Complete the event interview and date review", "due_at": due(timedelta(days=1)), "status": "queued"},
        {"key": "follow_up", "label": "Follow up if the prospect has not decided", "due_at": due(timedelta(days=3)), "status": "queued"},
        {"key": "close_loop", "label": "Close the loop or record the next agreed date", "due_at": due(timedelta(days=7)), "status": "queued"},
    ]


class SupabaseStore:
    """Service-role writer. RLS stays closed to anon and authenticated keys."""

    def __init__(self, url: str, service_key: str, http: HttpClient | None = None, owner: str = "building-operator") -> None:
        self.url = (url or "").rstrip("/")
        self.service_key = service_key or ""
        self.owner = owner
        self.http = http
        self._requests: Any = None

    @property
    def configured(self) -> bool:
        return bool(self.url and self.service_key)

    def save_call(self, session: CallSession, *, sms_notified: bool) -> dict[str, str]:
        if not self.configured:
            logger.warning("Supabase is not configured; call %s was not logged", session.call_sid)
            return {"call_log": "skipped", "inquiry_id": ""}
        inquiry_id = ""
        inquiry_state = "skipped"
        if should_create_inquiry(session):
            try:
                inquiry_id = self._save_inquiry(session)
            except Exception:
                logger.exception("Inquiry insert failed for call %s", session.call_sid)
                inquiry_id = ""
            else:
                if not inquiry_id:
                    logger.error("Inquiry insert failed for call %s", session.call_sid)
            inquiry_state = "saved" if inquiry_id else "failed"
            session.inquiry_id = inquiry_id
        row = build_call_row(session, inquiry_id=inquiry_id, sms_notified=sms_notified)
        self._request(
            "POST",
            "/rest/v1/call_log?on_conflict=call_sid",
            row,
            prefer="resolution=merge-duplicates,return=minimal",
        )
        return {"call_log": "saved", "inquiry_id": inquiry_id, "inquiry": inquiry_state}

    def set_sms_notified(self, call_sid: str, notified: bool) -> None:
        if not self.configured or not call_sid:
            return
        self._request(
            "PATCH",
            "/rest/v1/call_log?call_sid=eq." + quote(call_sid, safe=""),
            {"sms_notified": notified, "updated_at": _now().isoformat()},
            prefer="return=minimal",
        )

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
        now = _now().isoformat()
        self._request(
            "POST",
            "/rest/v1/call_log?on_conflict=call_sid",
            {
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
                "category": category,
                "lead_fields": dict(lead_fields or {}),
                "action_taken": "sms_forwarded" if sms_notified else "sms_logged",
                "transfer_attempted": False,
                "transfer_outcome": "",
                "sms_notified": sms_notified,
                "building_inquiry_id": "",
                "updated_at": now,
            },
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def _save_inquiry(self, session: CallSession) -> str:
        row = build_inquiry_row(session, owner=self.owner)
        try:
            created = self._request(
                "POST",
                "/rest/v1/building_inquiries",
                row,
                prefer="return=representation",
            )
        except StoreError as exc:
            if exc.status != 409:
                logger.warning("Inquiry insert failed with status %s", exc.status)
                return ""
            created = self._request(
                "GET",
                "/rest/v1/building_inquiries?idempotency_key=eq."
                + quote(row["idempotency_key"], safe="")
                + "&select=id",
                None,
            )
        inquiry_id = _first_id(created) or str(row["id"])
        email = str(row["email"] or "")
        if email:
            self._link_contact(session, inquiry_id, email, str(row["name"]), str(row["phone"]))
        self._audit(inquiry_id, row)
        return inquiry_id

    def _link_contact(
        self,
        session: CallSession,
        inquiry_id: str,
        email: str,
        name: str,
        phone: str,
    ) -> None:
        try:
            existing = self._request(
                "GET",
                "/rest/v1/building_contacts?email=eq."
                + quote(email, safe="")
                + "&select=id,phone,full_name",
                None,
            )
        except StoreError as exc:
            logger.warning("Contact lookup failed with status %s", exc.status)
            return
        rows = existing if isinstance(existing, list) else []
        if rows:
            contact_id = str(rows[0].get("id") or "")
            patch: dict[str, Any] = {"updated_at": _now().isoformat()}
            if not str(rows[0].get("phone") or "") and phone:
                patch["phone"] = phone
            if not str(rows[0].get("full_name") or "") and name:
                patch["full_name"] = name
            if session.company:
                patch["company_name"] = session.company[:255]
            try:
                self._request(
                    "PATCH",
                    "/rest/v1/building_contacts?id=eq." + quote(contact_id, safe=""),
                    patch,
                    prefer="return=minimal",
                )
            except StoreError as exc:
                logger.warning("Contact update failed with status %s", exc.status)
        else:
            contact_id = str(uuid4())
            try:
                self._request(
                    "POST",
                    "/rest/v1/building_contacts",
                    {
                        "id": contact_id,
                        "email": email,
                        "full_name": name,
                        "phone": phone,
                        "company_name": session.company[:255],
                        "hubspot_contact_id": "",
                        "source": "voice",
                        "status": "active",
                        "metadata_json": {"channel": "phone", "inquiry_id": inquiry_id},
                        "created_at": _now().isoformat(),
                        "updated_at": _now().isoformat(),
                    },
                    prefer="return=minimal",
                )
            except StoreError as exc:
                logger.warning("Contact insert failed with status %s", exc.status)
                return
        if not contact_id:
            return
        reference = f"inquiry:{inquiry_id}"
        try:
            existing_rel = self._request(
                "GET",
                "/rest/v1/building_relationships?source_reference=eq."
                + quote(reference, safe="")
                + "&select=id",
                None,
            )
        except StoreError:
            existing_rel = []
        if isinstance(existing_rel, list) and existing_rel:
            return
        try:
            self._request(
                "POST",
                "/rest/v1/building_relationships",
                {
                    "id": str(uuid4()),
                    "contact_id": contact_id,
                    "relationship_type": "prospect",
                    "status": "active",
                    "organization": session.company[:255],
                    "source_reference": reference,
                    "metadata_json": {
                        "inquiry_kind": inquiry_kind(session),
                        "channel": "phone",
                    },
                    "created_at": _now().isoformat(),
                    "updated_at": _now().isoformat(),
                },
                prefer="return=minimal",
            )
        except StoreError as exc:
            if exc.status != 409:
                logger.warning("Relationship insert failed with status %s", exc.status)
        try:
            self._request(
                "POST",
                "/rest/v1/building_communication_preferences",
                {
                    "contact_id": contact_id,
                    "marketing_status": "unknown",
                    "marketing_source": "",
                    "marketing_changed_at": _now().isoformat(),
                    "transactional_allowed": True,
                    "operational_source": "",
                    "operational_evidence_reference": "",
                    "updated_by": "voice-receptionist",
                    "updated_at": _now().isoformat(),
                },
                prefer="resolution=ignore-duplicates,return=minimal",
            )
        except StoreError as exc:
            logger.warning("Preference insert failed with status %s", exc.status)

    def _audit(self, inquiry_id: str, row: dict[str, Any]) -> None:
        try:
            self._request(
                "POST",
                "/rest/v1/building_audit_events",
                {
                    "entity_type": "inquiry",
                    "entity_id": inquiry_id,
                    "action": "created",
                    "actor": "voice-receptionist",
                    "before_json": {},
                    "after_json": {
                        "kind": row["kind"],
                        "source": "voice",
                        "assigned_owner": row["assigned_owner"],
                        "channel": "phone",
                    },
                    "created_at": _now().isoformat(),
                },
                prefer="return=minimal",
            )
        except StoreError as exc:
            logger.warning("Audit insert failed with status %s", exc.status)

    def _request(self, method: str, path: str, body: Any, prefer: str = "return=representation") -> Any:
        headers = {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }
        client = self.http or self._client()
        url = self.url + path
        if method == "GET":
            response = client.get(url, headers=headers, timeout=20)
        elif method == "PATCH":
            response = client.patch(url, json=body, headers=headers, timeout=20)
        else:
            response = client.post(url, json=body, headers=headers, timeout=20)
        status = int(getattr(response, "status_code", 0))
        if status >= 400:
            raise StoreError(status)
        if status == 204:
            return []
        try:
            return response.json()
        except Exception:
            return []

    def _client(self) -> Any:
        if self._requests is None:
            import requests

            self._requests = requests
        return self._requests


class StoreError(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__(f"supabase status {status}")
        self.status = status


def _first_id(payload: Any) -> str:
    if isinstance(payload, list) and payload:
        return str(payload[0].get("id") or "")
    if isinstance(payload, dict):
        return str(payload.get("id") or "")
    return ""
