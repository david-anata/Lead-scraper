"""Operational tenant directory for Anata Building notices.

The marketing CRM (``building_contacts``) keeps prospects, consent, and
campaigns. This directory is the living list of people in the building: who
they work for, which office they use, whether they have an August lock, and
whether the building manager bot should email them.
"""

from __future__ import annotations

import csv
import hashlib
import hmac
import io
import re
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingTenantDirectoryContact,
)

TABLE_NAME = BuildingTenantDirectoryContact.__tablename__
MAX_IMPORT_ROWS = 500
MAX_CSV_BYTES = 1_000_000
LIST_LIMIT = 200
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_FALSE_VALUES = {"0", "n", "no", "false", "off", "inactive", "deactivated", "disabled", "none"}
_HEADER_FIELDS: dict[str, tuple[str, ...]] = {
    "full_name": (
        "name",
        "full name",
        "display name",
        "user name",
        "employee name",
        "contact name",
    ),
    "first_name": ("first name", "firstname", "given name", "first"),
    "last_name": ("last name", "lastname", "surname", "family name", "last"),
    "email": ("email", "e mail", "email address", "mail", "user email"),
    "phone": (
        "phone",
        "phone number",
        "mobile",
        "mobile phone",
        "cell",
        "cell phone",
        "telephone",
    ),
    "company_name": (
        "company",
        "company name",
        "organization",
        "organisation",
        "tenant",
        "employer",
        "business",
    ),
    "group": ("user group", "group", "department", "access group"),
    "office_suite": (
        "office",
        "suite",
        "office suite",
        "office/suite",
        "office number",
        "suite number",
        "room",
        "unit",
        "door",
        "location",
    ),
    "august_lock": ("august lock", "lock", "lock name", "august"),
    "receives_notices": (
        "receives notices",
        "receives building notices",
        "notices",
        "notify",
    ),
    "is_active": ("active", "status", "user status"),
    "notes": ("notes", "note", "comment", "comments"),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_email(value: str) -> str:
    """Return a lowercase email, stripping a mailto: prefix from exports."""

    email = " ".join(str(value or "").strip().split()).lower()
    if email.startswith("mailto:"):
        email = email[len("mailto:"):].strip()
    return email


def valid_email(value: str) -> bool:
    return bool(_EMAIL_RE.match(value))


def collapse(value: str, *, limit: int) -> str:
    return " ".join(str(value or "").split())[:limit]


def lock_value(has_lock: bool, name: str, *, checkbox_wins: bool = False) -> str:
    """Store a blank for no lock, ``yes`` when unnamed, or the lock name.

    Admin saves pass ``checkbox_wins`` so unchecking "Has an August lock"
    clears a previously saved name. CSV import leaves the flag false and still
    treats a supplied lock name as a lock.
    """

    cleaned = collapse(name, limit=255)
    if cleaned.lower() in _FALSE_VALUES:
        cleaned = ""
    if cleaned.lower() in {"yes", "y", "true"}:
        cleaned = "yes"
    if checkbox_wins and not has_lock:
        return ""
    if cleaned:
        return cleaned
    return "yes" if has_lock else ""


def _checked(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def contact_input_from_form(form: dict[str, str]) -> dict[str, Any]:
    """Normalize an admin form into directory fields. Raises ValueError."""

    full_name = collapse(form.get("full_name", ""), limit=255)
    company_name = collapse(form.get("company_name", ""), limit=255)
    email = normalize_email(form.get("email", ""))
    if not full_name:
        raise ValueError("Enter the person's name.")
    if not company_name:
        raise ValueError("Enter the company.")
    if not valid_email(email):
        raise ValueError("Enter a valid email address.")
    notes = str(form.get("notes") or "").strip()
    if len(notes) > 5000:
        raise ValueError("Notes must be 5000 characters or fewer.")
    return {
        "full_name": full_name,
        "company_name": company_name,
        "email": email,
        "phone": collapse(form.get("phone", ""), limit=128),
        "office_suite": collapse(form.get("office_suite", ""), limit=128),
        "august_lock": lock_value(
            _checked(form.get("has_august_lock", "")),
            form.get("august_lock_name", ""),
            checkbox_wins=True,
        ),
        "is_active": _checked(form.get("is_active", "")),
        "receives_notices": _checked(form.get("receives_notices", "")),
        "notes": notes,
    }


def snapshot(row: BuildingTenantDirectoryContact) -> dict[str, Any]:
    return {
        "id": row.id,
        "full_name": row.full_name,
        "company_name": row.company_name,
        "email": row.email,
        "phone": row.phone,
        "office_suite": row.office_suite,
        "august_lock": row.august_lock,
        "is_active": bool(row.is_active),
        "receives_notices": bool(row.receives_notices),
        "notes": row.notes,
    }


def recipient_payload(row: BuildingTenantDirectoryContact) -> dict[str, Any]:
    """Fields the building bot needs. Internal notes stay on the admin page."""

    return {
        "id": row.id,
        "full_name": row.full_name,
        "company_name": row.company_name,
        "email": row.email,
        "phone": row.phone,
        "office_suite": row.office_suite,
        "august_lock": row.august_lock,
    }


def _audit(
    session: Session,
    *,
    entity_id: str,
    action: str,
    actor: str,
    before: dict[str, Any],
    after: dict[str, Any],
) -> None:
    session.add(
        BuildingAuditEvent(
            entity_type="tenant_directory_contact",
            entity_id=entity_id,
            action=action,
            actor=actor[:255],
            before_json=before,
            after_json=after,
            created_at=utc_now(),
        )
    )


def _email_taken(session: Session, email: str, *, except_id: str = "") -> bool:
    query = select(BuildingTenantDirectoryContact.id).where(
        func.lower(BuildingTenantDirectoryContact.email) == email
    )
    if except_id:
        query = query.where(BuildingTenantDirectoryContact.id != except_id)
    return session.scalar(query) is not None


def save_contact(
    session: Session,
    *,
    contact_id: str,
    fields: dict[str, Any],
    actor: str,
) -> BuildingTenantDirectoryContact:
    """Create or update one directory contact. Raises ValueError on conflict."""

    if _email_taken(session, fields["email"], except_id=contact_id):
        raise ValueError("A directory contact with that email already exists.")
    row = session.get(BuildingTenantDirectoryContact, contact_id) if contact_id else None
    creating = row is None
    if creating:
        row = BuildingTenantDirectoryContact(
            id=contact_id,
            created_by=actor[:255],
            created_at=utc_now(),
        )
    before = {} if creating else snapshot(row)
    for key, value in fields.items():
        setattr(row, key, value)
    row.updated_by = actor[:255]
    row.updated_at = utc_now()
    session.add(row)
    try:
        session.flush()
    except IntegrityError as exc:
        raise ValueError("A directory contact with that email already exists.") from exc
    _audit(
        session,
        entity_id=row.id,
        action="tenant_directory_created" if creating else "tenant_directory_updated",
        actor=actor,
        before=before,
        after=snapshot(row),
    )
    return row


def set_active(
    session: Session,
    contact_id: str,
    *,
    active: bool,
    actor: str,
) -> BuildingTenantDirectoryContact:
    row = session.get(BuildingTenantDirectoryContact, contact_id)
    if row is None:
        raise ValueError("That directory contact was not found.")
    before = snapshot(row)
    row.is_active = active
    row.updated_by = actor[:255]
    row.updated_at = utc_now()
    session.add(row)
    session.flush()
    _audit(
        session,
        entity_id=row.id,
        action="tenant_directory_reactivated" if active else "tenant_directory_deactivated",
        actor=actor,
        before=before,
        after=snapshot(row),
    )
    return row


def directory_metrics(session: Session) -> dict[str, int]:
    model = BuildingTenantDirectoryContact
    active = session.scalar(
        select(func.count()).select_from(model).where(model.is_active.is_(True))
    ) or 0
    notices = session.scalar(
        select(func.count())
        .select_from(model)
        .where(
            model.is_active.is_(True),
            model.receives_notices.is_(True),
            model.email != "",
        )
    ) or 0
    inactive = session.scalar(
        select(func.count()).select_from(model).where(model.is_active.is_(False))
    ) or 0
    companies = session.scalar(
        select(func.count(func.distinct(model.company_name))).where(
            model.is_active.is_(True),
            model.company_name != "",
        )
    ) or 0
    return {
        "active": int(active),
        "notices": int(notices),
        "inactive": int(inactive),
        "companies": int(companies),
    }


def list_contacts(
    session: Session,
    *,
    query: str = "",
    status: str = "active",
) -> tuple[list[BuildingTenantDirectoryContact], int]:
    model = BuildingTenantDirectoryContact
    filters = []
    if status == "inactive":
        filters.append(model.is_active.is_(False))
    elif status == "notices":
        filters.append(model.is_active.is_(True))
        filters.append(model.receives_notices.is_(True))
    elif status != "all":
        filters.append(model.is_active.is_(True))
    needle = query.strip().lower()
    if needle:
        escaped = needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{escaped}%"
        filters.append(
            or_(
                func.lower(model.full_name).like(pattern, escape="\\"),
                func.lower(model.company_name).like(pattern, escape="\\"),
                func.lower(model.email).like(pattern, escape="\\"),
                func.lower(model.phone).like(pattern, escape="\\"),
                func.lower(model.office_suite).like(pattern, escape="\\"),
                func.lower(model.august_lock).like(pattern, escape="\\"),
                func.lower(model.notes).like(pattern, escape="\\"),
            )
        )
    statement = select(model)
    if filters:
        statement = statement.where(*filters)
    statement = statement.order_by(model.company_name, model.full_name, model.email)
    rows = list(session.scalars(statement).all())
    return rows[:LIST_LIMIT], len(rows)


def notice_recipients(session: Session) -> list[BuildingTenantDirectoryContact]:
    """Active contacts who should receive building notices."""

    model = BuildingTenantDirectoryContact
    statement = (
        select(model)
        .where(
            model.is_active.is_(True),
            model.receives_notices.is_(True),
            model.email != "",
        )
        .order_by(model.company_name, model.full_name, model.email)
    )
    return list(session.scalars(statement).all())


def recipient_document(rows: list[BuildingTenantDirectoryContact]) -> dict[str, Any]:
    recipients = [recipient_payload(row) for row in rows]
    return {
        "count": len(recipients),
        "emails": [item["email"] for item in recipients],
        "recipients": recipients,
    }


def _header_key(value: str) -> str:
    text = str(value or "").strip().lower().replace("_", " ").replace("-", " ").replace("/", " ")
    return " ".join(text.split())


def _field_for_header(header: str) -> str:
    key = _header_key(header)
    for field, aliases in _HEADER_FIELDS.items():
        if key in aliases:
            return field
    return ""


def _truthy_cell(value: str, *, default: bool) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return default
    if token in _FALSE_VALUES:
        return False
    return True


def decode_csv_bytes(payload: bytes) -> str:
    if payload.startswith(b"\xff\xfe") or payload.startswith(b"\xfe\xff"):
        return payload.decode("utf-16")
    try:
        return payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        return payload.decode("latin-1")


def canonical_csv_text(csv_text: str) -> str:
    """Use LF so a browser textarea submit cannot change the signed preview."""

    return str(csv_text or "").replace("\r\n", "\n").replace("\r", "\n")


def parse_directory_csv(raw: str) -> tuple[list[dict[str, Any]], str]:
    """Parse a tenant or UniFi Access export. Returns rows and a file-level error."""

    text = canonical_csv_text(raw).lstrip("\ufeff")
    if text.startswith("sep="):
        text = text.split("\n", 1)[1] if "\n" in text else ""
    if not text.strip():
        return [], "The file is empty."
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if not reader.fieldnames:
        return [], "The file needs a header row."
    mapping = {header: _field_for_header(header) for header in reader.fieldnames if header}
    if "email" not in mapping.values():
        return [], (
            "The file needs a header row with an email column. "
            "Name, phone, company, and office columns are picked up when present."
        )
    rows: list[dict[str, Any]] = []
    for index, source in enumerate(reader, start=2):
        if index - 1 > MAX_IMPORT_ROWS:
            return [], f"Import at most {MAX_IMPORT_ROWS} contacts at a time."
        if not source or not any(str(value or "").strip() for value in source.values()):
            continue
        pulled: dict[str, str] = {}
        for header, field in mapping.items():
            if not field or field in pulled:
                continue
            pulled[field] = str(source.get(header) or "").strip()
        full_name = collapse(pulled.get("full_name", ""), limit=255)
        if not full_name:
            full_name = collapse(
                f"{pulled.get('first_name', '')} {pulled.get('last_name', '')}",
                limit=255,
            )
        company = collapse(pulled.get("company_name", ""), limit=255) or collapse(
            pulled.get("group", ""),
            limit=255,
        )
        email = normalize_email(pulled.get("email", ""))
        reason = ""
        status = "ready"
        if not full_name:
            status, reason = "invalid", "Name is required."
        elif not company:
            status, reason = "invalid", "Company is required."
        elif not valid_email(email):
            status, reason = "invalid", "A valid email is required."
        rows.append(
            {
                "line": index,
                "full_name": full_name,
                "company_name": company,
                "email": email,
                "phone": collapse(pulled.get("phone", ""), limit=128),
                "office_suite": collapse(pulled.get("office_suite", ""), limit=128),
                "august_lock": lock_value(False, pulled.get("august_lock", "")),
                "is_active": _truthy_cell(pulled.get("is_active", ""), default=True),
                "receives_notices": _truthy_cell(
                    pulled.get("receives_notices", ""),
                    default=True,
                ),
                "notes": str(pulled.get("notes") or "").strip()[:5000],
                "status": status,
                "reason": reason,
            }
        )
    if not rows:
        return [], "The file has a header but no contacts."
    return rows, ""


def classify_import_rows(
    rows: list[dict[str, Any]],
    existing_emails: set[str],
) -> list[dict[str, Any]]:
    """Mark duplicates against the directory and within the file. First row wins."""

    seen: set[str] = set()
    classified: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        email = item["email"]
        if item["status"] == "ready":
            if email in existing_emails:
                item["status"] = "duplicate"
                item["reason"] = "This email is already in the directory."
            elif email in seen:
                item["status"] = "duplicate_in_file"
                item["reason"] = "An earlier row in this file uses this email."
            else:
                seen.add(email)
        classified.append(item)
    return classified


def import_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "ready": sum(1 for row in rows if row["status"] == "ready"),
        "duplicate": sum(1 for row in rows if row["status"] in {"duplicate", "duplicate_in_file"}),
        "invalid": sum(1 for row in rows if row["status"] == "invalid"),
    }


def import_preview_token(secret: str, csv_text: str) -> str:
    payload = canonical_csv_text(csv_text).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def existing_emails(session: Session) -> set[str]:
    values = session.scalars(select(BuildingTenantDirectoryContact.email)).all()
    return {normalize_email(value) for value in values if value}


def apply_import(
    session: Session,
    rows: list[dict[str, Any]],
    *,
    actor: str,
) -> dict[str, int]:
    """Insert rows still marked ready. Recheck email so a race cannot duplicate."""

    created = 0
    skipped = 0
    for row in rows:
        if row["status"] != "ready":
            skipped += 1
            continue
        if _email_taken(session, row["email"]):
            skipped += 1
            continue
        contact_id = _new_id()
        record = BuildingTenantDirectoryContact(
            id=contact_id,
            full_name=row["full_name"],
            company_name=row["company_name"],
            email=row["email"],
            phone=row["phone"],
            office_suite=row["office_suite"],
            august_lock=row["august_lock"],
            is_active=bool(row["is_active"]),
            receives_notices=bool(row["receives_notices"]),
            notes=row["notes"],
            created_by=actor[:255],
            updated_by=actor[:255],
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        session.add(record)
        try:
            session.flush()
        except IntegrityError as exc:
            raise ValueError("A directory contact with that email already exists.") from exc
        _audit(
            session,
            entity_id=record.id,
            action="tenant_directory_imported",
            actor=actor,
            before={},
            after=snapshot(record),
        )
        created += 1
    return {"created": created, "skipped": skipped}


def _new_id() -> str:
    from uuid import uuid4

    return uuid4().hex


def preview_import(session: Session, csv_text: str) -> tuple[list[dict[str, Any]], str]:
    rows, error = parse_directory_csv(csv_text)
    if error:
        return [], error
    return classify_import_rows(rows, existing_emails(session)), ""


def directory_table_ready(session: Session) -> bool:
    bind = session.get_bind()
    if bind is None:
        return False
    from sqlalchemy import inspect

    return inspect(bind).has_table(TABLE_NAME)


def find_contact(session: Session, contact_id: str) -> Optional[BuildingTenantDirectoryContact]:
    if not contact_id:
        return None
    return session.get(BuildingTenantDirectoryContact, contact_id)
