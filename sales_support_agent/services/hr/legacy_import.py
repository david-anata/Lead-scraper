"""Guarded, idempotent recovery of the Base44 HR CSV export.

The importer deliberately preserves historical facts without treating legacy
tax settings as authoritative. New employee profiles are created inactive so a
human must review current employment and compensation before payroll can use
them. Opening balances are draft candidates and still require independent
approval in the existing settings workflow.
"""

from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from io import BytesIO, TextIOWrapper
import re
from typing import Any
from zipfile import BadZipFile, ZipFile

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HREmployee,
    HROpeningPayrollBalance,
    HRPaycheck,
    HRPayPeriod,
    HRPayrollLineItem,
    HRPayrollRun,
    HRPrintedCheck,
    HRTeam,
    HRTimeEntry,
)


MAX_ARCHIVE_BYTES = 2_000_000
MAX_UNCOMPRESSED_BYTES = 10_000_000
MAX_ENTRIES = 25
MAX_ROWS_PER_TABLE = 10_000
EXPECTED_TABLES = {
    "Organization_export.csv",
    "Paycheck_export.csv",
    "PayPeriod_export.csv",
    "PayrollLineItem_export.csv",
    "PayrollRun_export.csv",
    "PayrollSettings_export.csv",
    "PrintedCheck_export.csv",
    "Team_export.csv",
    "TimeEntry_export.csv",
}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class LegacyImportError(ValueError):
    """A user-safe validation error; no archive content is echoed."""


def _money(value: Any) -> int:
    try:
        amount = Decimal(str(value or "0").replace("$", "").replace(",", "").strip() or "0")
    except InvalidOperation as exc:
        raise LegacyImportError("The export contains an invalid money value.") from exc
    if not amount.is_finite():
        raise LegacyImportError("The export contains a non-finite money value.")
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _number(value: Any) -> float:
    try:
        number = Decimal(str(value or "0").strip() or "0")
    except InvalidOperation as exc:
        raise LegacyImportError("The export contains an invalid numeric value.") from exc
    if not number.is_finite():
        raise LegacyImportError("The export contains a non-finite numeric value.")
    return float(number)


def _date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise LegacyImportError("The export contains an invalid date.") from exc


def _datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LegacyImportError("The export contains an invalid timestamp.") from exc


def _email(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if EMAIL_RE.match(text) else ""


def _required_email(row: dict[str, str]) -> str:
    email = _email(row.get("employee_email"))
    if not email:
        raise LegacyImportError("A non-sample employee record has an invalid email.")
    return email


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _is_sample(row: dict[str, str]) -> bool:
    email = _email(row.get("employee_email"))
    return _truthy(row.get("is_sample")) or email.endswith("@example.com")


def _read_archive(payload: bytes) -> tuple[dict[str, list[dict[str, str]]], str]:
    if not payload or len(payload) > MAX_ARCHIVE_BYTES:
        raise LegacyImportError("Choose the Base44 HR data-table ZIP (maximum 2 MB).")
    try:
        archive = ZipFile(BytesIO(payload))
    except BadZipFile as exc:
        raise LegacyImportError("The selected file is not a valid ZIP archive.") from exc
    with archive:
        entries = [item for item in archive.infolist() if not item.is_dir()]
        if len(entries) > MAX_ENTRIES:
            raise LegacyImportError("The ZIP contains too many files.")
        if any(item.flag_bits & 1 for item in entries):
            raise LegacyImportError("Password-protected ZIP files are not supported.")
        if sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
            raise LegacyImportError("The ZIP expands beyond the safe import limit.")
        names = {item.filename for item in entries}
        if any("/" in name or "\\" in name or name.startswith(".") for name in names):
            raise LegacyImportError("The ZIP must contain only the exported CSV files.")
        missing = EXPECTED_TABLES - names
        unexpected = names - EXPECTED_TABLES
        if missing or unexpected:
            raise LegacyImportError(
                "This is not the expected Base44 HR data-table export."
            )
        tables: dict[str, list[dict[str, str]]] = {}
        for name in sorted(EXPECTED_TABLES):
            with archive.open(name) as raw:
                wrapper = TextIOWrapper(raw, encoding="utf-8-sig", newline="")
                reader = csv.DictReader(wrapper)
                if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
                    raise LegacyImportError(f"{name} has invalid column headings.")
                rows: list[dict[str, str]] = []
                for index, row in enumerate(reader, start=1):
                    if index > MAX_ROWS_PER_TABLE:
                        raise LegacyImportError(f"{name} contains too many rows.")
                    rows.append({str(key): str(value or "") for key, value in row.items()})
                tables[name] = rows
    return tables, sha256(payload).hexdigest()


def _employee_candidates(tables: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    def identity(row: dict[str, str]) -> dict[str, Any] | None:
        email = _email(row.get("employee_email"))
        if not email or _is_sample(row):
            return None
        item = grouped.setdefault(email, {
            "email": email, "name": "", "rates": [], "gross_cents": 0,
            "federal_cents": 0, "state_cents": 0, "ss_cents": 0,
            "medicare_cents": 0, "line_count": 0, "time_count": 0,
        })
        if row.get("employee_name") and not item["name"]:
            item["name"] = row["employee_name"].strip()
        if "hourly_rate" in row and _money(row.get("hourly_rate")) > 0:
            item["rates"].append(_money(row.get("hourly_rate")))
        return item

    for table_name in (
        "PayrollLineItem_export.csv", "PrintedCheck_export.csv",
        "TimeEntry_export.csv", "PayPeriod_export.csv", "Paycheck_export.csv",
    ):
        for row in tables[table_name]:
            identity(row)
    for row in tables["PayrollLineItem_export.csv"]:
        item = identity(row)
        if item:
            item["line_count"] += 1
    # Printed checks are the strongest evidence in this export that a
    # calculation became actual pay. Attempted/duplicate line items are not
    # allowed to inflate the opening-balance proposal.
    for row in tables["PrintedCheck_export.csv"]:
        item = identity(row)
        if item and row.get("status", "").strip().lower() != "voided":
            item["gross_cents"] += _money(row.get("gross_pay"))
            item["federal_cents"] += _money(row.get("federal_income_tax"))
            item["state_cents"] += _money(row.get("state_income_tax"))
            item["ss_cents"] += _money(row.get("social_security_tax"))
            item["medicare_cents"] += _money(row.get("medicare_tax"))
    for row in tables["TimeEntry_export.csv"]:
        item = identity(row)
        if item:
            item["time_count"] += 1
    for item in grouped.values():
        item["hourly_rate_cents"] = item["rates"][-1] if item["rates"] else 0
        item["proposed_type"] = "hourly" if item["hourly_rate_cents"] else "salaried"
        item.pop("rates", None)
    return sorted(grouped.values(), key=lambda item: (item["name"], item["email"]))


def preview_legacy_export(payload: bytes) -> dict[str, Any]:
    """Validate an export and return a privacy-conscious review summary."""
    tables, digest = _read_archive(payload)
    employees = _employee_candidates(tables)
    return {
        "digest": digest,
        "employees": employees,
        "counts": {name.removesuffix("_export.csv"): len(rows) for name, rows in tables.items()},
        "sample_rows_excluded": sum(
            1 for rows in tables.values() for row in rows if _is_sample(row)
        ),
        "warnings": [
            "Imported employee profiles stay inactive until David or Val reviews them.",
            "Legacy tax settings are archived as source evidence but never activated.",
            "Opening balances are draft candidates and still need independent approval.",
            "No bank details, Social Security numbers, invitations, or money movement are included.",
        ],
    }


def _base_fields(row: dict[str, str]) -> dict[str, Any]:
    return {"base44_id": (row.get("id") or "").strip() or None}


def _insert_missing(session: Session, model, row: dict[str, str], **values: Any) -> bool:
    base44_id = (row.get("id") or "").strip()
    if not base44_id or len(base44_id) > 64:
        raise LegacyImportError("A historical row is missing its source identifier.")
    if session.query(model).filter_by(base44_id=base44_id).first():
        return False
    session.add(model(**_base_fields(row), **values))
    return True


def import_legacy_export(
    payload: bytes, *, actor: str, expected_digest: str, attested: bool
) -> dict[str, Any]:
    """Import historical facts once, leaving current payroll setup unapproved."""
    if not attested:
        raise LegacyImportError("Confirm that you reviewed the recovery preview.")
    tables, digest = _read_archive(payload)
    if not expected_digest or digest != expected_digest.strip().lower():
        raise LegacyImportError("The uploaded ZIP does not match the reviewed preview.")
    candidates = _employee_candidates(tables)
    counts = {
        "employees_created": 0, "employees_existing": 0, "teams": 0,
        "periods": 0, "time_entries": 0, "runs": 0, "line_items": 0,
        "paychecks": 0, "printed_checks": 0, "opening_balances": 0,
        "existing_rows_skipped": 0,
    }
    session = Session(get_engine(), expire_on_commit=False)
    try:
        for item in candidates:
            existing = session.query(HREmployee).filter_by(email=item["email"]).first()
            if existing:
                counts["employees_existing"] += 1
            else:
                session.add(HREmployee(
                    email=item["email"], full_name=item["name"],
                    employee_type=item["proposed_type"],
                    hourly_rate_cents=item["hourly_rate_cents"],
                    status="inactive", onboarding_complete=False,
                ))
                counts["employees_created"] += 1

        for row in tables["Team_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRTeam, row, name=row.get("name", "").strip(),
                manager_email=_email(row.get("manager_email")),
                description=row.get("description", "").strip(),
            )
            counts["teams" if inserted else "existing_rows_skipped"] += 1

        for row in tables["PayPeriod_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRPayPeriod, row,
                employee_email=_required_email(row),
                start_date=_date(row.get("start_date")),
                end_date=_date(row.get("end_date")),
                total_hours=_number(row.get("total_hours")),
                status=(row.get("status", "pending").strip() or "pending")[:16],
                approved_by=_email(row.get("approved_by")),
                approved_date=_datetime(row.get("approved_date")),
            )
            counts["periods" if inserted else "existing_rows_skipped"] += 1

        for row in tables["TimeEntry_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRTimeEntry, row,
                employee_email=_required_email(row),
                date=_date(row.get("date")),
                start_time=row.get("start_time", "").strip()[:8],
                stop_time=row.get("stop_time", "").strip()[:8],
                hours=_number(row.get("hours")),
                project=row.get("project", "").strip(),
                tag=row.get("tag", "").strip(),
                notes=row.get("notes", "").strip(),
                clocked_in_at=_datetime(row.get("clocked_in_at")),
                pay_period_id=(row.get("pay_period_id") or "").strip() or None,
            )
            counts["time_entries" if inserted else "existing_rows_skipped"] += 1

        for row in tables["PayrollRun_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRPayrollRun, row,
                pay_period_start=_date(row.get("pay_period_start")),
                pay_period_end=_date(row.get("pay_period_end")),
                pay_date=_date(row.get("pay_date")),
                status=(row.get("status", "completed").strip() or "completed")[:16],
                total_gross_cents=_money(row.get("total_gross")),
                total_net_cents=_money(row.get("total_net")),
                total_taxes_cents=_money(row.get("total_taxes")),
                employee_count=int(_number(row.get("employee_count"))),
                initiated_by=_email(row.get("initiated_by")),
                notes=("Historical Base44 import. " + row.get("notes", "").strip()).strip(),
            )
            counts["runs" if inserted else "existing_rows_skipped"] += 1

        for row in tables["PayrollLineItem_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRPayrollLineItem, row,
                payroll_run_id=(row.get("payroll_run_id") or "").strip() or None,
                employee_email=_required_email(row),
                employee_name=row.get("employee_name", "").strip(),
                total_hours=_number(row.get("total_hours")),
                hourly_rate_cents=_money(row.get("hourly_rate")),
                gross_pay_cents=_money(row.get("gross_pay")),
                federal_income_tax_cents=_money(row.get("federal_income_tax")),
                social_security_tax_cents=_money(row.get("social_security_tax")),
                medicare_tax_cents=_money(row.get("medicare_tax")),
                state_income_tax_cents=_money(row.get("state_income_tax")),
                extra_withholding_cents=_money(row.get("extra_withholding")),
                total_deductions_cents=_money(row.get("total_deductions")),
                net_pay_cents=_money(row.get("net_pay")),
                status=(row.get("status", "sent").strip() or "sent")[:16],
                error_message=row.get("error_message", "").strip(),
            )
            counts["line_items" if inserted else "existing_rows_skipped"] += 1

        for row in tables["Paycheck_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRPaycheck, row,
                employee_email=_required_email(row),
                pay_period_id=(row.get("pay_period_id") or "").strip() or None,
                pay_date=_date(row.get("pay_date")),
                gross_pay_cents=_money(row.get("gross_pay")),
                deductions_cents=_money(row.get("deductions")),
                net_pay_cents=_money(row.get("net_pay")),
                total_hours=_number(row.get("total_hours")),
                notes=("Historical Base44 import. " + row.get("notes", "").strip()).strip(),
            )
            counts["paychecks" if inserted else "existing_rows_skipped"] += 1

        for row in tables["PrintedCheck_export.csv"]:
            if _is_sample(row):
                continue
            inserted = _insert_missing(
                session, HRPrintedCheck, row,
                payroll_run_id=(row.get("payroll_run_id") or "").strip() or None,
                payroll_line_item_id=(row.get("payroll_line_item_id") or "").strip() or None,
                employee_email=_required_email(row),
                employee_name=row.get("employee_name", "").strip(),
                pay_period_start=_date(row.get("pay_period_start")),
                pay_period_end=_date(row.get("pay_period_end")),
                pay_date=_date(row.get("pay_date")),
                check_number=row.get("check_number", "").strip(),
                gross_pay_cents=_money(row.get("gross_pay")),
                federal_income_tax_cents=_money(row.get("federal_income_tax")),
                social_security_tax_cents=_money(row.get("social_security_tax")),
                medicare_tax_cents=_money(row.get("medicare_tax")),
                state_income_tax_cents=_money(row.get("state_income_tax")),
                extra_withholding_cents=_money(row.get("extra_withholding")),
                total_deductions_cents=_money(row.get("total_deductions")),
                net_pay_cents=_money(row.get("net_pay")),
                total_hours=_number(row.get("total_hours")),
                hourly_rate_cents=_money(row.get("hourly_rate")),
                status=(row.get("status", "ready").strip() or "ready")[:16],
                notes=("Historical Base44 import. " + row.get("notes", "").strip()).strip(),
            )
            counts["printed_checks" if inserted else "existing_rows_skipped"] += 1

        for item in candidates:
            if not item["line_count"]:
                continue
            existing = session.query(HROpeningPayrollBalance).filter_by(
                employee_email=item["email"], tax_year=2026
            ).first()
            if existing:
                counts["existing_rows_skipped"] += 1
                continue
            session.add(HROpeningPayrollBalance(
                employee_email=item["email"], tax_year=2026,
                gross_wages_cents=item["gross_cents"],
                social_security_wages_cents=item["gross_cents"],
                medicare_wages_cents=item["gross_cents"],
                futa_wages_cents=item["gross_cents"],
                utah_ui_wages_cents=item["gross_cents"],
                federal_withheld_cents=item["federal_cents"],
                utah_withheld_cents=item["state_cents"],
                employee_ss_withheld_cents=item["ss_cents"],
                employee_medicare_withheld_cents=item["medicare_cents"],
                source_note=(
                    f"Draft recovered from Base44 export SHA-256 {digest[:12]}. "
                    "Taxable wage bases require QuickBooks or qualified-review reconciliation."
                ),
                confirmed_by=(actor or "system").strip().lower(),
            ))
            counts["opening_balances"] += 1

        session.add(HRAuditEvent(
            actor_email=(actor or "system").strip().lower(),
            action="legacy_hr.imported", entity_type="legacy_hr_export",
            entity_id=digest[:16],
            details={"sha256": digest, "counts": counts, "legacy_tax_settings_activated": False},
        ))
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return {"digest": digest, "counts": counts}
