from __future__ import annotations

import csv
import io
import re
from typing import Any


_TEMPLATE_PATTERN = re.compile(r"{{\s*([^{}]+?)\s*}}")
_EMAIL_FIELDS = ("email", "recipient_email", "to", "recipient")
_SUBJECT_FIELDS = ("subject", "email_subject", "draft_subject")
_BODY_FIELDS = ("body", "email_body", "message", "draft_body")
_FIRST_NAME_FIELDS = ("first_name", "firstname", "first")
_LAST_NAME_FIELDS = ("last_name", "lastname", "last")
_COMPANY_FIELDS = ("company", "company_name", "account", "account_name", "brand")


def _normalize_key(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower())
    return normalized.strip("_")


def _decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _first_present(context: dict[str, str], field_names: tuple[str, ...]) -> str:
    for field_name in field_names:
        value = context.get(field_name, "").strip()
        if value:
            return value
    return ""


def _render_template(template: str, context: dict[str, str]) -> str:
    if not template:
        return ""

    def replace(match: re.Match[str]) -> str:
        key = _normalize_key(match.group(1))
        return context.get(key, "")

    return _TEMPLATE_PATTERN.sub(replace, template).strip()


def _row_context(row: dict[str, str | None], sales_objective: str) -> dict[str, str]:
    context: dict[str, str] = {}
    for raw_key, raw_value in row.items():
        normalized_key = _normalize_key(str(raw_key or ""))
        if not normalized_key:
            continue
        context[normalized_key] = str(raw_value or "").strip()

    first_name = _first_present(context, _FIRST_NAME_FIELDS)
    last_name = _first_present(context, _LAST_NAME_FIELDS)
    company = _first_present(context, _COMPANY_FIELDS)
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()

    if first_name:
        context.setdefault("first_name", first_name)
    if last_name:
        context.setdefault("last_name", last_name)
    if full_name:
        context.setdefault("full_name", full_name)
    if company:
        context.setdefault("company", company)

    email = _first_present(context, _EMAIL_FIELDS)
    if email:
        context.setdefault("email", email)

    if sales_objective.strip():
        context["objective"] = sales_objective.strip()

    return context


def create_bulk_draft_payloads(
    *,
    csv_bytes: bytes,
    sales_objective: str,
    subject_template: str,
    body_template: str,
) -> dict[str, Any]:
    decoded = _decode_csv(csv_bytes)
    reader = csv.DictReader(io.StringIO(decoded))
    if not reader.fieldnames:
        raise ValueError("CSV must include a header row.")

    prepared_rows: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    rows_total = 0

    for row_number, row in enumerate(reader, start=2):
        rows_total += 1
        context = _row_context(row, sales_objective)
        email = _first_present(context, _EMAIL_FIELDS)
        if not email:
            failures.append(
                {
                    "row_number": str(row_number),
                    "error": "Missing email column value.",
                }
            )
            continue

        subject = _first_present(context, _SUBJECT_FIELDS) or _render_template(subject_template, context)
        body = _first_present(context, _BODY_FIELDS) or _render_template(body_template, context)

        if not subject:
            failures.append(
                {
                    "row_number": str(row_number),
                    "email": email,
                    "error": "Missing subject. Add a subject column or provide a subject template.",
                }
            )
            continue

        if not body:
            failures.append(
                {
                    "row_number": str(row_number),
                    "email": email,
                    "error": "Missing body. Add a body column or provide a body template.",
                }
            )
            continue

        prepared_rows.append(
            {
                "row_number": str(row_number),
                "email": email,
                "subject": subject,
                "body": body,
                "first_name": context.get("first_name", ""),
                "company": context.get("company", ""),
            }
        )

    return {
        "rows_total": rows_total,
        "prepared_rows": prepared_rows,
        "failed_rows": failures,
        "available_placeholders": sorted({_normalize_key(str(name or "")) for name in (reader.fieldnames or []) if _normalize_key(str(name or ""))} | {"email", "first_name", "last_name", "full_name", "company", "objective"}),
    }
