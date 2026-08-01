"""Authoring, validation, and deterministic rendering of contract templates.

The rendered document is text, not a provider object. Rendering never sends,
signs, invoices, or charges. The same function produces the operator preview and
the frozen document stored inside a prepared package, so what an operator
approves is exactly what the package records.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo


MOUNTAIN = ZoneInfo("America/Denver")

#: ``{{ field }}`` with tolerant whitespace. Anything else is left untouched so
#: an author can still write literal braces in prose.
TOKEN_RE = re.compile(r"\{\{\s*([a-z0-9_]+)\s*\}\}")

CONTRACT_TYPES = ("event", "membership")

#: Merge fields available per contract type. The event list mirrors
#: ``ALLOWED_MERGE_FIELDS`` in the agreement-readiness router, which remains the
#: enforcement point for prepared packages.
EVENT_MERGE_FIELDS = (
    "customer_name",
    "customer_email",
    "event_space",
    "setup_starts_at",
    "guest_starts_at",
    "guest_ends_at",
    "teardown_ends_at",
    "attendance",
    "subtotal_before_discount",
    "discount_amount",
    "discount_reason",
    "quote_total",
    "currency",
    "deposit_amount",
    "deposit_type",
    "cancellation_policy",
    "tax_terms",
    "included",
    "addons",
)
MEMBERSHIP_MERGE_FIELDS = (
    "member_name",
    "member_email",
    "workspace",
    "desk_count",
    "term_start",
    "term_end",
    "monthly_rate",
    "auto_renew",
    "notice_period_days",
    "included",
    "addons",
    "cancellation_policy",
    "tax_terms",
)

MERGE_FIELD_HELP = {
    "customer_name": "Responsible contact full name",
    "customer_email": "Responsible contact email",
    "event_space": "Reviewed event space name",
    "setup_starts_at": "Start of the setup window",
    "guest_starts_at": "Guest arrival time",
    "guest_ends_at": "Guest departure time",
    "teardown_ends_at": "End of the teardown window",
    "attendance": "Expected attendance",
    "subtotal_before_discount": "Quote subtotal before any discount",
    "discount_amount": "Discount applied to this booking",
    "discount_reason": "Recorded business reason for the discount",
    "quote_total": "Frozen quote total",
    "currency": "Quote currency",
    "deposit_amount": "Required payment amount",
    "deposit_type": "Deposit basis (fixed, percent, none)",
    "cancellation_policy": "Approved cancellation language",
    "tax_terms": "Resolved tax treatment",
    "included": "Included items from the approved rate plan",
    "addons": "Selected add-ons",
    "member_name": "Member full name",
    "member_email": "Member email",
    "workspace": "Assigned workspace or office",
    "desk_count": "Number of desks",
    "term_start": "Membership start date",
    "term_end": "Membership end date",
    "monthly_rate": "Monthly rate",
    "auto_renew": "Whether the term auto-renews",
    "notice_period_days": "Notice period in days",
}


class TemplateValidationError(ValueError):
    """Raised when authored template content cannot be safely approved."""


def merge_fields_for(contract_type: str) -> tuple[str, ...]:
    if contract_type == "membership":
        return MEMBERSHIP_MERGE_FIELDS
    return EVENT_MERGE_FIELDS


def tokens_used(*sources: str) -> list[str]:
    """Return every distinct merge token used, in first-appearance order."""

    seen: list[str] = []
    for source in sources:
        for match in TOKEN_RE.finditer(source or ""):
            token = match.group(1)
            if token not in seen:
                seen.append(token)
    return seen


def normalize_clauses(raw: Any) -> list[dict[str, str]]:
    """Coerce authored clauses into ordered ``{title, body}`` records."""

    clauses: list[dict[str, str]] = []
    for item in list(raw or []):
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        body = str(item.get("body") or "").strip()
        if not title and not body:
            continue
        clauses.append({"title": title, "body": body})
    return clauses


def validate_template_content(
    *,
    contract_type: str,
    body_markdown: str,
    clauses: Iterable[dict[str, str]],
) -> list[str]:
    """Validate authored content and return the derived merge-field list.

    Raises ``TemplateValidationError`` naming every unknown token, so an author
    never discovers a typo as a silently blank contract term.
    """

    if contract_type not in CONTRACT_TYPES:
        raise TemplateValidationError(
            f"Unsupported contract type: {contract_type}. "
            f"Choose one of {', '.join(CONTRACT_TYPES)}."
        )
    body = str(body_markdown or "").strip()
    clause_list = normalize_clauses(clauses)
    if not body and not clause_list:
        raise TemplateValidationError(
            "A template needs contract body text or at least one clause."
        )
    for clause in clause_list:
        if not clause["title"]:
            raise TemplateValidationError("Every clause needs a title.")
        if not clause["body"]:
            raise TemplateValidationError(
                f"Clause '{clause['title']}' has no body text."
            )
    sources = [body] + [c["title"] for c in clause_list] + [c["body"] for c in clause_list]
    used = tokens_used(*sources)
    allowed = set(merge_fields_for(contract_type))
    unknown = [token for token in used if token not in allowed]
    if unknown:
        raise TemplateValidationError(
            f"Unknown merge {'field' if len(unknown) == 1 else 'fields'}: "
            f"{', '.join(unknown)}. Allowed for a {contract_type} contract: "
            f"{', '.join(sorted(allowed))}."
        )
    if not used:
        raise TemplateValidationError(
            "A reusable template must use at least one merge field; otherwise it "
            "is customer-specific paper, not a template."
        )
    return used


def format_merge_value(field: str, value: Any) -> str:
    """Render one merge value as contract-ready text."""

    if value is None or value == "":
        return "[not provided]"
    if field in {
        "quote_total",
        "deposit_amount",
        "monthly_rate",
        "subtotal_before_discount",
        "discount_amount",
    }:
        try:
            return f"{int(value) / 100:,.2f}"
        except (TypeError, ValueError):
            return str(value)
    if field in {"setup_starts_at", "guest_starts_at", "guest_ends_at", "teardown_ends_at"}:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return str(value)
        return parsed.astimezone(MOUNTAIN).strftime("%B %d, %Y at %I:%M %p MT")
    if field in {"term_start", "term_end"}:
        return str(value)[:10]
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "none"
    if isinstance(value, dict):
        parts = [
            f"{key.replace('_', ' ')}: {item}"
            for key, item in value.items()
            if item not in (None, "")
        ]
        return "; ".join(parts) if parts else json.dumps(value, sort_keys=True)
    return str(value)


def render_document_text(
    *,
    name: str,
    body_markdown: str,
    clauses: Iterable[dict[str, str]],
    merge_values: dict[str, Any],
) -> str:
    """Substitute merge values and return the complete contract text.

    Deterministic: the same template and merge values always produce the same
    string, so the checksum stored on a prepared package is stable.
    """

    def substitute(source: str) -> str:
        def _replace(match: "re.Match[str]") -> str:
            field = match.group(1)
            if field not in merge_values:
                return "[not provided]"
            return format_merge_value(field, merge_values[field])

        return TOKEN_RE.sub(_replace, source or "")

    sections = [f"# {name.strip()}"] if name.strip() else []
    body = substitute(str(body_markdown or "").strip())
    if body:
        sections.append(body)
    for index, clause in enumerate(normalize_clauses(clauses), start=1):
        sections.append(f"## {index}. {substitute(clause['title'])}")
        sections.append(substitute(clause["body"]))
    return "\n\n".join(section for section in sections if section).strip() + "\n"


def document_checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def unresolved_fields(text: str) -> bool:
    """True when the rendered document still contains a missing value."""

    return "[not provided]" in text


def render_document_html(text: str) -> str:
    """Convert rendered contract text to HTML for preview and printing."""

    try:
        import markdown as markdown_lib
    except ModuleNotFoundError:  # pragma: no cover — markdown is a hard dependency
        import html as html_lib

        return f"<pre>{html_lib.escape(text)}</pre>"
    return markdown_lib.markdown(text, extensions=["tables", "sane_lists"])


def next_version(existing: Iterable[int]) -> int:
    versions = [int(item) for item in existing if item is not None]
    return (max(versions) + 1) if versions else 1


def template_payload(row: Any) -> dict[str, Optional[Any]]:
    """Shape a template row for the editor."""

    return {
        "id": row.id,
        "template_key": row.template_key,
        "version": row.version,
        "name": row.name,
        "status": row.status,
        "contract_type": getattr(row, "contract_type", "") or "event",
        "template_reference": row.template_reference or "",
        "body_markdown": getattr(row, "body_markdown", "") or "",
        "clauses": normalize_clauses(getattr(row, "clauses_json", []) or []),
        "merge_fields": list(row.merge_fields_json or []),
        "approval_evidence": row.approval_evidence or "",
        "approved_by": row.approved_by or "",
        "approved_at": row.approved_at,
        "editable": row.status == "draft",
    }
