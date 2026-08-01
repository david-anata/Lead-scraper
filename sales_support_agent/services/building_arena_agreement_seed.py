"""Seed the owner-approved Arena business terms as a legal-review template.

The seed is additive and provider-neutral. It never approves the template,
generates a customer document, sends anything, or changes a booking.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAgreementTemplate,
    BuildingAuditEvent,
)
from sales_support_agent.services.building_contract_templates import (
    validate_template_content,
)


ARENA_TEMPLATE_ID = "arena-event-agreement-business-terms-v2"
ARENA_TEMPLATE_KEY = "arena-event-agreement"
ARENA_TEMPLATE_VERSION = 2
ARENA_TEMPLATE_NAME = "Arena event agreement business terms"
ARENA_DOCUMENT_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "building"
    / "agreements"
    / "arena-event-agreement-business-terms-v2.md"
)


def _artifact() -> tuple[str, str, str, list[str]]:
    body = ARENA_DOCUMENT_PATH.read_text(encoding="utf-8").strip()
    checksum = hashlib.sha256(body.encode("utf-8")).hexdigest()
    reference = (
        "repository:docs/building/agreements/"
        f"arena-event-agreement-business-terms-v2.md#sha256={checksum}"
    )
    merge_fields = validate_template_content(
        contract_type="event",
        body_markdown=body,
        clauses=[],
    )
    return body, checksum, reference, merge_fields


def ensure_arena_review_template(
    session_factory,
    *,
    actor: str = "system:arena-agreement-seed",
) -> str:
    """Create or reconcile the Arena legal-review template.

    Returns ``created``, ``reconciled``, or ``unchanged``. Existing approved or
    retired versions are never edited. Conflicting evidence fails closed so a
    deployment cannot silently replace operator-authored legal material.
    """

    body, checksum, reference, merge_fields = _artifact()
    expected_identity = {
        "template_key": ARENA_TEMPLATE_KEY,
        "version": ARENA_TEMPLATE_VERSION,
        "name": ARENA_TEMPLATE_NAME,
        "contract_type": "event",
        "template_reference": reference,
    }
    now = datetime.now(timezone.utc)
    with session_scope(session_factory) as session:
        row = session.get(BuildingAgreementTemplate, ARENA_TEMPLATE_ID)
        if row is None:
            row = BuildingAgreementTemplate(
                id=ARENA_TEMPLATE_ID,
                **expected_identity,
                status="in_review",
                body_markdown=body,
                clauses_json=[],
                merge_fields_json=merge_fields,
                created_by=actor,
                updated_at=now,
            )
            session.add(row)
            outcome = "created"
        else:
            actual_identity = {
                "template_key": row.template_key,
                "version": row.version,
                "name": row.name,
                "contract_type": row.contract_type or "event",
                "template_reference": row.template_reference,
            }
            if actual_identity != expected_identity:
                raise RuntimeError(
                    "Existing Arena agreement template conflicts with the "
                    "versioned repository artifact; create a reviewed new "
                    "version instead of overwriting it."
                )
            if row.status in {"approved", "retired"}:
                return "unchanged"
            existing_body = (row.body_markdown or "").strip()
            if existing_body and existing_body != body:
                raise RuntimeError(
                    "Existing Arena agreement body conflicts with the versioned "
                    "repository artifact; deployment stopped without changes."
                )
            row.status = "in_review"
            row.body_markdown = body
            row.clauses_json = []
            row.merge_fields_json = merge_fields
            row.updated_at = now
            outcome = "reconciled" if not existing_body else "unchanged"
        if outcome != "unchanged":
            session.add(
                BuildingAuditEvent(
                    entity_type="agreement_template",
                    entity_id=ARENA_TEMPLATE_ID,
                    action=f"arena_agreement_business_terms_{outcome}",
                    actor=actor,
                    after_json={
                        "status": "in_review",
                        "artifact_checksum": checksum,
                        "merge_fields": merge_fields,
                        "legal_approval": False,
                        "provider_write": False,
                        "customer_delivery": False,
                    },
                )
            )
        return outcome
