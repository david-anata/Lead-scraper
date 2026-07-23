"""Retry-safe HubSpot contact sync for durable building inquiries."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sales_support_agent.integrations.hubspot import HubSpotClient
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingContact,
    BuildingInquiry,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sync_state(inquiry: BuildingInquiry) -> dict[str, Any]:
    payload = dict(inquiry.payload_json or {})
    state = dict(payload.get("_hubspot_sync") or {})
    state.setdefault("attempt_count", 0)
    state.setdefault("note_synced", False)
    return state


def _save_sync_state(inquiry: BuildingInquiry, state: dict[str, Any]) -> None:
    payload = dict(inquiry.payload_json or {})
    payload["_hubspot_sync"] = state
    inquiry.payload_json = payload
    inquiry.updated_at = _now()


def sync_building_inquiry_to_hubspot(
    *,
    session,
    inquiry: BuildingInquiry,
    contact: BuildingContact,
    client: HubSpotClient,
    actor: str,
) -> bool:
    """Upsert one HubSpot contact and note, retaining any failure for operator retry."""

    state = _sync_state(inquiry)
    state["attempt_count"] = int(state.get("attempt_count") or 0) + 1
    state["last_attempt_at"] = _now().isoformat()
    try:
        contact_id = inquiry.hubspot_contact_id or contact.hubspot_contact_id
        if not contact_id:
            existing = client.find_contact_by_email(inquiry.email)
            if existing:
                contact_id = str(existing.get("id") or "")
            else:
                created = client.create_contact({
                    "email": inquiry.email,
                    "firstname": inquiry.name,
                    **({"phone": inquiry.phone} if inquiry.phone else {}),
                })
                contact_id = str((created or {}).get("id", "") or "")
        if not contact_id:
            raise RuntimeError("HubSpot did not return a contact ID.")
        inquiry.hubspot_contact_id = contact_id
        contact.hubspot_contact_id = contact_id
        if not bool(state.get("note_synced")):
            client.create_contact_note(
                contact_id=contact_id,
                body=(
                    f"Anata Building {inquiry.kind} inquiry."
                    f"<br>Source: {inquiry.source}"
                    f"<br>Preferred date: {inquiry.preferred_date or 'Not supplied'}"
                    f"<br>Agent inquiry ID: {inquiry.id}"
                ),
            )
            state["note_synced"] = True
        state["last_error"] = ""
        state["synced_at"] = _now().isoformat()
        inquiry.status = "new"
        _save_sync_state(inquiry, state)
        session.add(
            BuildingAuditEvent(
                entity_type="inquiry",
                entity_id=inquiry.id,
                action="hubspot_sync_succeeded",
                actor=actor,
                after_json={
                    "hubspot_contact_id": contact_id,
                    "attempt_count": state["attempt_count"],
                },
            )
        )
        return True
    except Exception as exc:  # external failure must not lose the inquiry
        state["last_error"] = str(exc)[:1000]
        inquiry.status = "crm_sync_needed"
        _save_sync_state(inquiry, state)
        session.add(
            BuildingAuditEvent(
                entity_type="inquiry",
                entity_id=inquiry.id,
                action="hubspot_sync_failed",
                actor=actor,
                after_json={
                    "error": state["last_error"],
                    "attempt_count": state["attempt_count"],
                },
            )
        )
        return False
