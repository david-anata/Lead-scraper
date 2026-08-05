"""Deterministic expiration for temporary Anata Building holds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, select

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import (
    BuildingAuditEvent,
    BuildingAvailabilityBlock,
    BuildingReservation,
)
from sales_support_agent.services.building_calendar import queue_calendar_projection
from sales_support_agent.services.building_agreement_readiness import (
    propagate_event_readiness_terminal_state,
)
from sales_support_agent.integrations.slack import SlackClient


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def expire_building_holds(
    session_factory,
    *,
    as_of: datetime | None = None,
    dry_run: bool = False,
    settings: Any | None = None,
    actor: str = "job:building-hold-expiration",
) -> dict[str, Any]:
    """Release every soft hold whose approved expiration has passed."""

    now = _aware(as_of or datetime.now(timezone.utc))
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(BuildingReservation)
            .where(BuildingReservation.status == "soft_hold")
            .order_by(BuildingReservation.hold_expires_at, BuildingReservation.id)
        ).scalars().all()
        expired = [
            row
            for row in rows
            if row.hold_expires_at is not None
            and _aware(row.hold_expires_at) <= now
        ]
        delivered_warning_ids = set(
            session.execute(
                select(BuildingAuditEvent.entity_id).where(
                    BuildingAuditEvent.entity_type == "reservation",
                    BuildingAuditEvent.action == "hold_expiry_warning_delivered",
                )
            ).scalars().all()
        )
        expiring = [
            row for row in rows
            if row.hold_expires_at is not None
            and now < _aware(row.hold_expires_at) <= now + timedelta(hours=24)
            and row.id not in delivered_warning_ids
        ]
        preview = [
            {
                "reservation_id": row.id,
                "space_id": row.space_id,
                "hold_expires_at": _aware(row.hold_expires_at).isoformat(),
            }
            for row in expired
        ]
        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "expired_count": len(expired),
                "expired": preview,
                "expiring_count": len(expiring),
                "expiring_ids": [row.id for row in expiring],
            }

        warning_status = "skipped"
        warning_reference = ""
        warning_reason = ""
        if expiring:
            client = SlackClient(settings) if settings is not None else None
            if client is None or not client.is_configured():
                warning_status = "not_configured"
                warning_reason = "slack_not_configured"
            else:
                lines = [
                    f"• <https://agent.anatainc.com/admin/building/bookings/{row.id}|{row.id}> expires {_aware(row.hold_expires_at).strftime('%b %d · %I:%M %p UTC')}"
                    for row in expiring[:20]
                ]
                try:
                    provider_result = client.post_message(
                        text=f"{len(expiring)} Building hold(s) expire within 24 hours.",
                        blocks=[
                            {"type": "header", "text": {"type": "plain_text", "text": "Building holds expire soon"}},
                            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
                            {"type": "context", "elements": [{"type": "mrkdwn", "text": "Review or release each hold. This alert does not contact customers."}]},
                        ],
                    )
                except Exception as exc:
                    provider_result = {"ok": False, "reason": str(exc)[:500]}
                warning_status = "delivered" if provider_result.get("ok") else "failed"
                warning_reference = str(provider_result.get("ts") or "")
                warning_reason = str(provider_result.get("reason") or "")
            for row in expiring:
                session.add(BuildingAuditEvent(
                    entity_type="reservation",
                    entity_id=row.id,
                    action=f"hold_expiry_warning_{warning_status}",
                    actor=actor,
                    after_json={
                        "provider": "slack",
                        "provider_reference": warning_reference,
                        "reason": warning_reason,
                        "hold_expires_at": _aware(row.hold_expires_at).isoformat(),
                        "customer_contacted": False,
                    },
                ))

        for row in expired:
            before = {
                "status": row.status,
                "hold_expires_at": _aware(row.hold_expires_at).isoformat(),
            }
            session.execute(
                delete(BuildingAvailabilityBlock).where(
                    BuildingAvailabilityBlock.source_reference
                    == f"reservation:{row.id}"
                )
            )
            row.status = "expired"
            row.hold_expires_at = None
            row.updated_at = now
            propagate_event_readiness_terminal_state(
                session,
                row,
                terminal_status="expired",
                actor=actor,
            )
            queue_calendar_projection(session, row)
            session.add(BuildingAuditEvent(
                entity_type="reservation",
                entity_id=row.id,
                action="hold_expired_automatically",
                actor=actor,
                before_json=before,
                after_json={
                    "status": "expired",
                    "availability_released": True,
                    "expired_at": now.isoformat(),
                },
            ))
        return {
            "ok": True,
            "dry_run": False,
            "expired_count": len(expired),
            "expired": preview,
            "expiring_count": len(expiring),
            "expiring_ids": [row.id for row in expiring],
            "warning_status": warning_status,
            "warning_provider_reference": warning_reference,
        }
