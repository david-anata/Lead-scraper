"""Audited overdue Building lead escalation and digest processing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, or_, select

from sales_support_agent.integrations.slack import SlackClient
from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import BuildingAuditEvent, BuildingInquiry
from sales_support_agent.services.building_inquiry_workspace import is_test_inquiry


#: Sequence steps top out at seven days after intake, so a month of lookback
#: covers every step that can newly come due, with slack for a paused job.
SEQUENCE_LOOKBACK_DAYS = 30
#: Past this, an unanswered lead is a dead record rather than today's work.
#: Reported in the result so the boundary is visible, never silent.
ESCALATION_WINDOW_DAYS = 90
#: Ceiling so one runaway query cannot pin the worker. Reported when reached.
MAX_SCAN = 2000
#: Rows rendered in the Slack digest before it summarises the remainder.
DIGEST_ROW_LIMIT = 20


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _step_due_at(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _stage(inquiry: BuildingInquiry) -> str:
    return str(((inquiry.payload_json or {}).get("_lifecycle") or {}).get("stage") or "new")


def _digest_blocks(inquiries: list[BuildingInquiry]) -> list[dict[str, Any]]:
    rows = []
    for inquiry in inquiries[:DIGEST_ROW_LIMIT]:
        due = _aware(inquiry.response_due_at).strftime("%b %d · %I:%M %p UTC") if inquiry.response_due_at else "Not set"
        rows.append(
            f"• <https://agent.anatainc.com/admin/building/inquiries/{inquiry.id}|{inquiry.name}>"
            f" · {inquiry.kind.title()} · due {due} · {inquiry.assigned_owner or 'unassigned'}"
        )
    # The header states the true total, so name the remainder rather than
    # letting the list read as the whole set.
    hidden = len(inquiries) - len(rows)
    if hidden > 0:
        rows.append(
            f"• _and {hidden} more — "
            "<https://agent.anatainc.com/admin/building|open Building Control>_"
        )
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Building leads need a response"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(rows),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "This is an internal escalation. It does not contact prospects or change booking status.",
                }
            ],
        },
    ]


def process_building_lead_follow_up(
    session_factory: Any,
    *,
    settings: Any,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Advance internal due states and send one escalation per unanswered lead."""

    effective_now = _aware(now or datetime.now(timezone.utc))
    candidates: list[BuildingInquiry] = []
    changed_sequences = 0
    with session_scope(session_factory) as session:
        # Select the work, not the first N rows ever created. The previous
        # `order_by(created_at).limit(500)` froze the window on the oldest 500
        # inquiries, so once that many existed no new lead was ever escalated.
        sequence_start = effective_now - timedelta(days=SEQUENCE_LOOKBACK_DAYS)
        escalation_start = effective_now - timedelta(days=ESCALATION_WINDOW_DAYS)
        rows = session.execute(
            select(BuildingInquiry)
            .where(
                or_(
                    BuildingInquiry.created_at >= sequence_start,
                    and_(
                        BuildingInquiry.response_due_at.is_not(None),
                        BuildingInquiry.response_due_at <= effective_now,
                        BuildingInquiry.response_due_at >= escalation_start,
                    ),
                )
            )
            .order_by(BuildingInquiry.created_at.desc())
            .limit(MAX_SCAN + 1)
        ).scalars().all()
        truncated = len(rows) > MAX_SCAN
        inquiries = list(rows[:MAX_SCAN])
        for inquiry in inquiries:
            if is_test_inquiry(
                name=inquiry.name,
                email=inquiry.email,
                source=inquiry.source,
            ):
                continue
            payload = dict(inquiry.payload_json or {})
            stage = _stage(inquiry)
            sequence = list(payload.get("_follow_up_sequence") or [])
            changed = False
            for raw in sequence:
                if raw.get("status") not in {"queued", "due"}:
                    continue
                due_at = _step_due_at(raw.get("due_at"))
                if due_at and due_at <= effective_now:
                    raw["status"] = "overdue"
                    changed = True
            if changed:
                changed_sequences += 1
                if not dry_run:
                    payload["_follow_up_sequence"] = sequence
                    inquiry.payload_json = payload
                    inquiry.updated_at = effective_now
                    session.add(inquiry)
            if stage != "new" or not inquiry.response_due_at:
                continue
            if _aware(inquiry.response_due_at) > effective_now:
                continue
            escalation = dict(payload.get("_lead_escalation") or {})
            # Re-escalate daily until the lead leaves the "new" stage. Previously
            # a single delivered nudge silenced a lead forever, however long it
            # went unanswered.
            last_attempt = _step_due_at(escalation.get("attempted_at"))
            if (
                escalation.get("status") == "delivered"
                and last_attempt is not None
                and last_attempt.date() == effective_now.date()
            ):
                continue
            candidates.append(inquiry)

        result: dict[str, Any] = {
            "status": "preview" if dry_run else "skipped",
            "overdue_count": len(candidates),
            "sequence_updates": changed_sequences,
            "inquiry_ids": [item.id for item in candidates],
            "provider": "slack",
            # Coverage is explicit so a bounded scan can never read as full cover.
            "scanned": len(inquiries),
            "scan_truncated": truncated,
            "sequence_lookback_days": SEQUENCE_LOOKBACK_DAYS,
            "escalation_window_days": ESCALATION_WINDOW_DAYS,
        }
        if dry_run or not candidates:
            return result

        client = SlackClient(settings)
        if not client.is_configured():
            provider_result = {"ok": False, "reason": "slack_not_configured"}
        else:
            try:
                provider_result = client.post_message(
                    text=f"{len(candidates)} Building lead(s) need a response.",
                    blocks=_digest_blocks(candidates),
                )
            except Exception as exc:
                provider_result = {"ok": False, "reason": str(exc)[:500]}
        status = (
            "delivered"
            if provider_result.get("ok")
            else "not_configured"
            if provider_result.get("reason") == "slack_not_configured"
            else "failed"
        )
        attempted_at = effective_now.isoformat()
        provider_reference = str(provider_result.get("ts") or "")
        for inquiry in candidates:
            payload = dict(inquiry.payload_json or {})
            previous = dict(payload.get("_lead_escalation") or {})
            attempt_count = int(previous.get("attempt_count") or 0) + 1
            payload["_lead_escalation"] = {
                "status": status,
                "attempt_count": attempt_count,
                "first_attempted_at": str(
                    previous.get("first_attempted_at") or attempted_at
                ),
                "provider": "slack",
                "provider_reference": provider_reference,
                "reason": str(provider_result.get("reason") or ""),
                "attempted_at": attempted_at,
            }
            inquiry.payload_json = payload
            inquiry.updated_at = effective_now
            session.add(inquiry)
            session.add(
                BuildingAuditEvent(
                    entity_type="inquiry",
                    entity_id=inquiry.id,
                    action=f"lead_overdue_escalation_{status}",
                    actor="job:building-lead-follow-up",
                    after_json={
                        "provider": "slack",
                        "provider_reference": provider_reference,
                        "customer_contacted": False,
                    },
                )
            )
        session.add(
            BuildingAuditEvent(
                entity_type="building_lead_digest",
                entity_id=effective_now.date().isoformat(),
                action=f"overdue_digest_{status}",
                actor="job:building-lead-follow-up",
                after_json={
                    "inquiry_count": len(candidates),
                    "provider_reference": provider_reference,
                },
            )
        )
        result.update(
            status=status,
            provider_reference=provider_reference,
            reason=str(provider_result.get("reason") or ""),
        )
        return result
