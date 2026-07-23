"""Deterministic attribution and funnel reporting for Anata Building operations."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingAgreement,
    BuildingAuditEvent,
    BuildingCampaign,
    BuildingCampaignRecipient,
    BuildingDepositEvidence,
    BuildingEmailEvent,
    BuildingInquiry,
    BuildingInvoice,
    BuildingPayment,
    BuildingReservation,
    BuildingSpace,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime | None) -> str:
    aware = _aware(value)
    return aware.isoformat() if aware else ""


def _detail(details: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(details.get(key) or "").strip()
        if value:
            return value[:500]
    return ""


def build_attribution(
    *,
    source: str,
    source_reference: str,
    details: dict[str, Any],
    captured_at: datetime | None = None,
) -> dict[str, str]:
    """Normalize one attributable touch without inventing unavailable fields."""

    captured = _aware(captured_at) or _now()
    return {
        "source": (source or "unknown").strip()[:64],
        "medium": _detail(details, "medium", "utm_medium", "utmMedium"),
        "campaign": _detail(details, "campaign", "utm_campaign", "utmCampaign"),
        "content": _detail(details, "content", "utm_content", "utmContent"),
        "term": _detail(details, "term", "utm_term", "utmTerm"),
        "source_reference": (source_reference or "").strip()[:255],
        "landing_page": _detail(details, "landing_page", "landingPage"),
        "offering_id": _detail(details, "offering_id", "offeringId")[:64],
        "captured_at": captured.isoformat(),
    }


def apply_attribution(
    *,
    inquiry: BuildingInquiry,
    contact_metadata: dict[str, Any],
    attribution: dict[str, str],
    first_attribution: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Persist inquiry attribution and update contact first/latest touch safely."""

    inquiry_payload = dict(inquiry.payload_json or {})
    inquiry_payload["_attribution"] = dict(attribution)
    inquiry.payload_json = inquiry_payload

    metadata = dict(contact_metadata or {})
    history = dict(metadata.get("_building_attribution") or {})
    if not history.get("first_touch"):
        history["first_touch"] = dict(first_attribution or attribution)
    history["latest_touch"] = dict(attribution)
    history["touch_count"] = int(history.get("touch_count") or 0) + 1
    metadata["_building_attribution"] = history
    return metadata


def _lifecycle(inquiry: BuildingInquiry) -> dict[str, Any]:
    return dict((inquiry.payload_json or {}).get("_lifecycle") or {})


def _stage_history(
    audit_rows: list[BuildingAuditEvent],
) -> tuple[dict[str, set[str]], dict[str, dict[str, datetime]]]:
    reached: dict[str, set[str]] = defaultdict(set)
    reached_at: dict[str, dict[str, datetime]] = defaultdict(dict)
    for event in audit_rows:
        if event.entity_type != "reservation":
            continue
        if event.action == "created":
            stage = str((event.after_json or {}).get("status") or "inquiry")
        elif event.action == "status_changed":
            stage = str((event.after_json or {}).get("status") or "")
        else:
            continue
        if not stage:
            continue
        reached[event.entity_id].add(stage)
        when = _aware(event.created_at)
        if when and stage not in reached_at[event.entity_id]:
            reached_at[event.entity_id][stage] = when
    return reached, reached_at


def _elapsed_hours(start: datetime | None, end: datetime | None) -> float | None:
    start_at = _aware(start)
    end_at = _aware(end)
    if not start_at or not end_at or end_at < start_at:
        return None
    return round((end_at - start_at).total_seconds() / 3600, 2)


def _median(values: list[float]) -> float | None:
    return round(float(median(values)), 2) if values else None


def build_building_analytics(session, *, now: datetime | None = None) -> dict[str, Any]:
    """Return evidence-backed all-time funnels and current operating measures."""

    generated_at = _aware(now) or _now()
    inquiries = session.execute(
        select(BuildingInquiry).order_by(BuildingInquiry.created_at)
    ).scalars().all()
    reservations = session.execute(select(BuildingReservation)).scalars().all()
    audits = session.execute(
        select(BuildingAuditEvent).where(
            BuildingAuditEvent.entity_type.in_(("inquiry", "reservation"))
        )
    ).scalars().all()
    agreements = session.execute(select(BuildingAgreement)).scalars().all()
    deposits = session.execute(select(BuildingDepositEvidence)).scalars().all()
    invoices = session.execute(select(BuildingInvoice)).scalars().all()
    payments = session.execute(select(BuildingPayment)).scalars().all()
    campaigns = session.execute(select(BuildingCampaign)).scalars().all()
    recipients = session.execute(select(BuildingCampaignRecipient)).scalars().all()
    email_events = session.execute(select(BuildingEmailEvent)).scalars().all()
    spaces = session.execute(select(BuildingSpace)).scalars().all()

    reached, reached_at = _stage_history(audits)
    reservations_by_inquiry: dict[str, list[BuildingReservation]] = defaultdict(list)
    reservations_by_id = {item.id: item for item in reservations}
    for item in reservations:
        if item.inquiry_id:
            reservations_by_inquiry[item.inquiry_id].append(item)
        reached[item.id].add(item.status)

    inquiry_counts = Counter(item.kind for item in inquiries)
    source_counts = Counter(item.source or "unknown" for item in inquiries)
    response_hours: list[float] = []
    response_by_source: dict[str, list[float]] = defaultdict(list)
    qualified_workspace_inquiries = 0
    for inquiry in inquiries:
        lifecycle = _lifecycle(inquiry)
        first_response_raw = str(lifecycle.get("first_responded_at") or "")
        first_response = None
        if first_response_raw:
            try:
                first_response = datetime.fromisoformat(first_response_raw.replace("Z", "+00:00"))
            except ValueError:
                first_response = None
        elapsed = _elapsed_hours(inquiry.created_at, first_response)
        if elapsed is not None:
            response_hours.append(elapsed)
            response_by_source[inquiry.source or "unknown"].append(elapsed)
        linked = reservations_by_inquiry.get(inquiry.id, [])
        if inquiry.kind == "workspace" and (
            lifecycle.get("stage") == "qualified" or any(
                row.kind == "workspace"
                and reached[row.id] - {"inquiry", "cancelled"}
                for row in linked
            )
        ):
            qualified_workspace_inquiries += 1

    workspace_reservations = [item for item in reservations if item.kind == "workspace"]
    event_reservations = [item for item in reservations if item.kind == "event"]
    agreement_by_reservation = defaultdict(list)
    for item in agreements:
        agreement_by_reservation[item.reservation_id].append(item)
    deposit_by_reservation = defaultdict(list)
    for item in deposits:
        deposit_by_reservation[item.reservation_id].append(item)

    def ever(rows: list[BuildingReservation], stages: set[str]) -> int:
        return sum(1 for row in rows if reached[row.id] & stages)

    workspace_funnel = {
        "inquiries": inquiry_counts.get("workspace", 0),
        "qualified": max(
            qualified_workspace_inquiries,
            ever(workspace_reservations, {"qualified", "tour_scheduled", "tour_completed", "proposal_sent", "contract_pending", "deposit_due", "confirmed", "occupied", "renewal", "move_out", "completed"}),
        ),
        "tours": ever(workspace_reservations, {"tour_scheduled", "tour_completed"}),
        "proposals": ever(workspace_reservations, {"proposal_sent"}),
        "signed": sum(
            1
            for row in workspace_reservations
            if any(item.status == "signed" for item in agreement_by_reservation[row.id])
        ),
        "paid": sum(
            1
            for row in workspace_reservations
            if any(item.status == "paid" for item in deposit_by_reservation[row.id])
        ),
        "occupied": ever(workspace_reservations, {"occupied", "renewal", "move_out", "completed"}),
    }
    event_funnel = {
        "inquiries": inquiry_counts.get("event", 0),
        "holds": ever(event_reservations, {"soft_hold"}),
        "quotes": ever(event_reservations, {"quote_sent"}),
        "signed": sum(
            1
            for row in event_reservations
            if any(item.status == "signed" for item in agreement_by_reservation[row.id])
        ),
        "deposits": sum(
            1
            for row in event_reservations
            if any(item.status == "paid" for item in deposit_by_reservation[row.id])
        ),
        "confirmed": ever(event_reservations, {"confirmed", "pre_event", "completed"}),
        "completed": ever(event_reservations, {"completed"}),
    }

    holds_started = sum(1 for item in reservations if "soft_hold" in reached[item.id])
    holds_expired = sum(1 for item in reservations if "expired" in reached[item.id])
    contract_cycle_hours: list[float] = []
    deposit_cycle_hours: list[float] = []
    for reservation in reservations:
        contract_start = reached_at[reservation.id].get("contract_pending")
        signed_times = [
            _aware(item.signed_at)
            for item in agreement_by_reservation[reservation.id]
            if item.status == "signed" and item.signed_at
        ]
        signed_times = [item for item in signed_times if item]
        elapsed = _elapsed_hours(contract_start, min(signed_times) if signed_times else None)
        if elapsed is not None:
            contract_cycle_hours.append(elapsed)
        deposit_start = reached_at[reservation.id].get("deposit_due")
        paid_times = [
            _aware(item.recorded_at)
            for item in deposit_by_reservation[reservation.id]
            if item.status == "paid"
        ]
        paid_times = [item for item in paid_times if item]
        elapsed = _elapsed_hours(deposit_start, min(paid_times) if paid_times else None)
        if elapsed is not None:
            deposit_cycle_hours.append(elapsed)

    posted_payments = [
        item for item in payments if item.status == "paid" and item.posted_at is not None
    ]
    collected_cents = sum(item.amount_cents for item in posted_payments)
    invoiced_cents = sum(item.amount_due_cents for item in invoices)
    open_invoices = [
        item for item in invoices
        if item.status not in {"paid", "void", "uncollectible"}
        and item.amount_due_cents > item.amount_paid_cents
    ]
    overdue_invoices = [
        item for item in open_invoices
        if _aware(item.due_at) and _aware(item.due_at) < generated_at
    ]

    source_finance: dict[str, dict[str, int]] = defaultdict(
        lambda: {"inquiries": 0, "invoiced_cents": 0, "posted_collected_cents": 0}
    )
    inquiry_by_id = {item.id: item for item in inquiries}
    for inquiry in inquiries:
        source_finance[inquiry.source or "unknown"]["inquiries"] += 1
    invoice_by_id = {item.id: item for item in invoices}
    for invoice in invoices:
        reservation = reservations_by_id.get(invoice.reservation_id or "")
        inquiry = inquiry_by_id.get(reservation.inquiry_id or "") if reservation else None
        source = (inquiry.source if inquiry else (reservation.source if reservation else "")) or "unattributed"
        source_finance[source]["invoiced_cents"] += invoice.amount_due_cents
    for payment in posted_payments:
        invoice = invoice_by_id.get(payment.invoice_id)
        reservation = reservations_by_id.get(invoice.reservation_id or "") if invoice else None
        inquiry = inquiry_by_id.get(reservation.inquiry_id or "") if reservation else None
        source = (inquiry.source if inquiry else (reservation.source if reservation else "")) or "unattributed"
        source_finance[source]["posted_collected_cents"] += payment.amount_cents

    window_start = generated_at - timedelta(days=30)
    rentable_spaces = [item for item in spaces if item.is_public]
    reserved_hours = 0.0
    for row in reservations:
        if not (reached[row.id] & {"confirmed", "pre_event", "occupied", "renewal", "completed"}):
            continue
        start = max(_aware(row.starts_at) or window_start, window_start)
        end = min(_aware(row.ends_at) or generated_at, generated_at)
        if end > start:
            reserved_hours += (end - start).total_seconds() / 3600
    capacity_hours = len(rentable_spaces) * 30 * 24

    recipient_counts = Counter(item.status for item in recipients)
    provider_event_counts = Counter(item.event_type for item in email_events)
    return {
        "generated_at": generated_at.isoformat(),
        "inquiries": {
            "total": len(inquiries),
            "by_kind": dict(sorted(inquiry_counts.items())),
            "by_source": dict(sorted(source_counts.items())),
            "responded": len(response_hours),
            "median_first_response_hours": _median(response_hours),
            "median_first_response_hours_by_source": {
                key: _median(values) for key, values in sorted(response_by_source.items())
            },
        },
        "workspace_funnel": workspace_funnel,
        "event_funnel": event_funnel,
        "operations": {
            "hold_expiration_rate": round(holds_expired / holds_started, 4) if holds_started else None,
            "holds_started": holds_started,
            "holds_expired": holds_expired,
            "median_contract_cycle_hours": _median(contract_cycle_hours),
            "median_deposit_cycle_hours": _median(deposit_cycle_hours),
            "scheduled_utilization_30d": round(reserved_hours / capacity_hours, 4) if capacity_hours else None,
            "rentable_space_count": len(rentable_spaces),
            "renewals_started": ever(workspace_reservations, {"renewal"}),
            "move_outs_started": ever(workspace_reservations, {"move_out", "completed"}),
        },
        "finance": {
            "invoiced_cents": invoiced_cents,
            "posted_collected_cents": collected_cents,
            "open_invoice_count": len(open_invoices),
            "overdue_invoice_count": len(overdue_invoices),
            "overdue_cents": sum(
                max(0, item.amount_due_cents - item.amount_paid_cents)
                for item in overdue_invoices
            ),
            "by_source": [
                {"source": source, **values}
                for source, values in sorted(source_finance.items())
            ],
        },
        "campaigns": {
            "campaign_count": len(campaigns),
            "sent_campaign_count": sum(
                1 for item in campaigns if item.status in {"sent", "sent_with_errors"}
            ),
            "recipient_statuses": dict(sorted(recipient_counts.items())),
            "provider_event_counts": dict(sorted(provider_event_counts.items())),
            "engagement_tracking": "not_configured",
        },
    }
