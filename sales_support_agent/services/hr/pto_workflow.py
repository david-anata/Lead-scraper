"""Privacy-safe PTO notifications and approved-OOO calendar projection."""

from __future__ import annotations

from datetime import timedelta
import logging

from sales_support_agent.integrations.hr_google_calendar import HRGoogleCalendarClient
from sales_support_agent.services.access.notify import _send
from sales_support_agent.services.hr import store

logger = logging.getLogger(__name__)


def notify_reviewer(settings, *, request_id: int, base_url: str) -> bool:
    item = store.get_pto_request(request_id)
    if not item or not item.get("reviewer_login_email"):
        return False
    sent = _send(
        settings,
        to_email=item["reviewer_login_email"],
        subject=f"Time-off request from {item['employee_name']}",
        text=(
            f"{item['employee_name']} requested {item['hours']:.2f} hours of time off "
            f"from {item['start_date']} through {item['end_date']}.\n\n"
            f"Review and approve or deny it inside Anata:\n{base_url.rstrip('/')}/admin/hr/time\n\n"
            "This email does not approve the request. Sign in to Anata to make and audit the decision."
        ),
    )
    store.record_pto_notification(request_id, sent=sent)
    return sent


def notify_employee(settings, *, request_id: int, base_url: str) -> bool:
    item = store.get_pto_request(request_id)
    if not item:
        return False
    return _send(
        settings, to_email=item["employee_login_email"],
        subject=f"Your time-off request was {item['status']}",
        text=(
            f"Your request for {item['hours']:.2f} hours from {item['start_date']} "
            f"through {item['end_date']} was {item['status']}.\n\n"
            f"View the current status in Anata:\n{base_url.rstrip('/')}/admin/hr/time"
        ),
    )


def sync_approved_request(request_id: int) -> tuple[bool, str]:
    item = store.get_pto_request(request_id)
    if not item or item["status"] != "approved":
        return False, "not_approved"
    client = HRGoogleCalendarClient()
    if not client.configured:
        store.record_pto_calendar_sync(request_id, status="setup_required",
                                       error=client.readiness_error)
        return False, "setup_required"
    payload = {
        "summary": f"{item['employee_name']} — OOO",
        "description": "Approved in Anata HR. Manage the approval in Anata, not on this calendar event.",
        "start": {"date": item["start_date"].isoformat()},
        # Google all-day event end dates are exclusive.
        "end": {"date": (item["end_date"] + timedelta(days=1)).isoformat()},
        "transparency": "transparent",
        "extendedProperties": {"private": {"anataPtoRequestId": str(request_id)}},
    }
    try:
        event_id = client.upsert_event(
            request_id=request_id, payload=payload,
            provider_event_id=item.get("calendar_event_id", ""),
        )
    except Exception as exc:  # approval remains authoritative; retry is visible
        logger.exception("OOO calendar sync failed for PTO request %s", request_id)
        store.record_pto_calendar_sync(request_id, status="failed", error=str(exc)[:500])
        return False, "failed"
    store.record_pto_calendar_sync(request_id, status="synced", event_id=event_id)
    return True, "synced"
