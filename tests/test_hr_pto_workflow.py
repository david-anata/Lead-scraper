from datetime import date
from unittest.mock import MagicMock, patch

from sales_support_agent.services.hr import pto_workflow


def _item(status="approved"):
    return {
        "id": 42, "employee_email": "gabe@anatainc.com",
        "employee_login_email": "gabe@example.com", "employee_name": "Gabe",
        "start_date": date(2026, 8, 3), "end_date": date(2026, 8, 4),
        "hours": 16.0, "status": status, "reviewer_email": "val@anatainc.com",
        "reviewer_login_email": "val.personal@example.com",
        "calendar_event_id": "",
    }


@patch("sales_support_agent.services.hr.pto_workflow.store")
@patch("sales_support_agent.services.hr.pto_workflow._send", return_value=True)
def test_manager_email_links_to_authenticated_review(send, store):
    store.get_pto_request.return_value = _item("pending")
    assert pto_workflow.notify_reviewer(object(), request_id=42,
                                        base_url="https://agent.anatainc.com")
    message = send.call_args.kwargs["text"]
    assert send.call_args.kwargs["to_email"] == "val.personal@example.com"
    assert "/admin/hr/time" in message
    assert "does not approve" in message
    store.record_pto_notification.assert_called_once_with(42, sent=True)


@patch("sales_support_agent.services.hr.pto_workflow.HRGoogleCalendarClient")
@patch("sales_support_agent.services.hr.pto_workflow.store")
def test_only_approved_request_is_projected_and_end_date_is_exclusive(store, client_type):
    store.get_pto_request.return_value = _item()
    client = MagicMock(configured=True)
    client.upsert_event.return_value = "event-42"
    client_type.return_value = client
    assert pto_workflow.sync_approved_request(42) == (True, "synced")
    payload = client.upsert_event.call_args.kwargs["payload"]
    assert payload["end"]["date"] == "2026-08-05"
    store.record_pto_calendar_sync.assert_called_once_with(
        42, status="synced", event_id="event-42"
    )


@patch("sales_support_agent.services.hr.pto_workflow.store")
def test_pending_request_never_reaches_calendar(store):
    store.get_pto_request.return_value = _item("pending")
    assert pto_workflow.sync_approved_request(42) == (False, "not_approved")
