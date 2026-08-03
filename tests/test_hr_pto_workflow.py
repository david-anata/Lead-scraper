from datetime import date
from unittest.mock import MagicMock, patch

from sales_support_agent.services.hr import pto_workflow
from sales_support_agent.integrations.hr_google_calendar import HRGoogleCalendarClient


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


@patch("sales_support_agent.services.hr.pto_workflow.HRGoogleCalendarClient")
@patch("sales_support_agent.services.hr.pto_workflow.store")
def test_revoked_request_deletes_calendar_event_idempotently(store, client_type):
    item = _item("revoked")
    item["calendar_event_id"] = "event-42"
    store.get_pto_request.return_value = item
    client = MagicMock(configured=True)
    client_type.return_value = client
    assert pto_workflow.sync_revoked_request(42) == (True, "deleted")
    client.delete_event.assert_called_once_with("event-42")
    store.record_pto_calendar_sync.assert_called_once_with(
        42, status="deleted", clear_event=True
    )


@patch("sales_support_agent.services.hr.pto_workflow.HRGoogleCalendarClient")
def test_calendar_readiness_exposes_identity_but_never_credentials(client_type):
    client_type.return_value = MagicMock(
        configured=False, readiness_error="Calendar ID missing",
        readiness_state="calendar_id_missing",
        calendar_id="", service_account_email="calendar-agent@example.com",
    )
    result = pto_workflow.calendar_readiness()
    assert result == {
        "configured": False, "state": "calendar_id_missing", "status": "Setup needed",
        "reason": "Calendar ID missing", "calendar_id": "",
        "service_account_email": "calendar-agent@example.com",
    }


@patch("sales_support_agent.services.hr.pto_workflow.HRGoogleCalendarClient")
def test_calendar_connection_test_is_non_mutating(client_type):
    client_type.return_value.check_connection.return_value = (
        True, "ready", "Calendar connection confirmed."
    )
    assert pto_workflow.test_calendar_connection() == (
        True, "ready", "Calendar connection confirmed."
    )
    client_type.return_value.upsert_event.assert_not_called()
    client_type.return_value.delete_event.assert_not_called()


def test_calendar_connection_distinguishes_writer_permission_and_denial():
    client = HRGoogleCalendarClient(
        calendar_id="ooo@example.com",
        service_account_json='{"client_email":"agent@example.com","private_key":"test"}',
    )
    session = MagicMock()
    client._session = session
    response = MagicMock(status_code=200)
    response.json.return_value = {"accessRole": "writer"}
    session.get.return_value = response
    assert client.check_connection() == (
        True, "ready", "Calendar connection and event-write permission confirmed."
    )
    session.post.assert_not_called()
    session.patch.assert_not_called()

    response.json.return_value = {"accessRole": "reader"}
    ready, state, message = client.check_connection()
    assert not ready
    assert state == "permission_missing"
    assert "cannot change events" in message
