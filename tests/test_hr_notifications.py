"""Privacy-safe and idempotent HR reminder tests."""

from datetime import date, datetime, timezone
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.hr import (
    HREmployee,
    HREmployeeHandbook,
    HREmployeeOnboarding,
    HRHandbookAcknowledgement,
    HRTimeEntry,
)
from sales_support_agent.services.hr import notifications


def test_digest_is_aggregate_and_sent_once_per_recipient():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(HREmployeeOnboarding(
            employee_email="employee@anatainc.com", status="employee_in_progress"
        ))
        session.add(HRTimeEntry(
            employee_email="employee@anatainc.com", date=date(2026, 7, 22),
            start_time="09:00", stop_time="",
        ))
        session.commit()

    with (
        mock.patch.object(notifications, "get_engine", return_value=engine),
        mock.patch.object(
            notifications, "_recipients", return_value=["david@anatainc.com"]
        ),
        mock.patch.object(notifications, "_send", return_value=True) as send,
    ):
        first = notifications.run_daily_digest(
            object(), base_url="https://agent.anatainc.com",
            today=date(2026, 7, 23),
        )
        second = notifications.run_daily_digest(
            object(), base_url="https://agent.anatainc.com",
            today=date(2026, 7, 23),
        )

    assert first["sent"] == 1
    assert second["skipped"] == 1
    assert send.call_count == 1
    message = send.call_args.kwargs["text"]
    assert "employee@anatainc.com" not in message
    assert "compensation" in message
    assert "/admin/hr/time" in message


def test_dry_run_never_sends_or_writes_dedupe_event():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(HREmployeeOnboarding(
            employee_email="employee@anatainc.com", status="employer_review"
        ))
        session.commit()

    with (
        mock.patch.object(notifications, "get_engine", return_value=engine),
        mock.patch.object(notifications, "_send") as send,
    ):
        result = notifications.run_daily_digest(
            object(), base_url="https://agent.anatainc.com", dry_run=True,
            today=date(2026, 7, 23),
        )

    assert result["dry_run"] is True
    assert result["items"]
    send.assert_not_called()


def test_reminder_counts_only_active_employees_missing_current_handbook():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all([
            HREmployee(
                email="pending@anatainc.com", full_name="Pending", status="active"
            ),
            HREmployee(
                email="done@anatainc.com", full_name="Done", status="active"
            ),
            HREmployee(
                email="former@anatainc.com", full_name="Former", status="inactive"
            ),
        ])
        handbook = HREmployeeHandbook(
            base44_id="handbook-current",
            title="Handbook",
            file_url="https://example.com/handbook",
            version="2026.1",
            uploaded_by="david@anatainc.com",
            is_active=True,
        )
        session.add(handbook)
        session.add(HRHandbookAcknowledgement(
            base44_id="ack-done",
            handbook_id="handbook-current",
            employee_email="done@anatainc.com",
            employee_name="Done",
            acknowledged_at=datetime.now(timezone.utc),
        ))
        session.commit()

    with mock.patch.object(notifications, "get_engine", return_value=engine):
        items = notifications.reminder_items(date(2026, 7, 23))

    handbook_item = next(
        item for item in items if "handbook acknowledgement" in item["label"]
    )
    assert handbook_item["count"] == 1
    assert handbook_item["path"] == "/admin/hr/policies"
