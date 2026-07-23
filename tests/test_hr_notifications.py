"""Privacy-safe and idempotent HR reminder tests."""

from datetime import date
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.hr import HREmployeeOnboarding, HRTimeEntry
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
