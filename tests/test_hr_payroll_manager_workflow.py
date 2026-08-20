from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.hr import (
    HREmployee,
    HREmployeeOnboarding,
    HRTaxElection,
    HRTimeEntry,
    HRTimesheetApproval,
)
from sales_support_agent.services.hr import store


@contextmanager
def _test_database(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    monkeypatch.setattr(store, "get_engine", lambda: engine)
    yield engine


def _seed_employee(engine, email: str = "worker@anatainc.com") -> None:
    with Session(engine) as session:
        session.add(HREmployee(
            email=email,
            full_name="Payroll Worker",
            employee_type="hourly",
            status="active",
            hourly_rate_cents=2000,
        ))
        session.commit()


def test_manager_can_transcribe_a_prior_signed_w4_without_ssn(monkeypatch):
    with _test_database(monkeypatch) as engine:
        _seed_employee(engine)
        ok, message = store.record_prior_w4(
            "worker@anatainc.com",
            effective_date=date(2026, 1, 1),
            filing_status="single",
            two_jobs=False,
            dependents_credit="0",
            other_income="0",
            deductions="0",
            extra_withholding="25.00",
            exempt=False,
            source_reference="Prior signed W-4 reviewed",
            attested=True,
            actor="manager@anatainc.com",
        )
        assert (ok, message) == (True, "prior_w4_recorded")
        with Session(engine) as session:
            election = session.query(HRTaxElection).one()
            onboarding = session.query(HREmployeeOnboarding).one()
            assert election.filing_status == "single"
            assert election.extra_withholding_cents == 2500
            assert election.sealed_ssn == ""
            assert election.attested_by == "prior-record:manager@anatainc.com"
            assert onboarding.w4_complete is True


def test_prior_w4_requires_source_and_exact_attestation(monkeypatch):
    with _test_database(monkeypatch) as engine:
        _seed_employee(engine)
        ok, message = store.record_prior_w4(
            "worker@anatainc.com",
            effective_date=date(2026, 1, 1),
            filing_status="single",
            two_jobs=False,
            dependents_credit="0",
            other_income="0",
            deductions="0",
            extra_withholding="0",
            exempt=False,
            source_reference="",
            attested=False,
            actor="manager@anatainc.com",
        )
        assert (ok, message) == (False, "prior_w4_invalid")


def test_manager_approves_exact_time_and_later_change_makes_it_stale(monkeypatch):
    with _test_database(monkeypatch) as engine:
        _seed_employee(engine)
        with Session(engine) as session:
            session.add(HRTimeEntry(
                employee_email="worker@anatainc.com",
                date=date(2026, 8, 10),
                start_time="09:00",
                stop_time="17:00",
                hours=Decimal("8"),
                elapsed_seconds=8 * 3600,
            ))
            session.commit()
        ok, message = store.approve_timesheet_as_manager(
            "worker@anatainc.com",
            period_start=date(2026, 8, 1),
            period_end=date(2026, 8, 15),
            review_note="Reviewed the punch against the work schedule.",
            attested=True,
            actor="manager@anatainc.com",
        )
        assert (ok, message) == (True, "timesheet_manager_approved")
        assert store.list_timesheet_approvals(
            date(2026, 8, 1), date(2026, 8, 15)
        )[0]["status"] == "approved"
        with Session(engine) as session:
            entry = session.query(HRTimeEntry).one()
            entry.hours = Decimal("7.5")
            entry.elapsed_seconds = int(7.5 * 3600)
            session.commit()
        assert store.list_timesheet_approvals(
            date(2026, 8, 1), date(2026, 8, 15)
        )[0]["status"] == "stale"


def test_manager_cannot_approve_their_own_time(monkeypatch):
    with _test_database(monkeypatch) as engine:
        _seed_employee(engine)
        ok, message = store.approve_timesheet_as_manager(
            "worker@anatainc.com",
            period_start=date(2026, 8, 1),
            period_end=date(2026, 8, 15),
            review_note="Reviewed.",
            attested=True,
            actor="worker@anatainc.com",
        )
        assert (ok, message) == (False, "timesheet_manager_review_invalid")

