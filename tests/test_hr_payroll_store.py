"""State-safety tests for payroll approvals and manual checks."""

from datetime import date
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.hr import (
    HRPayrollApproval,
    HRPayrollRun,
    HRPrintedCheck,
)
from sales_support_agent.services.hr import payroll_store


def _engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


def _run(run_id: str = "pay_test") -> HRPayrollRun:
    return HRPayrollRun(
        base44_id=run_id, status="approved", employee_count=1,
        pay_period_start=date(2026, 8, 1), pay_period_end=date(2026, 8, 15),
        pay_date=date(2026, 8, 20), initiated_by="val@anatainc.com",
    )


def _check(run_id: str, number: str, email: str = "employee@anatainc.com"):
    return HRPrintedCheck(
        base44_id=f"check_{number}", payroll_run_id=run_id,
        payroll_line_item_id="line_1", employee_email=email,
        employee_name="Employee", pay_period_start=date(2026, 8, 1),
        pay_period_end=date(2026, 8, 15), pay_date=date(2026, 8, 20),
        check_number=number, net_pay_cents=85000, gross_pay_cents=100000,
        status="ready",
    )


def test_repeated_approval_and_check_actions_are_idempotent():
    engine = _engine()
    with Session(engine) as session:
        session.add(_run())
        session.add(HRPayrollApproval(
            payroll_run_id="pay_test", snapshot_hash="abc",
            approved_by="david@anatainc.com", approval_text="I approve this payroll",
        ))
        session.add(_check("pay_test", "1001"))
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        assert payroll_store.approve_payroll(
            "pay_test", actor="david@anatainc.com",
            approval_text="I approve this payroll",
        ) == (True, "payroll_already_approved")
        assert payroll_store.issue_printed_check(
            "pay_test", employee_email="employee@anatainc.com",
            check_number="1001", actor="val@anatainc.com",
        ) == (True, "check_already_issued")
        assert payroll_store.issue_printed_check(
            "pay_test", employee_email="employee@anatainc.com",
            check_number="1002", actor="val@anatainc.com",
        ) == (False, "employee_check_already_issued")

    with Session(engine) as session:
        assert session.query(HRPayrollApproval).count() == 1
        assert session.query(HRPrintedCheck).count() == 1


def test_reissue_is_atomic_and_preserves_original_evidence():
    engine = _engine()
    with Session(engine) as session:
        session.add(_run())
        session.add(_check("pay_test", "1001"))
        session.add(_check("other_run", "2001", "other@anatainc.com"))
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        assert payroll_store.void_and_reissue_check(
            "pay_test", employee_email="employee@anatainc.com",
            reason="Damaged check", new_check_number="2001",
            actor="david@anatainc.com",
        ) == (False, "check_number_used")
        with Session(engine) as session:
            original = session.query(HRPrintedCheck).filter_by(
                payroll_run_id="pay_test", check_number="1001"
            ).one()
            assert original.status == "ready"

        assert payroll_store.void_and_reissue_check(
            "pay_test", employee_email="employee@anatainc.com",
            reason="Damaged check", new_check_number="1002",
            actor="david@anatainc.com",
        ) == (True, "check_reissued")

    with Session(engine) as session:
        original = session.query(HRPrintedCheck).filter_by(check_number="1001").one()
        replacement = session.query(HRPrintedCheck).filter_by(check_number="1002").one()
        assert original.status == "voided"
        assert replacement.status == "ready"
        assert replacement.net_pay_cents == original.net_pay_cents
        assert "Damaged check" in replacement.notes
