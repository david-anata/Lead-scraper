"""State-safety tests for payroll approvals and manual checks."""

from datetime import date
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.hr import (
    HREmployee,
    HRPayrollApproval,
    HRPayrollCalculation,
    HRPayrollInput,
    HRPayrollProviderHandoff,
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


def test_provider_handoff_detects_exact_match_and_variance():
    engine = _engine()
    run = _run()
    run.total_gross_cents = 100000
    run.total_net_cents = 80000
    run.total_taxes_cents = 25000
    run.notes = '{"total_employer_cost_cents": 110000}'
    with Session(engine) as session:
        session.add(run)
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        assert payroll_store.record_provider_handoff(
            "pay_test", action="submitted", provider_name="Outside Provider",
            provider_reference="run-123", evidence_note="Entered from provider",
            actor="val@anatainc.com",
        ) == (True, "provider_submitted")
        assert payroll_store.record_provider_handoff(
            "pay_test", action="confirmed", provider_name="Outside Provider",
            provider_reference="run-123", gross="1000", net="800",
            taxes="250", employer_cost="1100", evidence_note="Final report",
            actor="david@anatainc.com",
        ) == (True, "provider_matched")
        assert payroll_store.record_provider_handoff(
            "pay_test", action="confirmed", provider_name="Outside Provider",
            provider_reference="run-123", gross="1001", net="800",
            taxes="250", employer_cost="1100", evidence_note="Revised report",
            actor="david@anatainc.com",
        ) == (True, "provider_variance")

    with Session(engine) as session:
        handoff = session.query(HRPayrollProviderHandoff).one()
        assert handoff.status == "variance"
        assert handoff.variance_json["gross_cents"] == 100


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


def test_employee_statement_summary_uses_only_that_employee():
    engine = _engine()
    run = _run()
    run.status = "closed"
    run.total_gross_cents = 300000
    run.total_net_cents = 250000
    run.total_taxes_cents = 50000
    with Session(engine) as session:
        session.add(run)
        session.add_all([
            HRPayrollCalculation(
                payroll_run_id="pay_test", employee_email="one@anatainc.com",
                version=1, inputs_json={}, snapshot_hash="one",
                results_json={
                    "taxable_gross_cents": 100000, "net_cents": 80000,
                    "federal_cents": 7000, "utah_cents": 3000,
                    "social_security_cents": 6200, "medicare_cents": 1450,
                    "employer_taxes_cents": 9000, "deductions_cents": 2350,
                    "total_employer_cost_cents": 109000,
                },
            ),
            HRPayrollCalculation(
                payroll_run_id="pay_test", employee_email="two@anatainc.com",
                version=1, inputs_json={}, snapshot_hash="two",
                results_json={
                    "taxable_gross_cents": 200000, "net_cents": 170000,
                    "employer_taxes_cents": 18000,
                    "total_employer_cost_cents": 218000,
                },
            ),
        ])
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        statement = payroll_store.payroll_run_detail(
            "pay_test", employee_email="one@anatainc.com"
        )

    assert statement is not None
    assert statement["gross"] == "1,000.00"
    assert statement["net"] == "800.00"
    assert statement["cash_impact"] == "1,090.00"
    assert len(statement["calculations"]) == 1
    assert statement["calculations"][0]["employee_email"] == "one@anatainc.com"


def test_reimbursement_evidence_and_recurring_deduction_controls():
    engine = _engine()
    with Session(engine) as session:
        session.add(HREmployee(
            email="employee@anatainc.com", full_name="Employee", status="active"
        ))
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        assert payroll_store.add_payroll_input(
            employee_email="employee@anatainc.com",
            period_start=date(2026, 7, 1), period_end=date(2026, 7, 15),
            input_type="reimbursement", amount="50", taxable=False,
            description="Office supplies", source_reference="", actor="val@anatainc.com",
        ) == (False, "reimbursement_evidence_required")
        assert payroll_store.add_payroll_input(
            employee_email="employee@anatainc.com",
            period_start=date(2026, 7, 1), period_end=date(2026, 7, 15),
            input_type="deduction", amount="25", taxable=False,
            description="Employee-authorized deduction", recurring=True,
            actor="val@anatainc.com",
        ) == (True, "input_added")

    with Session(engine) as session:
        prior = session.query(HRPayrollInput).one()
        prior.status = "approved"
        session.commit()

    with mock.patch.object(payroll_store, "get_engine", return_value=engine):
        current = payroll_store.list_payroll_inputs(
            date(2026, 7, 16), date(2026, 7, 31)
        )
        repeated = payroll_store.list_payroll_inputs(
            date(2026, 7, 16), date(2026, 7, 31)
        )

    assert len(current) == 1
    assert current[0]["recurring"] is True
    assert current[0]["status"] == "pending"
    assert current[0]["submitted_by"] == "system"
    assert len(repeated) == 1
