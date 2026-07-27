"""Contractor and offboarding workflow state-safety tests."""

from datetime import date
import csv
import io
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sales_support_agent.models.database import Base
from sales_support_agent.models.entities import AppUser
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRContractorPayment,
    HRContractorProfile,
    HREmployee,
    HREmploymentProfile,
    HROffboardingChecklist,
)
from sales_support_agent.services.hr import reporting, workforce


def _engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


def _contractor(email: str = "va@example.com") -> HREmployee:
    return HREmployee(
        email=email, full_name="Overseas VA", employee_type="contractor",
        status="active",
    )


def test_contractor_profile_captures_engagement_without_bank_details():
    engine = _engine()
    with Session(engine) as session:
        session.add(_contractor())
        session.commit()

    with mock.patch.object(workforce, "get_engine", return_value=engine):
        assert workforce.save_contractor_profile(
            contractor_email="va@example.com", country_code="ph",
            engagement_start=date(2026, 1, 1), engagement_end=None,
            flat_fee="1000.00", currency="usd", fee_terms="Per month",
            contract_reference="Agreement 2026-01",
            engagement_status="active", tax_form_type="w8ben",
            tax_form_status="requested", received_date=None,
            expiration_date=None, wise_recipient_reference="wise-recipient-42",
            review_note="Country and engagement terms reviewed.",
            actor="val@anatainc.com",
        ) == (True, "contractor_profile_saved")
        profile = workforce.list_contractor_profiles()[0]

    assert profile["country_code"] == "PH"
    assert profile["engagement_start"] == date(2026, 1, 1)
    assert profile["engagement_end"] is None
    assert profile["flat_fee"] == "1,000.00"
    assert profile["currency"] == "USD"
    assert profile["fee_terms"] == "Per month"
    assert profile["contract_reference"] == "Agreement 2026-01"
    assert profile["status"] == "active"
    assert "bank" not in profile

    with Session(engine) as session:
        row = session.query(HRContractorProfile).one()
        assert row.flat_fee_minor == 100_000
        event = session.query(HRAuditEvent).filter_by(
            action="contractor.profile_reviewed"
        ).one()
        assert event.details["country_code"] == "PH"
        assert "wise-recipient-42" not in str(event.details)

    with mock.patch.object(reporting, "get_engine", return_value=engine):
        exported = list(csv.DictReader(io.StringIO(
            reporting.export_csv("contractors")
        )))
    assert len(exported) == 1
    assert exported[0]["contractor_email"] == "va@example.com"
    assert exported[0]["country_code"] == "PH"
    assert exported[0]["engagement_flat_fee"] == "1000.0"
    assert exported[0]["amount"] == ""


def test_contractor_profile_rejects_incomplete_or_impossible_engagement():
    engine = _engine()
    with Session(engine) as session:
        session.add(_contractor())
        session.commit()

    common = {
        "contractor_email": "va@example.com",
        "engagement_start": date(2026, 2, 1),
        "engagement_end": None,
        "flat_fee": "1000",
        "currency": "USD",
        "fee_terms": "Per month",
        "contract_reference": "",
        "engagement_status": "active",
        "tax_form_type": "undetermined",
        "tax_form_status": "missing",
        "received_date": None,
        "expiration_date": None,
        "wise_recipient_reference": "",
        "review_note": "Reviewed.",
        "actor": "val@anatainc.com",
    }
    with mock.patch.object(workforce, "get_engine", return_value=engine):
        assert workforce.save_contractor_profile(
            **common, country_code=""
        ) == (False, "contractor_profile_invalid")
        assert workforce.save_contractor_profile(
            **{**common, "engagement_end": date(2026, 1, 31)},
            country_code="PH",
        ) == (False, "contractor_profile_invalid")
        assert workforce.save_contractor_profile(
            **{**common, "flat_fee": "0"}, country_code="PH"
        ) == (False, "contractor_profile_invalid")


def test_contractor_payment_requires_independent_approval_and_wise_evidence():
    engine = _engine()
    with Session(engine) as session:
        session.add(_contractor())
        session.commit()

    with mock.patch.object(workforce, "get_engine", return_value=engine):
        assert workforce.create_contractor_payment(
            contractor_email="va@example.com",
            service_start=date(2026, 7, 1),
            service_end=date(2026, 7, 31),
            due_date=date(2026, 8, 5),
            amount="1000", currency="USD", description="July support",
            invoice_reference="INV-7", actor="val@anatainc.com",
        ) == (True, "contractor_payment_prepared")
        payment_id = workforce.list_contractor_payments()[0]["id"]
        assert workforce.contractor_payment_action(
            payment_id, action="approve", wise_reference="",
            evidence_note="", actor="val@anatainc.com",
        ) == (False, "self_approval_blocked")
        assert workforce.contractor_payment_action(
            payment_id, action="approve", wise_reference="",
            evidence_note="", actor="david@anatainc.com",
        ) == (True, "contractor_payment_approved")
        assert workforce.contractor_payment_action(
            payment_id, action="record_paid", wise_reference="",
            evidence_note="", actor="val@anatainc.com",
        ) == (False, "wise_evidence_required")
        assert workforce.contractor_payment_action(
            payment_id, action="record_paid", wise_reference="wise-transfer-7",
            evidence_note="Matched to Wise confirmation.",
            actor="val@anatainc.com",
        ) == (True, "contractor_payment_paid")
        assert workforce.contractor_payment_action(
            payment_id, action="reconcile", wise_reference="",
            evidence_note="Amount and recipient matched.",
            actor="david@anatainc.com",
        ) == (True, "contractor_payment_reconciled")

    with Session(engine) as session:
        row = session.get(HRContractorPayment, payment_id)
        assert row.status == "reconciled"
        assert row.wise_transfer_reference == "wise-transfer-7"
        assert row.approved_by == "david@anatainc.com"
        assert row.reconciled_by == "david@anatainc.com"


def test_offboarding_preserves_history_and_suspends_access_only_when_complete():
    engine = _engine()
    email = "employee@anatainc.com"
    with Session(engine) as session:
        session.add(HREmployee(
            email=email, full_name="Employee", employee_type="hourly", status="active"
        ))
        session.add(HREmploymentProfile(
            employee_email=email, hire_date=date(2026, 1, 1)
        ))
        session.add(AppUser(
            id="employee-user", email=email, name="Employee", status="active"
        ))
        session.commit()

    with mock.patch.object(workforce, "get_engine", return_value=engine):
        assert workforce.create_offboarding(
            employee_email=email, separation_type="resignation",
            last_working_day=date(2026, 8, 15),
            final_pay_date=date(2026, 8, 20),
            reason="", actor="val@anatainc.com",
        ) == (False, "offboarding_reason_required")
        assert workforce.create_offboarding(
            employee_email=email, separation_type="resignation",
            last_working_day=date(2026, 8, 15),
            final_pay_date=date(2026, 8, 20),
            reason="Employee resignation", actor="val@anatainc.com",
        ) == (True, "offboarding_started")
        checklist_id = workforce.list_offboarding()[0]["id"]
        assert workforce.update_offboarding(
            checklist_id, completed_steps=["time_reviewed", "pto_reviewed"],
            actor="val@anatainc.com",
        ) == (True, "offboarding_saved")

    with Session(engine) as session:
        assert session.query(HREmployee).filter_by(email=email).one().status == "active"
        assert session.query(AppUser).filter_by(email=email).one().status == "active"

    completed = [
        "time_reviewed", "final_pay_confirmed", "pto_reviewed",
        "company_property_returned", "app_access_removed", "records_retained",
    ]
    with mock.patch.object(workforce, "get_engine", return_value=engine):
        assert workforce.update_offboarding(
            checklist_id, completed_steps=completed,
            actor="val@anatainc.com",
        ) == (False, "final_pay_evidence_required")
        assert workforce.update_offboarding(
            checklist_id, completed_steps=completed,
            final_pay_reference="Check 1042",
            final_pay_evidence_note="Approved final payroll and issued check matched.",
            actor="val@anatainc.com",
        ) == (True, "offboarding_complete")

    with Session(engine) as session:
        assert session.query(HREmployee).filter_by(email=email).one().status == "inactive"
        assert session.query(AppUser).filter_by(email=email).one().status == "suspended"
        assert session.query(HREmploymentProfile).filter_by(
            employee_email=email
        ).one().termination_date == date(2026, 8, 15)
        checklist = session.get(HROffboardingChecklist, checklist_id)
        assert checklist.status == "complete"
        assert checklist.final_pay_reference == "Check 1042"
        assert "issued check matched" in checklist.final_pay_evidence_note
