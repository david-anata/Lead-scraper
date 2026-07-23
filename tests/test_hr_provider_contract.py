from datetime import date, datetime, timedelta, timezone

import pytest

from sales_support_agent.services.hr.provider_contract import (
    AuthoritativeTotals,
    EmployeePayrollInput,
    PayrollRunRequest,
    ProviderCapabilities,
    ProviderConfirmation,
    ProviderManifest,
    ProviderState,
    validate_confirmation,
)


def _request() -> PayrollRunRequest:
    return PayrollRunRequest(
        idempotency_key="pay_2026_08_01:version-1",
        anata_run_id="pay_2026_08_01",
        anata_version_hash="a" * 64,
        period_start=date(2026, 8, 1),
        period_end=date(2026, 8, 15),
        pay_date=date(2026, 8, 20),
        approved_by="david@anatainc.com",
        approved_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        employees=(EmployeePayrollInput(
            provider_employee_id="emp_opaque_1", regular_minutes=4800
        ),),
    )


def _totals() -> AuthoritativeTotals:
    return AuthoritativeTotals(
        gross_cents=200000,
        employee_tax_cents=30000,
        employer_tax_cents=16000,
        deduction_cents=0,
        reimbursement_cents=0,
        net_cents=170000,
        total_debit_cents=216000,
    )


def test_confirmation_must_match_the_approved_immutable_version():
    request = _request()
    manifest = ProviderManifest(
        service_name="Internal payroll sandbox",
        environment="sandbox",
        authority_owner="Anata payroll team",
        capabilities=ProviderCapabilities(authoritative_calculation=True),
    )
    confirmation = ProviderConfirmation(
        provider_run_id="provider_1",
        anata_run_id=request.anata_run_id,
        anata_version_hash="b" * 64,
        state=ProviderState.CALCULATED,
        occurred_at=datetime.now(timezone.utc),
        evidence_reference="event_1",
        totals=_totals(),
    )
    with pytest.raises(ValueError, match="does not match"):
        validate_confirmation(manifest, request, confirmation)


@pytest.mark.parametrize(
    ("state", "message"),
    (
        (ProviderState.WAGES_DISTRIBUTED, "wage distribution"),
        (ProviderState.TAXES_PAID, "tax payment"),
        (ProviderState.TAXES_FILED, "tax filing"),
    ),
)
def test_service_cannot_claim_a_capability_it_does_not_own(state, message):
    request = _request()
    manifest = ProviderManifest(
        service_name="Preparation only",
        environment="sandbox",
        authority_owner="Anata",
        capabilities=ProviderCapabilities(authoritative_calculation=True),
    )
    confirmation = ProviderConfirmation(
        provider_run_id="provider_1",
        anata_run_id=request.anata_run_id,
        anata_version_hash=request.anata_version_hash,
        state=state,
        occurred_at=datetime.now(timezone.utc),
        evidence_reference="event_1",
        totals=_totals(),
    )
    with pytest.raises(ValueError, match=message):
        validate_confirmation(manifest, request, confirmation)


def test_fully_capable_service_can_confirm_completed_payroll():
    request = _request()
    manifest = ProviderManifest(
        service_name="Internal payroll production",
        environment="production",
        authority_owner="Named payroll authority",
        capabilities=ProviderCapabilities(
            authoritative_calculation=True,
            wage_distribution=True,
            tax_payment=True,
            tax_filing=True,
            pay_statements=True,
            signed_webhooks=True,
        ),
    )
    confirmation = ProviderConfirmation(
        provider_run_id="provider_1",
        anata_run_id=request.anata_run_id,
        anata_version_hash=request.anata_version_hash,
        state=ProviderState.COMPLETED,
        occurred_at=datetime.now(timezone.utc),
        evidence_reference="signed_event_1",
        totals=_totals(),
    )
    validate_confirmation(manifest, request, confirmation)


def test_run_payload_rejects_duplicate_employees_and_negative_values():
    request = _request()
    duplicate = PayrollRunRequest(
        **{
            **request.__dict__,
            "employees": (
                request.employees[0],
                request.employees[0],
            ),
        }
    )
    with pytest.raises(ValueError, match="only once"):
        duplicate.validate()

    negative = EmployeePayrollInput(
        provider_employee_id="emp_2", approved_deductions_cents=-1
    )
    with pytest.raises(ValueError, match="cannot be negative"):
        negative.validate()

