"""Safety contract for a future authoritative payroll service.

The HR control room can prepare and approve payroll without knowing which system
will ultimately calculate statutory withholding, file taxes, or distribute
wages.  This module defines the boundary that any future provider—including an
Anata-built payroll service—must satisfy before Agent can trust those claims.

It intentionally contains no network client and no provider credentials.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import Protocol


CONTRACT_VERSION = "2026-07-23"


class ProviderState(StrEnum):
    """States confirmed by the authoritative payroll service."""

    ACCEPTED = "accepted"
    CALCULATED = "calculated"
    APPROVED = "approved"
    WAGES_DISTRIBUTED = "wages_distributed"
    TAXES_PAID = "taxes_paid"
    TAXES_FILED = "taxes_filed"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass(frozen=True)
class ProviderCapabilities:
    """Capabilities the service explicitly owns in the selected environment."""

    authoritative_calculation: bool = False
    wage_distribution: bool = False
    tax_payment: bool = False
    tax_filing: bool = False
    pay_statements: bool = False
    new_hire_reporting: bool = False
    signed_webhooks: bool = False


@dataclass(frozen=True)
class ProviderManifest:
    """Identity and authority declaration returned by the payroll service."""

    service_name: str
    environment: str
    authority_owner: str
    capabilities: ProviderCapabilities
    contract_version: str = CONTRACT_VERSION

    def validate(self) -> None:
        if self.contract_version != CONTRACT_VERSION:
            raise ValueError("Unsupported payroll provider contract version.")
        if self.environment not in {"sandbox", "production"}:
            raise ValueError("Payroll provider environment must be sandbox or production.")
        if not self.service_name.strip() or not self.authority_owner.strip():
            raise ValueError("Payroll provider identity and authority owner are required.")


@dataclass(frozen=True)
class EmployeePayrollInput:
    """Payroll input keyed by an opaque provider employee ID.

    SSNs, bank accounts, identity documents, and W-4 elections do not cross this
    run-submission boundary.  The authoritative service owns those records in
    its separately secured employee profile.
    """

    provider_employee_id: str
    regular_minutes: int = 0
    overtime_minutes: int = 0
    holiday_minutes: int = 0
    pto_minutes: int = 0
    taxable_additions_cents: int = 0
    nontaxable_reimbursements_cents: int = 0
    approved_deductions_cents: int = 0

    def validate(self) -> None:
        if not self.provider_employee_id.strip():
            raise ValueError("Opaque provider employee ID is required.")
        values = (
            self.regular_minutes,
            self.overtime_minutes,
            self.holiday_minutes,
            self.pto_minutes,
            self.taxable_additions_cents,
            self.nontaxable_reimbursements_cents,
            self.approved_deductions_cents,
        )
        if any(value < 0 for value in values):
            raise ValueError("Payroll input values cannot be negative.")


@dataclass(frozen=True)
class PayrollRunRequest:
    """Immutable, approved Anata version offered to an authoritative service."""

    idempotency_key: str
    anata_run_id: str
    anata_version_hash: str
    period_start: date
    period_end: date
    pay_date: date
    approved_by: str
    approved_at: datetime
    employees: tuple[EmployeePayrollInput, ...] = field(default_factory=tuple)

    def validate(self) -> None:
        if not self.idempotency_key.strip() or not self.anata_run_id.strip():
            raise ValueError("Run ID and idempotency key are required.")
        if len(self.anata_version_hash) < 32:
            raise ValueError("An immutable Anata version hash is required.")
        if not self.approved_by.strip() or not self.employees:
            raise ValueError("Approval identity and at least one employee are required.")
        if self.period_start > self.period_end or self.pay_date < self.period_end:
            raise ValueError("Payroll dates are inconsistent.")
        if self.approved_at.tzinfo is None:
            raise ValueError("Approval timestamp must include a timezone.")
        ids = [employee.provider_employee_id for employee in self.employees]
        if len(ids) != len(set(ids)):
            raise ValueError("A provider employee can appear only once per run.")
        for employee in self.employees:
            employee.validate()


@dataclass(frozen=True)
class AuthoritativeTotals:
    gross_cents: int
    employee_tax_cents: int
    employer_tax_cents: int
    deduction_cents: int
    reimbursement_cents: int
    net_cents: int
    total_debit_cents: int

    def validate(self) -> None:
        if any(value < 0 for value in self.__dict__.values()):
            raise ValueError("Authoritative payroll totals cannot be negative.")


@dataclass(frozen=True)
class ProviderConfirmation:
    """Evidence returned by the service for one state transition."""

    provider_run_id: str
    anata_run_id: str
    anata_version_hash: str
    state: ProviderState
    occurred_at: datetime
    evidence_reference: str
    totals: AuthoritativeTotals | None = None
    failure_code: str = ""
    failure_message: str = ""


def validate_confirmation(
    manifest: ProviderManifest,
    request: PayrollRunRequest,
    confirmation: ProviderConfirmation,
) -> None:
    """Reject unsupported or unprovable payroll-state claims."""

    manifest.validate()
    request.validate()
    if not confirmation.provider_run_id.strip() or not confirmation.evidence_reference.strip():
        raise ValueError("Provider run ID and evidence reference are required.")
    if (
        confirmation.anata_run_id != request.anata_run_id
        or confirmation.anata_version_hash != request.anata_version_hash
    ):
        raise ValueError("Provider confirmation does not match the approved Anata version.")
    if confirmation.occurred_at.tzinfo is None:
        raise ValueError("Provider confirmation timestamp must include a timezone.")
    if confirmation.occurred_at > datetime.now(timezone.utc):
        raise ValueError("Provider confirmation cannot be dated in the future.")

    capabilities = manifest.capabilities
    if confirmation.state in {
        ProviderState.CALCULATED,
        ProviderState.APPROVED,
        ProviderState.COMPLETED,
    } and not capabilities.authoritative_calculation:
        raise ValueError("Service cannot claim authoritative calculation.")
    if confirmation.state in {
        ProviderState.WAGES_DISTRIBUTED,
        ProviderState.COMPLETED,
    } and not capabilities.wage_distribution:
        raise ValueError("Service cannot claim wage distribution.")
    if confirmation.state in {
        ProviderState.TAXES_PAID,
        ProviderState.COMPLETED,
    } and not capabilities.tax_payment:
        raise ValueError("Service cannot claim tax payment.")
    if confirmation.state in {
        ProviderState.TAXES_FILED,
        ProviderState.COMPLETED,
    } and not capabilities.tax_filing:
        raise ValueError("Service cannot claim tax filing.")

    if confirmation.state in {
        ProviderState.CALCULATED,
        ProviderState.APPROVED,
        ProviderState.WAGES_DISTRIBUTED,
        ProviderState.TAXES_PAID,
        ProviderState.TAXES_FILED,
        ProviderState.COMPLETED,
    }:
        if confirmation.totals is None:
            raise ValueError("Authoritative totals are required for this state.")
        confirmation.totals.validate()
    if confirmation.state == ProviderState.FAILED and not confirmation.failure_code.strip():
        raise ValueError("A failed payroll confirmation requires a failure code.")


class PayrollAuthority(Protocol):
    """Interface the internal payroll adapter must implement."""

    def manifest(self) -> ProviderManifest:
        """Return declared capabilities for the active environment."""

    def submit(self, request: PayrollRunRequest) -> ProviderConfirmation:
        """Submit one immutable version using request.idempotency_key."""

    def status(self, provider_run_id: str) -> ProviderConfirmation:
        """Return the latest authoritative, evidenced state."""

