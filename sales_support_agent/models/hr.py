"""HR / payroll data model — the agent-native port of the Base44 HR product.

Single-org (Anata): the Base44 `Organization` tenant layer is dropped; every
record implicitly belongs to Anata. Money is stored in integer **cents**
(house style, exact); tax/contribution rates are stored as fractions (Numeric).
Each table carries `base44_id` — the source record id — so the historical-data
migration is idempotent (upsert by base44_id) and cross-entity relationships can
be re-linked after import.

Tables are created on fresh DBs via Base.metadata.create_all and on existing
deploys via the CREATE TABLE IF NOT EXISTS blocks in models/database.py
(_apply_postgres_compat_migrations + _apply_sqlite_compat_migrations).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean, Date, DateTime, Integer, JSON, Numeric, String, Text, Index,
)
from sqlalchemy.orm import Mapped, mapped_column

from sales_support_agent.models.database import Base


class HREmployee(Base):
    """A person on payroll. Ports Base44 `User` (org layer dropped). PII is
    minimized exactly as Base44 stored it — only the last 4 of SSN/bank."""

    __tablename__ = "hr_employees"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    # HR-internal role (distinct from agent tool-permissions): employee|manager|owner|admin
    hr_role: Mapped[str] = mapped_column(String(16), default="employee", index=True)
    team_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="active", index=True)  # active|inactive
    onboarding_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    employee_type: Mapped[str] = mapped_column(String(16), default="hourly")  # hourly|salaried|contractor
    hourly_rate_cents: Mapped[int] = mapped_column(Integer, default=0)
    annual_salary_cents: Mapped[int] = mapped_column(Integer, default=0)

    # Contact / personal (light PII)
    phone: Mapped[str] = mapped_column(String(32), default="")
    address_line1: Mapped[str] = mapped_column(String(255), default="")
    address_line2: Mapped[str] = mapped_column(String(255), default="")
    city: Mapped[str] = mapped_column(String(128), default="")
    state: Mapped[str] = mapped_column(String(32), default="")
    zip: Mapped[str] = mapped_column(String(16), default="")
    ssn_last4: Mapped[str] = mapped_column(String(4), default="")
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # W-4
    w4_filing_status: Mapped[str] = mapped_column(String(32), default="")
    w4_allowances: Mapped[int] = mapped_column(Integer, default=0)
    w4_extra_withholding_cents: Mapped[int] = mapped_column(Integer, default=0)
    w4_two_jobs: Mapped[bool] = mapped_column(Boolean, default=False)
    state_tax_state: Mapped[str] = mapped_column(String(32), default="")

    # Stripe Connect / bank (last-4 only)
    stripe_account_id: Mapped[str] = mapped_column(String(64), default="")
    bank_account_last4: Mapped[str] = mapped_column(String(4), default="")
    bank_account_name: Mapped[str] = mapped_column(String(128), default="")
    stripe_onboarding_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    # I-9
    i9_status: Mapped[str] = mapped_column(String(32), default="")
    i9_verified_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    i9_expiration_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    i9_document_type: Mapped[str] = mapped_column(String(64), default="")
    i9_notes: Mapped[str] = mapped_column(Text, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRTeam(Base):
    __tablename__ = "hr_teams"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    name: Mapped[str] = mapped_column(String(128), default="")
    manager_email: Mapped[str] = mapped_column(String(255), default="")
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollSettings(Base):
    """Org-level payroll config — effectively a singleton for single-org Anata."""
    __tablename__ = "hr_payroll_settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    pay_periods_per_year: Mapped[int] = mapped_column(Integer, default=24)
    ss_rate: Mapped[float] = mapped_column(Numeric(8, 5), default=0.062)
    medicare_rate: Mapped[float] = mapped_column(Numeric(8, 5), default=0.0145)
    ss_wage_base_cents: Mapped[int] = mapped_column(Integer, default=0)
    futa_rate: Mapped[float] = mapped_column(Numeric(8, 5), default=0)
    futa_wage_base_cents: Mapped[int] = mapped_column(Integer, default=0)
    suta_rate: Mapped[float] = mapped_column(Numeric(8, 5), default=0)
    suta_wage_base_cents: Mapped[int] = mapped_column(Integer, default=0)
    state_tax_overrides: Mapped[dict] = mapped_column(JSON, default=dict)   # {state: rate}
    schedule_assignments: Mapped[dict] = mapped_column(JSON, default=dict)  # {employee_type: pay_schedule_id}
    notes: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRCompanyProfile(Base):
    """Non-secret employer identity used on payroll records and filings."""

    __tablename__ = "hr_company_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    legal_name: Mapped[str] = mapped_column(String(255), default="")
    trade_name: Mapped[str] = mapped_column(String(255), default="")
    ein_last4: Mapped[str] = mapped_column(String(4), default="")
    address_line1: Mapped[str] = mapped_column(String(255), default="")
    address_line2: Mapped[str] = mapped_column(String(255), default="")
    city: Mapped[str] = mapped_column(String(128), default="")
    state: Mapped[str] = mapped_column(String(2), default="UT")
    zip_code: Mapped[str] = mapped_column(String(16), default="")
    payroll_contact_email: Mapped[str] = mapped_column(String(255), default="")
    utah_withholding_account_last4: Mapped[str] = mapped_column(String(4), default="")
    utah_ui_account_last4: Mapped[str] = mapped_column(String(4), default="")
    federal_deposit_schedule: Mapped[str] = mapped_column(String(16), default="unknown")
    utah_withholding_payment_frequency: Mapped[str] = mapped_column(String(16), default="unknown")
    source_note: Mapped[str] = mapped_column(Text, default="")
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPaySchedule(Base):
    __tablename__ = "hr_pay_schedules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    name: Mapped[str] = mapped_column(String(128), default="")
    frequency: Mapped[str] = mapped_column(String(16), default="biweekly")  # weekly|biweekly|semimonthly|monthly
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sm_p1_start_day: Mapped[int] = mapped_column(Integer, default=1)
    sm_p1_end_day: Mapped[int] = mapped_column(Integer, default=15)
    sm_p1_pay_day: Mapped[int] = mapped_column(Integer, default=0)
    sm_p2_pay_offset_months: Mapped[int] = mapped_column(Integer, default=0)
    sm_p2_pay_day: Mapped[int] = mapped_column(Integer, default=0)
    monthly_pay_day: Mapped[int] = mapped_column(Integer, default=0)
    anchor_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    week_pay_lag_days: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayPeriod(Base):
    __tablename__ = "hr_pay_periods"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    start_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    total_hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending|approved|frozen
    approved_by: Mapped[str] = mapped_column(String(255), default="")
    approved_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_schedule_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pay_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    employee_type: Mapped[str] = mapped_column(String(16), default="")


class HRTimeEntry(Base):
    __tablename__ = "hr_time_entries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    start_time: Mapped[str] = mapped_column(String(8), default="")   # HH:MM
    stop_time: Mapped[str] = mapped_column(String(8), default="")
    hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    elapsed_seconds: Mapped[int] = mapped_column(Integer, default=0)
    project: Mapped[str] = mapped_column(String(128), default="")
    tag: Mapped[str] = mapped_column(String(128), default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    clocked_in_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_period_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)


class HRPayrollRun(Base):
    __tablename__ = "hr_payroll_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    pay_period_start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    pay_period_end: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    pay_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="draft", index=True)  # draft|processing|completed|partial|failed
    total_gross_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_net_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_taxes_cents: Mapped[int] = mapped_column(Integer, default=0)
    employee_count: Mapped[int] = mapped_column(Integer, default=0)
    initiated_by: Mapped[str] = mapped_column(String(255), default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollLineItem(Base):
    __tablename__ = "hr_payroll_line_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    payroll_run_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)  # base44 run id
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    employee_name: Mapped[str] = mapped_column(String(255), default="")
    total_hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    hourly_rate_cents: Mapped[int] = mapped_column(Integer, default=0)
    gross_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    federal_income_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    social_security_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    medicare_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    state_income_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    extra_withholding_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_deductions_cents: Mapped[int] = mapped_column(Integer, default=0)
    net_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    custom_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    stripe_transfer_id: Mapped[str] = mapped_column(String(64), default="")
    stripe_account_id: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending|sent|failed|skipped
    error_message: Mapped[str] = mapped_column(Text, default="")
    employee_type: Mapped[str] = mapped_column(String(16), default="")
    is_1099: Mapped[bool] = mapped_column(Boolean, default=False)
    periods_per_year: Mapped[int] = mapped_column(Integer, default=0)


class HRPaycheck(Base):
    __tablename__ = "hr_paychecks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    pay_period_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pay_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    gross_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    deductions_cents: Mapped[int] = mapped_column(Integer, default=0)
    net_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    notes: Mapped[str] = mapped_column(Text, default="")


class HRPrintedCheck(Base):
    __tablename__ = "hr_printed_checks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    payroll_run_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    payroll_line_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="")
    employee_name: Mapped[str] = mapped_column(String(255), default="")
    pay_period_start: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    pay_period_end: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    pay_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    check_number: Mapped[str] = mapped_column(String(32), default="")
    gross_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    federal_income_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    social_security_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    medicare_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    state_income_tax_cents: Mapped[int] = mapped_column(Integer, default=0)
    extra_withholding_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_deductions_cents: Mapped[int] = mapped_column(Integer, default=0)
    net_pay_cents: Mapped[int] = mapped_column(Integer, default=0)
    total_hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    hourly_rate_cents: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default="pending")  # pending|ready|voided
    notes: Mapped[str] = mapped_column(Text, default="")


class HREmployeeHandbook(Base):
    __tablename__ = "hr_employee_handbooks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    title: Mapped[str] = mapped_column(String(255), default="")
    file_url: Mapped[str] = mapped_column(String(512), default="")
    version: Mapped[str] = mapped_column(String(32), default="")
    uploaded_by: Mapped[str] = mapped_column(String(255), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRHandbookAcknowledgement(Base):
    __tablename__ = "hr_handbook_acknowledgements"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    base44_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True, index=True, nullable=True)
    handbook_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    employee_name: Mapped[str] = mapped_column(String(255), default="")
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class HRPTORequest(Base):
    """Employee PTO request; the event trail records every decision."""

    __tablename__ = "hr_pto_requests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    start_date: Mapped[date] = mapped_column(Date, index=True)
    end_date: Mapped[date] = mapped_column(Date)
    hours: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    reason: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    decided_by: Mapped[str] = mapped_column(String(255), default="")
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRAuditEvent(Base):
    """Append-only evidence for HR writes, approvals, and payroll controls."""

    __tablename__ = "hr_audit_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor_email: Mapped[str] = mapped_column(String(255), index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[str] = mapped_column(String(64), default="")
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)


class HREmploymentProfile(Base):
    """Employer-owned, effective employment facts kept separate from self-service."""

    __tablename__ = "hr_employment_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    hire_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True, index=True)
    termination_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    title: Mapped[str] = mapped_column(String(128), default="")
    manager_email: Mapped[str] = mapped_column(String(255), default="")
    work_state: Mapped[str] = mapped_column(String(2), default="UT")
    classification: Mapped[str] = mapped_column(String(16), default="nonexempt")
    pay_basis: Mapped[str] = mapped_column(String(24), default="hourly")
    fixed_pay_per_period_cents: Mapped[int] = mapped_column(Integer, default=0)
    standard_weekly_hours: Mapped[float] = mapped_column(Numeric(8, 2), default=40)
    standard_period_hours: Mapped[float] = mapped_column(Numeric(8, 2), default=86.67)
    pto_eligible_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    holiday_eligible_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    updated_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HREmployeeOnboarding(Base):
    """Progress and evidence for employee-owned and employer-owned onboarding."""

    __tablename__ = "hr_employee_onboarding"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    app_invite_id: Mapped[str] = mapped_column(String(64), default="", index=True)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    profile_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    w4_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    i9_employee_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    i9_employer_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    policies_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    emergency_contact_name: Mapped[str] = mapped_column(String(255), default="")
    emergency_contact_relationship: Mapped[str] = mapped_column(String(64), default="")
    emergency_contact_phone: Mapped[str] = mapped_column(String(32), default="")
    emergency_contact_email: Mapped[str] = mapped_column(String(255), default="")
    correction_reason: Mapped[str] = mapped_column(Text, default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRTaxElection(Base):
    """Effective-dated W-4 snapshot. Full SSN is sealed; ordinary reads use last4."""

    __tablename__ = "hr_tax_elections"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    effective_date: Mapped[date] = mapped_column(Date, index=True)
    filing_status: Mapped[str] = mapped_column(String(32), default="")
    two_jobs: Mapped[bool] = mapped_column(Boolean, default=False)
    exempt_from_federal_withholding: Mapped[bool] = mapped_column(Boolean, default=False)
    dependents_credit_cents: Mapped[int] = mapped_column(Integer, default=0)
    other_income_cents: Mapped[int] = mapped_column(Integer, default=0)
    deductions_cents: Mapped[int] = mapped_column(Integer, default=0)
    extra_withholding_cents: Mapped[int] = mapped_column(Integer, default=0)
    sealed_ssn: Mapped[str] = mapped_column(Text, default="")
    ssn_last4: Mapped[str] = mapped_column(String(4), default="")
    attested_by: Mapped[str] = mapped_column(String(255), default="")
    attested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    snapshot_hash: Mapped[str] = mapped_column(String(64), default="", unique=True)
    superseded_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class HRTimeCorrection(Base):
    """Employee or manager request preserving original, proposed, and final time."""

    __tablename__ = "hr_time_corrections"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    time_entry_id: Mapped[int] = mapped_column(Integer, index=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    original_json: Mapped[dict] = mapped_column(JSON, default=dict)
    proposed_json: Mapped[dict] = mapped_column(JSON, default=dict)
    final_json: Mapped[dict] = mapped_column(JSON, default=dict)
    reason: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(16), default="requested", index=True)
    requested_by: Mapped[str] = mapped_column(String(255), default="")
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRTimesheetApproval(Base):
    """Employee attestation and independent review for one semimonthly period."""

    __tablename__ = "hr_timesheet_approvals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    period_start: Mapped[date] = mapped_column(Date, index=True)
    period_end: Mapped[date] = mapped_column(Date, index=True)
    source_hash: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="submitted", index=True)
    submitted_by: Mapped[str] = mapped_column(String(255), default="")
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    review_note: Mapped[str] = mapped_column(Text, default="")
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class HRPTOLedger(Base):
    """Immutable earned, reserved, used, released, and adjusted PTO movements."""

    __tablename__ = "hr_pto_ledger"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    entry_type: Mapped[str] = mapped_column(String(16), index=True)
    hours: Mapped[float] = mapped_column(Numeric(10, 4), default=0)
    effective_date: Mapped[date] = mapped_column(Date, index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="")
    source_id: Mapped[str] = mapped_column(String(64), default="")
    note: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollInput(Base):
    """Auditable additions and deductions collected before preparation."""

    __tablename__ = "hr_payroll_inputs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    pay_period_start: Mapped[date] = mapped_column(Date, index=True)
    pay_period_end: Mapped[date] = mapped_column(Date, index=True)
    input_type: Mapped[str] = mapped_column(String(32))
    amount_cents: Mapped[int] = mapped_column(Integer)
    taxable: Mapped[bool] = mapped_column(Boolean, default=True)
    description: Mapped[str] = mapped_column(String(255), default="")
    source_reference: Mapped[str] = mapped_column(String(255), default="")
    recurring: Mapped[bool] = mapped_column(Boolean, default=False)
    recurrence_key: Mapped[str] = mapped_column(String(64), default="", index=True)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    submitted_by: Mapped[str] = mapped_column(String(255), default="")
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollApproval(Base):
    """Immutable human approval tied to one prepared payroll snapshot."""

    __tablename__ = "hr_payroll_approvals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payroll_run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    snapshot_hash: Mapped[str] = mapped_column(String(64))
    approved_by: Mapped[str] = mapped_column(String(255))
    approval_text: Mapped[str] = mapped_column(Text, default="")
    approved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollProviderHandoff(Base):
    """Outside-provider evidence and variance check for an approved payroll."""

    __tablename__ = "hr_payroll_provider_handoffs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payroll_run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    provider_name: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="not_submitted", index=True)
    provider_reference: Mapped[str] = mapped_column(String(128), default="")
    confirmed_gross_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    confirmed_net_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    confirmed_taxes_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    confirmed_employer_cost_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    variance_json: Mapped[dict] = mapped_column(JSON, default=dict)
    evidence_note: Mapped[str] = mapped_column(Text, default="")
    submitted_by: Mapped[str] = mapped_column(String(255), default="")
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    confirmed_by: Mapped[str] = mapped_column(String(255), default="")
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRTaxLiability(Base):
    """A payroll-created liability reconciled to its payment and filing."""

    __tablename__ = "hr_tax_liabilities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payroll_run_id: Mapped[str] = mapped_column(String(64), index=True)
    agency: Mapped[str] = mapped_column(String(64), index=True)
    liability_type: Mapped[str] = mapped_column(String(32))
    amount_cents: Mapped[int] = mapped_column(Integer)
    due_date: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String(24), default="due")
    confirmation_number: Mapped[str] = mapped_column(String(128), default="")
    filing_confirmation_number: Mapped[str] = mapped_column(String(128), default="")
    confirmed_amount_cents: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    filed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    reconciled_by: Mapped[str] = mapped_column(String(255), default="")
    evidence_note: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollCalculation(Base):
    """Versioned employee calculation used for preparation and approval."""

    __tablename__ = "hr_payroll_calculations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    payroll_run_id: Mapped[str] = mapped_column(String(64), index=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    inputs_json: Mapped[dict] = mapped_column(JSON, default=dict)
    results_json: Mapped[dict] = mapped_column(JSON, default=dict)
    trace_json: Mapped[dict] = mapped_column(JSON, default=dict)
    snapshot_hash: Mapped[str] = mapped_column(String(64), index=True)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HROpeningPayrollBalance(Base):
    """Reviewed year-to-date amounts brought in when Anata starts midyear."""

    __tablename__ = "hr_opening_payroll_balances"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    tax_year: Mapped[int] = mapped_column(Integer, index=True)
    gross_wages_cents: Mapped[int] = mapped_column(Integer, default=0)
    social_security_wages_cents: Mapped[int] = mapped_column(Integer, default=0)
    medicare_wages_cents: Mapped[int] = mapped_column(Integer, default=0)
    futa_wages_cents: Mapped[int] = mapped_column(Integer, default=0)
    utah_ui_wages_cents: Mapped[int] = mapped_column(Integer, default=0)
    federal_withheld_cents: Mapped[int] = mapped_column(Integer, default=0)
    utah_withheld_cents: Mapped[int] = mapped_column(Integer, default=0)
    employee_ss_withheld_cents: Mapped[int] = mapped_column(Integer, default=0)
    employee_medicare_withheld_cents: Mapped[int] = mapped_column(Integer, default=0)
    source_note: Mapped[str] = mapped_column(Text, default="")
    confirmed_by: Mapped[str] = mapped_column(String(255), default="")
    confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRPayrollReview(Base):
    """Evidence that an external qualified reviewer validated one tax year."""

    __tablename__ = "hr_payroll_reviews"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tax_year: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    status: Mapped[str] = mapped_column(String(24), default="approved", index=True)
    reviewer_name: Mapped[str] = mapped_column(String(255), default="")
    reviewer_email: Mapped[str] = mapped_column(String(255), default="")
    reviewed_on: Mapped[date] = mapped_column(Date)
    evidence_reference: Mapped[str] = mapped_column(String(255), default="")
    review_note: Mapped[str] = mapped_column(Text, default="")
    recorded_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class HROpeningBalanceApproval(Base):
    """Independent approval for imported year-to-date employee totals."""

    __tablename__ = "hr_opening_balance_approvals"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    opening_balance_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    source_hash: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="approved", index=True)
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    review_note: Mapped[str] = mapped_column(Text, default="")
    reviewed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class HRContractorProfile(Base):
    """Employer-tracked contractor compliance status; no tax form image."""

    __tablename__ = "hr_contractor_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contractor_email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    tax_form_type: Mapped[str] = mapped_column(String(32), default="undetermined")
    tax_form_status: Mapped[str] = mapped_column(String(24), default="missing", index=True)
    received_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    wise_recipient_reference: Mapped[str] = mapped_column(String(128), default="")
    review_note: Mapped[str] = mapped_column(Text, default="")
    reviewed_by: Mapped[str] = mapped_column(String(255), default="")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


class HRContractorPayment(Base):
    """Approved contractor obligation later matched to Wise payment evidence."""

    __tablename__ = "hr_contractor_payments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contractor_email: Mapped[str] = mapped_column(String(255), index=True)
    service_start: Mapped[date] = mapped_column(Date)
    service_end: Mapped[date] = mapped_column(Date)
    due_date: Mapped[date] = mapped_column(Date, index=True)
    amount_minor: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    description: Mapped[str] = mapped_column(String(255), default="")
    invoice_reference: Mapped[str] = mapped_column(String(128), default="")
    status: Mapped[str] = mapped_column(String(24), default="draft", index=True)
    prepared_by: Mapped[str] = mapped_column(String(255), default="")
    approved_by: Mapped[str] = mapped_column(String(255), default="")
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    wise_transfer_reference: Mapped[str] = mapped_column(String(128), default="")
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    evidence_note: Mapped[str] = mapped_column(Text, default="")
    reconciled_by: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HROffboardingChecklist(Base):
    """Right-sized separation workflow with explicit final-pay controls."""

    __tablename__ = "hr_offboarding_checklists"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), index=True)
    separation_type: Mapped[str] = mapped_column(String(24))
    last_working_day: Mapped[date] = mapped_column(Date)
    final_pay_date: Mapped[date] = mapped_column(Date)
    reason: Mapped[str] = mapped_column(Text, default="")
    checklist_json: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(24), default="open", index=True)
    created_by: Mapped[str] = mapped_column(String(255), default="")
    completed_by: Mapped[str] = mapped_column(String(255), default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class HRComplianceTask(Base):
    """Evidence-backed employer filing or registration task."""

    __tablename__ = "hr_compliance_tasks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    employee_email: Mapped[str] = mapped_column(String(255), default="", index=True)
    task_type: Mapped[str] = mapped_column(String(64), index=True)
    due_date: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String(24), default="open", index=True)
    confirmation_reference: Mapped[str] = mapped_column(String(128), default="")
    evidence_note: Mapped[str] = mapped_column(Text, default="")
    completed_by: Mapped[str] = mapped_column(String(255), default="")
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow
    )


Index("ix_hr_line_items_run_email", HRPayrollLineItem.payroll_run_id, HRPayrollLineItem.employee_email)
Index("ix_hr_calculation_run_email_version", HRPayrollCalculation.payroll_run_id,
      HRPayrollCalculation.employee_email, HRPayrollCalculation.version, unique=True)
Index("ix_hr_opening_balance_employee_year", HROpeningPayrollBalance.employee_email,
      HROpeningPayrollBalance.tax_year, unique=True)
Index("ix_hr_timesheet_employee_period", HRTimesheetApproval.employee_email,
      HRTimesheetApproval.period_start, HRTimesheetApproval.period_end, unique=True)
Index("ix_hr_compliance_employee_type", HRComplianceTask.employee_email,
      HRComplianceTask.task_type, unique=True)
