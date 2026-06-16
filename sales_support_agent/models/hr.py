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
    pay_periods_per_year: Mapped[int] = mapped_column(Integer, default=26)
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


Index("ix_hr_line_items_run_email", HRPayrollLineItem.payroll_run_id, HRPayrollLineItem.employee_email)
