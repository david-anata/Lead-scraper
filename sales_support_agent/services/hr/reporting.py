"""Explicit HR CSV exports without sensitive SSNs or sealed tax data."""

from __future__ import annotations

from contextlib import contextmanager
import csv
from io import StringIO

from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRContractorPayment,
    HRContractorProfile,
    HREmployee,
    HREmploymentProfile,
    HRPayrollCalculation,
    HRPayrollRun,
    HRTaxLiability,
    HRTimeEntry,
)


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()


def _csv(headers: list[str], rows: list[list]) -> str:
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue()


def export_csv(kind: str) -> str | None:
    with _session() as session:
        if kind == "employees":
            employees = session.query(HREmployee).order_by(HREmployee.email).all()
            employment = {
                row.employee_email: row for row in session.query(HREmploymentProfile).all()
            }
            return _csv(
                ["email", "full_name", "status", "employee_type", "hire_date",
                 "termination_date", "title", "classification", "pay_basis"],
                [[
                    row.email, row.full_name, row.status, row.employee_type,
                    employment.get(row.email).hire_date if employment.get(row.email) else "",
                    employment.get(row.email).termination_date if employment.get(row.email) else "",
                    employment.get(row.email).title if employment.get(row.email) else "",
                    employment.get(row.email).classification if employment.get(row.email) else "",
                    employment.get(row.email).pay_basis if employment.get(row.email) else "",
                ] for row in employees],
            )
        if kind == "time":
            rows = session.query(HRTimeEntry).order_by(
                HRTimeEntry.date, HRTimeEntry.employee_email
            ).all()
            return _csv(
                ["employee_email", "date", "start_time", "stop_time", "hours", "notes"],
                [[row.employee_email, row.date, row.start_time, row.stop_time,
                  row.hours, row.notes] for row in rows],
            )
        if kind == "payroll":
            rows = session.query(HRPayrollCalculation).join(
                HRPayrollRun, HRPayrollRun.base44_id == HRPayrollCalculation.payroll_run_id
            ).order_by(HRPayrollRun.pay_date, HRPayrollCalculation.employee_email).all()
            return _csv(
                ["run_id", "employee_email", "pay_date", "period_start", "period_end",
                 "gross", "federal", "utah", "social_security", "medicare",
                 "deductions", "reimbursements", "net", "snapshot_hash"],
                [[
                    row.payroll_run_id, row.employee_email,
                    session.query(HRPayrollRun).filter_by(base44_id=row.payroll_run_id).one().pay_date,
                    row.inputs_json.get("period_start"), row.inputs_json.get("period_end"),
                    row.results_json.get("taxable_gross_cents", 0) / 100,
                    row.results_json.get("federal_cents", 0) / 100,
                    row.results_json.get("utah_cents", 0) / 100,
                    row.results_json.get("social_security_cents", 0) / 100,
                    row.results_json.get("medicare_cents", 0) / 100,
                    row.results_json.get("deductions_cents", 0) / 100,
                    row.results_json.get("reimbursements_cents", 0) / 100,
                    row.results_json.get("net_cents", 0) / 100, row.snapshot_hash,
                ] for row in rows],
            )
        if kind == "liabilities":
            rows = session.query(HRTaxLiability).order_by(HRTaxLiability.due_date).all()
            return _csv(
                ["run_id", "agency", "type", "amount", "due_date", "status",
                 "payment_confirmation_number", "filing_confirmation_number",
                 "paid_at", "filed_at", "reconciled_by"],
                [[row.payroll_run_id, row.agency, row.liability_type,
                  row.amount_cents / 100, row.due_date, row.status,
                  row.confirmation_number, row.filing_confirmation_number,
                  row.paid_at, row.filed_at,
                  row.reconciled_by] for row in rows],
            )
        if kind == "contractors":
            rows = session.query(HRContractorPayment).order_by(
                HRContractorPayment.due_date
            ).all()
            profiles = {
                row.contractor_email: row
                for row in session.query(HRContractorProfile).all()
            }
            return _csv(
                ["contractor_email", "service_start", "service_end", "due_date",
                 "amount", "currency", "status", "invoice_reference",
                 "wise_transfer_reference", "approved_by", "tax_form_type",
                 "tax_form_status", "tax_form_expiration"],
                [[row.contractor_email, row.service_start, row.service_end,
                  row.due_date, row.amount_minor / 100, row.currency, row.status,
                  row.invoice_reference, row.wise_transfer_reference,
                  row.approved_by,
                  profiles.get(row.contractor_email).tax_form_type
                  if profiles.get(row.contractor_email) else "",
                  profiles.get(row.contractor_email).tax_form_status
                  if profiles.get(row.contractor_email) else "",
                  profiles.get(row.contractor_email).expiration_date
                  if profiles.get(row.contractor_email) else "",
                  ] for row in rows],
            )
        if kind == "audit":
            rows = session.query(HRAuditEvent).order_by(HRAuditEvent.created_at).all()
            return _csv(
                ["created_at", "actor_email", "action", "entity_type", "entity_id", "details"],
                [[row.created_at, row.actor_email, row.action, row.entity_type,
                  row.entity_id, row.details] for row in rows],
            )
    return None
