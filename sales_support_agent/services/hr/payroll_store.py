"""Persistence and orchestration for the HR payroll control room."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import secrets

from sqlalchemy import func
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRCompanyProfile,
    HREmployee,
    HREmploymentProfile,
    HROpeningPayrollBalance,
    HRPayrollApproval,
    HRPayrollCalculation,
    HRPayrollInput,
    HRPayrollLineItem,
    HRPayrollRun,
    HRPayrollSettings,
    HRPaycheck,
    HRPrintedCheck,
    HRPTOLedger,
    HRPTORequest,
    HRTaxElection,
    HRTaxLiability,
    HRTimeCorrection,
    HRTimeEntry,
)
from sales_support_agent.services.hr.payroll import (
    hourly_gross,
    payroll_readiness,
    semimonthly_period,
    weekly_overtime,
)
from sales_support_agent.services.hr.store import (
    cents_to_dollars,
    holiday_pay_proposals,
    list_employees,
)
from sales_support_agent.services.hr.tax import (
    employer_unemployment_2026,
    federal_income_tax_2026,
    federal_deposit_due_date,
    fica_2026,
    month_due_date,
    quarter_due_date,
    utah_income_tax_2026,
)


@contextmanager
def _session():
    session = Session(get_engine(), expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _audit(session: Session, actor: str, action: str, entity_type: str,
           entity_id: object = "", details: dict | None = None) -> None:
    session.add(HRAuditEvent(
        actor_email=(actor or "system").strip().lower(), action=action,
        entity_type=entity_type, entity_id=str(entity_id or ""), details=details or {},
    ))


def _settings_dict(row: HRPayrollSettings | None) -> dict:
    overrides = dict((row.state_tax_overrides if row else {}) or {})
    return {
        "periods_per_year": 24,
        "utah_ui_rate": str(overrides.get("utah_ui_rate", "")),
        "qualified_tax_review": bool(overrides.get("qualified_tax_review")),
        "eftps_ready": bool(overrides.get("eftps_ready")),
        "utah_tap_ready": bool(overrides.get("utah_tap_ready")),
        "utah_ui_ready": bool(overrides.get("utah_ui_ready")),
        "federal_deposit_schedule": overrides.get("federal_deposit_schedule", "unknown"),
        "opening_balances_confirmed": bool(overrides.get("opening_balances_confirmed")),
        "opening_balance_note": overrides.get("opening_balance_note", ""),
    }


def get_payroll_settings() -> dict:
    with _session() as session:
        return _settings_dict(session.query(HRPayrollSettings).first())


def get_company_profile() -> dict:
    with _session() as session:
        row = session.query(HRCompanyProfile).first()
        if not row:
            return {}
        return {
            "legal_name": row.legal_name, "trade_name": row.trade_name,
            "ein_last4": row.ein_last4, "address_line1": row.address_line1,
            "address_line2": row.address_line2, "city": row.city, "state": row.state,
            "zip_code": row.zip_code, "payroll_contact_email": row.payroll_contact_email,
            "utah_withholding_account_last4": row.utah_withholding_account_last4,
            "utah_ui_account_last4": row.utah_ui_account_last4,
            "federal_deposit_schedule": row.federal_deposit_schedule,
            "utah_withholding_payment_frequency": row.utah_withholding_payment_frequency,
            "source_note": row.source_note,
        }


def save_company_profile(
    *, legal_name: str, trade_name: str, ein_last4: str, address_line1: str,
    address_line2: str, city: str, state: str, zip_code: str,
    payroll_contact_email: str, utah_withholding_account_last4: str,
    utah_ui_account_last4: str, federal_deposit_schedule: str,
    utah_withholding_payment_frequency: str,
    source_note: str, actor: str,
) -> tuple[bool, str]:
    digits = "".join(character for character in (ein_last4 or "") if character.isdigit())
    withholding_last4 = "".join(
        character for character in (utah_withholding_account_last4 or "")
        if character.isdigit()
    )
    ui_last4 = "".join(
        character for character in (utah_ui_account_last4 or "") if character.isdigit()
    )
    if (
        not legal_name.strip() or len(digits) != 4 or not address_line1.strip()
        or not city.strip() or state.strip().upper() != "UT" or not zip_code.strip()
        or "@" not in payroll_contact_email
        or federal_deposit_schedule not in {"monthly", "semiweekly"}
        or utah_withholding_payment_frequency not in {"monthly", "quarterly"}
        or not source_note.strip()
    ):
        return False, "company_profile_invalid"
    with _session() as session:
        row = session.query(HRCompanyProfile).first()
        if not row:
            row = HRCompanyProfile()
            session.add(row)
        row.legal_name = legal_name.strip()
        row.trade_name = trade_name.strip()
        row.ein_last4 = digits
        row.address_line1 = address_line1.strip()
        row.address_line2 = address_line2.strip()
        row.city = city.strip()
        row.state = "UT"
        row.zip_code = zip_code.strip()
        row.payroll_contact_email = payroll_contact_email.strip().lower()
        row.utah_withholding_account_last4 = withholding_last4[-4:]
        row.utah_ui_account_last4 = ui_last4[-4:]
        row.federal_deposit_schedule = federal_deposit_schedule
        row.utah_withholding_payment_frequency = utah_withholding_payment_frequency
        row.source_note = source_note.strip()
        row.reviewed_by = actor
        row.updated_at = datetime.now(timezone.utc)
        settings = session.query(HRPayrollSettings).first()
        if settings:
            overrides = dict(settings.state_tax_overrides or {})
            overrides["federal_deposit_schedule"] = federal_deposit_schedule
            settings.state_tax_overrides = overrides
        session.flush()
        _audit(session, actor, "payroll.company_profile_saved", "company_profile", row.id)
        return True, "company_profile_saved"


def save_payroll_settings(*, utah_ui_rate: str, qualified_tax_review: bool,
                          eftps_ready: bool, utah_tap_ready: bool,
                          utah_ui_ready: bool, opening_balances_confirmed: bool,
                          opening_balance_note: str, actor: str) -> None:
    try:
        rate = Decimal(utah_ui_rate)
    except Exception:
        rate = Decimal("-1")
    if rate < 0 or rate > Decimal("0.071"):
        raise ValueError("Utah UI rate must be between 0 and 0.071.")
    with _session() as session:
        row = session.query(HRPayrollSettings).first()
        if not row:
            row = HRPayrollSettings(pay_periods_per_year=24)
            session.add(row)
        row.pay_periods_per_year = 24
        company = session.query(HRCompanyProfile).first()
        row.state_tax_overrides = {
            "utah_ui_rate": str(rate),
            "qualified_tax_review": bool(qualified_tax_review),
            "eftps_ready": bool(eftps_ready),
            "utah_tap_ready": bool(utah_tap_ready),
            "utah_ui_ready": bool(utah_ui_ready),
            "federal_deposit_schedule": (
                company.federal_deposit_schedule if company else "unknown"
            ),
            "opening_balances_confirmed": bool(opening_balances_confirmed),
            "opening_balance_note": (opening_balance_note or "").strip(),
        }
        row.suta_rate = rate
        row.suta_wage_base_cents = 5_070_000
        row.ss_rate = Decimal("0.062")
        row.medicare_rate = Decimal("0.0145")
        row.ss_wage_base_cents = 18_450_000
        row.futa_rate = Decimal("0.006")
        row.futa_wage_base_cents = 700_000
        row.updated_at = datetime.now(timezone.utc)
        _audit(session, actor, "payroll.settings_saved", "payroll_settings", row.id)


def save_opening_balance(
    *, employee_email: str, tax_year: int, gross_wages: str,
    social_security_wages: str, medicare_wages: str, futa_wages: str,
    utah_ui_wages: str, federal_withheld: str, utah_withheld: str,
    employee_ss_withheld: str, employee_medicare_withheld: str,
    source_note: str, actor: str,
) -> tuple[bool, str]:
    from sales_support_agent.services.hr.store import dollars_to_cents
    email = (employee_email or "").strip().lower()
    if not source_note.strip():
        return False, "opening_source_required"
    with _session() as session:
        if not session.query(HREmployee).filter_by(email=email).first():
            return False, "employee_not_found"
        row = session.query(HROpeningPayrollBalance).filter_by(
            employee_email=email, tax_year=tax_year
        ).first()
        if not row:
            row = HROpeningPayrollBalance(employee_email=email, tax_year=tax_year)
            session.add(row)
        row.gross_wages_cents = dollars_to_cents(gross_wages)
        row.social_security_wages_cents = dollars_to_cents(social_security_wages)
        row.medicare_wages_cents = dollars_to_cents(medicare_wages)
        row.futa_wages_cents = dollars_to_cents(futa_wages)
        row.utah_ui_wages_cents = dollars_to_cents(utah_ui_wages)
        row.federal_withheld_cents = dollars_to_cents(federal_withheld)
        row.utah_withheld_cents = dollars_to_cents(utah_withheld)
        row.employee_ss_withheld_cents = dollars_to_cents(employee_ss_withheld)
        row.employee_medicare_withheld_cents = dollars_to_cents(employee_medicare_withheld)
        row.source_note = source_note.strip()
        row.confirmed_by = actor
        row.confirmed_at = datetime.now(timezone.utc)
        session.flush()
        _audit(session, actor, "payroll.opening_balance_saved",
               "opening_payroll_balance", row.id, {"employee_email": email, "year": tax_year})
        return True, "opening_balance_saved"


def list_opening_balances(tax_year: int) -> list[dict]:
    with _session() as session:
        rows = session.query(HROpeningPayrollBalance).filter_by(
            tax_year=tax_year
        ).order_by(HROpeningPayrollBalance.employee_email).all()
        return [{
            "employee_email": row.employee_email, "tax_year": row.tax_year,
            "gross_wages": cents_to_dollars(row.gross_wages_cents),
            "social_security_wages": cents_to_dollars(row.social_security_wages_cents),
            "medicare_wages": cents_to_dollars(row.medicare_wages_cents),
            "futa_wages": cents_to_dollars(row.futa_wages_cents),
            "utah_ui_wages": cents_to_dollars(row.utah_ui_wages_cents),
            "federal_withheld": cents_to_dollars(row.federal_withheld_cents),
            "utah_withheld": cents_to_dollars(row.utah_withheld_cents),
            "employee_ss_withheld": cents_to_dollars(row.employee_ss_withheld_cents),
            "employee_medicare_withheld": cents_to_dollars(row.employee_medicare_withheld_cents),
            "source_note": row.source_note, "confirmed_by": row.confirmed_by,
        } for row in rows]


def add_payroll_input(*, employee_email: str, period_start: date, period_end: date,
                      input_type: str, amount: str, taxable: bool,
                      description: str, actor: str) -> tuple[bool, str]:
    allowed = {"bonus", "commission", "reimbursement", "deduction", "contractor_fee"}
    if input_type not in allowed:
        return False, "invalid_input"
    from sales_support_agent.services.hr.store import dollars_to_cents
    amount_cents = dollars_to_cents(amount)
    if amount_cents <= 0:
        return False, "invalid_input"
    if input_type == "reimbursement":
        taxable = False
    with _session() as session:
        employee = session.query(HREmployee).filter_by(
            email=(employee_email or "").strip().lower()
        ).first()
        if not employee:
            return False, "employee_not_found"
        row = HRPayrollInput(
            employee_email=employee.email, pay_period_start=period_start,
            pay_period_end=period_end, input_type=input_type,
            amount_cents=amount_cents, taxable=taxable,
            description=(description or "").strip(), submitted_by=actor,
        )
        session.add(row)
        session.flush()
        affected_runs = session.query(HRPayrollRun).filter_by(
            pay_period_start=period_start, pay_period_end=period_end
        ).filter(HRPayrollRun.status.in_(("prepared", "approved"))).all()
        for run in affected_runs:
            run.status = "superseded"
            _audit(session, actor, "payroll.superseded_by_input",
                   "payroll_run", run.base44_id, {"input_id": row.id})
        _audit(session, actor, "payroll.input_added", "payroll_input", row.id)
        return True, "input_added"


def decide_payroll_input(input_id: int, *, decision: str, actor: str) -> tuple[bool, str]:
    if decision not in {"approved", "rejected"}:
        return False, "invalid_input"
    with _session() as session:
        row = session.get(HRPayrollInput, input_id)
        if not row or row.status != "pending":
            return False, "input_not_found"
        if row.submitted_by.strip().lower() == actor.strip().lower():
            return False, "self_approval_blocked"
        row.status = decision
        row.reviewed_by = actor
        row.reviewed_at = datetime.now(timezone.utc)
        _audit(session, actor, f"payroll.input_{decision}", "payroll_input", row.id)
        return True, f"input_{decision}"


def list_payroll_inputs(period_start: date, period_end: date) -> list[dict]:
    with _session() as session:
        rows = session.query(HRPayrollInput).filter_by(
            pay_period_start=period_start, pay_period_end=period_end
        ).order_by(HRPayrollInput.created_at.desc()).all()
        return [{
            "id": row.id, "employee_email": row.employee_email,
            "input_type": row.input_type, "amount_cents": row.amount_cents,
            "amount": cents_to_dollars(row.amount_cents), "taxable": row.taxable,
            "description": row.description, "status": row.status,
            "submitted_by": row.submitted_by, "reviewed_by": row.reviewed_by,
        } for row in rows]


def _period_context(containing: date) -> tuple:
    period = semimonthly_period(containing)
    settings = get_payroll_settings()
    employees = [
        employee for employee in list_employees(include_inactive=False)
        if employee.get("employee_type") != "contractor"
    ]
    inputs = list_payroll_inputs(period.start_date, period.end_date)
    company_profile = get_company_profile()
    with _session() as session:
        open_entries = [{
            "employee_email": row.employee_email, "date": row.date,
        } for row in session.query(HRTimeEntry).filter(
            HRTimeEntry.date >= period.start_date, HRTimeEntry.date <= period.end_date,
            HRTimeEntry.stop_time == "",
        ).all()]
        corrections = [{
            "employee_email": row.employee_email,
        } for row in session.query(HRTimeCorrection).filter_by(status="requested").all()]
        w4_emails = {
            row[0] for row in session.query(HRTaxElection.employee_email).filter(
                HRTaxElection.effective_date <= period.end_date,
                HRTaxElection.superseded_at.is_(None),
            ).all()
        }
        balance_emails = {
            row[0] for row in session.query(HROpeningPayrollBalance.employee_email).filter_by(
                tax_year=period.end_date.year
            ).all()
        }
        closed_time_emails = {
            row[0] for row in session.query(HRTimeEntry.employee_email).filter(
                HRTimeEntry.date >= period.start_date, HRTimeEntry.date <= period.end_date,
                HRTimeEntry.stop_time != "",
            ).distinct().all()
        }
    readiness = payroll_readiness(
        employees=employees, open_time_entries=open_entries,
        pending_corrections=corrections,
        pending_inputs=[item for item in inputs if item["status"] == "pending"],
        tax_engine_configured=settings["qualified_tax_review"],
        eftps_ready=settings["eftps_ready"],
        utah_tax_ready=settings["utah_tap_ready"] and settings["utah_ui_ready"],
    )
    if not settings["opening_balances_confirmed"]:
        readiness["blockers"].append({
            "kind": "opening_balances", "severity": "blocker",
            "message": "Opening payroll year-to-date balances are not confirmed",
        })
    if not company_profile.get("legal_name") or not company_profile.get("ein_last4"):
        readiness["blockers"].append({
            "kind": "company_profile", "severity": "blocker",
            "message": "Employer legal profile is incomplete",
        })
    if settings.get("federal_deposit_schedule") not in {"monthly", "semiweekly"}:
        readiness["blockers"].append({
            "kind": "deposit_schedule", "severity": "blocker",
            "message": "Federal deposit schedule is not confirmed from lookback evidence",
        })
    if company_profile.get("utah_withholding_payment_frequency") not in {
        "monthly", "quarterly"
    }:
        readiness["blockers"].append({
            "kind": "utah_payment_frequency", "severity": "blocker",
            "message": "Utah withholding payment frequency is not confirmed",
        })
    for employee in employees:
        if employee["email"] not in w4_emails:
            readiness["blockers"].append({
                "kind": "w4", "severity": "blocker",
                "employee_email": employee["email"], "message": "Current W-4 is missing",
            })
        if employee["email"] not in balance_emails:
            readiness["blockers"].append({
                "kind": "opening_balance", "severity": "blocker",
                "employee_email": employee["email"],
                "message": f"{period.end_date.year} year-to-date opening balance is missing",
            })
        employment = employee.get("employment") or {}
        if employment.get("pay_basis") == "hourly" and employee["email"] not in closed_time_emails:
            readiness["blockers"].append({
                "kind": "time_missing", "severity": "blocker",
                "employee_email": employee["email"],
                "message": "No closed time entries exist for this pay period",
            })
    readiness["ready"] = not readiness["blockers"]
    return period, settings, employees, inputs, readiness


def _period_source_hash(session: Session, period, employees: list[dict],
                        inputs: list[dict], settings: dict) -> str:
    """Hash every mutable source that can change a prepared payroll."""
    emails = [employee["email"] for employee in employees]
    time_rows = session.query(HRTimeEntry).filter(
        HRTimeEntry.employee_email.in_(emails),
        HRTimeEntry.date >= period.start_date, HRTimeEntry.date <= period.end_date,
    ).order_by(HRTimeEntry.id).all()
    pto_rows = session.query(HRPTORequest).filter(
        HRPTORequest.employee_email.in_(emails),
        HRPTORequest.start_date <= period.end_date,
        HRPTORequest.end_date >= period.start_date,
    ).order_by(HRPTORequest.id).all()
    elections = session.query(HRTaxElection).filter(
        HRTaxElection.employee_email.in_(emails),
        HRTaxElection.effective_date <= period.end_date,
        HRTaxElection.superseded_at.is_(None),
    ).order_by(HRTaxElection.employee_email, HRTaxElection.id).all()
    openings = session.query(HROpeningPayrollBalance).filter(
        HROpeningPayrollBalance.employee_email.in_(emails),
        HROpeningPayrollBalance.tax_year == period.end_date.year,
    ).order_by(HROpeningPayrollBalance.employee_email).all()
    source = {
        "rule_version": "2026.1",
        "period": [period.start_date, period.end_date, period.pay_date],
        "employees": employees, "settings": settings, "inputs": inputs,
        "time": [[row.id, row.employee_email, row.date, row.start_time, row.stop_time,
                  row.hours, row.elapsed_seconds] for row in time_rows],
        "pto": [[row.id, row.employee_email, row.start_date, row.end_date,
                 row.hours, row.status] for row in pto_rows],
        "elections": [[row.employee_email, row.effective_date, row.snapshot_hash]
                      for row in elections],
        "openings": [[row.employee_email, row.gross_wages_cents,
                      row.social_security_wages_cents, row.medicare_wages_cents,
                      row.futa_wages_cents, row.utah_ui_wages_cents,
                      row.confirmed_at] for row in openings],
    }
    payload = json.dumps(source, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def control_room(containing: date) -> dict:
    period, settings, employees, inputs, readiness = _period_context(containing)
    with _session() as session:
        runs = session.query(HRPayrollRun).filter_by(
            pay_period_start=period.start_date, pay_period_end=period.end_date
        ).order_by(HRPayrollRun.id.desc()).all()
        run_rows = [{
            "id": row.base44_id, "status": row.status,
            "gross": cents_to_dollars(row.total_gross_cents),
            "net": cents_to_dollars(row.total_net_cents),
            "taxes": cents_to_dollars(row.total_taxes_cents),
            "cash_impact": cents_to_dollars(row.total_net_cents + row.total_taxes_cents),
            "employee_count": row.employee_count, "initiated_by": row.initiated_by,
        } for row in runs]
        liabilities = session.query(HRTaxLiability).filter(
            HRTaxLiability.payroll_run_id.in_([row.base44_id for row in runs])
        ).order_by(HRTaxLiability.due_date, HRTaxLiability.agency).all() if runs else []
        liability_rows = [{
            "id": row.id, "payroll_run_id": row.payroll_run_id,
            "agency": row.agency, "liability_type": row.liability_type,
            "amount": cents_to_dollars(row.amount_cents), "due_date": row.due_date,
            "status": row.status, "confirmation_number": row.confirmation_number,
            "filing_confirmation_number": row.filing_confirmation_number,
            "paid": bool(row.paid_at), "filed": bool(row.filed_at),
            "evidence_note": row.evidence_note,
        } for row in liabilities]
    return {
        "period": period, "settings": settings, "employees": employees,
        "inputs": inputs, "readiness": readiness, "runs": run_rows,
        "opening_balances": list_opening_balances(period.end_date.year),
        "liabilities": liability_rows,
    }


def record_liability_action(liability_id: int, *, action: str,
                            confirmation_number: str, confirmed_amount: str,
                            filing_confirmation_number: str,
                            evidence_note: str,
                            actor: str) -> tuple[bool, str]:
    if action not in {"paid", "filed", "reconciled"}:
        return False, "liability_action_invalid"
    evidence_id = (
        filing_confirmation_number if action == "filed" else confirmation_number
    )
    if action in {"paid", "filed"} and not evidence_id.strip():
        return False, "confirmation_required"
    if not evidence_note.strip():
        return False, "evidence_required"
    with _session() as session:
        row = session.get(HRTaxLiability, liability_id)
        if not row:
            return False, "liability_not_found"
        now = datetime.now(timezone.utc)
        if action == "paid":
            from sales_support_agent.services.hr.store import dollars_to_cents
            confirmed_cents = dollars_to_cents(confirmed_amount)
            if confirmed_cents != row.amount_cents:
                _audit(session, actor, "tax_liability.amount_mismatch",
                       "tax_liability", row.id, {
                           "expected_cents": row.amount_cents,
                           "confirmed_cents": confirmed_cents,
                       })
                return False, "liability_amount_mismatch"
            row.paid_at = now
            row.confirmation_number = confirmation_number.strip()
            row.confirmed_amount_cents = confirmed_cents
            row.status = "paid"
        elif action == "filed":
            row.filed_at = now
            row.filing_confirmation_number = filing_confirmation_number.strip()
            row.status = "filed" if not row.paid_at else "paid_and_filed"
        else:
            if not row.paid_at or not row.filed_at:
                return False, "payment_and_filing_required"
            row.status = "reconciled"
            row.reconciled_by = actor
        row.evidence_note = evidence_note.strip()
        _audit(session, actor, f"tax_liability.{action}", "tax_liability", row.id, {
            "confirmation_number": row.confirmation_number,
            "filing_confirmation_number": row.filing_confirmation_number,
        })
        return True, f"liability_{action}"


def prepare_payroll(containing: date, *, actor: str) -> tuple[bool, str]:
    period, settings, employees, inputs, readiness = _period_context(containing)
    if not readiness["ready"]:
        return False, "payroll_blocked"
    run_id = "pay_" + secrets.token_hex(12)
    calculations = []
    totals = {"gross": 0, "net": 0, "taxes": 0}
    with _session() as session:
        source_hash = _period_source_hash(session, period, employees, inputs, settings)
        for employee in employees:
            email = employee["email"]
            employment = employee.get("employment") or {}
            entries = session.query(HRTimeEntry).filter(
                HRTimeEntry.employee_email == email,
                HRTimeEntry.date >= period.start_date,
                HRTimeEntry.date <= period.end_date,
            ).all()
            hours_by_date: dict[date, Decimal] = {}
            for row in entries:
                exact_hours = (
                    Decimal(row.elapsed_seconds) / Decimal(3600)
                    if row.elapsed_seconds else Decimal(str(row.hours or 0))
                )
                hours_by_date[row.date] = hours_by_date.get(row.date, Decimal("0")) + exact_hours
            regular_hours, overtime_hours = weekly_overtime(hours_by_date)
            approved_pto = Decimal(str(session.query(
                func.coalesce(func.sum(HRPTORequest.hours), 0)
            ).filter(
                HRPTORequest.employee_email == email,
                HRPTORequest.status == "approved",
                HRPTORequest.start_date <= period.end_date,
                HRPTORequest.end_date >= period.start_date,
            ).scalar() or 0))
            holidays = holiday_pay_proposals(email, period.start_date, period.end_date)
            holiday_hours = sum((Decimal(str(row["hours"])) for row in holidays), Decimal("0"))
            if employment.get("pay_basis") == "fixed_semimonthly":
                base_gross = int(employment.get("fixed_pay_per_period_cents") or 0)
                gross_parts = {
                    "regular_cents": base_gross, "overtime_cents": 0,
                    "holiday_cents": 0, "pto_cents": 0, "gross_cents": base_gross,
                }
            else:
                gross_parts = hourly_gross(
                    rate_cents=int(employee.get("hourly_rate_cents") or 0),
                    regular_hours=regular_hours, overtime_hours=overtime_hours,
                    holiday_hours=holiday_hours, pto_hours=approved_pto,
                )
            emp_inputs = [
                item for item in inputs
                if item["employee_email"] == email and item["status"] == "approved"
            ]
            taxable_additions = sum(
                item["amount_cents"] for item in emp_inputs
                if item["input_type"] in {"bonus", "commission"} and item["taxable"]
            )
            reimbursements = sum(
                item["amount_cents"] for item in emp_inputs
                if item["input_type"] == "reimbursement"
            )
            deductions = sum(
                item["amount_cents"] for item in emp_inputs
                if item["input_type"] == "deduction"
            )
            taxable_gross = gross_parts["gross_cents"] + taxable_additions
            election = session.query(HRTaxElection).filter(
                HRTaxElection.employee_email == email,
                HRTaxElection.effective_date <= period.end_date,
                HRTaxElection.superseded_at.is_(None),
            ).order_by(HRTaxElection.effective_date.desc()).first()
            federal = (
                {"withholding_cents": 0, "trace": {
                    "method": "Form W-4 exempt election", "effective_date": "2026-01-01",
                }}
                if election.exempt_from_federal_withholding else
                federal_income_tax_2026(
                    taxable_gross, filing_status=election.filing_status,
                    two_jobs=election.two_jobs,
                    dependents_credit_cents=election.dependents_credit_cents,
                    other_income_cents=election.other_income_cents,
                    deductions_cents=election.deductions_cents,
                    extra_withholding_cents=election.extra_withholding_cents,
                )
            )
            utah = utah_income_tax_2026(taxable_gross, filing_status=election.filing_status)
            opening = session.query(HROpeningPayrollBalance).filter_by(
                employee_email=email, tax_year=period.end_date.year
            ).one()
            prior_calculations = session.query(HRPayrollCalculation).join(
                HRPayrollRun,
                HRPayrollRun.base44_id == HRPayrollCalculation.payroll_run_id,
            ).filter(
                HRPayrollCalculation.employee_email == email,
                HRPayrollRun.status.in_(("approved", "checks_issued", "closed")),
                HRPayrollRun.pay_date >= date(period.end_date.year, 1, 1),
                HRPayrollRun.pay_date < period.pay_date,
            ).all()
            prior_taxable_wages_cents = sum(
                int((row.results_json or {}).get("taxable_gross_cents", 0))
                for row in prior_calculations
            )
            ss_ytd_cents = opening.social_security_wages_cents + prior_taxable_wages_cents
            futa_ytd_cents = opening.futa_wages_cents + prior_taxable_wages_cents
            utah_ui_ytd_cents = opening.utah_ui_wages_cents + prior_taxable_wages_cents
            fica = fica_2026(taxable_gross, ytd_before_cents=ss_ytd_cents)
            unemployment = employer_unemployment_2026(
                taxable_gross,
                futa_ytd_before_cents=futa_ytd_cents,
                utah_ui_ytd_before_cents=utah_ui_ytd_cents,
                utah_ui_rate=Decimal(settings["utah_ui_rate"]),
            )
            employee_taxes = (
                federal["withholding_cents"] + utah["withholding_cents"]
                + fica["social_security_employee_cents"] + fica["medicare_employee_cents"]
            )
            net = taxable_gross + reimbursements - employee_taxes - deductions
            if net < 0:
                return False, "negative_net_pay"
            results = {
                **gross_parts, "taxable_additions_cents": taxable_additions,
                "taxable_gross_cents": taxable_gross, "reimbursements_cents": reimbursements,
                "deductions_cents": deductions, "federal_cents": federal["withholding_cents"],
                "utah_cents": utah["withholding_cents"],
                "social_security_cents": fica["social_security_employee_cents"],
                "medicare_cents": fica["medicare_employee_cents"],
                "net_cents": net, "employer_taxes_cents": (
                    fica["social_security_employer_cents"]
                    + fica["medicare_employer_cents"] + unemployment["futa_cents"]
                    + unemployment["utah_ui_cents"]
                ),
            }
            input_snapshot = {
                "period_start": period.start_date.isoformat(),
                "period_end": period.end_date.isoformat(), "pay_date": period.pay_date.isoformat(),
                "employee_email": email, "regular_hours": str(regular_hours),
                "overtime_hours": str(overtime_hours), "holiday_hours": str(holiday_hours),
                "pto_hours": str(approved_pto), "payroll_inputs": emp_inputs,
            }
            trace = {"federal": federal["trace"], "utah": utah["trace"],
                     "fica": {**fica, "ytd_before_cents": ss_ytd_cents},
                     "unemployment": {
                         **unemployment, "futa_ytd_before_cents": futa_ytd_cents,
                         "utah_ui_ytd_before_cents": utah_ui_ytd_cents,
                     }}
            payload = json.dumps(
                {"inputs": input_snapshot, "results": results, "trace": trace},
                sort_keys=True, default=str, separators=(",", ":"),
            )
            snapshot_hash = hashlib.sha256(payload.encode()).hexdigest()
            calculations.append((email, input_snapshot, results, trace, snapshot_hash))
            totals["gross"] += taxable_gross
            totals["net"] += net
            totals["taxes"] += employee_taxes + results["employer_taxes_cents"]
        run = HRPayrollRun(
            base44_id=run_id, pay_period_start=period.start_date,
            pay_period_end=period.end_date, pay_date=period.pay_date,
            status="prepared", total_gross_cents=totals["gross"],
            total_net_cents=totals["net"], total_taxes_cents=totals["taxes"],
            employee_count=len(calculations), initiated_by=actor,
            notes=json.dumps({
                "source_hash": source_hash, "rule_version": "2026.1",
                "statement": "Prepared snapshot; no money moved and no taxes filed.",
            }, sort_keys=True),
        )
        session.add(run)
        for email, input_snapshot, results, trace, snapshot_hash in calculations:
            session.add(HRPayrollCalculation(
                payroll_run_id=run_id, employee_email=email, version=1,
                inputs_json=input_snapshot, results_json=results, trace_json=trace,
                snapshot_hash=snapshot_hash, created_by=actor,
            ))
            session.add(HRPayrollLineItem(
                base44_id=f"line_{secrets.token_hex(10)}", payroll_run_id=run_id,
                employee_email=email, total_hours=Decimal(input_snapshot["regular_hours"])
                + Decimal(input_snapshot["overtime_hours"]),
                hourly_rate_cents=next(
                    employee["hourly_rate_cents"] for employee in employees
                    if employee["email"] == email
                ),
                gross_pay_cents=results["taxable_gross_cents"],
                federal_income_tax_cents=results["federal_cents"],
                social_security_tax_cents=results["social_security_cents"],
                medicare_tax_cents=results["medicare_cents"],
                state_income_tax_cents=results["utah_cents"],
                total_deductions_cents=results["deductions_cents"],
                net_pay_cents=results["net_cents"], status="prepared",
            ))
        _audit(session, actor, "payroll.prepared", "payroll_run", run_id, {
            "gross_cents": totals["gross"], "net_cents": totals["net"],
            "tax_cash_impact_cents": totals["taxes"],
        })
    return True, run_id


def approve_payroll(run_id: str, *, actor: str, approval_text: str) -> tuple[bool, str]:
    required_words = "I approve this payroll"
    if approval_text.strip() != required_words:
        return False, "approval_attestation_required"
    with _session() as session:
        run = session.query(HRPayrollRun).filter_by(base44_id=run_id).first()
        if not run or run.status != "prepared":
            return False, "run_not_prepared"
        if run.initiated_by.strip().lower() == actor.strip().lower():
            return False, "self_approval_blocked"
        period, settings, employees, inputs, readiness = _period_context(run.pay_period_start)
        if not readiness["ready"]:
            return False, "payroll_blocked"
        try:
            prepared_meta = json.loads(run.notes or "{}")
        except json.JSONDecodeError:
            prepared_meta = {}
        current_hash = _period_source_hash(session, period, employees, inputs, settings)
        if prepared_meta.get("source_hash") != current_hash:
            run.status = "superseded"
            _audit(session, actor, "payroll.source_changed", "payroll_run", run_id)
            return False, "payroll_inputs_changed"
        calculations = session.query(HRPayrollCalculation).filter_by(
            payroll_run_id=run_id, version=1
        ).order_by(HRPayrollCalculation.employee_email).all()
        combined_hash = hashlib.sha256(
            "|".join(row.snapshot_hash for row in calculations).encode()
        ).hexdigest()
        session.add(HRPayrollApproval(
            payroll_run_id=run_id, snapshot_hash=combined_hash,
            approved_by=actor, approval_text=approval_text,
        ))
        approved_pto = session.query(HRPTORequest).filter(
            HRPTORequest.status == "approved",
            HRPTORequest.start_date <= run.pay_period_end,
            HRPTORequest.end_date >= run.pay_period_start,
        ).all()
        for request in approved_pto:
            source_id = str(request.id)
            if session.query(HRPTOLedger).filter_by(
                employee_email=request.employee_email, entry_type="used",
                source_type="pto_request", source_id=source_id,
            ).first():
                continue
            session.add_all([
                HRPTOLedger(
                    employee_email=request.employee_email, entry_type="released",
                    hours=request.hours, effective_date=run.pay_date,
                    source_type="pto_request", source_id=source_id,
                    note=f"Released reservation in payroll {run_id}.", created_by=actor,
                ),
                HRPTOLedger(
                    employee_email=request.employee_email, entry_type="used",
                    hours=-request.hours, effective_date=run.pay_date,
                    source_type="pto_request", source_id=source_id,
                    note=f"Paid in payroll {run_id}.", created_by=actor,
                ),
            ])
        run.status = "approved"
        federal_amount = sum(
            int(row.results_json.get("federal_cents", 0))
            + int(row.results_json.get("social_security_cents", 0)) * 2
            + int(row.results_json.get("medicare_cents", 0))
            + int((row.trace_json.get("fica") or {}).get("medicare_employer_cents", 0))
            for row in calculations
        )
        utah_withholding = sum(
            int(row.results_json.get("utah_cents", 0)) for row in calculations
        )
        futa = sum(
            int((row.trace_json.get("unemployment") or {}).get("futa_cents", 0))
            for row in calculations
        )
        utah_ui = sum(
            int((row.trace_json.get("unemployment") or {}).get("utah_ui_cents", 0))
            for row in calculations
        )
        settings = _settings_dict(session.query(HRPayrollSettings).first())
        federal_due = federal_deposit_due_date(
            run.pay_date, settings["federal_deposit_schedule"]
        )
        quarter_due = quarter_due_date(run.pay_date)
        utah_due = (
            month_due_date(run.pay_date)
            if get_company_profile().get("utah_withholding_payment_frequency") == "monthly"
            else quarter_due
        )
        liabilities = (
            ("IRS", "federal_payroll_deposit", federal_amount, federal_due),
            ("Utah Tax Commission", "utah_withholding", utah_withholding, utah_due),
            ("IRS", "futa", futa, quarter_due),
            ("Utah Workforce Services", "utah_unemployment", utah_ui, quarter_due),
        )
        for agency, liability_type, amount, due_date in liabilities:
            if amount:
                session.add(HRTaxLiability(
                    payroll_run_id=run_id, agency=agency,
                    liability_type=liability_type, amount_cents=amount,
                    due_date=due_date,
                ))
        _audit(session, actor, "payroll.approved", "payroll_run", run_id, {
            "snapshot_hash": combined_hash, "liability_count": sum(
                1 for _, _, amount, _ in liabilities if amount
            ),
        })
        return True, "payroll_approved"


def payroll_run_detail(run_id: str, *, employee_email: str | None = None) -> dict | None:
    with _session() as session:
        run = session.query(HRPayrollRun).filter_by(base44_id=run_id).first()
        if not run:
            return None
        query = session.query(HRPayrollCalculation).filter_by(payroll_run_id=run_id)
        if employee_email:
            query = query.filter_by(employee_email=employee_email.strip().lower())
        calculations = query.order_by(HRPayrollCalculation.employee_email).all()
        if employee_email and calculations:
            _audit(session, employee_email, "pay_statement.viewed", "payroll_run", run_id)
        checks = {
            row.employee_email: row for row in session.query(HRPrintedCheck).filter(
                HRPrintedCheck.payroll_run_id == run_id,
                HRPrintedCheck.status != "voided",
            ).all()
        }
        return {
            "id": run.base44_id, "status": run.status,
            "period_start": run.pay_period_start, "period_end": run.pay_period_end,
            "pay_date": run.pay_date, "prepared_by": run.initiated_by,
            "gross": cents_to_dollars(run.total_gross_cents),
            "net": cents_to_dollars(run.total_net_cents),
            "taxes": cents_to_dollars(run.total_taxes_cents),
            "cash_impact": cents_to_dollars(run.total_net_cents + run.total_taxes_cents),
            "calculations": [{
                "employee_email": row.employee_email, "inputs": row.inputs_json or {},
                "results": row.results_json or {}, "trace": row.trace_json or {},
                "snapshot_hash": row.snapshot_hash,
                "check_number": checks.get(row.employee_email).check_number
                if checks.get(row.employee_email) else "",
                "check_status": checks.get(row.employee_email).status
                if checks.get(row.employee_email) else "",
            } for row in calculations],
        }


def employee_pay_statements(employee_email: str) -> list[dict]:
    email = (employee_email or "").strip().lower()
    with _session() as session:
        run_ids = [
            row[0] for row in session.query(HRPrintedCheck.payroll_run_id).filter_by(
                employee_email=email, status="ready"
            ).all()
        ]
        if not run_ids:
            return []
        runs = session.query(HRPayrollRun).filter(
            HRPayrollRun.base44_id.in_(run_ids),
            HRPayrollRun.status.in_(("checks_issued", "closed")),
        ).order_by(HRPayrollRun.pay_date.desc()).all()
        statements = []
        for run in runs:
            calculation = session.query(HRPayrollCalculation).filter_by(
                payroll_run_id=run.base44_id, employee_email=email, version=1
            ).first()
            if calculation:
                statements.append({
                    "id": run.base44_id, "pay_date": run.pay_date,
                    "period_start": run.pay_period_start, "period_end": run.pay_period_end,
                    "status": run.status,
                    "gross": cents_to_dollars(
                        calculation.results_json.get("taxable_gross_cents", 0)
                    ),
                    "net": cents_to_dollars(calculation.results_json.get("net_cents", 0)),
                })
        return statements


def issue_printed_check(run_id: str, *, employee_email: str, check_number: str,
                        actor: str) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if not check_number.strip():
        return False, "check_number_required"
    with _session() as session:
        run = session.query(HRPayrollRun).filter_by(base44_id=run_id).first()
        if not run or run.status not in {"approved", "checks_issued"}:
            return False, "payroll_not_approved"
        if session.query(HRPrintedCheck).filter_by(check_number=check_number.strip()).first():
            return False, "check_number_used"
        if session.query(HRPrintedCheck).filter(
            HRPrintedCheck.payroll_run_id == run_id,
            HRPrintedCheck.employee_email == email,
            HRPrintedCheck.status != "voided",
        ).first():
            return False, "check_already_issued"
        calculation = session.query(HRPayrollCalculation).filter_by(
            payroll_run_id=run_id, employee_email=email, version=1
        ).first()
        line = session.query(HRPayrollLineItem).filter_by(
            payroll_run_id=run_id, employee_email=email
        ).first()
        if not calculation or not line:
            return False, "employee_not_in_run"
        results = calculation.results_json or {}
        check = HRPrintedCheck(
            base44_id=f"check_{secrets.token_hex(10)}", payroll_run_id=run_id,
            payroll_line_item_id=line.base44_id, employee_email=email,
            employee_name=email, pay_period_start=run.pay_period_start,
            pay_period_end=run.pay_period_end, pay_date=run.pay_date,
            check_number=check_number.strip(), gross_pay_cents=results["taxable_gross_cents"],
            federal_income_tax_cents=results["federal_cents"],
            social_security_tax_cents=results["social_security_cents"],
            medicare_tax_cents=results["medicare_cents"],
            state_income_tax_cents=results["utah_cents"],
            total_deductions_cents=results["deductions_cents"],
            net_pay_cents=results["net_cents"],
            total_hours=Decimal(calculation.inputs_json.get("regular_hours", "0"))
            + Decimal(calculation.inputs_json.get("overtime_hours", "0")),
            hourly_rate_cents=line.hourly_rate_cents, status="ready",
            notes="Recorded manual check; bank clearing must be reconciled separately.",
        )
        session.add(check)
        session.add(HRPaycheck(
            base44_id=f"stub_{secrets.token_hex(10)}", employee_email=email,
            pay_period_id=run_id, pay_date=run.pay_date,
            gross_pay_cents=results["taxable_gross_cents"],
            deductions_cents=(
                results["federal_cents"] + results["utah_cents"]
                + results["social_security_cents"] + results["medicare_cents"]
                + results["deductions_cents"]
            ),
            net_pay_cents=results["net_cents"],
            total_hours=check.total_hours,
            notes=f"Check {check_number.strip()}",
        ))
        session.flush()
        issued_count = session.query(func.count(HRPrintedCheck.id)).filter_by(
            payroll_run_id=run_id
        ).scalar() or 0
        if issued_count >= run.employee_count:
            run.status = "checks_issued"
        _audit(session, actor, "payroll.check_issued", "printed_check", check.id, {
            "run_id": run_id, "employee_email": email, "check_number": check_number.strip(),
        })
        return True, "check_issued"


def void_and_reissue_check(
    run_id: str, *, employee_email: str, reason: str,
    new_check_number: str, actor: str,
) -> tuple[bool, str]:
    email = (employee_email or "").strip().lower()
    if not reason.strip() or not new_check_number.strip():
        return False, "void_reason_required"
    with _session() as session:
        run = session.query(HRPayrollRun).filter_by(base44_id=run_id).first()
        old = session.query(HRPrintedCheck).filter(
            HRPrintedCheck.payroll_run_id == run_id,
            HRPrintedCheck.employee_email == email,
            HRPrintedCheck.status != "voided",
        ).first()
        if not run or not old:
            return False, "check_not_found"
        old.status = "voided"
        old.notes = f"{old.notes}\nVoided by {actor}: {reason.strip()}".strip()
        run.status = "approved"
        _audit(session, actor, "payroll.check_voided", "printed_check", old.id, {
            "reason": reason.strip(), "original_check_number": old.check_number,
        })
    ok, message = issue_printed_check(
        run_id, employee_email=email, check_number=new_check_number, actor=actor
    )
    return (ok, "check_reissued" if ok else message)


def close_payroll_run(run_id: str, *, actor: str) -> tuple[bool, str]:
    with _session() as session:
        run = session.query(HRPayrollRun).filter_by(base44_id=run_id).first()
        if not run or run.status != "checks_issued":
            return False, "checks_not_complete"
        active_checks = session.query(func.count(HRPrintedCheck.id)).filter(
            HRPrintedCheck.payroll_run_id == run_id,
            HRPrintedCheck.status == "ready",
        ).scalar() or 0
        if active_checks != run.employee_count:
            return False, "checks_not_complete"
        liabilities = session.query(HRTaxLiability).filter_by(
            payroll_run_id=run_id
        ).all()
        if any(row.status != "reconciled" for row in liabilities):
            return False, "liabilities_not_reconciled"
        run.status = "closed"
        _audit(session, actor, "payroll.closed", "payroll_run", run_id)
        return True, "payroll_closed"
