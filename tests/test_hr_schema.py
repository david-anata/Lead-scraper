"""HR / payroll schema — Phase 0 foundation (single-org Anata port of Base44 HR)."""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid

os.environ.setdefault("SALES_AGENT_DB_URL", "sqlite:///" + tempfile.gettempdir() + "/hr_schema_test.db")

try:
    from sqlalchemy import inspect
    from sales_support_agent.main import app  # noqa: F401 — triggers init_database
    from sales_support_agent.models import database as db
    from sales_support_agent.models.hr import HREmployee, HRPayrollRun, HRPayrollLineItem
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


_EXPECTED_TABLES = {
    "hr_employees", "hr_teams", "hr_payroll_settings", "hr_company_profiles", "hr_pay_schedules",
    "hr_pay_periods", "hr_time_entries", "hr_payroll_runs", "hr_payroll_line_items",
    "hr_paychecks", "hr_printed_checks", "hr_employee_handbooks",
    "hr_handbook_acknowledgements",
    "hr_pto_requests", "hr_audit_events",
    "hr_employment_profiles", "hr_employee_onboarding", "hr_tax_elections",
    "hr_time_corrections", "hr_pto_ledger", "hr_payroll_inputs",
    "hr_payroll_approvals", "hr_tax_liabilities", "hr_payroll_calculations",
    "hr_opening_payroll_balances",
    "hr_contractor_payments", "hr_offboarding_checklists",
}


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class HRSchemaTests(unittest.TestCase):
    def test_all_hr_tables_created(self) -> None:
        names = set(inspect(db.get_engine()).get_table_names())
        self.assertTrue(_EXPECTED_TABLES.issubset(names),
                        f"missing HR tables: {_EXPECTED_TABLES - names}")
        liability_columns = {
            row["name"] for row in inspect(db.get_engine()).get_columns("hr_tax_liabilities")
        }
        self.assertIn("confirmed_amount_cents", liability_columns)
        self.assertIn("filing_confirmation_number", liability_columns)
        time_columns = {
            row["name"] for row in inspect(db.get_engine()).get_columns("hr_time_entries")
        }
        self.assertIn("elapsed_seconds", time_columns)

    def test_employee_money_in_cents_and_base44_id(self) -> None:
        Sess = db.create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        s = Sess()
        try:
            suffix = uuid.uuid4().hex
            email = f"cents-{suffix}@anatainc.com"
            employee_id = f"u-{suffix}"
            s.add(HREmployee(base44_id=employee_id, email=email,
                             full_name="Cents Test", hr_role="manager",
                             hourly_rate_cents=2575, annual_salary_cents=8500000))
            s.commit()
            e = s.query(HREmployee).filter_by(email=email).first()
            self.assertEqual(e.hourly_rate_cents, 2575)   # $25.75 stored exactly
            self.assertEqual(e.hr_role, "manager")
            self.assertEqual(e.base44_id, employee_id)
        finally:
            s.close()

    def test_payroll_run_and_line_item_link(self) -> None:
        Sess = db.create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        s = Sess()
        try:
            suffix = uuid.uuid4().hex
            run_id = f"run-{suffix}"
            s.add(HRPayrollRun(base44_id=run_id, status="completed",
                               total_gross_cents=500000, total_net_cents=400000,
                               total_taxes_cents=100000, employee_count=2))
            s.add(HRPayrollLineItem(base44_id=f"li-{suffix}", payroll_run_id=run_id,
                                    employee_email=f"cents-{suffix}@anatainc.com",
                                    gross_pay_cents=250000, net_pay_cents=200000))
            s.commit()
            li = s.query(HRPayrollLineItem).filter_by(payroll_run_id=run_id).all()
            self.assertEqual(len(li), 1)
            self.assertEqual(li[0].net_pay_cents, 200000)
        finally:
            s.close()


if __name__ == "__main__":
    unittest.main()
