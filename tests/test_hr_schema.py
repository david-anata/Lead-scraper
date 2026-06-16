"""HR / payroll schema — Phase 0 foundation (single-org Anata port of Base44 HR)."""

from __future__ import annotations

import os
import tempfile
import unittest

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
    "hr_employees", "hr_teams", "hr_payroll_settings", "hr_pay_schedules",
    "hr_pay_periods", "hr_time_entries", "hr_payroll_runs", "hr_payroll_line_items",
    "hr_paychecks", "hr_printed_checks", "hr_employee_handbooks",
    "hr_handbook_acknowledgements",
}


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class HRSchemaTests(unittest.TestCase):
    def test_all_hr_tables_created(self) -> None:
        names = set(inspect(db.get_engine()).get_table_names())
        self.assertTrue(_EXPECTED_TABLES.issubset(names),
                        f"missing HR tables: {_EXPECTED_TABLES - names}")

    def test_employee_money_in_cents_and_base44_id(self) -> None:
        Sess = db.create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        s = Sess()
        try:
            s.add(HREmployee(base44_id="u-test-1", email="cents@anatainc.com",
                             full_name="Cents Test", hr_role="manager",
                             hourly_rate_cents=2575, annual_salary_cents=8500000))
            s.commit()
            e = s.query(HREmployee).filter_by(email="cents@anatainc.com").first()
            self.assertEqual(e.hourly_rate_cents, 2575)   # $25.75 stored exactly
            self.assertEqual(e.hr_role, "manager")
            self.assertEqual(e.base44_id, "u-test-1")
        finally:
            s.close()

    def test_payroll_run_and_line_item_link(self) -> None:
        Sess = db.create_session_factory(os.environ["SALES_AGENT_DB_URL"])
        s = Sess()
        try:
            s.add(HRPayrollRun(base44_id="run-1", status="completed",
                               total_gross_cents=500000, total_net_cents=400000,
                               total_taxes_cents=100000, employee_count=2))
            s.add(HRPayrollLineItem(base44_id="li-1", payroll_run_id="run-1",
                                    employee_email="cents@anatainc.com",
                                    gross_pay_cents=250000, net_pay_cents=200000))
            s.commit()
            li = s.query(HRPayrollLineItem).filter_by(payroll_run_id="run-1").all()
            self.assertEqual(len(li), 1)
            self.assertEqual(li[0].net_pay_cents, 200000)
        finally:
            s.close()


if __name__ == "__main__":
    unittest.main()
