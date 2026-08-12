"""Safety and idempotency tests for the Base44 HR recovery tool."""

from __future__ import annotations

import csv
from io import BytesIO, StringIO
import os
import tempfile
import unittest
import uuid
from zipfile import ZIP_DEFLATED, ZipFile

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/hr_section_test.db",
)
os.environ.setdefault("HR_PII_SECRET", "test-only-hr-pii-secret")

try:
    from sales_support_agent.services.hr import legacy_import
    from sales_support_agent.services.hr import payroll_store
    from sales_support_agent.services.hr import store as hr_store
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _csv_bytes(rows: list[dict[str, object]], headers: list[str]) -> bytes:
    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _archive(email: str, suffix: str) -> bytes:
    files = {
        "Organization_export.csv": _csv_bytes([{"id": f"org-{suffix}"}], ["id"]),
        "PayrollSettings_export.csv": _csv_bytes(
            [{"id": f"settings-{suffix}", "futa_rate": "0.99"}],
            ["id", "futa_rate"],
        ),
        "Team_export.csv": _csv_bytes(
            [{"id": f"team-{suffix}", "name": "Recovery", "manager_email": ""}],
            ["id", "name", "manager_email"],
        ),
        "PayPeriod_export.csv": _csv_bytes(
            [{
                "id": f"period-{suffix}", "employee_email": email,
                "start_date": "2026-07-01", "end_date": "2026-07-15",
                "total_hours": "8", "status": "approved",
            }],
            ["id", "employee_email", "start_date", "end_date", "total_hours", "status"],
        ),
        "TimeEntry_export.csv": _csv_bytes(
            [{
                "id": f"time-{suffix}", "employee_email": email,
                "date": "2026-07-01", "start_time": "09:00",
                "stop_time": "17:00", "hours": "8",
                "pay_period_id": f"period-{suffix}",
            }, {
                "id": f"sample-time-{suffix}", "employee_email": "demo@example.com",
                "date": "2026-07-01", "hours": "8", "is_sample": "true",
            }, {
                "id": f"orphan-time-{suffix}", "employee_email": f"orphan-{suffix}@anatainc.com",
                "date": "2026-07-01", "start_time": "09:00",
                "stop_time": "09:00", "hours": "", "is_sample": "false",
            }],
            [
                "id", "employee_email", "date", "start_time", "stop_time",
                "hours", "pay_period_id", "is_sample",
            ],
        ),
        "PayrollRun_export.csv": _csv_bytes(
            [{
                "id": f"run-{suffix}", "pay_period_start": "2026-07-01",
                "pay_period_end": "2026-07-15", "pay_date": "2026-07-20",
                "status": "completed", "total_gross": "200",
                "total_net": "150", "total_taxes": "50", "employee_count": "1",
            }],
            [
                "id", "pay_period_start", "pay_period_end", "pay_date", "status",
                "total_gross", "total_net", "total_taxes", "employee_count",
            ],
        ),
        "PayrollLineItem_export.csv": _csv_bytes(
            [{
                "id": f"line-paid-{suffix}", "payroll_run_id": f"run-{suffix}",
                "employee_email": email, "employee_name": "Recovery Worker",
                "total_hours": "8", "hourly_rate": "25", "gross_pay": "200",
                "federal_income_tax": "20", "social_security_tax": "12.40",
                "medicare_tax": "2.90", "state_income_tax": "8",
                "total_deductions": "43.30", "net_pay": "156.70", "status": "sent",
            }, {
                # This attempted calculation has no printed-check evidence and
                # must not inflate the opening balance.
                "id": f"line-attempt-{suffix}", "payroll_run_id": f"run-{suffix}",
                "employee_email": email, "employee_name": "Recovery Worker",
                "total_hours": "8", "hourly_rate": "25", "gross_pay": "999",
                "federal_income_tax": "99", "social_security_tax": "0",
                "medicare_tax": "0", "state_income_tax": "0",
                "total_deductions": "99", "net_pay": "900", "status": "failed",
            }],
            [
                "id", "payroll_run_id", "employee_email", "employee_name",
                "total_hours", "hourly_rate", "gross_pay", "federal_income_tax",
                "social_security_tax", "medicare_tax", "state_income_tax",
                "total_deductions", "net_pay", "status",
            ],
        ),
        "PrintedCheck_export.csv": _csv_bytes(
            [{
                "id": f"check-{suffix}", "payroll_run_id": f"run-{suffix}",
                "payroll_line_item_id": f"line-paid-{suffix}",
                "employee_email": email, "employee_name": "Recovery Worker",
                "pay_period_start": "2026-07-01", "pay_period_end": "2026-07-15",
                "pay_date": "2026-07-20", "check_number": "101",
                "gross_pay": "200", "federal_income_tax": "20",
                "social_security_tax": "12.40", "medicare_tax": "2.90",
                "state_income_tax": "8", "total_deductions": "43.30",
                "net_pay": "156.70", "total_hours": "8",
                "hourly_rate": "25", "status": "ready",
            }],
            [
                "id", "payroll_run_id", "payroll_line_item_id", "employee_email",
                "employee_name", "pay_period_start", "pay_period_end", "pay_date",
                "check_number", "gross_pay", "federal_income_tax",
                "social_security_tax", "medicare_tax", "state_income_tax",
                "total_deductions", "net_pay", "total_hours", "hourly_rate", "status",
            ],
        ),
        "Paycheck_export.csv": _csv_bytes(
            [{
                "id": f"paycheck-{suffix}", "employee_email": email,
                "pay_period_id": f"period-{suffix}", "pay_date": "2026-07-20",
                "gross_pay": "200", "deductions": "43.30",
                "net_pay": "156.70", "total_hours": "8",
            }],
            [
                "id", "employee_email", "pay_period_id", "pay_date",
                "gross_pay", "deductions", "net_pay", "total_hours",
            ],
        ),
    }
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return output.getvalue()


@unittest.skipUnless(DEPS, "SQLAlchemy required")
class LegacyHRImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from sales_support_agent.models import database
        from sales_support_agent.models.database import create_session_factory, init_database

        cls._previous_engine = database.engine
        cls._database_dir = tempfile.TemporaryDirectory()
        path = os.path.join(cls._database_dir.name, "legacy-import.sqlite3")
        cls._session_factory = create_session_factory(f"sqlite:///{path}")
        init_database(cls._session_factory)

    @classmethod
    def tearDownClass(cls):
        from sales_support_agent.models import database

        cls._session_factory.kw["bind"].dispose()
        database.engine = cls._previous_engine
        cls._database_dir.cleanup()

    def test_preview_and_import_are_guarded_and_idempotent(self):
        suffix = uuid.uuid4().hex[:10]
        email = f"recovery-{suffix}@anatainc.com"
        payload = _archive(email, suffix)

        preview = legacy_import.preview_legacy_export(payload)
        self.assertEqual(len(preview["employees"]), 1)
        self.assertEqual(preview["employees"][0]["gross_cents"], 20_000)
        self.assertEqual(preview["sample_rows_excluded"], 1)
        self.assertEqual(preview["orphan_rows_excluded"], 1)

        with self.assertRaises(legacy_import.LegacyImportError):
            legacy_import.import_legacy_export(
                payload, actor="david@anatainc.com",
                expected_digest="wrong", attested=True,
            )

        result = legacy_import.import_legacy_export(
            payload, actor="david@anatainc.com",
            expected_digest=preview["digest"], attested=True,
        )
        self.assertEqual(result["counts"]["employees_created"], 1)
        self.assertEqual(result["counts"]["time_entries"], 1)
        self.assertIsNone(
            hr_store.get_employee_by_email(f"orphan-{suffix}@anatainc.com")
        )
        employee = hr_store.get_employee_by_email(email)
        self.assertEqual(employee["status"], "inactive")
        self.assertEqual(employee["hourly_rate_cents"], 2_500)

        balance = next(
            item for item in payroll_store.list_opening_balances(2026)
            if item["employee_email"] == email
        )
        self.assertEqual(balance["gross_wages"], "200.00")
        self.assertNotEqual(balance["approval_status"], "approved")

        again = legacy_import.import_legacy_export(
            payload, actor="david@anatainc.com",
            expected_digest=preview["digest"], attested=True,
        )
        self.assertEqual(again["counts"]["employees_created"], 0)
        self.assertGreater(again["counts"]["existing_rows_skipped"], 0)

    def test_rejects_unexpected_files(self):
        output = BytesIO()
        with ZipFile(output, "w", ZIP_DEFLATED) as archive:
            archive.writestr("../not-allowed.csv", b"id\n1\n")
        with self.assertRaises(legacy_import.LegacyImportError):
            legacy_import.preview_legacy_export(output.getvalue())


if __name__ == "__main__":
    unittest.main()
