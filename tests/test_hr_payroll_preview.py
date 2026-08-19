"""Payroll preview: it must calculate exactly like preparation and save nothing.

The preview exists so the payroll rules can be checked before the readiness gate
opens. Two properties make it trustworthy and both are covered here:

1. It never persists. A read path that can write is not a preview.
2. Its numbers are the numbers preparation would produce. A preview that can
   drift from the real run is worse than no preview.

It also must not invent a figure when a record is missing. A missing W-4 gates
the withholding lines; it never falls back to a default filing status.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from unittest import mock

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/hr_payroll_preview_test.db",
)
os.environ.setdefault("HR_PII_SECRET", "test-only-hr-pii-secret")

try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from sales_support_agent.models.database import Base
    from sales_support_agent.models.hr import (
        HRAuditEvent,
        HROpeningPayrollBalance,
        HRPayrollCalculation,
        HRPayrollRun,
        HRTaxElection,
        HRTimeEntry,
    )
    from sales_support_agent.services.hr import payroll_store
    from sales_support_agent.services.hr import store as hr_store
    from sales_support_agent.services.hr.payroll import semimonthly_period
    DEPS = True
except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


PERIOD_DATE = date(2026, 8, 16)
EMAIL = "hourly@anatainc.com"
SETTINGS = {"utah_ui_rate": "0.006"}


def _engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return engine


def _employee() -> dict:
    return {
        "email": EMAIL,
        "full_name": "Hourly Person",
        "hourly_rate_cents": 3000,
        "employment": {
            "hire_date": date(2026, 1, 1),
            "classification": "non_exempt",
            "pay_basis": "hourly",
        },
    }


def _seed_time(session: Session) -> None:
    """Eight hours a day for five weekdays inside the period."""
    for day in (17, 18, 19, 20, 21):
        session.add(HRTimeEntry(
            employee_email=EMAIL, date=date(2026, 8, day),
            start_time="09:00", stop_time="17:00",
            hours=Decimal("8"), elapsed_seconds=8 * 3600,
        ))


def _seed_w4(session: Session) -> None:
    session.add(HRTaxElection(
        employee_email=EMAIL, effective_date=date(2026, 1, 1),
        filing_status="single", snapshot_hash="w4-preview-test",
    ))


def _seed_opening_balance(session: Session) -> None:
    session.add(HROpeningPayrollBalance(
        employee_email=EMAIL, tax_year=2026,
        gross_wages_cents=1_200_000, social_security_wages_cents=1_200_000,
        medicare_wages_cents=1_200_000, futa_wages_cents=700_000,
        utah_ui_wages_cents=1_200_000, source_note="test",
    ))


@contextmanager
def _patched(engine):
    """Point both HR modules at the test engine; each resolves its own."""
    with mock.patch.object(payroll_store, "get_engine", return_value=engine), \
         mock.patch.object(hr_store, "get_engine", return_value=engine):
        yield


def _calculate(session: Session, engine, *, strict: bool) -> dict:
    with _patched(engine):
        return payroll_store._calculate_employee_period(
            session, employee=_employee(), period=semimonthly_period(PERIOD_DATE),
            settings=SETTINGS, inputs=[], strict=strict,
        )


@unittest.skipUnless(DEPS, "sqlalchemy required")
class PreviewCalculationTests(unittest.TestCase):
    def test_preview_and_preparation_produce_identical_numbers(self):
        """The whole point of sharing one code path. If this fails, they drifted."""
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)
            _seed_opening_balance(session)
            session.commit()

            strict = _calculate(session, engine, strict=True)
            preview = _calculate(session, engine, strict=False)

        self.assertEqual(strict["results"], preview["results"])
        self.assertEqual(strict["trace"], preview["trace"])
        self.assertEqual(preview["unavailable"], [])
        self.assertEqual(preview["caveats"], [])
        self.assertTrue(preview["complete"])

    def test_gross_pay_is_real_arithmetic(self):
        """40 hours at $30 with no overtime is $1,200.00."""
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)
            _seed_opening_balance(session)
            session.commit()
            preview = _calculate(session, engine, strict=False)

        self.assertEqual(preview["results"]["gross_cents"], 120_000)
        self.assertEqual(preview["results"]["overtime_cents"], 0)
        self.assertEqual(preview["regular_hours"], Decimal("40"))

    def test_missing_w4_gates_withholding_rather_than_assuming_a_status(self):
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_opening_balance(session)  # no W-4
            session.commit()
            preview = _calculate(session, engine, strict=False)

        results = preview["results"]
        self.assertIsNone(results["federal_cents"])
        self.assertIsNone(results["utah_cents"])
        self.assertIsNone(results["net_cents"])
        self.assertIsNone(results["employee_taxes_cents"])
        self.assertFalse(preview["complete"])
        self.assertEqual(len(preview["unavailable"]), 1)
        self.assertIn("W-4", preview["unavailable"][0]["reason"])
        # Gross and FICA do not depend on the W-4, so they are still real.
        self.assertEqual(results["gross_cents"], 120_000)
        self.assertGreater(results["social_security_cents"], 0)

    def test_preparation_refuses_to_calculate_without_a_w4(self):
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_opening_balance(session)
            session.commit()
            with self.assertRaises(ValueError):
                _calculate(session, engine, strict=True)

    def test_missing_opening_balance_is_caveated_not_silently_zero(self):
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)  # no opening balance
            session.commit()
            preview = _calculate(session, engine, strict=False)

        self.assertEqual(len(preview["caveats"]), 1)
        caveat = preview["caveats"][0]
        self.assertIn("opening balance", caveat["reason"])
        self.assertIn("overstated", caveat["detail"])
        # The row still computes; the assumption is disclosed rather than hidden.
        self.assertTrue(preview["complete"])
        self.assertEqual(preview["results"]["gross_cents"], 120_000)

    def test_opening_balance_changes_the_unemployment_answer(self):
        """Proves the caveat above is about a real difference, not boilerplate."""
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)
            session.commit()
            without = _calculate(session, engine, strict=False)
            _seed_opening_balance(session)
            session.commit()
            with_balance = _calculate(session, engine, strict=False)

        # $7,000 of FUTA wages are already used up in the opening balance, so a
        # confirmed balance produces less FUTA than assuming a clean slate.
        self.assertGreater(
            without["trace"]["unemployment"]["futa_cents"],
            with_balance["trace"]["unemployment"]["futa_cents"],
        )


    def test_unset_utah_rate_gates_the_line_instead_of_crashing(self):
        """A fresh install has no Utah unemployment rate. That must not 500."""
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)
            _seed_opening_balance(session)
            session.commit()
            with _patched(engine):
                preview = payroll_store._calculate_employee_period(
                    session, employee=_employee(),
                    period=semimonthly_period(PERIOD_DATE),
                    settings={"utah_ui_rate": ""}, inputs=[], strict=False,
                )

        self.assertIsNone(preview["results"]["utah_unemployment_cents"])
        self.assertIsNone(preview["results"]["employer_taxes_cents"])
        self.assertIsNone(preview["results"]["total_employer_cost_cents"])
        self.assertFalse(preview["complete"])
        self.assertTrue(any(
            "unemployment rate" in item["reason"]
            for item in preview["unavailable"]
        ))
        # Employee-side figures do not depend on the employer's rate.
        self.assertEqual(preview["results"]["gross_cents"], 120_000)
        self.assertIsNotNone(preview["results"]["net_cents"])


@unittest.skipUnless(DEPS, "sqlalchemy required")
class PreviewPersistenceTests(unittest.TestCase):
    def test_preview_writes_nothing(self):
        engine = _engine()
        with Session(engine) as session:
            _seed_time(session)
            _seed_w4(session)
            _seed_opening_balance(session)
            session.commit()

        def _counts() -> tuple[int, int, int]:
            with Session(engine) as session:
                return (
                    session.query(HRPayrollRun).count(),
                    session.query(HRPayrollCalculation).count(),
                    session.query(HRAuditEvent).count(),
                )

        before = _counts()
        with _patched(engine):
            payroll_store.preview_payroll(PERIOD_DATE)
        self.assertEqual(_counts(), before)

    def test_readonly_session_discards_writes(self):
        """The preview session rolls back even if a caller adds a row."""
        engine = _engine()
        with _patched(engine):
            with payroll_store._readonly_session() as session:
                session.add(HRTimeEntry(
                    employee_email="leak@anatainc.com", date=date(2026, 8, 18),
                    start_time="09:00", stop_time="17:00", hours=Decimal("8"),
                ))
                session.flush()

        with Session(engine) as session:
            leaked = session.query(HRTimeEntry).filter_by(
                employee_email="leak@anatainc.com"
            ).count()
        self.assertEqual(leaked, 0)


@unittest.skipUnless(DEPS, "fastapi required")
class PreviewRouteTests(unittest.TestCase):
    """The page must render on a real install, including a bare one."""

    @classmethod
    def setUpClass(cls):
        try:
            from fastapi.testclient import TestClient
            from sales_support_agent.main import app
            from sales_support_agent.services.admin_auth import (
                create_user_session_token,
            )
            from sales_support_agent.services.hr import store as store_mod
        except ModuleNotFoundError:  # pragma: no cover - dependency guard
            raise unittest.SkipTest("fastapi required")
        cls.app = app
        cls.client = TestClient(app)
        settings = app.state.agent_settings
        cls.cookie = (
            settings.admin_cookie_name,
            create_user_session_token(
                settings, email="david@anatainc.com", name="David", role="admin",
            ),
        )
        if not store_mod.get_employee_by_email("david@anatainc.com"):
            store_mod.create_employee(
                email="david@anatainc.com", full_name="David",
            )
        store_mod.upsert_employment_profile(
            "david@anatainc.com", hire_date=date(2026, 1, 1),
            classification="exempt", pay_basis="fixed_semimonthly",
            fixed_pay_per_period="1000", actor="test",
        )

    def test_preview_page_renders_and_states_it_saves_nothing(self):
        self.client.cookies.set(*self.cookie)
        try:
            response = self.client.get(
                "/admin/hr/payroll/preview", follow_redirects=False,
            )
        finally:
            self.client.cookies.clear()

        self.assertEqual(response.status_code, 200)
        body = response.text
        self.assertIn("Payroll preview", body)
        self.assertIn("saves nothing", body)
        self.assertIn("Per employee", body)
        # A bare install has no Utah rate, so the gate must be visible, not a zero.
        self.assertIn("not available", body)
        self.assertIn("cannot be calculated", body)

    def test_preview_page_creates_no_payroll_run(self):
        from sales_support_agent.services.hr import payroll_store as store_mod

        before = len(store_mod.control_room(PERIOD_DATE)["runs"])
        self.client.cookies.set(*self.cookie)
        try:
            self.client.get("/admin/hr/payroll/preview", follow_redirects=False)
        finally:
            self.client.cookies.clear()
        after = len(store_mod.control_room(PERIOD_DATE)["runs"])
        self.assertEqual(before, after)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
