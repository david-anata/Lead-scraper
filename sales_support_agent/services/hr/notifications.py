"""Privacy-safe HR reminder digest generation and delivery."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import os

from sqlalchemy import func
from sqlalchemy.orm import Session

from sales_support_agent.models.database import get_engine
from sales_support_agent.models.hr import (
    HRAuditEvent,
    HRComplianceTask,
    HRContractorProfile,
    HREmployee,
    HREmployeeOnboarding,
    HRPTORequest,
    HRTaxLiability,
    HRTimeCorrection,
    HRTimeEntry,
)
from sales_support_agent.services.access.notify import _send
from sales_support_agent.services.hr.store import ensure_annual_compliance_tasks


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


def reminder_items(today: date | None = None) -> list[dict]:
    """Return aggregate action items without compensation or sensitive values."""
    today = today or date.today()
    ensure_annual_compliance_tasks(today.year)
    due_cutoff = today + timedelta(days=7)
    expiry_cutoff = today + timedelta(days=30)
    items: list[dict] = []
    with _session() as session:
        counts = (
            (
                session.query(func.count(HREmployeeOnboarding.id)).filter(
                    HREmployeeOnboarding.status != "complete"
                ).scalar() or 0,
                "employee onboarding record(s) need review",
                "/admin/hr/employees",
            ),
            (
                session.query(func.count(HRTimeEntry.id)).filter(
                    HRTimeEntry.stop_time == "", HRTimeEntry.date < today
                ).scalar() or 0,
                "open punch(es) are older than today",
                "/admin/hr/time",
            ),
            (
                session.query(func.count(HRTimeCorrection.id)).filter_by(
                    status="requested"
                ).scalar() or 0,
                "time correction(s) await review",
                "/admin/hr/time",
            ),
            (
                session.query(func.count(HRPTORequest.id)).filter_by(
                    status="pending"
                ).scalar() or 0,
                "PTO request(s) await review",
                "/admin/hr/time",
            ),
            (
                session.query(func.count(HRComplianceTask.id)).filter(
                    HRComplianceTask.status != "confirmed",
                    HRComplianceTask.due_date <= due_cutoff,
                ).scalar() or 0,
                "employer compliance task(s) are due within seven days or overdue",
                "/admin/hr/compliance",
            ),
            (
                session.query(func.count(HRTaxLiability.id)).filter(
                    HRTaxLiability.status != "reconciled",
                    HRTaxLiability.due_date <= due_cutoff,
                ).scalar() or 0,
                "payroll liability item(s) are due within seven days or overdue",
                "/admin/hr/payroll",
            ),
            (
                session.query(func.count(HRContractorProfile.id)).filter(
                    HRContractorProfile.tax_form_status.in_(("missing", "requested", "expired"))
                ).scalar() or 0,
                "contractor tax-form record(s) need attention",
                "/admin/hr/contractors",
            ),
            (
                session.query(func.count(HREmployee.id)).filter(
                    HREmployee.i9_expiration_date.is_not(None),
                    HREmployee.i9_expiration_date <= expiry_cutoff,
                    HREmployee.i9_expiration_date >= today,
                ).scalar() or 0,
                "I-9 document expiration(s) fall within 30 days",
                "/admin/hr/employees",
            ),
        )
        for count, label, path in counts:
            if count:
                items.append({"count": int(count), "label": label, "path": path})
    return items


def _recipients() -> list[str]:
    configured = os.getenv(
        "HR_PAYROLL_ADMIN_EMAILS", "david@anatainc.com,val@anatainc.com"
    )
    return sorted({
        value.strip().lower() for value in configured.split(",") if "@" in value
    })


def run_daily_digest(settings, *, base_url: str, dry_run: bool = False,
                     today: date | None = None) -> dict:
    """Send at most one aggregate digest per recipient per calendar day."""
    today = today or date.today()
    items = reminder_items(today)
    recipients = _recipients()
    result = {
        "date": today.isoformat(), "items": items, "recipients": recipients,
        "sent": 0, "skipped": 0, "failed": 0, "dry_run": dry_run,
    }
    if dry_run or not items:
        return result
    base = base_url.rstrip("/")
    lines = [
        "Anata HR has the following items ready for human review:",
        "",
        *[
            f"- {item['count']} {item['label']}: {base}{item['path']}"
            for item in items
        ],
        "",
        "This email intentionally contains no compensation, SSN, tax-election, "
        "or pay-statement details.",
    ]
    for recipient in recipients:
        dedupe_key = f"{today.isoformat()}:{recipient}"
        with _session() as session:
            prior = session.query(HRAuditEvent).filter_by(
                action="hr.reminder_digest_sent", entity_id=dedupe_key
            ).first()
        if prior:
            result["skipped"] += 1
            continue
        sent = _send(
            settings, to_email=recipient, subject="Anata HR items need review",
            text="\n".join(lines),
        )
        if not sent:
            result["failed"] += 1
            continue
        with _session() as session:
            session.add(HRAuditEvent(
                actor_email="system", action="hr.reminder_digest_sent",
                entity_type="hr_notification", entity_id=dedupe_key,
                details={"item_count": len(items), "recipient": recipient},
                created_at=datetime.now(timezone.utc),
            ))
        result["sent"] += 1
    return result
