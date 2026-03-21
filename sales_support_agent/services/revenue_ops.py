"""Shared DB-backed state for the lead engine and sales support agent."""

from __future__ import annotations

import os
import secrets
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select

from sales_support_agent.models.database import create_session_factory, init_database, session_scope
from sales_support_agent.models.entities import (
    CampaignEnrollment,
    Company,
    Contact,
    Cooldown,
    LeadRecord,
    LeadRun,
    LeadRunItem,
    RevenueAction,
    RevenueEvent,
    SourceCursor,
)


def _default_db_url() -> str:
    runtime_dir = Path("runtime")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{runtime_dir / 'sales_support_agent.sqlite3'}"


REVENUE_OPS_DB_URL = (
    os.getenv("LEAD_ENGINE_DB_URL", "").strip()
    or os.getenv("SALES_AGENT_DB_URL", "").strip()
    or _default_db_url()
)

SESSION_FACTORY = create_session_factory(REVENUE_OPS_DB_URL)
init_database(SESSION_FACTORY)


def generate_run_id() -> str:
    return secrets.token_hex(8)


def load_processed_domains_db() -> set[str]:
    with session_scope(SESSION_FACTORY) as session:
        rows = session.execute(
            select(Company.domain).where(Company.last_exported_at.is_not(None))
        ).all()
    return {str(domain or "").strip().lower() for domain, in rows if str(domain or "").strip()}


def append_processed_domains_db(domains: set[str], run_date: str) -> None:
    if not domains:
        return
    exported_at = _parse_run_date(run_date)
    with session_scope(SESSION_FACTORY) as session:
        existing = {
            str(company.domain or "").strip().lower(): company
            for company in session.execute(
                select(Company).where(Company.domain.in_(sorted(domains)))
            ).scalars()
        }
        for domain in sorted(domains):
            normalized = str(domain or "").strip().lower()
            if not normalized:
                continue
            company = existing.get(normalized)
            if company is None:
                company = Company(domain=normalized, website=normalized)
                session.add(company)
                existing[normalized] = company
            company.last_exported_at = exported_at
            company.last_seen_at = datetime.now(timezone.utc)


def load_apollo_attempts_db() -> dict[str, dict[str, str]]:
    with session_scope(SESSION_FACTORY) as session:
        rows = session.execute(
            select(Cooldown).where(Cooldown.scope == "apollo_people_domain")
        ).scalars()
        attempts = {
            str(row.entity_key or "").strip().lower(): {
                "domain": str(row.entity_key or "").strip().lower(),
                "last_attempted_at": row.last_attempted_at.isoformat() if row.last_attempted_at else "",
                "result": row.result or "",
                "cooldown_until": row.cooldown_until.isoformat() if row.cooldown_until else "",
            }
            for row in rows
            if str(row.entity_key or "").strip()
        }
    return attempts


def upsert_apollo_attempts_db(attempt_rows: list[dict[str, str]]) -> None:
    if not attempt_rows:
        return
    with session_scope(SESSION_FACTORY) as session:
        existing = {
            str(row.entity_key or "").strip().lower(): row
            for row in session.execute(
                select(Cooldown).where(Cooldown.scope == "apollo_people_domain")
            ).scalars()
        }
        for attempt in attempt_rows:
            domain = str(attempt.get("domain") or "").strip().lower()
            if not domain:
                continue
            cooldown = existing.get(domain)
            if cooldown is None:
                cooldown = Cooldown(scope="apollo_people_domain", entity_key=domain)
                session.add(cooldown)
                existing[domain] = cooldown
            cooldown.result = str(attempt.get("result") or "")
            cooldown.last_attempted_at = _parse_iso_datetime(str(attempt.get("last_attempted_at") or ""))
            cooldown.cooldown_until = _parse_iso_datetime(str(attempt.get("cooldown_until") or ""))
            cooldown.metadata_json = dict(attempt)


def load_source_cursor_db(source_key: str, default: int) -> int:
    with session_scope(SESSION_FACTORY) as session:
        cursor = session.execute(
            select(SourceCursor).where(SourceCursor.source_key == source_key).limit(1)
        ).scalar_one_or_none()
    if cursor is None:
        return default
    try:
        value = int((cursor.next_cursor or "").strip() or default)
    except (TypeError, ValueError):
        value = default
    return max(value, 1)


def save_source_cursor_db(source_key: str, next_value: int, metadata: dict[str, Any] | None = None) -> None:
    with session_scope(SESSION_FACTORY) as session:
        cursor = session.execute(
            select(SourceCursor).where(SourceCursor.source_key == source_key).limit(1)
        ).scalar_one_or_none()
        if cursor is None:
            cursor = SourceCursor(source_key=source_key)
            session.add(cursor)
        cursor.next_cursor = str(max(next_value, 1))
        cursor.metadata_json = dict(metadata or {})
        cursor.updated_at = datetime.now(timezone.utc)


def load_processed_heyreach_leads_db() -> set[str]:
    with session_scope(SESSION_FACTORY) as session:
        rows = session.execute(
            select(CampaignEnrollment.enrollment_key).where(CampaignEnrollment.channel == "heyreach")
        ).all()
    return {str(key or "").strip() for key, in rows if str(key or "").strip()}


def append_processed_heyreach_leads_db(lead_rows: list[dict[str, str]], run_date: str) -> None:
    if not lead_rows:
        return
    run_dt = _parse_run_date(run_date)
    with session_scope(SESSION_FACTORY) as session:
        existing_keys = {
            str(key or "").strip()
            for key, in session.execute(
                select(CampaignEnrollment.enrollment_key).where(CampaignEnrollment.channel == "heyreach")
            ).all()
        }
        for row in lead_rows:
            lead_key = str(row.get("lead_key") or "").strip()
            if not lead_key or lead_key in existing_keys:
                continue
            session.add(
                CampaignEnrollment(
                    enrollment_key=lead_key,
                    channel="heyreach",
                    campaign_id=str(row.get("campaign_id") or ""),
                    status="created",
                    metadata_json=dict(row),
                    enrolled_at=run_dt,
                )
            )
            existing_keys.add(lead_key)


def load_daily_import_counts_db() -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    with session_scope(SESSION_FACTORY) as session:
        rows = session.execute(
            select(RevenueAction).where(RevenueAction.action_type == "instantly_import_count")
        ).scalars()
        for row in rows:
            date_key = row.payload_json.get("date", "") if isinstance(row.payload_json, dict) else ""
            count_value = row.payload_json.get("imported_count", 0) if isinstance(row.payload_json, dict) else 0
            try:
                imported_count = int(count_value or 0)
            except (TypeError, ValueError):
                imported_count = 0
            if date_key:
                counts[str(date_key)] += max(imported_count, 0)
    return dict(counts)


def append_daily_import_count_db(run_date: str, imported_count: int, run_id: str = "") -> None:
    if imported_count <= 0:
        return
    with session_scope(SESSION_FACTORY) as session:
        session.add(
            RevenueAction(
                action_type="instantly_import_count",
                subject_type="lead_run",
                subject_key=run_date,
                run_id=run_id,
                success=True,
                payload_json={"date": run_date, "imported_count": imported_count},
            )
        )


def create_lead_run(trigger_source: str, run_date: str, max_domains: int, request_payload: dict[str, Any]) -> str:
    run_id = generate_run_id()
    with session_scope(SESSION_FACTORY) as session:
        session.add(
            LeadRun(
                run_id=run_id,
                status="queued",
                trigger_source=trigger_source,
                current_stage="queued",
                run_date=run_date,
                max_domains=max_domains,
                request_json=request_payload,
                summary_json={},
            )
        )
        session.add(
            RevenueEvent(
                event_type="lead_run_queued",
                subject_type="lead_run",
                subject_key=run_id,
                run_id=run_id,
                payload_json={"trigger_source": trigger_source, "run_date": run_date, "max_domains": max_domains},
            )
        )
    return run_id


def mark_lead_run_started(run_id: str, stage: str) -> None:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return
        if run.started_at is None:
            run.started_at = datetime.now(timezone.utc)
        run.status = "running"
        run.current_stage = stage


def update_lead_run_stage(run_id: str, stage: str, summary_patch: dict[str, Any] | None = None) -> None:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return
        run.status = "running"
        run.current_stage = stage
        if summary_patch:
            merged = dict(run.summary_json or {})
            merged.update(summary_patch)
            run.summary_json = merged


def complete_lead_run(run_id: str, *, summary: dict[str, Any], csv_content: str) -> None:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return
        run.status = "completed"
        run.current_stage = "completed"
        run.summary_json = dict(summary)
        run.csv_content = csv_content
        run.completed_at = datetime.now(timezone.utc)
        session.add(
            RevenueEvent(
                event_type="lead_run_completed",
                subject_type="lead_run",
                subject_key=run_id,
                run_id=run_id,
                payload_json=dict(summary),
            )
        )


def fail_lead_run(run_id: str, *, stage: str, error_message: str, summary_patch: dict[str, Any] | None = None) -> None:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return
        merged = dict(run.summary_json or {})
        if summary_patch:
            merged.update(summary_patch)
        run.status = "failed"
        run.current_stage = stage
        run.error_message = error_message
        run.summary_json = merged
        run.completed_at = datetime.now(timezone.utc)
        session.add(
            RevenueAction(
                action_type="lead_run_failed",
                subject_type="lead_run",
                subject_key=run_id,
                run_id=run_id,
                success=False,
                payload_json={"stage": stage, "error": error_message, **merged},
            )
        )


def get_lead_run(run_id: str) -> dict[str, Any] | None:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return None
        return {
            "run_id": run.run_id,
            "status": run.status,
            "current_stage": run.current_stage,
            "run_date": run.run_date,
            "max_domains": run.max_domains,
            "request": dict(run.request_json or {}),
            "summary": dict(run.summary_json or {}),
            "error_message": run.error_message or "",
            "has_csv": bool(run.csv_content),
            "created_at": run.created_at.isoformat() if run.created_at else "",
            "started_at": run.started_at.isoformat() if run.started_at else "",
            "completed_at": run.completed_at.isoformat() if run.completed_at else "",
        }


def get_lead_run_csv(run_id: str) -> str:
    with session_scope(SESSION_FACTORY) as session:
        run = session.get(LeadRun, run_id)
        if run is None:
            return ""
        return run.csv_content or ""


def record_lead_run_item(
    run_id: str,
    *,
    stage: str,
    status: str,
    domain: str = "",
    company_id: int | None = None,
    contact_id: int | None = None,
    reason: str = "",
    payload: dict[str, Any] | None = None,
) -> None:
    with session_scope(SESSION_FACTORY) as session:
        session.add(
            LeadRunItem(
                run_id=run_id,
                company_id=company_id,
                contact_id=contact_id,
                domain=domain,
                stage=stage,
                status=status,
                reason=reason,
                payload_json=dict(payload or {}),
                updated_at=datetime.now(timezone.utc),
            )
        )


def upsert_lead_rows(run_id: str, instantly_rows: list[dict[str, Any]], heyreach_rows: list[dict[str, str]]) -> None:
    if not instantly_rows and not heyreach_rows:
        return
    with session_scope(SESSION_FACTORY) as session:
        companies_by_domain: dict[str, Company] = {
            str(company.domain or "").strip().lower(): company
            for company in session.execute(select(Company)).scalars()
        }
        contacts_by_email: dict[str, Contact] = {
            str(contact.email or "").strip().lower(): contact
            for contact in session.execute(select(Contact)).scalars()
            if str(contact.email or "").strip()
        }
        contacts_by_linkedin: dict[str, Contact] = {
            str(contact.linkedin_url or "").strip().lower(): contact
            for contact in session.execute(select(Contact)).scalars()
            if str(contact.linkedin_url or "").strip()
        }
        lead_records_by_key: dict[str, LeadRecord] = {
            str(record.lead_key or ""): record
            for record in session.execute(select(LeadRecord)).scalars()
        }
        enrollments_by_key: dict[str, CampaignEnrollment] = {
            str(enrollment.enrollment_key or ""): enrollment
            for enrollment in session.execute(select(CampaignEnrollment)).scalars()
        }

        for row in instantly_rows:
            domain = str(row.get("website") or "").strip().lower()
            if not domain:
                continue
            company = companies_by_domain.get(domain)
            if company is None:
                company = Company(domain=domain, website=domain)
                session.add(company)
                session.flush()
                companies_by_domain[domain] = company
            company.company_name = str(row.get("company_name") or company.company_name or "")
            company.platform = str(row.get("platform") or company.platform or "")
            company.location = str(row.get("location") or company.location or "")
            company.market_segment = str(row.get("market_segment") or company.market_segment or "")
            company.industry = str(row.get("industry") or company.industry or "")
            company.org_category = str(row.get("org_category") or company.org_category or "")
            company.last_seen_at = datetime.now(timezone.utc)
            company.last_exported_at = datetime.now(timezone.utc)
            company.metadata_json = dict(row)

            email = str(row.get("email") or "").strip().lower()
            linkedin_url = str(row.get("linkedin_url") or "").strip().lower()
            contact = contacts_by_email.get(email) if email else None
            if contact is None and linkedin_url:
                contact = contacts_by_linkedin.get(linkedin_url)
            if contact is None:
                contact = Contact(email=email, linkedin_url=linkedin_url)
                session.add(contact)
                session.flush()
                if email:
                    contacts_by_email[email] = contact
                if linkedin_url:
                    contacts_by_linkedin[linkedin_url] = contact
            contact.company_id = company.id
            contact.full_name = " ".join(part for part in [row.get("first_name", ""), row.get("last_name", "")] if part).strip()
            contact.first_name = str(row.get("first_name") or contact.first_name or "")
            contact.last_name = str(row.get("last_name") or contact.last_name or "")
            contact.role = str(row.get("role") or contact.role or "")
            contact.department = str(row.get("department") or contact.department or "")
            contact.updated_at = datetime.now(timezone.utc)
            contact.metadata_json = dict(row)

            lead_key = f"{company.id}:{contact.id}:email"
            lead_record = lead_records_by_key.get(lead_key)
            if lead_record is None:
                lead_record = LeadRecord(
                    lead_key=lead_key,
                    company_id=company.id,
                    contact_id=contact.id,
                    channel="email",
                )
                session.add(lead_record)
                lead_records_by_key[lead_key] = lead_record
            lead_record.status = "accepted"
            lead_record.source_run_id = run_id
            lead_record.last_seen_at = datetime.now(timezone.utc)
            lead_record.last_qualified_at = datetime.now(timezone.utc)
            lead_record.metadata_json = dict(row)

            campaign_id = str(row.get("campaign_id") or "")
            enrollment_key = f"instantly::{campaign_id}::{email}" if campaign_id and email else ""
            if enrollment_key and enrollment_key not in enrollments_by_key:
                enrollments_by_key[enrollment_key] = CampaignEnrollment(
                    enrollment_key=enrollment_key,
                    lead_record_id=lead_record.id,
                    company_id=company.id,
                    contact_id=contact.id,
                    channel="instantly",
                    campaign_id=campaign_id,
                    campaign_name=str(row.get("campaign_name") or ""),
                    status="created",
                    metadata_json=dict(row),
                    enrolled_at=datetime.now(timezone.utc),
                )
                session.add(enrollments_by_key[enrollment_key])

        for row in heyreach_rows:
            lead_key = str(row.get("lead_key") or "").strip()
            if not lead_key or lead_key in enrollments_by_key:
                continue
            session.add(
                CampaignEnrollment(
                    enrollment_key=lead_key,
                    channel="heyreach",
                    campaign_id=str(row.get("campaign_id") or ""),
                    status="created",
                    metadata_json=dict(row),
                    enrolled_at=datetime.now(timezone.utc),
                )
            )
            enrollments_by_key[lead_key] = True  # type: ignore[assignment]


def _parse_run_date(run_date: str) -> datetime:
    raw = str(run_date or "").strip()
    if not raw:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return datetime.now(timezone.utc)


def _parse_iso_datetime(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
