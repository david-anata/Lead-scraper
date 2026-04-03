"""CRUD for manual obligations and recurring template expansion."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    return datetime.utcnow().date()


def _next_occurrence(current: date, frequency: str, day_of_month: int | None) -> date:
    """Advance *current* by one period according to *frequency*."""
    if frequency == "weekly":
        return current + timedelta(weeks=1)
    if frequency == "biweekly":
        return current + timedelta(weeks=2)
    if frequency == "monthly":
        dom = day_of_month or current.day
        # Advance to same day-of-month next month
        month = current.month + 1
        year = current.year
        if month > 12:
            month = 1
            year += 1
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        dom = min(dom, last_day)
        return date(year, month, dom)
    if frequency == "quarterly":
        month = current.month + 3
        year = current.year
        while month > 12:
            month -= 12
            year += 1
        import calendar
        dom = day_of_month or current.day
        last_day = calendar.monthrange(year, month)[1]
        dom = min(dom, last_day)
        return date(year, month, dom)
    if frequency == "annual":
        try:
            return current.replace(year=current.year + 1)
        except ValueError:  # Feb 29 in non-leap year
            return current.replace(year=current.year + 1, day=28)
    raise ValueError(f"Unknown frequency: {frequency!r}")


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert a SQLAlchemy Row / ORM instance to a plain dict."""
    if hasattr(row, "__table__"):
        # ORM mapped object
        return {c.name: getattr(row, c.name) for c in row.__table__.columns}
    # Core Row
    return dict(row._mapping)


# ---------------------------------------------------------------------------
# Manual obligation CRUD
# ---------------------------------------------------------------------------

def create_obligation(
    db: Session,
    *,
    name: str,
    event_type: str,  # "inflow" | "outflow"
    category: str,
    vendor_or_customer: str = "",
    amount_cents: int,
    due_date: date,
    status: str = "planned",
    confidence: str = "confirmed",
    notes: str = "",
    recurring_template_id: str | None = None,
    clickup_task_id: str | None = None,
) -> dict[str, Any]:
    """Insert a new manual CashEvent and return it as a dict."""
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    event_id = str(uuid.uuid4())
    now = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO cash_events (
                    id, source, source_id, event_type, category,
                    name, vendor_or_customer, amount_cents,
                    due_date, status, confidence, notes,
                    recurring_template_id, clickup_task_id,
                    created_at, updated_at
                ) VALUES (
                    :id, 'manual', :id, :event_type, :category,
                    :name, :vendor_or_customer, :amount_cents,
                    :due_date, :status, :confidence, :notes,
                    :recurring_template_id, :clickup_task_id,
                    :now, :now
                )
            """),
            {
                "id": event_id,
                "event_type": event_type,
                "category": category,
                "name": name,
                "vendor_or_customer": vendor_or_customer,
                "amount_cents": amount_cents,
                "due_date": due_date.isoformat(),
                "status": status,
                "confidence": confidence,
                "notes": notes,
                "recurring_template_id": recurring_template_id,
                "clickup_task_id": clickup_task_id,
                "now": now.isoformat(),
            },
        )

    return get_obligation(event_id)


def get_obligation(event_id: str) -> dict[str, Any] | None:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM cash_events WHERE id = :id"),
            {"id": event_id},
        ).fetchone()
    return _row_to_dict(row) if row else None


def update_obligation(event_id: str, **fields: Any) -> dict[str, Any] | None:
    """Update specific fields on an existing obligation."""
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    if not fields:
        return get_obligation(event_id)

    # Whitelist updatable fields to prevent injection
    allowed = {
        "name", "event_type", "category", "vendor_or_customer",
        "amount_cents", "due_date", "status", "confidence",
        "notes", "matched_to_id",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return get_obligation(event_id)

    safe["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in safe)
    safe["id"] = event_id

    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE cash_events SET {set_clause} WHERE id = :id"),  # noqa: S608
            safe,
        )
    return get_obligation(event_id)


def delete_obligation(event_id: str) -> bool:
    """Hard-delete a manual obligation. Returns True if a row was removed."""
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM cash_events WHERE id = :id AND source = 'manual'"),
            {"id": event_id},
        )
    return result.rowcount > 0


def list_obligations(
    *,
    event_type: str | None = None,
    status: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    source: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return cash events matching the given filters, ordered by due_date."""
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    conditions = ["1=1"]
    params: dict[str, Any] = {}

    if event_type:
        conditions.append("event_type = :event_type")
        params["event_type"] = event_type
    if status:
        conditions.append("status = :status")
        params["status"] = status
    if from_date:
        conditions.append("due_date >= :from_date")
        params["from_date"] = from_date.isoformat()
    if to_date:
        conditions.append("due_date <= :to_date")
        params["to_date"] = to_date.isoformat()
    if source:
        conditions.append("source = :source")
        params["source"] = source

    params["limit"] = limit
    where = " AND ".join(conditions)

    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM cash_events WHERE {where} ORDER BY due_date ASC LIMIT :limit"),  # noqa: S608
            params,
        ).fetchall()

    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Recurring template CRUD
# ---------------------------------------------------------------------------

def create_recurring_template(
    *,
    name: str,
    vendor_or_customer: str = "",
    event_type: str,
    category: str,
    amount_cents: int,
    frequency: str,
    next_due_date: date,
    day_of_month: int | None = None,
) -> dict[str, Any]:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    template_id = str(uuid.uuid4())
    now = datetime.utcnow()

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO recurring_templates (
                    id, name, vendor_or_customer, event_type, category,
                    amount_cents, frequency, next_due_date, day_of_month,
                    is_active, created_at, updated_at
                ) VALUES (
                    :id, :name, :vendor_or_customer, :event_type, :category,
                    :amount_cents, :frequency, :next_due_date, :day_of_month,
                    1, :now, :now
                )
            """),
            {
                "id": template_id,
                "name": name,
                "vendor_or_customer": vendor_or_customer,
                "event_type": event_type,
                "category": category,
                "amount_cents": amount_cents,
                "frequency": frequency,
                "next_due_date": next_due_date.isoformat(),
                "day_of_month": day_of_month,
                "now": now.isoformat(),
            },
        )

    return get_recurring_template(template_id)


def get_recurring_template(template_id: str) -> dict[str, Any] | None:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM recurring_templates WHERE id = :id"),
            {"id": template_id},
        ).fetchone()
    return _row_to_dict(row) if row else None


def list_recurring_templates(*, active_only: bool = True) -> list[dict[str, Any]]:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    where = "WHERE is_active = 1" if active_only else ""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM recurring_templates {where} ORDER BY next_due_date ASC"),  # noqa: S608
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_recurring_template(template_id: str, **fields: Any) -> dict[str, Any] | None:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    allowed = {
        "name", "vendor_or_customer", "event_type", "category",
        "amount_cents", "frequency", "next_due_date", "day_of_month", "is_active",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return get_recurring_template(template_id)

    safe["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in safe)
    safe["id"] = template_id

    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE recurring_templates SET {set_clause} WHERE id = :id"),  # noqa: S608
            safe,
        )
    return get_recurring_template(template_id)


def delete_recurring_template(template_id: str) -> bool:
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM recurring_templates WHERE id = :id"),
            {"id": template_id},
        )
    return result.rowcount > 0


# ---------------------------------------------------------------------------
# Recurring generation
# ---------------------------------------------------------------------------

def generate_upcoming_from_templates(
    *,
    horizon_days: int = 90,
    advance_template: bool = True,
) -> list[dict[str, Any]]:
    """
    For each active template whose next_due_date falls within *horizon_days*,
    ensure a CashEvent exists (upsert by recurring_template_id + due_date).
    Returns the list of events that were created or already existed.

    If *advance_template* is True, bump next_due_date forward after creating.
    """
    from sales_support_agent.models.database import engine
    from sqlalchemy import text

    cutoff = _today() + timedelta(days=horizon_days)
    templates = list_recurring_templates(active_only=True)
    created: list[dict[str, Any]] = []

    for tmpl in templates:
        next_due_raw = tmpl["next_due_date"]
        if isinstance(next_due_raw, str):
            next_due = date.fromisoformat(next_due_raw[:10])
        else:
            next_due = next_due_raw

        if next_due > cutoff:
            continue

        # Check if an event already exists for this template + date
        with engine.connect() as conn:
            existing = conn.execute(
                text("""
                    SELECT id FROM cash_events
                    WHERE recurring_template_id = :tid AND due_date = :due
                """),
                {"tid": tmpl["id"], "due": next_due.isoformat()},
            ).fetchone()

        if existing:
            event = get_obligation(existing[0])
        else:
            event = create_obligation(
                db=None,  # type: ignore[arg-type]  # engine used directly
                name=tmpl["name"],
                event_type=tmpl["event_type"],
                category=tmpl["category"],
                vendor_or_customer=tmpl.get("vendor_or_customer", ""),
                amount_cents=tmpl["amount_cents"],
                due_date=next_due,
                status="planned",
                confidence="estimated",
                recurring_template_id=tmpl["id"],
            )

        if event:
            created.append(event)

        if advance_template:
            frequency = tmpl["frequency"]
            day_of_month = tmpl.get("day_of_month")
            new_next = _next_occurrence(next_due, frequency, day_of_month)
            update_recurring_template(tmpl["id"], next_due_date=new_next.isoformat())

    return created


# ---------------------------------------------------------------------------
# ClickUp import
# ---------------------------------------------------------------------------

def import_clickup_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    """
    Upsert a batch of raw ClickUp task dicts as CashEvents (source='clickup').
    Returns {"created": N, "updated": N, "skipped": N}.
    """
    from sales_support_agent.models.database import engine
    from sales_support_agent.services.cashflow.normalizers import normalize_clickup_task
    from sqlalchemy import text

    counts = {"created": 0, "updated": 0, "skipped": 0}

    for raw in tasks:
        normalised = normalize_clickup_task(raw)
        if normalised is None:
            counts["skipped"] += 1
            continue

        clickup_id = normalised.get("clickup_task_id") or raw.get("id", "")
        if not clickup_id:
            counts["skipped"] += 1
            continue

        due_date_raw = normalised.get("due_date")
        due_date_str = (
            due_date_raw.isoformat()
            if isinstance(due_date_raw, (date, datetime))
            else str(due_date_raw)[:10] if due_date_raw else None
        )

        with engine.connect() as conn:
            existing = conn.execute(
                text("SELECT id FROM cash_events WHERE clickup_task_id = :cid"),
                {"cid": clickup_id},
            ).fetchone()

        now = datetime.utcnow().isoformat()

        if existing:
            update_obligation(
                existing[0],
                name=normalised.get("name", ""),
                status=normalised.get("status", "planned"),
                amount_cents=normalised.get("amount_cents", 0),
                due_date=due_date_str,
                category=normalised.get("category", "other"),
                vendor_or_customer=normalised.get("vendor_or_customer", ""),
            )
            counts["updated"] += 1
        else:
            event_id = str(uuid.uuid4())
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO cash_events (
                            id, source, source_id, event_type, category,
                            name, vendor_or_customer, amount_cents,
                            due_date, status, confidence,
                            clickup_task_id, recurring_rule,
                            created_at, updated_at
                        ) VALUES (
                            :id, 'clickup', :source_id, :event_type, :category,
                            :name, :vendor_or_customer, :amount_cents,
                            :due_date, :status, 'estimated',
                            :clickup_task_id, :recurring_rule,
                            :now, :now
                        )
                    """),
                    {
                        "id": event_id,
                        "source_id": clickup_id,
                        "event_type": normalised.get("event_type", "outflow"),
                        "category": normalised.get("category", "other"),
                        "name": normalised.get("name", ""),
                        "vendor_or_customer": normalised.get("vendor_or_customer", ""),
                        "amount_cents": normalised.get("amount_cents", 0),
                        "due_date": due_date_str,
                        "status": normalised.get("status", "planned"),
                        "clickup_task_id": clickup_id,
                        "recurring_rule": normalised.get("recurring_rule"),
                        "now": now,
                    },
                )
            counts["created"] += 1

    return counts
