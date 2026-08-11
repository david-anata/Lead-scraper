"""CRUD for manual obligations and recurring template expansion."""

from __future__ import annotations

import json as _json
import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)


def get_events_for_range(
    from_date,
    to_date,
    *,
    event_type=None,       # "inflow" | "outflow" | None = both
    include_statuses=None, # list of statuses; None = all non-cancelled
    limit=5000,
) -> list[dict]:
    """Fetch cash_events with due_date in [from_date, to_date].

    Used by Calendar and Ledger views. Faster than list_obligations()
    for date-bounded queries because it uses the due_date index.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    from_str = from_date.isoformat() if hasattr(from_date, "isoformat") else str(from_date)
    to_str = to_date.isoformat() if hasattr(to_date, "isoformat") else str(to_date)

    where_clauses = ["due_date >= :from_date", "due_date <= :to_date", "status != 'cancelled'"]
    params = {"from_date": from_str, "to_date": to_str, "limit": limit}

    if event_type:
        where_clauses.append("event_type = :event_type")
        params["event_type"] = event_type

    if include_statuses:
        placeholders = ", ".join(f":s{i}" for i in range(len(include_statuses)))
        where_clauses.append(f"status IN ({placeholders})")
        for i, s in enumerate(include_statuses):
            params[f"s{i}"] = s

    sql = f"""
        SELECT id, source, source_id, event_type, category, subcategory,
               description, name, vendor_or_customer, amount_cents,
               due_date, status, confidence, account_balance_cents,
               bank_transaction_type, bank_reference, notes, recurring_rule,
               clickup_task_id, friendly_name, created_at, updated_at
        FROM cash_events
        WHERE {" AND ".join(where_clauses)}
        ORDER BY due_date ASC, created_at ASC
        LIMIT :limit
    """

    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    return [dict(row._mapping) for row in rows]



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    return datetime.utcnow().date()


def _next_occurrence(current: date, frequency: str, day_of_month: Optional[int]) -> date:
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
    recurring_template_id: Optional[str] = None,
    clickup_task_id: Optional[str] = None,
    commitment_type: str = "general",
    workflow_status: str = "draft",
    owner: str = "",
    approval_status: str = "not_required",
    created_by: str = "system",
    db=None,  # legacy/unused — kept for backwards compatibility
) -> dict[str, Any]:
    """Insert a new manual CashEvent and return it as a dict."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    event_id = str(uuid.uuid4())
    now = datetime.utcnow()

    with get_engine().begin() as conn:
        conn.execute(
            text("""
                INSERT INTO cash_events (
                    id, source, source_id, event_type, category,
                    subcategory, description, name, vendor_or_customer,
                    amount_cents, due_date, status, confidence, notes,
                    recurring_template_id, clickup_task_id,
                    bank_transaction_type, bank_reference,
                    recurring_rule, commitment_type, workflow_status, owner,
                    approval_status, created_by,
                    created_at, updated_at
                ) VALUES (
                    :id, 'manual', :id, :event_type, :category,
                    '', '', :name, :vendor_or_customer,
                    :amount_cents, :due_date, :status, :confidence, :notes,
                    :recurring_template_id, :clickup_task_id,
                    '', '',
                    '', :commitment_type, :workflow_status, :owner,
                    :approval_status, :created_by,
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
                "clickup_task_id": clickup_task_id or "",
                "commitment_type": commitment_type,
                "workflow_status": workflow_status,
                "owner": owner,
                "approval_status": approval_status,
                "created_by": created_by,
                "now": now.isoformat(),
            },
        )

    return get_obligation(event_id)


def get_obligation(event_id: str) -> Optional[dict[str, Any]]:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM cash_events WHERE id = :id"),
            {"id": event_id},
        ).fetchone()
    return _row_to_dict(row) if row else None


def update_obligation(event_id: str, **fields: Any) -> Optional[dict[str, Any]]:
    """Update specific fields on an existing obligation."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    if not fields:
        return get_obligation(event_id)

    # Whitelist updatable fields to prevent injection
    allowed = {
        "name", "event_type", "category", "vendor_or_customer",
        "amount_cents", "due_date", "status", "confidence",
        "notes", "matched_to_id", "commitment_type", "workflow_status",
        "owner", "approval_status", "archived_at", "pay_priority",
        "flexibility",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return get_obligation(event_id)

    safe["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in safe)
    safe["id"] = event_id

    with get_engine().begin() as conn:
        conn.execute(
            text(f"UPDATE cash_events SET {set_clause} WHERE id = :id"),  # noqa: S608
            safe,
        )
    return get_obligation(event_id)


def delete_obligation(event_id: str) -> bool:
    """Delete an unused manual obligation, or soft-cancel one with audit evidence."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import inspect, text

    with get_engine().begin() as conn:
        tables = set(inspect(conn).get_table_names())
        evidence = 0
        if {"settlement_allocations", "payment_installments"}.issubset(tables):
            evidence = conn.execute(
                text("""
                    SELECT
                      (SELECT COUNT(*) FROM settlement_allocations WHERE obligation_event_id = :id)
                      + (SELECT COUNT(*) FROM payment_installments WHERE obligation_event_id = :id)
                """),
                {"id": event_id},
            ).scalar_one()
        if evidence:
            result = conn.execute(
                text("""
                    UPDATE cash_events
                    SET status = 'cancelled', updated_at = :now
                    WHERE id = :id AND source = 'manual'
                """),
                {"id": event_id, "now": datetime.utcnow().isoformat()},
            )
            return result.rowcount > 0
        result = conn.execute(
            text("DELETE FROM cash_events WHERE id = :id AND source = 'manual'"),
            {"id": event_id},
        )
    return result.rowcount > 0


def list_obligations(
    *,
    event_type: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    source: Optional[str] = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return cash events matching the given filters, ordered by due_date."""
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    if source in {None, "hr_payroll"}:
        from sales_support_agent.services.cashflow.payroll_commitments import (
            sync_hr_payroll_commitments,
        )
        sync_hr_payroll_commitments()

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

    with get_engine().connect() as conn:
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
    day_of_month: Optional[int] = None,
    confidence: str = "estimated",
    flexibility: str = "unknown",
    notes: str = "",
    is_active: bool = True,
) -> dict[str, Any]:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    template_id = str(uuid.uuid4())
    now = datetime.utcnow()

    with get_engine().begin() as conn:
        # confidence, flexibility and notes are NOT NULL with only a Python-side
        # default, so this raw INSERT has to supply them explicitly or it fails
        # outright.
        conn.execute(
            text("""
                INSERT INTO recurring_templates (
                    id, name, vendor_or_customer, event_type, category,
                    amount_cents, confidence, flexibility, notes, clickup_task_id,
                    frequency, next_due_date, day_of_month,
                    is_active, created_at, updated_at
                ) VALUES (
                    :id, :name, :vendor_or_customer, :event_type, :category,
                    :amount_cents, :confidence, :flexibility, :notes, '',
                    :frequency, :next_due_date, :day_of_month,
                    :is_active, :now, :now
                )
            """),
            {
                "id": template_id,
                "name": name,
                "vendor_or_customer": vendor_or_customer,
                "event_type": event_type,
                "category": category,
                "amount_cents": amount_cents,
                "confidence": confidence,
                "flexibility": flexibility or "unknown",
                "notes": notes,
                "frequency": frequency,
                "next_due_date": next_due_date.isoformat(),
                "day_of_month": day_of_month,
                "is_active": bool(is_active),
                "now": now.isoformat(),
            },
        )

    return get_recurring_template(template_id)


def get_recurring_template(template_id: str) -> Optional[dict[str, Any]]:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM recurring_templates WHERE id = :id"),
            {"id": template_id},
        ).fetchone()
    return _row_to_dict(row) if row else None


def list_recurring_templates(*, active_only: bool = True) -> list[dict[str, Any]]:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    where = "WHERE is_active = TRUE" if active_only else ""
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM recurring_templates {where} ORDER BY next_due_date ASC"),  # noqa: S608
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_recurring_template(template_id: str, **fields: Any) -> Optional[dict[str, Any]]:
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    allowed = {
        "name", "vendor_or_customer", "event_type", "category",
        "amount_cents", "frequency", "next_due_date", "day_of_month", "is_active",
        # Both of these decide how the generated occurrences behave, so they have
        # to be changeable after the schedule is first written down.
        "confidence", "flexibility",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return get_recurring_template(template_id)

    safe["updated_at"] = datetime.utcnow().isoformat()
    set_clause = ", ".join(f"{k} = :{k}" for k in safe)
    safe["id"] = template_id

    with get_engine().begin() as conn:
        conn.execute(
            text(f"UPDATE recurring_templates SET {set_clause} WHERE id = :id"),  # noqa: S608
            safe,
        )
    return get_recurring_template(template_id)


def delete_recurring_template(template_id: str, *, actor: str = "system") -> bool:
    """Remove a schedule and retire the future it has already generated.

    Deleting the template on its own left its occurrences behind with nothing
    left to explain them, so a bill David had stopped kept demanding cash in the
    forecast for months. Occurrences that are already in the past are history and
    stay exactly as they are, and anything with a payment recorded against it is
    never touched.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import inspect, text

    today = _today()
    now = datetime.utcnow()

    with get_engine().begin() as conn:
        template = conn.execute(
            text("SELECT name FROM recurring_templates WHERE id = :id"),
            {"id": template_id},
        ).fetchone()
        if template is None:
            return False

        tables = set(inspect(conn).get_table_names())
        cancelled: list[dict[str, Any]] = []

        if "cash_events" in tables:
            # Older/minimal databases may not carry the settlement table yet, and
            # without it there is nothing to check against.
            settlement_guard = (
                "AND id NOT IN (SELECT obligation_event_id FROM settlement_allocations)"
                if "settlement_allocations" in tables
                else ""
            )
            rows = conn.execute(
                text(f"""
                    SELECT id, name, amount_cents, due_date
                    FROM cash_events
                    WHERE recurring_template_id = :tid
                      AND record_kind <> 'transaction'
                      AND archived_at IS NULL
                      AND LOWER(COALESCE(status,'')) IN ('planned','pending','overdue')
                      AND due_date IS NOT NULL
                      AND due_date >= :today
                      {settlement_guard}
                """),  # noqa: S608
                {"tid": template_id, "today": today.isoformat()},
            ).fetchall()

            for raw in rows:
                row = dict(raw._mapping)
                conn.execute(
                    text("""
                        UPDATE cash_events
                        SET workflow_status='cancelled', archived_at=:now,
                            snoozed_until=NULL, follow_up_on=NULL, updated_at=:now
                        WHERE id=:id
                    """),
                    {"now": now, "id": str(row["id"])},
                )
                cancelled.append({
                    "id": str(row["id"]),
                    "name": str(row.get("name") or ""),
                    "amount_cents": int(row.get("amount_cents") or 0),
                    "due_date": str(row.get("due_date") or "")[:10],
                })

        conn.execute(
            text("DELETE FROM recurring_templates WHERE id = :id"),
            {"id": template_id},
        )

        if "finance_action_audit" in tables:
            # The occurrence ids are recorded so this can be undone, not just seen.
            conn.execute(
                text("""
                    INSERT INTO finance_action_audit (
                        id, scope_key, action_type, entity_type, entity_id, actor,
                        evidence_json, created_at
                    ) VALUES (
                        :audit_id, 'default', 'recurring_template_deleted',
                        'recurring_template', :template_id, :actor, :evidence, :now
                    )
                """),
                {
                    "audit_id": str(uuid.uuid4()),
                    "template_id": template_id,
                    "actor": actor,
                    "evidence": _json.dumps({
                        "template_name": str(template[0] or ""),
                        "cancelled_count": len(cancelled),
                        "cancelled_amount_cents": sum(
                            item["amount_cents"] for item in cancelled
                        ),
                        "cancelled_event_ids": [item["id"] for item in cancelled],
                        "as_of": today.isoformat(),
                    }),
                    "now": now,
                },
            )

    if cancelled:
        logger.info(
            "Schedule deleted: %d future occurrence(s) retired with it",
            len(cancelled),
        )
    return True


# ---------------------------------------------------------------------------
# Recurring generation
# ---------------------------------------------------------------------------

def supersede_stale_template_occurrences(
    *,
    as_of: Optional[date] = None,
    grace_days: int = 5,
) -> list[dict[str, Any]]:
    """Roll a recurring forecast forward instead of letting it rot into debt.

    A recurring schedule entry is a plan, not a bill. When its date passes
    unpaid and a later occurrence in the same series already exists, the old one
    is schedule residue: it is archived so it stops counting as a cash
    requirement, and the live occurrence is the one still ahead.

    This is reliable here in a way it never was for ClickUp: every generated
    occurrence carries its template id, so the series is exact rather than
    inferred from names and dates. Only template-generated rows are touched,
    and an occurrence with any settlement against it is always left alone.

    Safe to call straight from a button as well as from the background job: pass
    an explicit *as_of* and it archives only, so a second run finds nothing left
    to roll forward and reports an empty list.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    as_of = as_of or _today()
    cutoff = as_of - timedelta(days=max(0, grace_days))
    now = datetime.utcnow()
    superseded: list[dict[str, Any]] = []

    with get_engine().begin() as conn:
        rows = conn.execute(
            text("""
                SELECT stale.id, stale.name, stale.amount_cents, stale.due_date,
                       stale.recurring_template_id
                FROM cash_events AS stale
                WHERE stale.recurring_template_id IS NOT NULL
                  AND stale.record_kind <> 'transaction'
                  AND stale.archived_at IS NULL
                  AND LOWER(COALESCE(stale.status,'')) IN ('planned','pending','overdue')
                  AND stale.due_date IS NOT NULL
                  AND stale.due_date < :cutoff
                  AND stale.id NOT IN (
                      SELECT obligation_event_id FROM settlement_allocations
                  )
                  AND EXISTS (
                      SELECT 1 FROM cash_events AS later
                      WHERE later.recurring_template_id = stale.recurring_template_id
                        AND later.due_date > stale.due_date
                        AND later.archived_at IS NULL
                  )
            """),
            {"cutoff": cutoff.isoformat()},
        ).fetchall()

        for raw in rows:
            row = dict(raw._mapping)
            conn.execute(
                text("""
                    UPDATE cash_events
                    SET workflow_status='cancelled', archived_at=:now,
                        snoozed_until=NULL, follow_up_on=NULL, updated_at=:now
                    WHERE id=:id
                """),
                {"now": now, "id": str(row["id"])},
            )
            superseded.append({
                "id": str(row["id"]),
                "name": str(row.get("name") or ""),
                "amount_cents": int(row.get("amount_cents") or 0),
                "due_date": str(row.get("due_date") or "")[:10],
            })

        if superseded:
            conn.execute(
                text("""
                    INSERT INTO finance_action_audit (
                        id, scope_key, action_type, entity_type, entity_id, actor,
                        evidence_json, created_at
                    ) VALUES (
                        :audit_id, 'default', 'recurring_superseded', 'cash_event_batch',
                        :first_id, 'system', :evidence, :now
                    )
                """),
                {
                    "audit_id": str(uuid.uuid4()),
                    "first_id": superseded[0]["id"],
                    "evidence": _json.dumps({
                        "count": len(superseded),
                        "amount_cents": sum(item["amount_cents"] for item in superseded),
                        "grace_days": grace_days,
                    }),
                    "now": now,
                },
            )

    if superseded:
        logger.info(
            "Recurring forecast rolled forward: %d occurrence(s) superseded",
            len(superseded),
        )
    return superseded


def generate_upcoming_from_templates(
    *,
    horizon_days: int = 90,
    advance_template: bool = True,
    respect_cooldown: bool = False,
) -> list[dict[str, Any]]:
    """
    For each active template whose next_due_date falls within *horizon_days*,
    ensure a CashEvent exists (upsert by recurring_template_id + due_date).
    Returns the list of events that were created or already existed.

    If *advance_template* is True, bump next_due_date forward after creating.
    If *respect_cooldown* is True, skip if ran within the last 6 hours
    (use this for background jobs; leave False for on-demand page loads).
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    # Cooldown gate — opt-in only for background/scheduled callers
    if respect_cooldown:
        try:
            with get_engine().connect() as conn:
                row = conn.execute(text("SELECT value FROM kv_store WHERE key='template_gen_last_run'")).fetchone()
            if row:
                last_run = datetime.fromisoformat(row[0])
                if (datetime.utcnow() - last_run).total_seconds() < 21600:  # 6 hours
                    return []  # skip
        except Exception as exc:
            logger.warning("Could not read template_gen cooldown from kv_store (will proceed): %s", exc)

    cutoff = _today() + timedelta(days=horizon_days)
    templates = list_recurring_templates(active_only=True)
    created: list[dict[str, Any]] = []

    for tmpl in templates:
        next_due_raw = tmpl["next_due_date"]
        if next_due_raw is None:
            logger.warning(
                "Skipping recurring template %s without next_due_date",
                tmpl.get("id", "unknown"),
            )
            continue
        if isinstance(next_due_raw, datetime):
            next_due = next_due_raw.date()
        elif isinstance(next_due_raw, str):
            next_due = date.fromisoformat(next_due_raw[:10])
        else:
            next_due = next_due_raw

        frequency   = tmpl["frequency"]
        day_of_month = tmpl.get("day_of_month")
        # A schedule David has already pinned down (auto-pay, signed lease) is not
        # a guess, so its occurrences must not be probability-weighted like one.
        tmpl_confidence = str(tmpl.get("confidence") or "").strip() or "estimated"
        # A bill paid in pieces has to say so on every occurrence, otherwise the
        # first part-payment leaves it looking overdue.
        tmpl_flexibility = str(tmpl.get("flexibility") or "").strip() or "unknown"

        # --- Fill the FULL horizon in one call ----------------------------
        # Previously this loop only created ONE occurrence per template per
        # call (the single next_due_date), then stopped.  That meant the
        # calendar/overview only ever showed the very next bill and nothing
        # further out.  Now we walk forward through every occurrence up to
        # the cutoff date so that all future events exist in cash_events
        # immediately after this function returns.
        while next_due <= cutoff:
            # Check if an event already exists for this template + date
            with get_engine().connect() as conn:
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
                    name=tmpl["name"],
                    event_type=tmpl["event_type"],
                    category=tmpl["category"],
                    vendor_or_customer=tmpl.get("vendor_or_customer", ""),
                    amount_cents=tmpl["amount_cents"],
                    due_date=next_due,
                    status="planned",
                    confidence=tmpl_confidence,
                    recurring_template_id=tmpl["id"],
                )
                if event and tmpl_flexibility != "unknown":
                    event = update_obligation(event["id"], flexibility=tmpl_flexibility)

            if event:
                created.append(event)

            next_due = _next_occurrence(next_due, frequency, day_of_month)

        # Advance the template pointer to the first date beyond the horizon
        # so the next call knows where to continue from.
        if advance_template:
            update_recurring_template(tmpl["id"], next_due_date=next_due.isoformat())

    # Update last-run timestamp
    try:
        with get_engine().begin() as conn:
            conn.execute(text("""
                INSERT INTO kv_store (key, value, updated_at) VALUES ('template_gen_last_run', :now, :now)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """), {"now": datetime.utcnow().isoformat()})
    except Exception as exc:
        logger.warning("Could not persist template_gen_last_run to kv_store: %s", exc)

    return created


# ---------------------------------------------------------------------------
# ClickUp import
# ---------------------------------------------------------------------------

def import_clickup_tasks(tasks: list[dict[str, Any]]) -> dict[str, int]:
    """
    Upsert a batch of raw ClickUp task dicts as CashEvents (source='clickup').
    Returns {"created": N, "updated": N, "skipped": N}.
    """
    from sales_support_agent.models.database import get_engine
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

        with get_engine().connect() as conn:
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
            with get_engine().begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO cash_events (
                            id, source, source_id, event_type, category,
                            subcategory, description, name, vendor_or_customer,
                            amount_cents, due_date, status, confidence,
                            clickup_task_id, recurring_rule,
                            bank_transaction_type, bank_reference, notes,
                            created_at, updated_at
                        ) VALUES (
                            :id, 'clickup', :source_id, :event_type, :category,
                            '', '', :name, :vendor_or_customer,
                            :amount_cents, :due_date, :status, 'estimated',
                            :clickup_task_id, :recurring_rule,
                            '', '', '',
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
                        "recurring_rule": normalised.get("recurring_rule") or "",
                        "now": now,
                    },
                )
            counts["created"] += 1

    return counts
