"""ClickUp Finance Sync — imports AP and AR tasks into cash_events."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# Custom field IDs for the AP/AR lists
FIELD_AMOUNT_ID = "6d61ee15-5e93-4b5f-8945-93a96659049e"
FIELD_FREQUENCY_ID = "6c6390ee-76ab-4071-8b0a-c60c883a1cc1"
FIELD_MAN_AUTO_ID = "85759b91-cebe-4f51-8305-5b53c071ddd4"
FIELD_WEEK_DUE_ID = "ed07bd92-1c8d-4b1e-8c2a-9564eac7256a"
FIELD_SERVICE_ID = "f190e13b-f64e-4537-ade0-0e75c9af48ce"

PRIORITY_MAP = {
    "urgent": "must_pay",
    "high": "should_pay",
    "normal": "review",
    "low": "can_hold",
}

CATEGORY_KEYWORDS = {
    "payroll": ["payroll", "salary", "wages"],
    "rent": ["rent", "lease"],
    "loan": ["loan", "capital", "fora", "stripe capital", "kyle loan"],
    "utilities": ["power", "electric", "gas", "water", "comcast", "enbridge", "lehi", "rocky mountain"],
    "insurance": ["insurance", "liberty mutual", "cincinnati", "bear river", "select benefits", "instamed"],
    "software": ["software", "google workspace", "quickbooks", "clickup", "zapier", "openai", "lovable"],
    "banking": ["chase", "citi", "american express", "capital one", "credit card"],
    "fulfillment": ["fulfillment", "von", "bonus"],
}


def _infer_category(name: str) -> str:
    name_lower = name.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "other"


def _get_custom_field_value(fields: list[dict], field_id: str) -> Any:
    for f in fields:
        if f.get("id") == field_id:
            return f.get("value")
    return None


def _parse_amount_cents(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(float(str(value)) * 100)
    except (ValueError, TypeError):
        return 0


def _parse_due_date(ts_ms: Any) -> Optional[date]:
    if not ts_ms:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).date()
    except Exception:
        return None


def _map_status(clickup_status: str, due: Optional[date], today: date) -> str:
    if clickup_status.lower() in ("closed", "done", "complete"):
        return "paid"
    if due and due < today:
        return "overdue"
    return "planned"


# ---------------------------------------------------------------------------
# Recurring template upsert
# ---------------------------------------------------------------------------

def _upsert_recurring_template(task_id: str, ev: dict) -> str:
    """Create or update a RecurringTemplate for a ClickUp recurring task.

    Uses a deterministic ID ``clickup-tmpl-{task_id}`` so re-syncing is
    idempotent.  After upserting the template it back-fills any existing
    cash_events for this task with the template ID so the horizon generator
    won't create duplicates.

    Returns 'created' or 'updated'.
    """
    from sales_support_agent.models.database import get_engine
    from sqlalchemy import text

    engine = get_engine()
    now_str = datetime.utcnow().isoformat()
    template_id = f"clickup-tmpl-{task_id}"

    due = ev.get("due_date")
    due_str = due.isoformat() if due else None
    day_of_month = due.day if due else None

    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT id FROM recurring_templates WHERE id = :id OR clickup_task_id = :cid"),
            {"id": template_id, "cid": task_id},
        ).fetchone()

    if existing:
        tmpl_id = existing[0]
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE recurring_templates SET
                        name=:name, vendor_or_customer=:vendor,
                        event_type=:event_type, category=:category,
                        amount_cents=:amount_cents, frequency=:frequency,
                        is_active=TRUE, updated_at=:now
                    WHERE id=:id
                """),
                {
                    "id": tmpl_id,
                    "name": ev["name"][:255],
                    "vendor": ev.get("vendor_or_customer", "")[:255],
                    "event_type": ev["event_type"],
                    "category": ev.get("category", "other"),
                    "amount_cents": ev["amount_cents"],
                    "frequency": ev["recurring_rule"],
                    "now": now_str,
                },
            )
        # Back-fill existing ClickUp cash_events with template linkage so the
        # horizon generator doesn't create duplicates for already-existing rows.
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE cash_events
                    SET recurring_template_id = :tmpl_id
                    WHERE clickup_task_id = :cid
                      AND source IN ('clickup', 'clickup-recurring')
                      AND (recurring_template_id IS NULL OR recurring_template_id = '')
                """),
                {"tmpl_id": tmpl_id, "cid": task_id},
            )
        return "updated"
    else:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO recurring_templates (
                        id, name, vendor_or_customer, event_type, category,
                        amount_cents, frequency, next_due_date, day_of_month,
                        is_active, clickup_task_id, created_at, updated_at
                    ) VALUES (
                        :id, :name, :vendor, :event_type, :category,
                        :amount_cents, :frequency, :next_due_date, :day_of_month,
                        TRUE, :clickup_task_id, :now, :now
                    )
                """),
                {
                    "id": template_id,
                    "name": ev["name"][:255],
                    "vendor": ev.get("vendor_or_customer", "")[:255],
                    "event_type": ev["event_type"],
                    "category": ev.get("category", "other"),
                    "amount_cents": ev["amount_cents"],
                    "frequency": ev["recurring_rule"],
                    "next_due_date": due_str,
                    "day_of_month": day_of_month,
                    "clickup_task_id": task_id,
                    "now": now_str,
                },
            )
        return "created"


def _fetch_tasks(api_token: str, list_id: str) -> list[dict]:
    """Fetch all tasks from a ClickUp list (paginated, including closed)."""
    tasks = []
    page = 0
    base_url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    headers = {"Authorization": api_token}
    while True:
        params = {
            "include_closed": "true",
            "subtasks": "false",
            "custom_fields": "true",
            "page": page,
        }
        resp = requests.get(base_url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("tasks", [])
        tasks.extend(batch)
        if not batch or data.get("last_page"):
            break
        page += 1
    return tasks


def _task_to_event_dict(task: dict, event_type: str, today: date) -> dict:
    """Convert a raw ClickUp task dict to a cash_event-compatible dict."""
    custom_fields = task.get("custom_fields") or []

    amount_cents = _parse_amount_cents(_get_custom_field_value(custom_fields, FIELD_AMOUNT_ID))
    frequency_val = _get_custom_field_value(custom_fields, FIELD_FREQUENCY_ID)
    man_auto_val = _get_custom_field_value(custom_fields, FIELD_MAN_AUTO_ID)
    service_val = _get_custom_field_value(custom_fields, FIELD_SERVICE_ID) or ""

    # Frequency: 0=Weekly, 1=Monthly, 2=Bi-weekly, None=one-time
    if frequency_val == 0:
        frequency = "weekly"
    elif frequency_val == 1:
        frequency = "monthly"
    elif frequency_val == 2:
        frequency = "biweekly"
    else:
        frequency = ""

    # Confidence: Auto=confirmed, Manual/null=estimated
    confidence = "confirmed" if man_auto_val == 1 else "estimated"

    # Priority → pay_priority
    priority_obj = task.get("priority") or {}
    priority_name = (priority_obj.get("priority") or "").lower() if isinstance(priority_obj, dict) else ""
    pay_priority = PRIORITY_MAP.get(priority_name, "review")

    due = _parse_due_date(task.get("due_date"))
    status = _map_status(task.get("status", {}).get("status", "open"), due, today)

    name = task.get("name", "").strip()
    description = task.get("text_content") or task.get("description") or service_val or ""

    category = _infer_category(name)

    notes = f"priority:{pay_priority}"
    if description:
        notes += f"|{description[:500]}"

    return {
        "id": f"clickup-{task['id']}",
        "source": "clickup",
        "source_id": task["id"],
        "clickup_task_id": task["id"],
        "event_type": event_type,
        "category": category,
        "subcategory": "",
        "description": description[:500] if description else "",
        "name": name,
        "vendor_or_customer": name,
        "amount_cents": amount_cents,
        "due_date": due,
        "status": status,
        "confidence": confidence,
        "recurring_rule": frequency,
        "bank_transaction_type": "",
        "bank_reference": "",
        "notes": notes,
    }



def sync_clickup_finance(settings):
    """Sync AP and AR ClickUp tasks into cash_events / recurring_templates.

    Routing logic
    -------------
    recurring task (weekly / biweekly / monthly) that is NOT yet paid
        → upsert a RecurringTemplate (keyed ``clickup-tmpl-{task_id}``)
          The template drives all future CashEvent rows via
          ``generate_upcoming_from_templates()``.  This gives us the full
          400-day horizon instead of the previous hard-coded 52/12 future
          occurrences.

    paid / closed recurring task
        → update the matching CashEvent's status to ``paid`` so the
          forecast removes it from the open AP/AR totals.

    one-time task (no frequency)
        → direct CashEvent upsert (existing behaviour).

    Returns UploadResult.
    """
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services.cashflow.upload import UploadResult

    result = UploadResult()

    try:
        if not settings.clickup_api_token:
            logger.warning(
                "CLICKUP_API_TOKEN not set — skipping ClickUp finance sync. "
                "Get your token from ClickUp → Profile → Apps → API Token (starts with pk_)."
            )
            return result

        today = datetime.utcnow().date()
        tmpl_created = tmpl_updated = ev_created = ev_updated = skipped = 0

        list_configs = [
            (settings.clickup_ap_list_id, "outflow"),
            (settings.clickup_ar_list_id, "inflow"),
        ]

        engine = get_engine()

        for list_id, event_type in list_configs:
            if not list_id:
                continue
            try:
                tasks = _fetch_tasks(settings.clickup_api_token, list_id)
            except Exception as exc:
                err_str = str(exc)
                if "401" in err_str or "Unauthorized" in err_str:
                    logger.warning(
                        "ClickUp 401 for list %s — API token invalid or expired. "
                        "Refresh at ClickUp → Profile → Apps → API Token and update CLICKUP_API_TOKEN on Render.",
                        list_id,
                    )
                else:
                    logger.error("Failed to fetch ClickUp list %s: %s", list_id, exc)
                result.errors.append(f"Failed to fetch list {list_id}: {exc}")
                continue

            for task in tasks:
                ev = _task_to_event_dict(task, event_type, today)
                rule = ev.get("recurring_rule", "")
                task_id = task["id"]

                # Skip zero-amount tasks that aren't already marked paid
                if ev["amount_cents"] == 0 and ev["status"] != "paid":
                    skipped += 1
                    continue

                if rule and ev["status"] != "paid":
                    # ---- Recurring, open → upsert template ----------------
                    tmpl_result = _upsert_recurring_template(task_id, ev)
                    if tmpl_result == "created":
                        tmpl_created += 1
                    else:
                        tmpl_updated += 1
                    # Note: generate_upcoming_from_templates() (called by the
                    # caller after sync) will fill all future CashEvent rows.

                elif ev["status"] == "paid":
                    # ---- Paid / closed → mark the cash_event paid ----------
                    # For recurring tasks that became paid: also deactivate
                    # the template's cash_event for that specific due_date.
                    upsert_result = _upsert_event(engine, ev)
                    if upsert_result == "created":
                        ev_created += 1
                    else:
                        ev_updated += 1

                else:
                    # ---- One-time task → direct cash_event -----------------
                    upsert_result = _upsert_event(engine, ev)
                    if upsert_result == "created":
                        ev_created += 1
                    else:
                        ev_updated += 1

        logger.info(
            "ClickUp finance sync complete: "
            "templates created=%d updated=%d | "
            "events created=%d updated=%d | skipped=%d",
            tmpl_created, tmpl_updated, ev_created, ev_updated, skipped,
        )
        result.rows_inserted = tmpl_created + ev_created
        result.rows_skipped_duplicate = tmpl_updated + ev_updated + skipped

    except Exception as exc:
        logger.error("ClickUp sync error: %s", exc)
        result.errors.append(str(exc))

    return result


def _upsert_event(engine, ev: dict) -> str:
    """Insert or update a cash_event row. Returns 'created' or 'updated'.

    Delegates to upsert_cash_event() in database.py which handles all columns:
    id, source, source_id, event_type, category, subcategory, description,
    name, vendor_or_customer, amount_cents, due_date, status, confidence,
    recurring_rule, clickup_task_id, bank_transaction_type, bank_reference,
    notes, friendly_name, created_at, updated_at.
    """
    from sales_support_agent.models.database import upsert_cash_event

    with engine.begin() as conn:
        return upsert_cash_event(conn, ev)
