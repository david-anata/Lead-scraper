"""ClickUp Finance Sync — imports AP and AR tasks into cash_events."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
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

    # Frequency: 0=Weekly, 1=Monthly, None=one-time
    if frequency_val == 0:
        frequency = "weekly"
    elif frequency_val == 1:
        frequency = "monthly"
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


def _next_weekly_dates(base: date, count: int) -> list[date]:
    return [base + timedelta(weeks=i) for i in range(1, count + 1)]


def _next_monthly_dates(base: date, count: int) -> list[date]:
    dates = []
    d = base
    for _ in range(count):
        month = d.month + 1
        year = d.year
        if month > 12:
            month = 1
            year += 1
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        d = d.replace(year=year, month=month, day=min(d.day, last_day))
        dates.append(d)
    return dates


def sync_clickup_finance(settings):
    """Sync AP and AR ClickUp tasks into cash_events. Returns UploadResult."""
    from sales_support_agent.models.database import get_engine
    from sales_support_agent.services.cashflow.upload import UploadResult
    from sqlalchemy import text

    result = UploadResult()

    try:
        if not settings.clickup_api_token:
            logger.warning("CLICKUP_API_TOKEN not set — skipping ClickUp finance sync")
            return result

        today = datetime.utcnow().date()
        created = updated = skipped = 0

        list_configs = [
            (settings.clickup_ap_list_id, "outflow"),
            (settings.clickup_ar_list_id, "inflow"),
        ]

        for list_id, event_type in list_configs:
            if not list_id:
                continue
            try:
                tasks = _fetch_tasks(settings.clickup_api_token, list_id)
            except Exception as exc:
                logger.error("Failed to fetch ClickUp list %s: %s", list_id, exc)
                result.errors.append(f"Failed to fetch list {list_id}: {exc}")
                continue

            # Track which task IDs generate forward occurrences so we don't duplicate
            forward_generated: set[str] = set()

            for task in tasks:
                ev = _task_to_event_dict(task, event_type, today)
                if ev["amount_cents"] == 0 and ev["status"] != "paid":
                    skipped += 1
                    continue

                upsert_result = _upsert_event(get_engine(), ev)
                if upsert_result == "created":
                    created += 1
                elif upsert_result == "updated":
                    updated += 1

                # Generate forward occurrences for recurring tasks
                rule = ev.get("recurring_rule", "")
                base_due = ev.get("due_date")
                task_id = task["id"]
                if rule and base_due and task_id not in forward_generated and ev["status"] != "paid":
                    forward_generated.add(task_id)
                    if rule == "weekly":
                        future_dates = _next_weekly_dates(base_due, 52)   # 1 year of weekly
                    elif rule == "monthly":
                        future_dates = _next_monthly_dates(base_due, 12)  # 1 year of monthly
                    else:
                        future_dates = []

                    for fdate in future_dates:
                        if fdate <= today:
                            continue
                        fev = dict(ev)
                        fev["id"] = f"clickup-{task_id}-{fdate.isoformat()}"
                        fev["source_id"] = f"{task_id}-{fdate.isoformat()}"
                        fev["clickup_task_id"] = task_id
                        fev["due_date"] = fdate
                        fev["status"] = "planned"
                        fev["source"] = "clickup-recurring"
                        upsert_result = _upsert_event(get_engine(), fev)
                        if upsert_result == "created":
                            created += 1
                        elif upsert_result == "updated":
                            updated += 1

        logger.info("ClickUp finance sync complete: created=%d updated=%d skipped=%d", created, updated, skipped)
        result.rows_inserted = created
        result.rows_skipped_duplicate = updated + skipped

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
