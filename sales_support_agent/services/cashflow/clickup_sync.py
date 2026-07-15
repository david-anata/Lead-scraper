"""ClickUp Finance Sync — imports AP and AR tasks into cash_events."""

from __future__ import annotations

import logging
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import requests
from sqlalchemy import bindparam, text

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


def _is_closed_task(task: dict) -> bool:
    """Use ClickUp's canonical status type before falling back to its label."""
    status = task.get("status") or {}
    if not isinstance(status, dict):
        return False
    if str(status.get("type") or "").lower() == "closed":
        return True
    # Older ClickUp responses and imported fixtures do not always include type.
    return str(status.get("status") or "").strip().lower() in {
        "closed", "complete", "completed", "done",
    }


def _source_timestamp(task: dict) -> datetime:
    for value in (task.get("date_closed"), task.get("date_updated")):
        if not value:
            continue
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).replace(tzinfo=None)
        except (TypeError, ValueError, OSError):
            continue
    return datetime.utcnow()


def _map_status(task: dict, due: Optional[date], today: date) -> str:
    if _is_closed_task(task):
        # Completion is operational evidence, not proof that cash left the bank.
        return "completed"
    if due and due < today:
        return "overdue"
    if due and due <= today + timedelta(days=7):
        return "pending"
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


def _quarantine_legacy_clickup_template_expansions(engine) -> tuple[int, int]:
    """Disable the duplicate forecast rows created by the old task-per-template model."""
    from sqlalchemy import text

    marker = "quarantined:legacy-clickup-template-expansion"
    now_str = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        events = conn.execute(
            text("""
                UPDATE cash_events
                SET status = 'cancelled',
                    notes = CASE
                        WHEN COALESCE(notes, '') = '' THEN :marker
                        WHEN notes NOT LIKE :marker_pattern THEN notes || '|' || :marker
                        ELSE notes
                    END,
                    updated_at = :now
                WHERE source = 'manual'
                  AND recurring_template_id LIKE 'clickup-tmpl-%'
                  AND status NOT IN ('paid', 'cancelled')
            """),
            {
                "marker": marker,
                "marker_pattern": f"%{marker}%",
                "now": now_str,
            },
        )
        templates = conn.execute(
            text("""
                UPDATE recurring_templates
                SET is_active = FALSE, updated_at = :now
                WHERE id LIKE 'clickup-tmpl-%'
                  AND is_active = TRUE
            """),
            {"now": now_str},
        )
    return events.rowcount, templates.rowcount


def _match_existing_posted_transactions(engine) -> int:
    """Match previously uploaded bank rows after new ClickUp obligations arrive."""
    from sales_support_agent.services.cashflow.matcher import auto_match_transactions
    from sales_support_agent.services.cashflow.obligations import list_obligations
    from sales_support_agent.services.cashflow.settlements import allocate_matched_transaction

    rows = list_obligations(limit=5000)
    posted = [
        row for row in rows
        if row.get("source") == "csv"
        and row.get("status") == "posted"
    ]
    planned = [
        row for row in rows
        if row.get("source") != "csv"
        and row.get("status") in ("planned", "pending", "overdue")
        and int(row.get("amount_cents") or 0) > 0
    ]
    if not posted or not planned:
        return 0

    matches = [
        match for match in auto_match_transactions(posted, planned)
        if match.planned_event_id is not None
    ]
    if not matches:
        return 0

    with engine.begin() as conn:
        for match in matches:
            allocate_matched_transaction(
                conn,
                obligation_event_id=str(match.planned_event_id),
                transaction_event_id=str(match.csv_event_id),
                idempotency_key=f"clickup-auto-match:{match.csv_event_id}:{match.planned_event_id}",
            )
    return len(matches)


def _record_successful_list_snapshot(engine, event_type: str, task_ids: set[str]) -> int:
    """Flag a source-missing task only after two successful list snapshots.

    A ClickUp task can briefly disappear while it is moved, archived, or while
    a list request is incomplete.  The first absence is recorded only; the
    second consecutive successful snapshot keeps the cash reservation and
    requires an operator to resolve its evidence.
    """
    key = f"clickup_finance_snapshot:{event_type}"
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        previous_row = conn.execute(
            text("SELECT value FROM kv_store WHERE key=:key"), {"key": key}
        ).fetchone()
        try:
            previous = json.loads(previous_row[0]) if previous_row else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            previous = {}
        prior_seen = {str(task_id) for task_id in previous.get("task_ids", [])}
        prior_missing = {
            str(task_id): int(count or 0)
            for task_id, count in (previous.get("missing_counts") or {}).items()
        }

        # The first successful snapshot establishes the baseline. Do not turn
        # pre-existing historical records into exceptions during a rollout.
        if not prior_seen:
            conn.execute(text("""
                INSERT INTO kv_store (key, value, updated_at)
                VALUES (:key, :value, :updated_at)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """), {
                "key": key,
                "value": json.dumps({"task_ids": sorted(task_ids), "missing_counts": {}}),
                "updated_at": now,
            })
            return 0

        rows = conn.execute(text("""
            SELECT id, source_id
            FROM cash_events
            WHERE source='clickup' AND event_type=:event_type
        """), {"event_type": event_type}).fetchall()
        missing_counts: dict[str, int] = {}
        flagged_ids: list[str] = []
        for row in rows:
            event_id, task_id = str(row[0]), str(row[1] or "")
            if not task_id or task_id in task_ids:
                continue
            absence_count = prior_missing.get(task_id, 0) + 1
            missing_counts[task_id] = absence_count
            if absence_count >= 2:
                flagged_ids.append(event_id)

        if flagged_ids:
            conn.execute(text("""
                UPDATE cash_events
                SET source_status='source_missing', updated_at=:updated_at
                WHERE id IN :ids
            """).bindparams(bindparam("ids", expanding=True)), {
                "ids": flagged_ids,
                "updated_at": now,
            })
        conn.execute(text("""
            INSERT INTO kv_store (key, value, updated_at)
            VALUES (:key, :value, :updated_at)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """), {
            "key": key,
            "value": json.dumps({"task_ids": sorted(task_ids), "missing_counts": missing_counts}),
            "updated_at": now,
        })
    return len(flagged_ids)


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
    status = _map_status(task, due, today)
    source_status = str((task.get("status") or {}).get("status") or "open").lower()

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
        "source_status": source_status,
        "source_open_amount_cents": 0 if status == "completed" else amount_cents,
        "source_updated_at": _source_timestamp(task),
        "preserve_settlement_truth": True,
        "apply_source_lifecycle": True,
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
    Every ClickUp task maps to exactly one CashEvent. ClickUp already creates
    separate task instances for recurring work; expanding every instance as a
    new template multiplies the forecast and makes the queue untrustworthy.

    Legacy task-per-template rows are cancelled, not deleted, and their
    templates are deactivated so the cleanup is reversible and auditable.

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
        ev_created = ev_updated = skipped = 0

        list_configs = [
            (settings.clickup_ap_list_id, "outflow"),
            (settings.clickup_ar_list_id, "inflow"),
        ]

        engine = get_engine()
        quarantined_events, quarantined_templates = (
            _quarantine_legacy_clickup_template_expansions(engine)
        )
        if quarantined_events or quarantined_templates:
            logger.warning(
                "Quarantined legacy ClickUp template expansion: events=%d templates=%d",
                quarantined_events,
                quarantined_templates,
            )

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
                if ev["amount_cents"] == 0:
                    skipped += 1
                    continue

                upsert_result = _upsert_event(engine, ev)
                if upsert_result == "created":
                    ev_created += 1
                else:
                    ev_updated += 1

            missing = _record_successful_list_snapshot(
                engine, event_type, {str(task.get("id") or "") for task in tasks if task.get("id")}
            )
            if missing:
                logger.warning(
                    "ClickUp %s list has %d task(s) absent from two successful snapshots",
                    event_type, missing,
                )

        result.matches_made = _match_existing_posted_transactions(engine)

        logger.info(
            "ClickUp finance sync complete: "
            "events created=%d updated=%d | skipped=%d | matched=%d",
            ev_created, ev_updated, skipped, result.matches_made,
        )
        result.rows_inserted = ev_created
        result.rows_skipped_duplicate = ev_updated + skipped

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
