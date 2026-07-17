"""Cashflow Overview — Finance OS landing page."""

from __future__ import annotations

import asyncio
import html
import inspect
import json
import os
from dataclasses import dataclass as _dc
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from sales_support_agent.services.cashflow.cashflow_helpers import (
    _dollar,
    _events_to_dtos,
    _page_shell,
    _name_cell,
    _display_name,
    CATEGORY_ORDER,
    _CAT_ORDER_INDEX,
    _CAT_ICON,
)
from sales_support_agent.services.cashflow.engine import (
    EventDTO,
    aggregate_weeks,
    flag_risks,
)
from sales_support_agent.services.cashflow.obligations import list_obligations
from sales_support_agent.services.cashflow.smart_cfo import load_smart_cfo_analysis


def _resolve_current_balance(rows: list[dict[str, Any]]) -> tuple[int, str, str]:
    """Resolve one canonical current balance for cards, forecasts, and charts."""
    balance_cents = 0
    balance_as_of = ""
    balance_source = ""
    snapshot_date: date | None = None

    try:
        from sales_support_agent.models.database import kv_get_json

        snapshot = kv_get_json("balance_snapshot")
        if snapshot and snapshot.get("balance_cents") is not None:
            balance_cents = int(snapshot["balance_cents"])
            balance_as_of = str(snapshot.get("as_of_date", ""))[:10]
            try:
                snapshot_date = date.fromisoformat(balance_as_of)
            except ValueError:
                snapshot_date = None
            balance_source = str(snapshot.get("source", ""))
    except Exception:
        pass

    csv_rows = [
        row
        for row in rows
        if row.get("source") == "csv" and row.get("account_balance_cents") is not None
    ]
    # max() is stable on ties. Bank exports are newest-first, so the first row
    # for the newest posting date is the closing balance for that date.
    latest_csv_row = max(
        csv_rows,
        key=lambda row: _row_due_date(row) or date.min,
        default=None,
    )
    latest_csv_date = _row_due_date(latest_csv_row) if latest_csv_row else None

    if latest_csv_row and (
        snapshot_date is None
        or (latest_csv_date is not None and latest_csv_date > snapshot_date)
    ):
        balance_cents = int(latest_csv_row["account_balance_cents"] or 0)
        balance_as_of = latest_csv_date.isoformat() if latest_csv_date else ""
        balance_source = "csv"
        try:
            from sales_support_agent.models.database import kv_set_json

            kv_set_json(
                "balance_snapshot",
                {
                    "balance_cents": balance_cents,
                    "as_of_date": balance_as_of,
                    "source": balance_source,
                },
            )
        except Exception:
            pass

    return balance_cents, balance_as_of, balance_source


def _build_daily_chart_data(days_back: int = 14, days_forward: int = 42) -> dict:
    """Build day-level chart data for the cashflow bar + balance line chart.

    Returns a dict consumed by the Chart.js mixed chart:
      labels          : list[str] — "Apr 06"
      actual_out      : list[float|None] — negative posted outflows (past only)
      actual_in       : list[float|None] — positive posted inflows (past only)
      planned_out     : list[float|None] — negative planned outflows (future only)
      planned_in      : list[float|None] — positive planned inflows (future only)
      balance_actual  : list[float|None] — actual EOD balance (past only)
      balance_projected: list[float|None] — projected EOD balance (future only)
      tooltips        : list[dict] — per-day event breakdown for hover cards
      cutoff_index    : int — index of today in the lists
      starting_balance: float — current bank balance in dollars
      threshold       : float — safety floor in dollars
    """
    from collections import defaultdict

    today = date.today()
    start = today - timedelta(days=days_back)
    end   = today + timedelta(days=days_forward)

    all_rows = list_obligations(limit=5000)

    balance_cents, _, _ = _resolve_current_balance(all_rows)

    # Per-day buckets
    actual_out_day:  dict = defaultdict(int)
    actual_in_day:   dict = defaultdict(int)
    planned_out_day: dict = defaultdict(int)
    planned_in_day:  dict = defaultdict(int)
    tooltip_day:     dict = defaultdict(list)

    for r in all_rows:
        raw = r.get("due_date")
        if raw is None:
            continue
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
        else:
            continue

        if d < start or d > end:
            continue

        amt    = int(r.get("amount_cents") or 0)
        is_in  = r.get("event_type") == "inflow"
        status = r.get("status", "")

        tooltip_day[d].append({
            "name":      (r.get("name") or r.get("vendor_or_customer") or "")[:40],
            "category":  r.get("category") or "other",
            "amount":    amt / 100,
            "dir":       "in" if is_in else "out",
            "status":    status,
            "is_actual": status in ("posted", "matched"),
        })

        if status in ("posted", "matched") and d <= today:
            (actual_in_day if is_in else actual_out_day)[d] += amt
        elif status in ("planned", "pending", "overdue"):
            (planned_in_day if is_in else planned_out_day)[d] += amt

    # --- Compute actual EOD balances (walk backward from today) -----------
    # Strategy: start_balance is as of close-of-business today.
    # Going back: subtract today's net, then yesterday's, etc.
    actual_bal: dict = {today: balance_cents}
    running = balance_cents
    d = today - timedelta(days=1)
    while d >= start:
        # Un-apply next day's changes to get balance at end of d
        next_d = d + timedelta(days=1)
        day_net = actual_in_day.get(next_d, 0) - actual_out_day.get(next_d, 0)
        running -= day_net
        actual_bal[d] = running
        d -= timedelta(days=1)

    # --- Compute projected EOD balances (walk forward from today) ----------
    proj_bal: dict = {}
    running = balance_cents
    d = today + timedelta(days=1)
    while d <= end:
        running += planned_in_day.get(d, 0) - planned_out_day.get(d, 0)
        proj_bal[d] = running
        d += timedelta(days=1)

    # --- Assemble output arrays -------------------------------------------
    labels           = []
    actual_out_list  = []
    actual_in_list   = []
    planned_out_list = []
    planned_in_list  = []
    bal_actual_list  = []
    bal_proj_list    = []
    tooltips         = []
    cutoff_index     = days_back   # index of "today"

    d = start
    while d <= end:
        labels.append(d.strftime("%b %d"))

        is_past_or_today = d <= today

        # Actual: past/today only (posted or matched transactions)
        actual_out_list.append( -(actual_out_day.get(d, 0) / 100)  if is_past_or_today else None)
        actual_in_list.append(   actual_in_day.get(d, 0) / 100     if is_past_or_today else None)
        # Planned: ALL dates — so you can see what was scheduled vs what happened
        # (past planned = overdue/unpaid scheduled items; future planned = upcoming)
        pout = planned_out_day.get(d, 0)
        pin  = planned_in_day.get(d, 0)
        planned_out_list.append( -(pout / 100) if pout else None)
        planned_in_list.append(    pin / 100   if pin  else None)
        bal_actual_list.append(  actual_bal[d] / 100               if d in actual_bal else None)
        bal_proj_list.append(    proj_bal[d] / 100                  if d in proj_bal else None)

        items = tooltip_day.get(d, [])
        tooltips.append({
            "date":  d.isoformat(),
            "items": items,
        })
        d += timedelta(days=1)

    return {
        "labels":           labels,
        "actual_out":       actual_out_list,
        "actual_in":        actual_in_list,
        "planned_out":      planned_out_list,
        "planned_in":       planned_in_list,
        "balance_actual":   bal_actual_list,
        "balance_projected":bal_proj_list,
        "tooltips":         tooltips,
        "cutoff_index":     cutoff_index,
        "starting_balance": balance_cents / 100,
        "threshold":        10000,
    }


def _build_chart_data(period_weeks: int = 12) -> dict:
    """Build Chart.js dataset for the cashflow chart.

    Performance note: uses a single DB call (list_obligations) for ALL data.
    Forward weeks are projected by Python-bucketing the already-loaded rows
    rather than issuing one get_events_for_range() query per future week.
    """
    today = date.today()

    # Single query for everything — historical CSV rows AND forward obligations.
    all_rows = list_obligations(limit=5000)

    csv_rows = sorted(
        [r for r in all_rows if r.get("source") == "csv" and r.get("account_balance_cents") is not None],
        key=lambda r: str(r.get("due_date", ""))
    )

    starting_balance, _, _ = _resolve_current_balance(all_rows)

    # ── Pre-bucket forward obligations by week-start (Monday) ──────────────
    # This replaces the old N+1 loop that called get_events_for_range() once
    # per future week.  One pass here; O(1) lookup below.
    def _row_monday(r: dict) -> date | None:
        raw = r.get("due_date")
        if raw is None:
            return None
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                return None
        else:
            return None
        return d - timedelta(days=d.weekday())

    from collections import defaultdict
    week_buckets: dict = defaultdict(int)   # monday -> cumulative net_cents for that week
    for r in all_rows:
        if r.get("status") not in ("planned", "overdue", "pending"):
            continue
        mon = _row_monday(r)
        if mon is None or mon <= today - timedelta(days=today.weekday()):
            continue  # skip historical / current week (already in starting_balance)
        amt = int(r.get("amount_cents") or 0)
        delta = amt if r.get("event_type") == "inflow" else -amt
        week_buckets[mon] += delta

    # ── Build weekly labels ─────────────────────────────────────────────────
    monday = today - timedelta(days=today.weekday())
    start_monday = monday - timedelta(weeks=4)  # 4 weeks back

    labels: list = []
    actual_data: list = []
    projected_data: list = []
    cumulative_net = 0  # cents accumulated week-by-week into the future

    for i in range(period_weeks + 4):
        week_date = start_monday + timedelta(weeks=i)
        labels.append(week_date.strftime("%b %d"))

        if week_date <= today:
            week_csv = [r for r in csv_rows if str(r.get("due_date", ""))[:10] <= week_date.isoformat()]
            actual_data.append(int(week_csv[-1].get("account_balance_cents") or 0) / 100 if week_csv else None)
            projected_data.append(None)
        else:
            actual_data.append(None)
            cumulative_net += week_buckets.get(week_date, 0)
            projected_data.append((starting_balance + cumulative_net) / 100)

    return {
        "labels": labels,
        "actual": actual_data,
        "projected": projected_data,
        "threshold": 10000,  # $10,000 default floor
        "starting_balance": starting_balance / 100,
    }


def _render_weekly_table(rows_or_events: list, today, balance_cents: int = 0) -> str:
    """Render the 8-week cashflow table: chronological, one row per transaction."""
    monday = today - timedelta(days=today.weekday())

    # Build 8-week buckets
    weeks = []
    for i in range(8):
        ws = monday + timedelta(weeks=i)
        we = ws + timedelta(days=6)
        weeks.append((ws, we, []))

    # Handle both EventDTO list and raw dict list
    def _get_due(item):
        raw = getattr(item, "due_date", None) or (item.get("due_date") if isinstance(item, dict) else None)
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, date):
            return raw
        if isinstance(raw, str):
            try:
                return date.fromisoformat(raw[:10])
            except Exception:
                return None
        return None

    def _get_attr(item, key, default=""):
        if isinstance(item, dict):
            return item.get(key, default)
        return getattr(item, key, default)

    # Assign events to weeks
    for item in rows_or_events:
        due = _get_due(item)
        if not due:
            continue
        due_monday = due - timedelta(days=due.weekday())
        for ws, we, evts in weeks:
            if ws == due_monday:
                evts.append((due, item))
                break

    running = balance_cents
    html_rows = ""

    for ws, we, evts in weeks:
        evts.sort(key=lambda x: x[0])
        week_in = sum(_get_attr(r, "amount_cents", 0) for _, r in evts if _get_attr(r, "event_type") == "inflow")
        week_out = sum(_get_attr(r, "amount_cents", 0) for _, r in evts if _get_attr(r, "event_type") == "outflow")
        week_net = week_in - week_out

        # Week header
        net_col = "#86efac" if week_net >= 0 else "#fca5a5"
        html_rows += f"""
        <tr style="background:#1e293b;color:#fff">
          <td colspan="3" style="font-weight:600;padding:6px 12px">
            Week of {ws.strftime("%b %d")} – {we.strftime("%b %d")}
          </td>
          <td style="text-align:right;color:#86efac">+{_dollar(week_in)}</td>
          <td style="text-align:right;color:#fca5a5">–{_dollar(week_out)}</td>
          <td style="text-align:right;font-weight:600;color:{net_col}">{'+' if week_net >= 0 else ''}{_dollar(week_net)}</td>
        </tr>"""

        if not evts:
            html_rows += '<tr><td colspan="6" style="color:#9ca3af;font-style:italic;padding:4px 12px">No transactions this week</td></tr>'

        for due_date, item in evts:
            running += _get_attr(item, "amount_cents", 0) if _get_attr(item, "event_type") == "inflow" else -_get_attr(item, "amount_cents", 0)
            is_in = _get_attr(item, "event_type") == "inflow"
            amt = _dollar(_get_attr(item, "amount_cents", 0))
            bal_color = "#16a34a" if running >= 0 else "#dc2626"

            # Build row dict for _name_cell
            if isinstance(item, dict):
                row_dict = item
            else:
                row_dict = {
                    "id": getattr(item, "id", ""),
                    "name": getattr(item, "name", ""),
                    "vendor_or_customer": getattr(item, "vendor_or_customer", ""),
                    "description": "",
                    "friendly_name": None,
                }

            html_rows += f"""
            <tr>
              <td style="color:#6b7280;white-space:nowrap">{due_date.strftime("%b %d")}</td>
              <td>{_name_cell(row_dict)}</td>
              <td style="color:#6b7280;font-size:0.8rem">{html.escape(str(_get_attr(item, "category", "")))}</td>
              <td style="text-align:right;color:#16a34a;font-weight:500">{"" if not is_in else amt}</td>
              <td style="text-align:right;color:#dc2626;font-weight:500">{"" if is_in else amt}</td>
              <td style="text-align:right;font-weight:600;color:{bal_color}">{_dollar(running)}</td>
            </tr>"""

    return f"""
    <table style="width:100%">
      <thead>
        <tr>
          <th>Date</th><th>Description</th><th>Category</th>
          <th style="text-align:right">In</th>
          <th style="text-align:right">Out</th>
          <th style="text-align:right">Balance</th>
        </tr>
      </thead>
      <tbody>{html_rows}</tbody>
    </table>"""


# ---------------------------------------------------------------------------
# Finance overview metrics dataclass and compute function
# ---------------------------------------------------------------------------

@_dc
class FinanceOverviewMetrics:
    balance_cents: int
    balance_class: str          # "negative" or ""
    net_4w_cents: int
    net_class: str              # "negative", "positive", or ""
    upcoming_total_cents: int
    upcoming_count: int
    upcoming_class: str         # "amount-out" if > 0, else ""
    overdue_count: int
    overdue_total_cents: int
    overdue_class: str          # "negative" if overdue > 0, else ""
    critical_count: int
    warning_count: int
    alerts_class: str           # "negative" if critical > 0, else ""
    weeks: list                 # list[WeekSummary]
    ai_text: str
    # Budget safeguard fields
    runway_days: int            # days until balance < min floor (default $10k)
    at_risk_dates: list         # list[date] of Monday dates going below floor
    min_balance_cents: int      # floor used for safeguard calc


def compute_cash_runway(
    balance_cents: int,
    all_rows: list,
    *,
    min_balance_cents: int = 1_000_000,  # $10,000 floor
    horizon_days: int = 365,
) -> tuple[int, list[date]]:
    """Calculate how many days until the projected balance breaches min_balance_cents.

    Args:
        balance_cents: Current bank balance in cents.
        all_rows: All cash_event rows (from list_obligations).
        min_balance_cents: Safety floor — default $10,000.
        horizon_days: How far to project (default 365 days).

    Returns:
        (runway_days, at_risk_dates) where:
          runway_days  = days until balance < min_balance_cents (or horizon_days if never).
          at_risk_dates = list of Monday dates where weekly projected balance < min_balance_cents.
    """
    from collections import defaultdict

    today = date.today()
    cutoff = today + timedelta(days=horizon_days)

    # Build daily net buckets from planned/overdue rows
    day_buckets: dict = defaultdict(int)
    for r in all_rows:
        if r.get("status") not in ("planned", "overdue", "pending"):
            continue
        raw = r.get("due_date")
        if raw is None:
            continue
        if isinstance(raw, datetime):
            d = raw.date()
        elif isinstance(raw, date):
            d = raw
        elif isinstance(raw, str):
            try:
                d = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
        else:
            continue
        if d <= today or d > cutoff:
            continue
        amt = int(r.get("amount_cents") or 0)
        delta = amt if r.get("event_type") == "inflow" else -amt
        day_buckets[d] += delta

    running = balance_cents
    at_risk_dates: list[date] = []
    runway_days = horizon_days  # assume no breach until proven otherwise

    current = today + timedelta(days=1)
    day_num = 0
    while current <= cutoff:
        running += day_buckets.get(current, 0)
        day_num += 1
        if running < min_balance_cents:
            # Track weekly breach dates (one per week to avoid noise)
            week_monday = current - timedelta(days=current.weekday())
            if not at_risk_dates or at_risk_dates[-1] != week_monday:
                at_risk_dates.append(week_monday)
            if runway_days == horizon_days:
                runway_days = day_num
        current += timedelta(days=1)

    return runway_days, at_risk_dates


def compute_finance_overview(
    events: list,
    alerts: list,
    weeks: list,
    balance_cents: int,
    *,
    today,
    ai_text: str = "",
    all_rows: list | None = None,
    min_balance_cents: int = 1_000_000,
) -> FinanceOverviewMetrics:
    balance_class = "negative" if balance_cents < 0 else ""
    net_4w = sum(w.net_cents for w in weeks)
    net_class = "negative" if net_4w < 0 else ("positive" if net_4w > 0 else "")

    soon = today + timedelta(days=14)
    upcoming = [
        e for e in events
        if e.status in ("planned", "pending", "overdue")
        and today <= e.due_date <= soon
        and e.event_type == "outflow"
    ]
    upcoming_total = sum(e.amount_cents for e in upcoming)
    upcoming_class = "amount-out" if upcoming_total > 0 else ""

    overdue = [e for e in events if e.status == "overdue"]
    overdue_total = sum(e.amount_cents for e in overdue)
    overdue_class = "negative" if overdue else ""

    critical_count = sum(1 for a in alerts if a.severity == "critical")
    warning_count = sum(1 for a in alerts if a.severity == "warning")
    alerts_class = "negative" if critical_count else ""

    # Budget safeguard — runway and at-risk weeks
    if all_rows is not None:
        runway_days, at_risk_dates = compute_cash_runway(
            balance_cents, all_rows, min_balance_cents=min_balance_cents,
        )
    else:
        runway_days, at_risk_dates = 365, []

    return FinanceOverviewMetrics(
        balance_cents=balance_cents,
        balance_class=balance_class,
        net_4w_cents=net_4w,
        net_class=net_class,
        upcoming_total_cents=upcoming_total,
        upcoming_count=len(upcoming),
        upcoming_class=upcoming_class,
        overdue_count=len(overdue),
        overdue_total_cents=overdue_total,
        overdue_class=overdue_class,
        critical_count=critical_count,
        warning_count=warning_count,
        alerts_class=alerts_class,
        weeks=weeks,
        ai_text=ai_text,
        runway_days=runway_days,
        at_risk_dates=at_risk_dates,
        min_balance_cents=min_balance_cents,
    )


# ---------------------------------------------------------------------------
# Overview page helpers
# ---------------------------------------------------------------------------

def _row_due_date(row: dict[str, Any]) -> date | None:
    raw = row.get("due_date")
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        try:
            return date.fromisoformat(str(raw)[:10])
        except ValueError:
            return None
    return None


def _is_chunk_payable(row: dict[str, Any]) -> bool:
    if row.get("category") == "rent":
        return True
    text = " ".join(
        str(row.get(field, "") or "")
        for field in ("name", "vendor_or_customer", "notes", "description")
    ).lower()
    return "chunk" in text or "partial" in text


def _source_label(row: dict[str, Any]) -> str:
    source = str(row.get("source") or "").strip().lower()
    if source == "csv":
        return "CSV"
    if source.startswith("qbo"):
        return "QuickBooks"
    if source == "clickup":
        return "ClickUp"
    if source == "manual":
        return "Manual"
    return source.replace("_", " ").title() or "System"


def _queue_reason(row: dict[str, Any], *, due: date | None, today: date) -> str:
    parts: list[str] = []
    if str(row.get("status", "")).lower() == "overdue" or (due and due < today):
        parts.append("Overdue")
    elif due:
        days = (due - today).days
        if days == 0:
            parts.append("Due today")
        elif days == 1:
            parts.append("Due tomorrow")
        elif days > 1:
            parts.append(f"Due in {days} days")
    if _is_chunk_payable(row):
        parts.append("Chunk-payable")
    parts.append(_source_label(row))
    return " · ".join(parts)


_MISSING = object()
_SETTLED_STATUSES = {"posted", "matched", "cancelled", "paid"}


def _control_value(value: Any, *names: str, default: Any = None) -> Any:
    """Read the first present field from a control dict or dataclass."""
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if value is not None and hasattr(value, name):
            return getattr(value, name)
    return default


def _cents(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _money(cents: int, *, exact: bool = False) -> str:
    sign = "-" if cents < 0 else ""
    decimals = 2 if exact else 0
    return f"{sign}${abs(cents) / 100:,.{decimals}f}"


def _safe_source_url(value: Any) -> str:
    url = str(value or "").strip()
    if url.startswith("/") and not url.startswith("//"):
        return url
    if url.startswith(("https://", "http://")):
        return url
    return ""


def _row_confidence(row: Mapping[str, Any]) -> str:
    return str(row.get("confidence") or "estimated").strip().lower()


def _fallback_chart(
    rows: list[dict[str, Any]], balance_cents: int, today: date, floor_cents: int
) -> dict[str, Any]:
    day_rows: dict[date, list[dict[str, Any]]] = {}
    for row in rows:
        due = _row_due_date(row)
        if due is not None:
            day_rows.setdefault(due, []).append(row)

    labels: list[str] = []
    committed: list[int] = []
    expected: list[int] = []
    stress: list[int] = []
    running_committed = running_expected = running_stress = balance_cents
    for offset in range(29):
        current = today + timedelta(days=offset)
        labels.append(current.strftime("%b %d"))
        if offset:
            for row in day_rows.get(current, []):
                if str(row.get("status") or "").lower() in _SETTLED_STATUSES:
                    continue
                amount = _cents(row.get("open_amount_cents"), _cents(row.get("amount_cents")))
                if row.get("event_type") == "inflow":
                    running_expected += amount
                    if _row_confidence(row) == "confirmed":
                        running_committed += amount
                        running_stress += amount
                else:
                    running_committed -= amount
                    running_expected -= amount
                    flexibility = str(row.get("flexibility") or "unknown")
                    if flexibility not in {"chunkable", "deferrable"} or row.get("installment_id"):
                        running_stress -= amount
        committed.append(running_committed)
        expected.append(running_expected)
        stress.append(running_stress)
    return {
        "labels": labels,
        "actual": [balance_cents] + [None] * 28,
        "committed": committed,
        "expected": expected,
        "stress": stress,
        "floor": floor_cents,
    }


def _fallback_finance_control(
    rows: list[dict[str, Any]], balance_cents: int, balance_as_of: str, today: date
) -> dict[str, Any]:
    floor_cents = 1_000_000
    soon = today + timedelta(days=14)
    active = [
        row for row in rows
        if str(row.get("status") or "planned").lower() not in _SETTLED_STATUSES
    ]
    incoming = [
        row for row in active
        if row.get("event_type") == "inflow"
        and (due := _row_due_date(row)) is not None
        and due <= soon
    ]
    outgoing = [
        row for row in active
        if row.get("event_type") == "outflow"
        and (due := _row_due_date(row)) is not None
        and due <= soon
    ]
    confirmed_in = sum(
        _cents(row.get("open_amount_cents"), _cents(row.get("amount_cents")))
        for row in incoming if _row_confidence(row) == "confirmed"
    )
    expected_in = sum(
        _cents(row.get("open_amount_cents"), _cents(row.get("amount_cents")))
        for row in incoming if _row_confidence(row) != "confirmed"
    )
    required_out = sum(
        _cents(row.get("scheduled_amount_cents"), _cents(row.get("amount_cents")))
        for row in outgoing
    )
    exposure_out = sum(
        _cents(row.get("open_amount_cents"), _cents(row.get("amount_cents")))
        for row in active
        if row.get("event_type") == "outflow" and row not in outgoing
    )
    balance_available = bool(balance_as_of)
    minimum_stress = balance_cents + confirmed_in - required_out
    funding_gap = max(0, floor_cents - minimum_stress) if balance_available else 0
    safe_to_commit = max(0, minimum_stress - floor_cents) if balance_available else 0
    overdue = [row for row in active if (due := _row_due_date(row)) is not None and due < today]
    missing_dates = [row for row in active if _row_due_date(row) is None]

    happening = (
        f"{_money(confirmed_in)} confirmed income and {_money(required_out)} required out "
        "are in the next 14 days."
    )
    broken_parts = []
    if not balance_available:
        broken_parts.append("The current bank balance needs an update.")
    if missing_dates:
        broken_parts.append(f"{len(missing_dates)} money items need dates.")
    if overdue:
        broken_parts.append(f"{len(overdue)} items are overdue.")
    broken = " ".join(broken_parts) or "No material blockers are detected in the selected window."
    if not balance_available:
        next_action = "Upload the latest bank CSV before making a payment decision."
    elif overdue:
        next_action = f"Review {_display_name(overdue[0])} before scheduling the next cash action."
    elif missing_dates:
        next_action = f"Confirm the date for {_display_name(missing_dates[0])}."
    else:
        next_action = "No urgent action is required; refresh sources before the next review."

    return {
        "cash_position": {
            "cash_on_hand_cents": balance_cents,
            "balance_available": balance_available,
            "incoming_confirmed_cents": confirmed_in,
            "incoming_expected_cents": expected_in,
            "required_out_cents": required_out,
            "exposure_out_cents": exposure_out,
            "safe_to_commit_cents": safe_to_commit,
            "funding_gap_cents": funding_gap,
            "floor_cents": floor_cents,
        },
        "smart_brief": {"happening": happening, "broken": broken, "next": next_action},
        "forecast": _fallback_chart(rows, balance_cents, today, floor_cents),
        "queue": rows,
        "recommendation": {
            "title": next_action,
            "why": broken,
            "before_minimum_cash_cents": minimum_stress,
            "after_minimum_cash_cents": max(minimum_stress, floor_cents) if funding_gap else minimum_stress,
            "depends_on": "Confirmed income only; expected trend income is excluded.",
            "confidence": "Low" if not balance_available else "Medium",
            "limitations": "Refresh source data before confirming any write.",
            "downside": "The open obligation remains visible until settlement is confirmed.",
            "action_label": "Create action preview",
        },
    }


def _status_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _trust_action_copy(value: Any) -> str:
    raw = str(value or "").strip()
    key = _status_key(raw)
    known = {
        "upload_latest_balance": "Upload the latest bank CSV before making a cash decision.",
        "refresh_cash_balance": "Refresh the bank CSV before making a cash decision.",
        "resolve_finance_data": "Resolve missing or conflicting finance evidence first.",
        "review_income_patterns": "Review inferred income patterns before making a cash decision.",
        "configure_income_forecast": "Configure forecast income before making a cash decision.",
        "review_cash_plan": "Review the refreshed cash plan.",
    }
    if key in known:
        return known[key]
    return raw if " " in raw else raw.replace("_", " ").capitalize() + "."


def _normalise_source_statuses(control: Any) -> list[dict[str, str]]:
    raw_statuses = _control_value(control, "source_status", "source_statuses", default=[])
    if isinstance(raw_statuses, Mapping):
        raw_statuses = [
            {"source": source, **(dict(value) if isinstance(value, Mapping) else {"status": value})}
            for source, value in raw_statuses.items()
        ]
    try:
        supplied = list(raw_statuses or [])
    except TypeError:
        supplied = []

    aliases = {
        "bank": "bank_csv", "bank_csv": "bank_csv", "csv": "bank_csv",
        "clickup": "clickup", "qbo": "qbo", "quickbooks": "qbo",
        "qbo_open_invoices": "qbo",
    }
    labels = {"bank_csv": "Bank CSV", "clickup": "ClickUp", "qbo": "QBO"}
    by_source: dict[str, dict[str, str]] = {}
    for item in supplied:
        source = _status_key(_control_value(item, "source", "key", "name", default=""))
        canonical = aliases.get(source)
        if not canonical:
            continue
        status = _status_key(_control_value(item, "status", "state", "readiness", default="unknown"))
        is_ready = _control_value(item, "ready", "is_ready", default=_MISSING)
        if is_ready is not _MISSING:
            # An explicit failed connection must never inherit a stale "ready" label.
            status = "ready" if bool(is_ready) else "not_ready"
        detail = str(
            _control_value(
                item, "detail", "message", "summary", "freshness", "as_of", "as_of_date",
                "latest_date", default="Status not reported.",
            )
            or "Status not reported."
        )
        by_source[canonical] = {
            "key": canonical,
            "label": labels[canonical],
            "status": status or "unknown",
            "detail": detail,
        }
    return [
        by_source.get(
            key,
            {"key": key, "label": labels[key], "status": "unknown", "detail": "Status not reported."},
        )
        for key in ("bank_csv", "clickup", "qbo")
    ]


def _normalise_income_patterns(income_projection: Any) -> list[dict[str, Any]]:
    raw_patterns = _control_value(income_projection, "patterns", "income_patterns", default=[])
    try:
        patterns = list(raw_patterns or [])
    except TypeError:
        patterns = []
    normalised = []
    for index, pattern in enumerate(patterns):
        evidence = _control_value(pattern, "evidence", "source_evidence", default={})
        pattern_key = str(
            _control_value(pattern, "pattern_key", "key", "id", default=f"pattern-{index}")
        )
        raw_dates = _control_value(
            pattern, "dates", "observed_dates", "evidence_dates", "posting_dates",
            default=_control_value(evidence, "occurrence_dates", "dates", default=[]),
        )
        if isinstance(raw_dates, str):
            dates = [raw_dates]
        else:
            try:
                dates = list(raw_dates or [])
            except TypeError:
                dates = []
        date_labels = []
        for raw_date in dates:
            if isinstance(raw_date, Mapping):
                raw_date = _control_value(raw_date, "date", "posted_date", default="")
            if isinstance(raw_date, (date, datetime)):
                date_labels.append(raw_date.strftime("%b %d, %Y"))
                continue
            try:
                date_labels.append(date.fromisoformat(str(raw_date)[:10]).strftime("%b %d, %Y"))
            except ValueError:
                if raw_date:
                    date_labels.append(str(raw_date))
        probability = _control_value(
            pattern, "probability", "confidence_probability",
            default=_control_value(evidence, "probability", "probability_bps", default=0),
        )
        try:
            probability_number = float(probability or 0)
        except (TypeError, ValueError):
            probability_number = 0
        if _control_value(evidence, "probability_bps", default=_MISSING) is not _MISSING:
            probability_number /= 100
        elif 0 < probability_number <= 1:
            probability_number *= 100
        decision = _status_key(_control_value(pattern, "decision", "status", default="needs_review"))
        normalised.append({
            "key": pattern_key,
            "label": str(
                _control_value(
                    pattern, "display_name", "counterparty", "party", "description",
                    default="Recurring deposit pattern",
                )
            ),
            "dates": date_labels,
            "cadence": str(_control_value(
                pattern, "cadence", "cadence_label",
                default=(
                    f"Every {_control_value(evidence, 'median_cadence_days')} days"
                    if _control_value(evidence, "median_cadence_days", default=None)
                    else "Cadence unavailable"
                ),
            )),
            "amount_cents": max(0, _cents(_control_value(
                pattern, "conservative_amount_cents", "conservative_cents", "amount_cents",
                default=_control_value(evidence, "projected_amount_cents", default=0),
            ))),
            "probability": max(0, min(100, round(probability_number))),
            "decision": decision or "needs_review",
        })
    return normalised


def _build_renderer_state(
    rows: list[dict[str, Any]], balance_cents: int, balance_as_of: str, today: date,
    settlement_annotations: list[dict[str, Any]] | None = None,
    income_decisions: Any = None,
    source_connections: Any = None,
) -> tuple[Any, dict[str, Any], bool]:
    """Load the canonical control builder, retaining a safe local read fallback."""
    fallback = _fallback_finance_control(rows, balance_cents, balance_as_of, today)
    try:
        from sales_support_agent.services.cashflow.control import build_finance_control

        supplied = {
            "smart_mode": True,
            "settlement_annotations": settlement_annotations,
            "income_decisions": income_decisions,
            "source_connections": source_connections,
        }
        signature = inspect.signature(build_finance_control)
        accepts_kwargs = any(
            parameter.kind == parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        kwargs = supplied if accepts_kwargs else {
            key: value for key, value in supplied.items() if key in signature.parameters
        }
        control = build_finance_control(rows, balance_cents, balance_as_of, **kwargs)
        if control is not None:
            return control, fallback, False
    except (ImportError, AttributeError):
        pass
    except Exception:
        return fallback, fallback, True
    return fallback, fallback, False


def _normalise_renderer_state(control: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    cash = _control_value(control, "cash_position", "cash_metrics", "metrics", default=control)
    fallback_cash = fallback["cash_position"]
    brief = _control_value(control, "smart_brief", "brief", default={})
    fallback_brief = fallback["smart_brief"]
    income_projection = _control_value(control, "income_projection", default={})
    projection_status = str(
        _control_value(income_projection, "status", "state", default="") or ""
    ).strip().lower().replace("-", "_").replace(" ", "_")
    projection_configured = _control_value(
        income_projection, "configured", "is_configured", default=_MISSING
    )
    projection_unconfigured = (
        projection_configured is not _MISSING and not bool(projection_configured)
    ) or projection_status in {
        "not_configured", "unconfigured", "missing", "setup_incomplete", "not_ready"
    }
    csv_trend_expected_cents = max(
        0,
        _cents(
            _control_value(
                income_projection,
                "csv_trend_expected_cents",
                "expected_csv_trend_cents",
                "expected_14d_cents",
                "projected_income_cents",
                default=0,
            )
        ),
    )
    posted_inflow_count = max(
        0,
        int(
            _control_value(
                income_projection,
                "posted_inflow_count",
                "historical_inflow_count",
                "posted_inflows",
                default=0,
            )
            or 0
        ),
    )
    recommendation = _control_value(
        control, "recommendation", "top_recommendation", "smart_recommendation", default={}
    )
    patterns = _normalise_income_patterns(income_projection)
    trust_gate = _control_value(control, "trust_gate", default=_MISSING)
    trust_ready_raw = _control_value(trust_gate, "ready", "is_ready", "passed", default=_MISSING)
    trust_status = _status_key(_control_value(trust_gate, "status", "state", default=""))
    if trust_ready_raw is _MISSING:
        trust_ready = None if trust_gate is _MISSING else trust_status in {"ready", "passed", "ok", "healthy"}
    else:
        trust_ready = bool(trust_ready_raw)
    raw_trust_issues = _control_value(
        trust_gate, "failures", "issues", "blockers", "reasons", default=[]
    )
    if isinstance(raw_trust_issues, str):
        trust_issues = [raw_trust_issues]
    else:
        try:
            trust_issues = [
                str(_control_value(issue, "message", "label", "summary", default=issue))
                for issue in list(raw_trust_issues or [])
            ]
        except TypeError:
            trust_issues = []
    trust_issues = [issue for issue in trust_issues if issue]
    default_trust_summary = (
        "; ".join(trust_issues).capitalize() + "."
        if trust_issues
        else "Source trust is ready." if trust_ready else "Source trust has not been verified."
    )
    data_quality = _control_value(control, "data_quality", default={})
    quality_counts = _control_value(data_quality, "counts", default={})
    quarantined_count = max(0, _cents(_control_value(
        data_quality, "quarantined_count",
        default=_control_value(quality_counts, "quarantined", default=0),
    )))
    missing_amount_count = max(0, _cents(_control_value(
        data_quality, "actionable_zero_obligation_count", "missing_amount_count",
        default=_control_value(quality_counts, "actionable_zero_obligations", default=0),
    )))
    default_quality_summary = (
        f"{quarantined_count} records excluded; {missing_amount_count} open items need amounts."
        if quarantined_count or missing_amount_count
        else "No data quality exceptions were reported."
    )
    raw_quality_issues = _control_value(data_quality, "issues", "warnings", "failures", default=[])
    if isinstance(raw_quality_issues, str):
        quality_issues = [raw_quality_issues]
    else:
        try:
            quality_issues = [
                str(_control_value(issue, "message", "label", "summary", default=issue))
                for issue in list(raw_quality_issues or [])
            ]
        except TypeError:
            quality_issues = []
    needs_review_raw = _control_value(
        income_projection, "needs_review_cents", "review_amount_cents", default=_MISSING
    )
    needs_review_cents = (
        sum(pattern["amount_cents"] for pattern in patterns if pattern["decision"] in {"needs_review", "review", "pending", "inferred"})
        if needs_review_raw is _MISSING
        else max(0, _cents(needs_review_raw))
    )
    return {
        "cash": {
            "cash_on_hand_cents": _cents(_control_value(cash, "cash_on_hand_cents", "balance_cents", default=fallback_cash["cash_on_hand_cents"])),
            "balance_available": bool(_control_value(cash, "balance_available", "has_balance", default=fallback_cash["balance_available"])),
            "incoming_confirmed_cents": _cents(_control_value(cash, "incoming_confirmed_cents", "confirmed_incoming_cents", default=fallback_cash["incoming_confirmed_cents"])),
            "incoming_expected_cents": _cents(_control_value(cash, "incoming_expected_cents", "expected_incoming_cents", default=fallback_cash["incoming_expected_cents"])),
            "required_out_cents": _cents(_control_value(cash, "required_out_cents", "required_outgoing_cents", "outgoing_cents", default=fallback_cash["required_out_cents"])),
            "exposure_out_cents": _cents(_control_value(cash, "exposure_out_cents", "outgoing_exposure_cents", default=fallback_cash["exposure_out_cents"])),
            "safe_to_commit_cents": max(0, _cents(_control_value(cash, "safe_to_commit_cents", default=fallback_cash["safe_to_commit_cents"]))),
            "funding_gap_cents": max(0, _cents(_control_value(cash, "funding_gap_cents", default=fallback_cash["funding_gap_cents"]))),
            "floor_cents": _cents(_control_value(cash, "floor_cents", "minimum_cash_floor_cents", default=fallback_cash["floor_cents"])),
        },
        "brief": {
            key: str(_control_value(brief, key, default=fallback_brief[key]) or fallback_brief[key])
            for key in ("happening", "broken", "next")
        },
        "income_projection": {
            "unconfigured": projection_unconfigured,
            "status": projection_status or ("unconfigured" if projection_unconfigured else "ready"),
            "csv_trend_expected_cents": csv_trend_expected_cents,
            "posted_inflow_count": posted_inflow_count,
            "needs_review_cents": needs_review_cents,
            "patterns": patterns,
        },
        "source_status": _normalise_source_statuses(control),
        "reconciliation_shadow": _control_value(control, "reconciliation_shadow", default={}),
        "trust_gate": {
            "ready": trust_ready,
            "status": trust_status or ("unknown" if trust_ready is None else "ready" if trust_ready else "not_ready"),
            "summary": str(_control_value(
                trust_gate, "summary", "message", "reason",
                default=default_trust_summary,
            )),
            "next_action": _trust_action_copy(_control_value(
                trust_gate, "next_action", "action", "next",
                default="Refresh the required source and resolve its data issues.",
            )),
            "issues": trust_issues,
        },
        "data_quality": {
            "status": _status_key(_control_value(data_quality, "status", "state", default="unknown")),
            "summary": str(_control_value(
                data_quality, "summary", "message", default=default_quality_summary
            )),
            "issues": [issue for issue in quality_issues if issue],
        },
        "forecast": _control_value(control, "forecast", "cash_trajectory", "forecast_paths", default=fallback["forecast"]),
        "queue": _control_value(control, "queue", "queue_items", "money_queue", default=fallback["queue"]),
        "recommendation": recommendation or fallback["recommendation"],
    }


def _flatten_queue(queue: Any) -> list[Any]:
    items = _control_value(queue, "items", "rows", default=_MISSING)
    if items is not _MISSING:
        return list(items or [])
    if isinstance(queue, Mapping):
        flattened: list[Any] = []
        seen: set[str] = set()
        for group_items in queue.values():
            if not isinstance(group_items, (list, tuple)):
                continue
            for item in group_items:
                item_id = str(_control_value(item, "id", "event_id", default=id(item)))
                if item_id not in seen:
                    flattened.append(item)
                    seen.add(item_id)
        return flattened
    return list(queue or [])


def _queue_item_data(item: Any, today: date) -> dict[str, Any]:
    row = _control_value(item, "row", "event", "obligation", default=item)
    if not isinstance(row, Mapping):
        row = vars(row) if hasattr(row, "__dict__") else {}
    direction = str(_control_value(item, "event_type", "direction", default=row.get("event_type") or "outflow"))
    status = str(_control_value(item, "status", default=row.get("status") or "planned")).lower()
    completion_requires_bank_evidence = bool(_control_value(
        item, "completion_requires_bank_evidence",
        default=row.get("completion_requires_bank_evidence"),
    ))
    historical_reconciliation_pending = bool(_control_value(
        item, "historical_reconciliation_pending",
        default=row.get("historical_reconciliation_pending"),
    ))
    due_raw = _control_value(item, "due_date", "date", default=row.get("due_date"))
    due = _row_due_date({"due_date": due_raw})
    amount_cents = _cents(
        _control_value(item, "open_amount_cents", "amount_cents", default=row.get("open_amount_cents") or row.get("amount_cents"))
    )
    party = str(_control_value(item, "party", "name", "vendor_or_customer", default=_display_name(dict(row))))
    action = _control_value(item, "action_label", "action", "recommended_action", default="")
    if not action:
        if historical_reconciliation_pending:
            action = "Reconcile history"
        elif status == "completed":
            action = "Completed in ClickUp"
        elif status in {"posted", "matched"}:
            action = "Review actual"
        elif due is None:
            action = "Confirm date"
        elif direction == "inflow" and due < today:
            action = "Collect now"
        elif direction == "inflow":
            action = "Track receipt"
        elif row.get("matched_to_id"):
            action = "Resolve match"
        elif due < today:
            action = "Review payment"
        elif _is_chunk_payable(dict(row)):
            action = "Protect cash"
        else:
            action = "Pay now"
    timing = _control_value(item, "timing", "timing_label", default="")
    if not timing:
        if due is None:
            timing = "Date missing"
        elif historical_reconciliation_pending:
            timing = "Historical occurrence - no forward cash impact"
        elif status == "completed":
            timing = "Completed in ClickUp - bank proof pending"
        elif due < today:
            late = (today - due).days
            timing = f"{due.strftime('%b %d, %Y')} - {late}d late"
        else:
            confidence = _row_confidence(row)
            suffix = f" - {confidence}" if direction == "inflow" else ""
            timing = f"{due.strftime('%b %d, %Y')}{suffix}"
    impact = _control_value(item, "cash_impact", "impact", "cash_impact_label", default="")
    if not impact:
        if due is None:
            impact = "Excluded until dated"
        elif historical_reconciliation_pending:
            impact = "$0 forward cash; bank allocation needed"
        elif status == "completed" and completion_requires_bank_evidence:
            impact = f"-{_money(amount_cents)} held pending bank evidence"
        elif status == "completed":
            impact = "$0 reserved; bank proof pending"
        elif direction == "inflow":
            impact = f"+{_money(amount_cents)} if received"
        else:
            impact = f"-{_money(amount_cents)} from cash"
    needs_action = not historical_reconciliation_pending and bool(
        _control_value(item, "needs_action", default=False)
        or due is None or (due is not None and due < today)
        or status in {"conflict", "duplicate", "review"}
    )
    inferred = bool(
        _control_value(item, "inferred", "is_inferred", "trend_inferred", default=False)
        or row.get("inferred")
        or row.get("is_inferred")
        or row.get("trend_inferred")
        or _status_key(row.get("origin")) in {"inferred", "income_pattern", "recurring_pattern"}
        or _status_key(row.get("source")) == "inferred"
    )
    tabs = ["reconciliation"] if historical_reconciliation_pending else ["incoming" if direction == "inflow" else "payables"]
    if status == "completed" and not completion_requires_bank_evidence:
        tabs = ["completed"]
    if needs_action:
        tabs.append("needs-action")
    if status in {"posted", "matched", "paid"}:
        tabs.append("recent")
    return {
        "id": str(_control_value(item, "id", "event_id", default=row.get("id") or "")),
        "action": str(action),
        "party": party,
        "meta": str(row.get("vendor_or_customer") or row.get("category") or _source_label(dict(row))),
        "timing": str(timing),
        "due_date": due.isoformat() if due else "",
        "amount_cents": amount_cents,
        "direction": direction,
        "impact": str(impact),
        "tabs": tabs,
        "source_url": _safe_source_url(
            _control_value(item, "source_url", "url", default=row.get("source_url") or row.get("clickup_url") or "")
        ),
        "quick_actions": list(_control_value(item, "quick_actions", default=[]) or []),
        "inferred": inferred,
    }


def _quick_action_menu(item: dict[str, Any], *, decision_actions_allowed: bool) -> str:
    action_labels = {
        "preview_cash_impact": "Preview cash impact",
        "record_partial_payment": "Record partial payment",
        "split_into_installments": "Split into installments",
        "defer_or_change_date": "Defer / change date",
        "match_bank_transaction": "Match bank transaction",
        "mark_paid": "Mark paid",
        "flag_duplicate": "Flag duplicate",
        "confirm_expected_date": "Confirm expected date",
        "mark_received": "Mark received",
        "match_bank_deposit": "Match bank deposit",
        "change_confidence": "Change confidence",
        "assign_follow_up": "Assign follow-up",
    }
    eligible = [
        action_labels.get(str(action.get("action_type") or ""))
        for action in item["quick_actions"]
        if isinstance(action, Mapping) and action.get("eligible", True)
    ]
    eligible = [label for label in eligible if label]
    if eligible:
        labels = tuple(eligible)
    elif item["direction"] == "inflow":
        labels = (
            "Confirm expected date", "Mark received", "Match bank deposit",
            "Change confidence", "Assign follow-up",
        )
    else:
        labels = (
            "Preview cash impact", "Record partial payment", "Split into installments",
            "Defer / change date", "Match bank transaction", "Mark paid", "Flag duplicate",
        )
    if item["inferred"] and "Explain" not in labels:
        labels = ("Explain", *labels)
    if not decision_actions_allowed:
        # Keep evidence and reconciliation work available, but never offer CFO
        # payment decisions until an explicit trust pass is present.
        labels = tuple(label for label in labels if label in {
            "Explain", "Preview cash impact", "Match bank transaction",
            "Match bank deposit", "Flag duplicate", "Assign follow-up",
        })
    buttons = "".join(
        f'<button type="button" role="menuitem" data-preview-action="{html.escape(label, quote=True)}" '
        f'data-event-id="{html.escape(item["id"], quote=True)}" '
        f'data-party="{html.escape(item["party"], quote=True)}" '
        f'data-amount="{item["amount_cents"] / 100:.2f}" '
        f'data-direction="{html.escape(item["direction"], quote=True)}" '
        f'data-source-url="{html.escape(item["source_url"], quote=True)}">{html.escape(label)}</button>'
        for label in labels
    )
    source_link = ""
    if item["source_url"]:
        source_label = "Open invoice or ClickUp source" if item["direction"] == "inflow" else "Open ClickUp source"
        source_link = f'<a role="menuitem" href="{html.escape(item["source_url"], quote=True)}">{source_label}</a>'
    return f"""
      <details class="finance-row-menu">
        <summary aria-label="Actions for {html.escape(item['party'], quote=True)}">&hellip;</summary>
        <div class="finance-row-menu__popover" role="menu">{buttons}{source_link}</div>
      </details>"""


def _queue_table_html(
    queue: Any, today: date, *, decision_actions_allowed: bool,
) -> tuple[str, dict[str, int]]:
    items = [_queue_item_data(item, today) for item in _flatten_queue(queue)]
    counts = {key: 0 for key in ("needs-action", "incoming", "payables", "completed", "reconciliation", "recent")}
    rows_html = []
    for item in items:
        if not decision_actions_allowed:
            item["action"] = "Review evidence"
        for tab in item["tabs"]:
            counts[tab] += 1
        rows_html.append(f"""
          <tr data-queue-tabs="{','.join(item['tabs'])}" data-queue-date="{html.escape(item['due_date'], quote=True)}">
            <td><strong>{html.escape(item['action'])}</strong></td>
            <td><div class="queue-vendor">{html.escape(item['party'])}</div><div class="queue-meta">{html.escape(item['meta'])}</div></td>
            <td>{html.escape(item['timing'])}</td>
            <td class="{'amount-in' if item['direction'] == 'inflow' else 'amount-out'}">{'+' if item['direction'] == 'inflow' else '-'}{_money(item['amount_cents'])}</td>
            <td>{html.escape(item['impact'])}</td>
            <td>{_quick_action_menu(item, decision_actions_allowed=decision_actions_allowed)}</td>
          </tr>""")
    return "".join(rows_html), counts


def _normalise_savings_opportunity(item: Any, index: int) -> dict[str, Any]:
    """Adapt the savings engine view model without reproducing its decisions."""
    opportunity_key = str(
        _control_value(item, "opportunity_key", "key", "id", default=f"savings-{index}")
    )
    display_name = str(
        _control_value(
            item, "display_name", "title", "normalized_merchant", "merchant", default="Cost review"
        )
    )
    reason_codes = list(_control_value(item, "reason_codes", default=[]) or [])
    reason = str(
        _control_value(
            item,
            "reason",
            "why",
            "evidence_summary",
            "summary",
            default=", ".join(str(code).replace("_", " ") for code in reason_codes),
        )
        or "Posted activity supports a closer review."
    )
    confidence = str(
        _control_value(item, "data_confidence", "confidence_label", "confidence", default="Unknown")
    ).strip().title()
    raw_freshness = _control_value(
        item, "source_freshness", "freshness_label", "freshness", default="Date unavailable"
    )
    if isinstance(raw_freshness, Mapping):
        freshness_date = raw_freshness.get("as_of_date") or raw_freshness.get("latest_date")
        freshness = f"Bank CSV through {freshness_date}" if freshness_date else "Bank CSV date unavailable"
    else:
        freshness = str(raw_freshness)
    next_expected = _control_value(item, "next_expected_date", "next_charge_date", default=None)
    if isinstance(next_expected, (date, datetime)):
        next_expected = next_expected.strftime("%b %d, %Y")
    elif next_expected:
        try:
            next_expected = date.fromisoformat(str(next_expected)[:10]).strftime("%b %d, %Y")
        except ValueError:
            next_expected = str(next_expected)
    else:
        next_expected = "Not available"

    one_time = _control_value(item, "one_time_potential_cents", "one_time_savings_cents", default=None)
    monthly = _control_value(item, "monthly_potential_cents", "monthly_savings_cents", default=None)
    annual = _control_value(
        item, "annual_gross_potential_cents", "annualized_savings_cents", "annual_potential_cents", default=None
    )
    observed_90d = _control_value(
        item,
        "observed_90d_potential_cents",
        "observed_90d_cents",
        "fee_90d_potential_cents",
        default=None,
    )
    if monthly is not None:
        potential = f"{_money(_cents(monthly))}/month"
        horizon = "monthly"
    elif observed_90d is not None:
        potential = f"{_money(_cents(observed_90d))}/90 days"
        horizon = "90-day"
    elif one_time is not None:
        potential = f"{_money(_cents(one_time))} one-time"
        horizon = "one-time"
    elif annual is not None:
        potential = f"{_money(_cents(annual))}/year"
        horizon = "annual"
    else:
        potential = "Amount under review"
        horizon = "unknown"

    evidence = list(_control_value(item, "evidence", "evidence_rows", "transactions", default=[]) or [])
    evidence_lines: list[str] = []
    for fact in evidence:
        if isinstance(fact, Mapping):
            fact_date = str(fact.get("date") or fact.get("posted_date") or "Date unavailable")[:10]
            fact_amount = fact.get("amount_cents")
            fact_label = str(fact.get("description") or fact.get("label") or "Posted outflow")
            amount_label = f" {_money(_cents(fact_amount), exact=True)}" if fact_amount is not None else ""
            evidence_lines.append(f"{fact_date}{amount_label} - {fact_label}")
        else:
            evidence_lines.append(str(fact))
    if not evidence_lines:
        dates = list(_control_value(item, "evidence_dates", default=[]) or [])
        amounts = list(_control_value(item, "evidence_amounts_cents", default=[]) or [])
        for fact_index, fact_date in enumerate(dates):
            amount = amounts[fact_index] if fact_index < len(amounts) else None
            amount_label = f" {_money(_cents(amount), exact=True)}" if amount is not None else ""
            evidence_lines.append(f"{str(fact_date)[:10]}{amount_label}")

    limitations = _control_value(item, "limitations", default=[])
    if isinstance(limitations, str):
        limitation_text = limitations
    else:
        limitation_text = "; ".join(str(value) for value in limitations or [])
    raw_calculation = _control_value(
        item,
        "calculation",
        "calculation_basis",
        "formula_explanation",
        default="Potential supplied by the deterministic savings engine from the posted evidence shown.",
    )
    if isinstance(raw_calculation, Mapping):
        calculation_parts = [f"Rule {raw_calculation.get('formula_id') or 'deterministic savings'}"]
        baseline = _control_value(item, "baseline_amount_cents", default=None)
        current = _control_value(item, "current_amount_cents", default=None)
        if baseline is not None:
            calculation_parts.append(f"baseline {_money(_cents(baseline), exact=True)}")
        if current is not None:
            calculation_parts.append(f"current {_money(_cents(current), exact=True)}")
        calculation = "; ".join(calculation_parts) + "."
    else:
        calculation = str(raw_calculation)
    cash_effect = _control_value(
        item, "scenario_28d_floor_improvement_cents", "cash_impact_cents", default=None
    )
    source_urls = list(_control_value(item, "source_urls", default=[]) or [])
    source_url = _safe_source_url(
        _control_value(item, "source_url", "open_source_url", default=source_urls[0] if source_urls else "")
    )
    return {
        "key": opportunity_key,
        "opportunity_key": opportunity_key,
        "evidence_hash": str(_control_value(item, "evidence_hash", default="")),
        "display_name": display_name,
        "normalized_merchant": str(_control_value(item, "normalized_merchant", default="")),
        "cadence": str(_control_value(item, "cadence", default="")),
        "baseline_amount_cents": _control_value(item, "baseline_amount_cents", default=None),
        "monthly_potential_cents": monthly,
        "reason": reason,
        "next_expected": str(next_expected),
        "potential": potential,
        "horizon": horizon,
        "one_time_cents": None if one_time is None else _cents(one_time),
        "monthly_cents": None if monthly is None else _cents(monthly),
        "annual_cents": None if annual is None else _cents(annual),
        "observed_90d_cents": None if observed_90d is None else _cents(observed_90d),
        "confidence": confidence,
        "freshness": freshness,
        "evidence": evidence_lines,
        "calculation": calculation,
        "cash_effect_cents": None if cash_effect is None else _cents(cash_effect),
        "limitations": limitation_text or "Contract terms, usage, and replacement costs are not confirmed.",
        "downside": str(
            _control_value(
                item,
                "downside",
                default="The cost may support an active workflow; verify necessity and terms before acting.",
            )
        ),
        "source_url": source_url,
        "protected": bool(_control_value(item, "protected", default=False)),
        "conflicted": bool(_control_value(item, "conflicted", "has_conflict", default=False)),
        "included_in_headline": bool(_control_value(item, "included_in_headline", default=True)),
    }


def _load_savings_renderer_state(
    rows: list[dict[str, Any]],
    *,
    today: date,
    balance_cents: int,
    balance_as_of: str,
    floor_cents: int,
    balance_stale: bool,
) -> dict[str, Any]:
    """Call the optional deterministic engine and isolate its failure to this card."""
    try:
        from sales_support_agent.services.cashflow import savings as savings_engine

        build_savings_opportunities = getattr(
            savings_engine,
            "build_savings_opportunities",
            getattr(savings_engine, "build_savings_view_model", None),
        )
        if build_savings_opportunities is None:
            raise AttributeError("Savings engine has no compatible builder")
    except (ImportError, AttributeError):
        return {"status": "empty", "opportunities": [], "total_count": 0}

    supplied = {
        "rows": rows,
        "events": rows,
        "cash_events": rows,
        "as_of": today,
        "today": today,
        "balance_cents": balance_cents,
        "balance_as_of": balance_as_of,
        "cash_floor_cents": floor_cents,
        "floor_cents": floor_cents,
        "source_freshness": balance_as_of,
    }
    try:
        signature = inspect.signature(build_savings_opportunities)
        parameters = signature.parameters
        kwargs = {name: supplied[name] for name in parameters if name in supplied}
        positional: list[Any] = []
        required_positional = [
            parameter
            for parameter in parameters.values()
            if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
            and parameter.default is parameter.empty
            and parameter.name not in kwargs
        ]
        if required_positional:
            positional.append(rows)
        result = build_savings_opportunities(*positional, **kwargs)
        try:
            from sales_support_agent.services.cashflow.savings_reviews import (
                load_savings_reviews,
                merge_savings_reviews,
            )
            result = merge_savings_reviews(
                result, load_savings_reviews(), events=rows, as_of=today
            )
        except RuntimeError:
            # The deterministic card also runs in isolated render/test contexts
            # before the shared Finance database has been initialized.
            pass
    except Exception:
        return {"status": "error", "opportunities": [], "total_count": 0}

    raw_items = _control_value(result, "opportunities", "items", "rows", default=result)
    if isinstance(raw_items, Mapping) or raw_items is None:
        raw_items = []
    try:
        iterable = list(raw_items)
    except TypeError:
        iterable = []
    normalised = [
        _normalise_savings_opportunity(item, index)
        for index, item in enumerate(iterable)
        if not bool(_control_value(item, "protected", default=False))
        and not bool(_control_value(item, "conflicted", "has_conflict", default=False))
    ]
    headline = _control_value(result, "headline", default={})
    total_count = _cents(
        _control_value(
            result,
            "total_count",
            "count",
            default=_control_value(headline, "opportunity_count", default=len(normalised)),
        ),
        len(normalised),
    )
    status = str(_control_value(result, "status", "state", default="ready" if normalised else "empty"))
    status = status.strip().lower().replace("-", "_")
    if status not in {"ready", "empty", "loading", "stale", "insufficient_history", "error"}:
        status = "ready" if normalised else "empty"
    if balance_stale and normalised and status == "ready":
        status = "stale"
    return {
        "status": status,
        "opportunities": normalised[:10],
        "total_count": total_count,
        "headline": dict(headline) if isinstance(headline, Mapping) else {},
    }


def _savings_release_mode() -> str:
    """Run conservative savings review by default; operators can still disable it."""
    mode = os.getenv("FINANCE_SAVINGS_MODE", "live").strip().lower()
    return mode if mode in {"off", "shadow", "live"} else "live"


def _savings_section_html(
    savings: Mapping[str, Any], *, release_mode: str = "live"
) -> tuple[str, dict[str, Any]]:
    status = str(savings.get("status") or "empty")
    opportunities = list(savings.get("opportunities") or [])
    total_count = max(len(opportunities), _cents(savings.get("total_count"), len(opportunities)))
    headline = savings.get("headline") if isinstance(savings.get("headline"), Mapping) else {}
    payloads: dict[str, Any] = {}

    state_copy = ""
    if release_mode != "live":
        message = (
            "Savings checks are disabled."
            if release_mode == "off"
            else "Savings checks are validating in shadow mode."
        )
        detail = (
            "Cash control remains available."
            if release_mode == "off"
            else "No opportunity will be shown or applied until the production evidence gate passes."
        )
        state_copy = f"""
          <div class="finance-savings-state" role="status">
            <strong>{html.escape(message)}</strong><p>{html.escape(detail)}</p>
          </div>"""
        opportunities = []
        total_count = 0
    elif status == "loading":
        state_copy = """
          <div class="finance-savings-state" role="status">
            <div class="finance-savings-skeleton" aria-hidden="true"><i></i><i></i><i></i></div>
            <strong>Finding savings opportunities.</strong><p>Checking posted costs and source evidence.</p>
          </div>"""
    elif status == "error":
        state_copy = """
          <div class="finance-savings-state is-error" role="alert">
            <strong>Savings review is unavailable.</strong><p>Cash control remains current.</p>
            <div><button type="button" class="btn btn-secondary btn-sm" data-retry-savings>Retry</button><button type="button" class="btn btn-secondary btn-sm" data-open-modal="finance-update-modal">Update money</button></div>
          </div>"""
    elif status == "insufficient_history":
        state_copy = """
          <div class="finance-savings-state">
            <strong>More history is needed.</strong><p>Upload at least 90 days with three comparable charges.</p>
            <button type="button" class="btn btn-secondary btn-sm" data-open-modal="finance-update-modal">Update money</button>
          </div>"""
    elif status == "stale" and not opportunities:
        state_copy = """
          <div class="finance-savings-state is-stale">
            <strong>Savings estimates need current bank data.</strong><p>Refresh sources before reviewing potential or cash impact.</p>
            <button type="button" class="btn btn-secondary btn-sm" data-open-modal="finance-update-modal">Refresh sources</button>
          </div>"""
    elif not opportunities:
        state_copy = """
          <div class="finance-savings-state">
            <strong>No evidence-backed savings opportunities need review.</strong><p>Potential savings stay empty until posted activity supports them.</p>
            <button type="button" class="btn btn-secondary btn-sm" data-open-modal="finance-update-modal">Update money</button>
          </div>"""
    else:
        headline_items = [item for item in opportunities if item.get("included_in_headline", True)]
        monthly_total = _cents(
            headline.get("recurring_monthly_potential_cents"),
            sum(item["monthly_cents"] or 0 for item in headline_items),
        )
        one_time_total = _cents(
            headline.get("one_time_potential_cents"),
            sum(item["one_time_cents"] or 0 for item in headline_items),
        )
        observed_total = _cents(
            headline.get("fee_90d_potential_cents"),
            sum(item["observed_90d_cents"] or 0 for item in headline_items),
        )
        annual_only_total = sum(
            item["annual_cents"] or 0 for item in headline_items if item["monthly_cents"] is None
        )
        summaries = []
        if monthly_total:
            summaries.append(f"Up to {_money(monthly_total)}/month recurring")
        if one_time_total:
            summaries.append(f"{_money(one_time_total)} one-time")
        if observed_total:
            summaries.append(f"{_money(observed_total)} observed in 90 days")
        if annual_only_total:
            summaries.append(f"{_money(annual_only_total)}/year")
        summary = " <span aria-hidden=\"true\">&middot;</span> ".join(html.escape(value) for value in summaries)
        if not summary:
            summary = "Potential amounts require review"
        rows_html = []
        for index, item in enumerate(opportunities):
            row_hidden = " hidden" if index >= 3 else ""
            rows_html.append(f"""
              <tr data-savings-extra="{'true' if index >= 3 else 'false'}"{row_hidden}>
                <td><strong>{html.escape(item['display_name'])}</strong></td>
                <td>{html.escape(item['reason'])}</td>
                <td>{html.escape(item['next_expected'])}</td>
                <td><strong>{html.escape(item['potential'])}</strong><small>Potential &middot; not realized</small></td>
                <td><span class="badge {'badge-ok' if item['confidence'].lower() == 'high' else 'badge-warning'}">{html.escape(item['confidence'])}</span><small>{html.escape(item['freshness'])}</small></td>
                <td><button type="button" class="btn btn-secondary btn-sm" data-savings-review="{html.escape(item['key'], quote=True)}">Review</button></td>
              </tr>""")
            cash_effect = (
                "Unavailable until cash is current."
                if status == "stale"
                else (
                    f"The 28-day stress minimum could improve by up to {_money(item['cash_effect_cents'])}. "
                    "This scenario is not applied to the Finance forecast."
                    if item["cash_effect_cents"] is not None and item["cash_effect_cents"] > 0
                    else "No 28-day stress-path improvement is expected at the current charge date. "
                    "This opportunity is not applied to the Finance forecast."
                    if item["cash_effect_cents"] == 0
                    else "Cash impact is not available. This opportunity is not applied to the Finance forecast."
                )
            )
            evidence_text = "; ".join(item["evidence"]) or "No transaction detail was supplied."
            payloads[item["key"]] = {
                "eyebrow": "Savings review",
                "title": item["display_name"],
                "why": item["reason"],
                "facts": [
                    ["Potential", f"{item['potential']} - not yet realized"],
                    ["Confidence", f"{item['confidence']} - {item['freshness']}"],
                    ["Evidence", evidence_text],
                    ["Calculation", item["calculation"]],
                    ["Cash effect", cash_effect],
                    ["Limitations", item["limitations"]],
                    ["Downside", item["downside"]],
                ],
                "sourceUrl": item["source_url"],
                "sourceAction": "" if item["source_url"] else "update-money",
                "sourceLabel": "Open source" if item["source_url"] else "Open bank source",
                "savings": {
                    "opportunity_key": item.get("opportunity_key") or item["key"],
                    "evidence_hash": item.get("evidence_hash") or item["key"],
                    "display_name": item["display_name"],
                    "normalized_merchant": item.get("normalized_merchant") or item["display_name"],
                    "cadence": item.get("cadence") or "unknown",
                    "monthly_potential_cents": item.get("monthly_potential_cents") or item["monthly_cents"],
                    "baseline_amount_cents": item.get("baseline_amount_cents"),
                    "realization_ready": bool(item.get("realization_ready")),
                    "reason": item["reason"],
                    "limitations": item["limitations"],
                    "evidence_dates": item["evidence"],
                },
                "note": "Potential only. Finance records review decisions but never cancels a service or changes the forecast.",
            }
        stale_notice = (
            '<div class="finance-savings-notice"><strong>Estimates are stale.</strong> Cash impact is unavailable until sources are refreshed.</div>'
            if status == "stale" else ""
        )
        expansion = ""
        if len(opportunities) > 3:
            expansion_label = f"Show {len(opportunities)} of {total_count}" if total_count > len(opportunities) else f"Show all {len(opportunities)}"
            expansion = f'<button type="button" class="finance-text-action finance-savings-expand" data-expand-savings aria-expanded="false">{html.escape(expansion_label)}</button>'
        truncated_note = (
            f'<span>Showing the strongest 10 of {total_count} opportunities.</span>'
            if total_count > len(opportunities) else ""
        )
        state_copy = f"""
          {stale_notice}
          <div class="finance-savings-summary"><strong>{total_count} costs worth review</strong><span>{summary}</span></div>
          <div class="finance-savings-scroll">
            <table class="finance-savings-table">
              <thead><tr><th>Opportunity</th><th>Evidence</th><th>Next charge</th><th>Potential savings</th><th>Confidence</th><th><span class="sr-only">Review</span></th></tr></thead>
              <tbody>{''.join(rows_html)}</tbody>
            </table>
          </div>
          <div class="finance-savings-footer"><span>Estimates are potential until later posted activity verifies a reduction.</span>{truncated_note}{expansion}</div>"""

    section = f"""
      <section class="card finance-savings" id="finance-savings" aria-labelledby="finance-savings-title" aria-busy="{'true' if status == 'loading' else 'false'}" data-savings-state="{html.escape(status, quote=True)}">
        <div class="section-head finance-savings__head">
          <div><p class="finance-eyebrow">Smart savings</p><h2 id="finance-savings-title">Savings opportunities</h2></div>
          <span class="finance-savings__label smart-only">Potential only</span>
        </div>
        <div class="finance-savings__off smart-off-only"><strong>Turn on Smart mode</strong><span>Review evidence-backed cost savings without changing the forecast.</span></div>
        <div class="finance-savings__smart smart-only">{state_copy}</div>
      </section>"""
    return section, payloads


def _smart_cfo_section_html(analysis: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    """Render LLM advice separately from deterministic savings evidence."""
    status = str(analysis.get("status") or "empty")
    recommendations = list(analysis.get("recommendations") or [])
    payloads: dict[str, Any] = {}
    if status == "not_configured":
        content = '<div class="finance-savings-state is-error"><strong>Smart review needs an Anthropic key.</strong><p>Cash and source calculations still work. Add ANTHROPIC_API_KEY to the production service, then run the review.</p></div>'
    elif status == "ready" and not recommendations:
        content = f'''<div class="finance-savings-state"><strong>Smart review found no eligible actions.</strong><p>{html.escape(str(analysis.get("summary") or "The current ledger did not support a recommendation."))}</p><span>Reconciled ledger rollup · {html.escape(str(analysis.get("record_count") or 0))} records</span><form method="post" action="/admin/finances/smart-review"><button class="btn btn-secondary btn-sm" type="submit">Re-run on current ledger</button></form></div>'''
    elif not recommendations:
        content = '''<div class="finance-savings-state"><strong>Run a Smart review when you want a CFO read.</strong><p>It evaluates the complete reconciled ledger rollup, not a sample, and never changes cash or source records.</p><form method="post" action="/admin/finances/smart-review"><button class="btn btn-primary btn-sm" type="submit">Run Smart review</button></form></div>'''
    else:
        rows = []
        for index, item in enumerate(recommendations):
            key = f"cfo-{index}"
            payloads[key] = {
                "eyebrow": f"Smart CFO · {item.get('category', 'review').replace('_', ' ')}",
                "title": item.get("title", "Review recommendation"),
                "why": item.get("reason", "Review the transaction evidence."),
                "facts": [
                    ["Priority", item.get("priority", "medium").title()],
                    ["Next action", item.get("next_action", "Review the source evidence.")],
                    ["Question", item.get("operator_question", "What needs to be confirmed?")],
                    ["Evidence", ", ".join(item.get("record_ids") or [])],
                ],
                "sourceUrl": "", "sourceAction": "", "sourceLabel": "", "note": "LLM advice only. It does not modify cash, forecast, bank data, or source records.",
            }
            rows.append(f'''<tr><td><span class="badge badge-warning">{html.escape(str(item.get("priority") or "medium").title())}</span></td><td><strong>{html.escape(str(item.get("title") or "Review recommendation"))}</strong><small>{html.escape(str(item.get("reason") or ""))}</small></td><td>{html.escape(str(item.get("next_action") or ""))}</td><td><button type="button" class="btn btn-secondary btn-sm" data-drawer-review="{key}">Review</button></td></tr>''')
        created = html.escape(str(analysis.get("created_at") or "just now")[:16].replace("T", " "))
        content = f'''<div class="finance-savings-summary"><strong>{len(recommendations)} evidence-backed actions</strong><span>Reconciled ledger rollup · {html.escape(str(analysis.get("record_count") or 0))} records · {created}</span></div><p class="finance-smart-cfo__summary">{html.escape(str(analysis.get("summary") or ""))}</p><table class="finance-savings-table finance-smart-cfo__table"><thead><tr><th>Priority</th><th>Recommendation</th><th>Next action</th><th></th></tr></thead><tbody>{''.join(rows)}</tbody></table><div class="finance-savings-footer"><span>Advice is cached until Finance records change.</span><form method="post" action="/admin/finances/smart-review"><button class="finance-text-action" type="submit">Re-run on current ledger</button></form></div>'''
    return f'''<section class="card finance-savings finance-smart-cfo smart-only" id="finance-smart-cfo" aria-labelledby="finance-smart-cfo-title"><div class="section-head finance-savings__head"><div><p class="finance-eyebrow">Smart CFO</p><h2 id="finance-smart-cfo-title">What deserves attention</h2></div><span class="finance-savings__label">Advice only</span></div>{content}</section>''', payloads


def _source_readiness_html(
    source_statuses: list[dict[str, str]],
    reconciliation_shadow: Mapping[str, Any] | None = None,
) -> str:
    ready_statuses = {"ready", "current", "connected", "healthy", "synced", "ok"}
    loading_statuses = {"loading", "syncing", "pending"}
    rows = []
    for source in source_statuses:
        status = source["status"]
        if status in ready_statuses:
            state_class, state_label = "is-ready", "Ready"
        elif status in loading_statuses:
            state_class, state_label = "is-loading", "Loading"
        elif status in {"unknown", "unconfigured", "not_configured", "disconnected"}:
            state_class, state_label = "is-unknown", (
                "Disconnected" if status == "disconnected" else "Not reported"
            )
        else:
            state_class, state_label = "is-blocked", "Needs attention"
        rows.append(f"""
          <article class="finance-source-readiness__item {state_class}" data-finance-source="{html.escape(source['key'], quote=True)}">
            <span class="finance-source-readiness__mark" aria-hidden="true"></span>
            <div><strong>{html.escape(source['label'])}</strong><small>{html.escape(source['detail'])}</small></div>
            <span>{state_label}</span>
          </article>""")
    shadow = reconciliation_shadow or {}
    review_count = max(0, _cents(shadow.get("supersession_review_count")))
    if str(shadow.get("mode") or "") == "shadow":
        if review_count:
            state_class, state_label = "is-blocked", "Review"
            excluded = max(0, _cents(shadow.get("candidate_superseded_count")))
            detail = (
                f"{review_count} recurring occurrence(s) need settlement evidence. "
                f"{excluded} historical occurrence(s) are excluded from forward cash."
            )
        else:
            state_class, state_label = "is-ready", "Clear"
            detail = "No recurring ClickUp history needs supersession review."
        rows.append(f"""
          <article class="finance-source-readiness__item {state_class}" data-finance-source="reconciliation-shadow">
            <span class="finance-source-readiness__mark" aria-hidden="true"></span>
            <div><strong>Reconciliation</strong><small>{html.escape(detail)}</small></div>
            <span>{state_label}</span>
          </article>""")
    return "".join(rows)


def _reconciliation_shadow_html(shadow: Mapping[str, Any] | None) -> str:
    """Render the small, review-only recurrence delta in Update Money."""
    shadow = shadow or {}
    if str(shadow.get("mode") or "") != "shadow":
        return ""
    review_records = list(shadow.get("review_records") or [])
    records = review_records[:8]
    review_count = max(0, _cents(shadow.get("supersession_review_count")))
    if not records:
        return """
          <section class="finance-reconciliation-review is-clear" aria-label="Recurring ClickUp reconciliation">
            <strong>Recurring ClickUp review</strong>
            <span>No historical recurring occurrences need review.</span>
          </section>"""
    rows = "".join(
        f"<tr><td>{html.escape(str(candidate.get('name') or 'Unnamed occurrence'))}</td>"
        f"<td>{html.escape(str(candidate.get('due_date') or 'date unavailable'))}</td>"
        f"<td>{html.escape(str(candidate.get('later_due_date') or 'date unavailable'))}</td>"
        f"<td>{html.escape('Review continuity' if candidate.get('candidate_state') == 'recurrence_continuity_review' else 'Review gap')}</td>"
        f"<td>{_money(_cents(candidate.get('amount_cents')))}</td></tr>"
        for candidate in records
    )
    suffix = (
        f" Showing the first {len(records)}."
        if review_count > len(records) else ""
    )
    return f"""
      <section class="finance-reconciliation-review" aria-labelledby="finance-reconciliation-title">
        <div><strong id="finance-reconciliation-title">Recurring ClickUp review</strong>
          <span>{review_count} recurrence(s) need settlement evidence. Historical occurrences are excluded from forward cash, but are not marked paid.{suffix}</span></div>
        <table><thead><tr><th>Occurrence</th><th>Older due</th><th>Newer due</th><th>Assessment</th><th>Amount</th></tr></thead>
        <tbody>{rows}</tbody></table>
        <small>Match a bank transaction or correct the source before recording a payment. Historical schedule residue never reserves forward cash.</small>
      </section>"""


def _income_review_drawer_html(income_projection: Mapping[str, Any]) -> str:
    status = str(income_projection.get("status") or "ready")
    patterns = list(income_projection.get("patterns") or [])
    if status in {"loading", "pending"}:
        content = """
          <div class="finance-income-review-state" role="status" aria-busy="true">
            <div class="finance-savings-skeleton" aria-hidden="true"><i></i><i></i><i></i></div>
            <strong>Income pattern review is loading.</strong>
            <p>Existing Confirmed and Expected totals remain unchanged.</p>
          </div>"""
    elif status in {"error", "unavailable", "failed"}:
        content = """
          <div class="finance-income-review-state is-error" role="alert">
            <strong>Income pattern review is unavailable.</strong>
            <p>No inferred income was promoted. Refresh finance sources and try again.</p>
          </div>"""
    elif not patterns:
        content = """
          <div class="finance-income-review-state">
            <strong>No income patterns need review.</strong>
            <p>Recurring deposits appear here only when posted bank history supports a conservative pattern.</p>
          </div>"""
    else:
        pattern_cards = []
        for pattern in patterns:
            key = pattern["key"].lower()
            valid_key = len(key) == 16 and all(character in "0123456789abcdef" for character in key)
            dates = ", ".join(pattern["dates"][-4:]) or "Dates unavailable"
            decision_label = pattern["decision"].replace("_", " ").title()
            if valid_key:
                action = f"/admin/finances/income-patterns/{key}/decision"
                forms = "".join(
                    f'<form method="post" action="{action}"><input type="hidden" name="decision" value="{value}"><button type="submit" class="btn {button_class} btn-sm">{label}</button></form>'
                    for value, label, button_class in (
                        ("track_expected", "Track as expected", "btn-primary"),
                        ("one_time", "One-time", "btn-secondary"),
                        ("exclude", "Exclude", "btn-secondary"),
                    )
                )
            else:
                forms = '<p class="finance-income-pattern__unavailable">Decision controls unavailable for this pattern.</p>'
            pattern_cards.append(f"""
              <article class="finance-income-pattern">
                <div class="finance-income-pattern__head">
                  <div><p class="finance-eyebrow">Inferred deposit</p><h3>{html.escape(pattern['label'])}</h3></div>
                  <span class="badge badge-warning">{html.escape(decision_label)}</span>
                </div>
                <dl>
                  <div><dt>Posting dates</dt><dd>{html.escape(dates)}</dd></div>
                  <div><dt>Cadence</dt><dd>{html.escape(pattern['cadence'])}</dd></div>
                  <div><dt>Conservative amount</dt><dd>{_money(pattern['amount_cents'], exact=True)}</dd></div>
                  <div><dt>Probability</dt><dd>{pattern['probability']}%</dd></div>
                </dl>
                <div class="finance-income-pattern__actions" aria-label="Decision for {html.escape(pattern['label'], quote=True)}">{forms}</div>
              </article>""")
        content = "".join(pattern_cards)
    return f"""
      <aside id="finance-income-review-drawer" class="finance-drawer finance-income-drawer" aria-hidden="true" aria-labelledby="finance-income-review-title">
        <div class="finance-drawer__scrim" data-close-income-review aria-hidden="true"></div>
        <div class="finance-drawer__panel" role="dialog" aria-modal="true" tabindex="-1">
          <div class="finance-drawer__head"><p class="finance-eyebrow">Income review</p><button type="button" class="finance-icon-button" data-close-income-review aria-label="Close income review">&times;</button></div>
          <h2 id="finance-income-review-title">Review inferred income patterns</h2>
          <p class="finance-drawer__why"><strong>Needs review:</strong> Bank history can suggest Expected income, but it never creates Confirmed income.</p>
          <div class="finance-income-patterns">{content}</div>
          <p class="finance-preview-note">Decisions affect forecast classification only. They do not move money or create a receivable.</p>
        </div>
      </aside>"""


def _chart_payload(forecast: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    points = _control_value(forecast, "points", "days", default=None)
    if points:
        labels = [str(_control_value(point, "label", "date", default=""))[:10] for point in points]
        def point_series(*names: str) -> list[Any]:
            return [_control_value(point, *names, default=None) for point in points]
        raw = {
            "labels": labels,
            "actual": point_series("actual_cents", "actual_balance_cents"),
            "committed": point_series("committed_cents", "committed_balance_cents"),
            "expected": point_series("expected_cents", "expected_balance_cents"),
            "stress": point_series("stress_cents", "stress_balance_cents"),
            "floor": _control_value(forecast, "floor_cents", default=fallback["floor"]),
        }
    else:
        raw = {
            "labels": _control_value(forecast, "labels", "dates", default=fallback["labels"]),
            "actual": _control_value(forecast, "actual", "actual_cents", default=fallback["actual"]),
            "committed": _control_value(forecast, "committed", "committed_cents", default=fallback["committed"]),
            "expected": _control_value(forecast, "expected", "expected_cents", default=fallback["expected"]),
            "stress": _control_value(forecast, "stress", "stress_cents", default=fallback["stress"]),
            "floor": _control_value(forecast, "floor_cents", "floor", default=fallback["floor"]),
        }
    return {
        "labels": list(raw["labels"] or []),
        "actual": [None if value is None else _cents(value) / 100 for value in raw["actual"] or []],
        "committed": [None if value is None else _cents(value) / 100 for value in raw["committed"] or []],
        "expected": [None if value is None else _cents(value) / 100 for value in raw["expected"] or []],
        "stress": [None if value is None else _cents(value) / 100 for value in raw["stress"] or []],
        "floor": _cents(raw["floor"]) / 100,
    }


def _load_settlement_context(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | None]:
    """Attach durable installments and load append-only settlement evidence."""
    try:
        from sqlalchemy import text
        from sales_support_agent.models.database import get_engine

        with get_engine().connect() as connection:
            allocations = [
                dict(row._mapping)
                for row in connection.execute(text("SELECT * FROM settlement_allocations")).fetchall()
            ]
            installments = [
                dict(row._mapping)
                for row in connection.execute(text("SELECT * FROM payment_installments")).fetchall()
            ]
    except Exception:
        unavailable = []
        for source_row in rows:
            row = dict(source_row)
            if str(row.get("record_kind") or "obligation") != "transaction":
                row["settlement_evidence_available"] = False
            unavailable.append(row)
        return unavailable, None

    by_obligation: dict[str, list[dict[str, Any]]] = {}
    for installment in installments:
        by_obligation.setdefault(str(installment["obligation_event_id"]), []).append(installment)
    enriched = []
    for source_row in rows:
        row = dict(source_row)
        row["payment_installments"] = by_obligation.get(str(row.get("id") or ""), [])
        enriched.append(row)
    return enriched, allocations


def _load_finance_control_inputs(settings: Any = None) -> tuple[Any, Any]:
    """Load persisted operator decisions and connection readiness for the control engine."""
    from sales_support_agent.services.cashflow.income_decisions import (
        load_finance_source_connections,
        load_income_pattern_decisions,
    )

    if settings is None:
        from sales_support_agent.config import load_settings

        settings = load_settings()
    return load_income_pattern_decisions(), load_finance_source_connections(settings)


async def render_cashflow_overview_page(
    *, flash: str = "", inline_result_html: str = "", settings: Any = None
) -> str:
    rows = list_obligations(limit=2000)
    rows, settlement_annotations = _load_settlement_context(rows)
    balance_cents, balance_as_of, balance_source = _resolve_current_balance(rows)
    try:
        income_decisions, source_connections = _load_finance_control_inputs(settings)
    except (ImportError, AttributeError):
        # Supports a rolling deploy while the integration helpers arrive.
        income_decisions, source_connections = None, None
    except Exception:
        income_decisions, source_connections = None, {"status": "error"}
    today = date.today()
    control, fallback, control_error = _build_renderer_state(
        rows,
        balance_cents,
        balance_as_of,
        today,
        settlement_annotations,
        income_decisions,
        source_connections,
    )
    state = _normalise_renderer_state(control, fallback)
    cash = state["cash"]
    income_projection = state["income_projection"]
    csv_trend_expected_cents = min(
        cash["incoming_expected_cents"],
        income_projection["csv_trend_expected_cents"],
    )
    dated_expected_cents = max(
        0, cash["incoming_expected_cents"] - csv_trend_expected_cents
    )
    if csv_trend_expected_cents and dated_expected_cents:
        incoming_expected_note = (
            f"Expected: {_money(cash['incoming_expected_cents'])} &middot; "
            f"{_money(csv_trend_expected_cents)} CSV trend &middot; "
            f"{_money(dated_expected_cents)} dated receivables"
        )
    elif csv_trend_expected_cents:
        incoming_expected_note = (
            f"Expected: {_money(cash['incoming_expected_cents'])} &middot; "
            "eligible CSV recurring deposits"
        )
    elif cash["incoming_expected_cents"]:
        incoming_expected_note = (
            f"Expected: {_money(cash['incoming_expected_cents'])} &middot; "
            "unconfirmed dated receivables"
        )
    else:
        incoming_expected_note = "Expected: $0 &middot; no eligible projection"

    projection_unconfigured = income_projection["unconfigured"]
    balance_stale = False
    if balance_as_of:
        try:
            balance_stale = (today - date.fromisoformat(balance_as_of[:10])).days > 3
        except ValueError:
            balance_stale = True
    cash_usable = bool(
        cash["balance_available"] and balance_as_of and not balance_stale
    )
    trust_gate = state["trust_gate"]
    trust_ready = trust_gate["ready"]
    # CFO recommendations are opt-in: any absent, errored, or failed contract
    # leaves evidence visible but disables decision controls.
    decision_actions_allowed = trust_ready is True and not control_error
    trust_blocking = not decision_actions_allowed
    data_quality = state["data_quality"]
    if trust_blocking:
        trust_issue = trust_gate["issues"][0] if trust_gate["issues"] else trust_gate["summary"]
        state["brief"] = {
            "happening": "Cash decisions are paused while finance source readiness is restored.",
            "broken": trust_issue,
            "next": trust_gate["next_action"],
        }
        smart_broken_html = (
            '<strong class="finance-readiness-title">Trust check failed</strong>'
            f'<p class="finance-readiness-copy">{html.escape(trust_issue)}</p>'
        )
        smart_next_html = f"<p>{html.escape(trust_gate['next_action'])}</p>"
    elif projection_unconfigured and cash_usable:
        history_count = income_projection["posted_inflow_count"]
        history_detail = (
            f"{history_count} comparable posted bank inflows are actual history only."
            if history_count
            else "Posted bank inflows are actual history only."
        )
        smart_broken_html = (
            '<strong class="finance-readiness-title">Income forecast setup incomplete</strong>'
            f'<p class="finance-readiness-copy">Forecast income not configured. '
            f'{html.escape(history_detail)} Eligible recurring deposits are not in Expected yet.</p>'
        )
        smart_next_html = (
            "<p>Review the CSV income projection first. Confirm eligible recurring deposits "
            "before auditing lower-priority queue cleanup.</p>"
        )
    else:
        smart_broken_html = f"<p>{html.escape(state['brief']['broken'])}</p>"
        smart_next_html = f"<p>{html.escape(state['brief']['next'])}</p>"

    source_label = "Bank CSV" if balance_source == "csv" else "QBO bank" if balance_source else "Bank source"
    if cash["balance_available"]:
        balance_display = _money(cash["cash_on_hand_cents"], exact=True)
        balance_note = f"{source_label} &middot; {html.escape(balance_as_of or 'date unavailable')}"
    else:
        balance_display = "Needs update"
        balance_note = "Upload the latest bank CSV"

    gap = cash["funding_gap_cents"]
    fourth_label = "Funding gap" if gap else "Safe to commit"
    calculation_unavailable = control_error or not cash["balance_available"]
    fourth_value = "Unavailable" if calculation_unavailable else _money(gap or cash["safe_to_commit_cents"])
    fourth_note = "Cash floor unavailable" if control_error else f"Floor: {_money(cash['floor_cents'])}"
    cash_floor_display = "Unavailable" if control_error else _money(cash["floor_cents"])
    cash_floor_input = "" if control_error else f'{cash["floor_cents"] / 100:.2f}'
    quality_badge = (
        "Low confidence"
        if control_error or settlement_annotations is None or balance_stale or not cash["balance_available"]
        else "Current"
    )
    quality_class = "badge-warning" if quality_badge == "Low confidence" else "badge-ok"
    reconciliation_shadow = state.get("reconciliation_shadow") or {}
    source_readiness_html = _source_readiness_html(
        state["source_status"], reconciliation_shadow
    )
    reconciliation_shadow_html = _reconciliation_shadow_html(reconciliation_shadow)
    if decision_actions_allowed:
        trust_state, trust_title = "ready", "Ready for cash decisions"
        trust_summary = trust_gate["summary"]
    elif trust_ready is False:
        trust_state, trust_title = "blocked", "Cash decisions paused"
        trust_summary = trust_gate["summary"]
    else:
        trust_state, trust_title = "blocked", "CFO decision support unavailable"
        trust_summary = "Source readiness was not verified. Cash data is visible for review only."
    trust_issues_html = "".join(
        f"<li>{html.escape(issue)}</li>" for issue in trust_gate["issues"]
    )
    trust_next_html = (
        f'<p><strong>Next:</strong> {html.escape(trust_gate["next_action"])}</p>'
        if trust_blocking else ""
    )
    trust_block_html = f"""
      <section class="finance-trust-gate is-{trust_state}" data-trust-ready="{'false' if trust_blocking else 'true' if trust_ready is True else 'unknown'}" aria-labelledby="finance-trust-title">
        <div class="finance-trust-gate__status" aria-hidden="true"></div>
        <div class="finance-trust-gate__body">
          <p class="finance-eyebrow">Decision trust</p>
          <h2 id="finance-trust-title">{trust_title}</h2>
          <p>{html.escape(trust_summary)}</p>
          {f'<ul>{trust_issues_html}</ul>' if trust_issues_html else ''}
          {trust_next_html}
        </div>
        <div class="finance-trust-gate__quality"><span>Data quality</span><strong>{html.escape(data_quality['summary'])}</strong></div>
        <button class="btn {'btn-primary' if trust_blocking else 'btn-secondary'} btn-sm" type="button" data-open-modal="finance-update-modal">{'Resolve source readiness' if trust_blocking else 'Review sources'}</button>
      </section>"""

    savings_release_mode = _savings_release_mode()
    savings = (
        {"status": "error", "opportunities": [], "total_count": 0}
        if control_error
        else _load_savings_renderer_state(
            rows,
            today=today,
            balance_cents=balance_cents,
            balance_as_of=balance_as_of,
            floor_cents=cash["floor_cents"],
            balance_stale=balance_stale or not cash["balance_available"],
        )
    )
    savings_section, savings_payloads = _savings_section_html(
        savings, release_mode=savings_release_mode
    )
    try:
        smart_cfo_section, smart_cfo_payloads = _smart_cfo_section_html(load_smart_cfo_analysis())
    except Exception:
        smart_cfo_section, smart_cfo_payloads = _smart_cfo_section_html({"status": "empty"})

    queue_items = _flatten_queue(state["queue"])
    queue_ids = {str(_control_value(item, "id", "event_id", default="")) for item in queue_items}
    recent_cutoff = today - timedelta(days=30)
    for row in rows:
        due = _row_due_date(row)
        row_id = str(row.get("id") or "")
        if (
            str(row.get("status") or "").lower() in {"posted", "matched", "paid"}
            and due is not None
            and recent_cutoff <= due <= today
            and row_id not in queue_ids
        ):
            queue_items.append(row)
            queue_ids.add(row_id)
    queue_rows_html, counts = _queue_table_html(
        queue_items, today, decision_actions_allowed=decision_actions_allowed,
    )
    empty_hidden = " hidden" if counts["needs-action"] else ""
    queue_table_hidden = "" if queue_rows_html else " hidden"
    income_review_drawer = _income_review_drawer_html(income_projection)

    recommendation = state["recommendation"]
    rec_title = str(_control_value(recommendation, "title", "action", "summary", default=state["brief"]["next"]))
    rec_why = str(_control_value(recommendation, "why", "explanation", default=state["brief"]["broken"]))
    rec_before = _cents(_control_value(recommendation, "before_minimum_cash_cents", "before_cents", default=balance_cents))
    rec_after = _cents(_control_value(recommendation, "after_minimum_cash_cents", "after_cents", default=rec_before))
    rec_depends = str(_control_value(recommendation, "depends_on", "dependencies", default="Confirmed income only."))
    rec_confidence = str(_control_value(recommendation, "confidence", default=quality_badge))
    rec_limitations = str(_control_value(recommendation, "limitations", "confidence_reason", default="Recalculate after source changes."))
    rec_downside = str(_control_value(recommendation, "downside", default="The remaining obligation stays open."))
    rec_action = str(_control_value(recommendation, "action_label", default="Create action preview"))

    if trust_blocking:
        recommendation_payload = {
            "eyebrow": "Source readiness",
            "title": "Restore finance source trust",
            "why": trust_gate["summary"],
            "facts": [
                ["Decision state", "Cash recommendations are paused"],
                ["Data quality", data_quality["summary"]],
                ["Next action", trust_gate["next_action"]],
                ["After refresh", "Recalculate before making any cash decision"],
            ],
            "sourceUrl": "",
            "sourceAction": "update-money",
            "sourceLabel": "Review finance sources",
            "note": "Readiness guidance only. CFO payment advice is unavailable until the trust gate passes.",
        }
        rec_title = recommendation_payload["title"]
        rec_why = recommendation_payload["why"]
    else:
        recommendation_payload = {
            "eyebrow": "Smart recommendation",
            "title": rec_title,
            "why": rec_why,
            "facts": [
                ["Before", f"Minimum stress cash {_money(rec_before)}"],
                ["After", f"Minimum stress cash {_money(rec_after)}"],
                ["Depends on", rec_depends],
                ["Confidence", f"{rec_confidence} - {rec_limitations}"],
                ["Downside", rec_downside],
                ["Next action", rec_action],
            ],
            "sourceUrl": "",
            "sourceAction": "",
            "sourceLabel": "Open source",
            "note": "Review only. No bank payment is initiated.",
        }

    drawer_payloads = {
        "recommendation": recommendation_payload,
        "savings": savings_payloads,
        "smart_cfo": smart_cfo_payloads,
    }
    drawer_json = json.dumps(drawer_payloads, separators=(",", ":")).replace("<", "\\u003c")

    chart_data = _chart_payload(state["forecast"], fallback["forecast"])
    chart_json = json.dumps(chart_data, separators=(",", ":")).replace("<", "\\u003c")
    updated_label = balance_as_of or "balance not loaded"
    inline_result = inline_result_html or ""

    body = f"""
    <main class="finance-control" data-smart-mode="on">
      <div id="finance-page-content" class="finance-control__content">
      <header class="finance-control__header">
        <div>
          <p class="finance-eyebrow">Finance control</p>
          <h1>Cash decisions, in one scan.</h1>
          <p class="page-sub">One page for cash, collections, payments, and the next safest action.</p>
        </div>
        <div class="finance-control__tools">
          <span class="finance-updated">Cash updated {html.escape(updated_label)}</span>
          <label class="finance-smart-toggle">
            <span>Smart mode</span>
            <input id="finance-smart-mode" type="checkbox" checked>
            <span class="finance-smart-toggle__track" aria-hidden="true"></span>
          </label>
          <button class="btn btn-primary" type="button" data-open-modal="finance-update-modal">Update money</button>
        </div>
      </header>

      <section class="finance-source-readiness" aria-label="Finance source readiness">
        <div class="finance-source-readiness__label"><span>Source readiness</span><small>Bank truth, work plan, receivables</small></div>
        {source_readiness_html}
      </section>

      {trust_block_html}

      <section class="finance-cash-strip" aria-label="Cash position">
        <article class="finance-cash-metric{' is-stale' if balance_stale or not cash['balance_available'] else ''}">
          <div class="finance-metric-head"><span>Cash on hand</span><span class="badge {quality_class}">{quality_badge}</span></div>
          <strong>{balance_display}</strong><small>{balance_note}</small>
        </article>
        <article class="finance-cash-metric finance-income-metric">
          <span>Incoming 14 days</span>
          <div class="finance-income-line is-confirmed"><span>Confirmed</span><strong class="amount-in">{_money(cash['incoming_confirmed_cents'])}</strong></div>
          <small>Confirmed &middot; dated receivables</small>
          <div class="finance-income-line"><span>Expected</span><strong>{_money(cash['incoming_expected_cents'])}</strong></div>
          <small class="finance-income-breakdown">{incoming_expected_note}</small>
          <div class="finance-income-line is-review"><span>Needs review</span><strong>{_money(income_projection['needs_review_cents'])}</strong></div>
          <button type="button" class="finance-text-action" data-open-income-review>Review income patterns</button>
        </article>
        <article class="finance-cash-metric">
          <span>Required out 14 days</span><strong class="amount-out">{_money(cash['required_out_cents'])}</strong>
          <small>Required &middot; +{_money(cash['exposure_out_cents'])} exposure</small>
        </article>
        <article class="finance-cash-metric {'is-gap' if gap else 'is-safe'}">
          <span>{fourth_label}</span><strong>{fourth_value}</strong><small>{fourth_note}</small>
        </article>
      </section>

      <section class="finance-smart-brief smart-only" aria-labelledby="smart-brief-title">
        <h2 id="smart-brief-title" class="sr-only">Smart brief</h2>
        <article><span>Happening</span><p>{html.escape(state['brief']['happening'])}</p></article>
        <article class="is-broken" data-income-readiness="{'unconfigured' if projection_unconfigured else 'configured'}"><span>Broken</span>{smart_broken_html}</article>
        <article class="is-next"><span>Next</span>{smart_next_html}<button type="button" class="finance-text-action" data-drawer-review="recommendation">{'Review next action' if trust_blocking else 'Review recommendation'}</button></article>
      </section>

      <section class="card finance-trajectory" aria-labelledby="trajectory-title">
        <div class="section-head finance-trajectory__head">
          <div><p class="finance-eyebrow">28 day control window</p><h2 id="trajectory-title">Cash trajectory</h2></div>
          <div class="finance-chart-legend" aria-label="Chart legend">
            <span class="is-actual">Actual</span><span class="is-committed">Committed</span>
            <span class="is-expected">Expected</span><span class="is-stress">Stress</span>
          </div>
          <button class="finance-icon-button" type="button" aria-label="Cash trajectory options">&hellip;</button>
        </div>
        <p class="finance-trajectory__helper">Expected includes probability-weighted CSV recurring-deposit trends and dated receivables. It is not committed cash.</p>
        <div class="finance-chart-wrap"><canvas id="finance-control-chart" aria-label="Actual, committed, expected, and stress cash paths"></canvas><p id="finance-chart-status">Calculating forecast</p></div>
      </section>

      <section class="card finance-money-queue" id="finance-queue" aria-labelledby="money-queue-title">
        <div class="section-head">
          <div><p class="finance-eyebrow">Operator queue</p><h2 id="money-queue-title">Money queue</h2></div>
          <div class="finance-queue-controls">
            <label class="finance-window-select">Window:<select id="finance-queue-window" aria-label="Queue window"><option value="14">14 days</option><option value="28">28 days</option></select></label>
            <label class="finance-window-select">Rows:<select id="finance-queue-page-size" aria-label="Rows per page"><option value="25" selected>25</option><option value="50">50</option><option value="100">100</option></select></label>
          </div>
        </div>
        <div class="finance-queue-tabs" role="group" aria-label="Money queue filters">
          <button type="button" aria-pressed="true" data-queue-filter="needs-action">Needs action <span>{counts['needs-action']}</span></button>
          <button type="button" aria-pressed="false" data-queue-filter="incoming">Incoming <span>{counts['incoming']}</span></button>
          <button type="button" aria-pressed="false" data-queue-filter="payables">Payables <span>{counts['payables']}</span></button>
          <button type="button" aria-pressed="false" data-queue-filter="completed">Completed <span>{counts['completed']}</span></button>
          <button type="button" aria-pressed="false" data-queue-filter="reconciliation">Reconciliation <span>{counts['reconciliation']}</span></button>
          <button type="button" aria-pressed="false" data-queue-filter="recent">Recent <span>{counts['recent']}</span></button>
        </div>
        <div class="finance-queue-scroll"{queue_table_hidden}>
          <table class="finance-queue-table" aria-describedby="finance-queue-range">
            <thead><tr><th>Action</th><th>Party</th><th>Timing</th><th>Amount</th><th>Cash impact</th><th><span class="sr-only">Actions</span></th></tr></thead>
            <tbody>{queue_rows_html}</tbody>
          </table>
        </div>
        <div id="finance-queue-empty" class="finance-empty-state"{empty_hidden}>
          <strong>No money decisions require attention in the selected window.</strong>
          <p>Update money or add an incoming or payable exception.</p>
          <div><button type="button" class="btn btn-secondary btn-sm" data-open-modal="finance-update-modal">Update money</button><a class="btn btn-secondary btn-sm" href="/admin/finances/ar/new">Add incoming</a><a class="btn btn-secondary btn-sm" href="/admin/finances/ap/new">Add payable</a></div>
        </div>
        <div id="finance-queue-pagination" class="finance-queue-pagination"{queue_table_hidden}>
          <span id="finance-queue-range" aria-live="polite">0 results</span>
          <nav aria-label="Money queue pages">
            <button id="finance-queue-previous" type="button">Previous</button>
            <span id="finance-queue-page-summary" aria-live="polite">Page 1 of 1</span>
            <button id="finance-queue-next" type="button">Next</button>
          </nav>
        </div>
      </section>

      {savings_section}
      {smart_cfo_section}

      <section class="card finance-review-guide" id="finance-review-guide" aria-labelledby="finance-review-guide-title">
        <div class="finance-review-guide__head">
          <div>
            <p class="finance-eyebrow">How to use Finance Control</p>
            <h2 id="finance-review-guide-title">Run the money review in five minutes.</h2>
            <p>Scan each workday. Refresh the sources Monday and Friday, and repeat after a large payment, deposit, or source correction.</p>
          </div>
          <div class="finance-review-guide__cadence" aria-label="Recommended finance review cadence">
            <span><strong>Daily</strong> Scan Broken and Next</span>
            <span><strong>Mon + Fri</strong> Refresh every source</span>
            <span><strong>Money moved</strong> Update the bank CSV</span>
          </div>
        </div>

        <div class="finance-review-guide__steps">
          <article>
            <span>01</span>
            <h3>Update reality</h3>
            <p>Upload the latest bank CSV for current cash and actual history. Eligible recurring deposits can appear only as Expected; the CSV does not create confirmed income. Refresh ClickUp for planned AP/AR dates, priority, and notes; use QBO open invoices for dated receivable balances; and use manual entries only for exceptions.</p>
          </article>
          <article>
            <span>02</span>
            <h3>Read left to right</h3>
            <dl>
              <div><dt>Cash on hand</dt><dd>What the bank says is available now.</dd></div>
              <div><dt>Incoming</dt><dd>Confirmed means a dated receivable. Expected separately shows weighted CSV trends and unconfirmed dated receivables.</dd></div>
              <div><dt>Required out</dt><dd>Overdue plus bills due in 14 days.</dd></div>
              <div><dt>Safe to commit / Funding gap</dt><dd>What remains after required out and the configured cash floor.</dd></div>
            </dl>
          </article>
          <article>
            <span>03</span>
            <h3>Work and close the loop</h3>
            <p>Read Happening, Broken, and Next. Clear Broken and Needs action first, then use the <strong>&hellip;</strong> menu to record a partial payment, plan an installment, defer, or confirm a receipt. Refresh once more and make sure the gap or trajectory improves. Finance records the decision; it never moves bank money.</p>
          </article>
        </div>

        <p class="finance-review-guide__trust"><strong>Trust check before deciding:</strong> Cash updated is current, no important item has a missing or zero amount, incoming money has a date and confidence, and unpaid remainders stay open until posted bank activity proves they cleared.</p>
      </section>
      </div>

      <aside id="finance-recommendation-drawer" class="finance-drawer" aria-hidden="true" aria-labelledby="finance-drawer-title">
        <div class="finance-drawer__scrim" data-close-drawer aria-hidden="true"></div>
        <div class="finance-drawer__panel" role="dialog" aria-modal="true" tabindex="-1">
          <div class="finance-drawer__head"><p id="finance-drawer-eyebrow" class="finance-eyebrow">Smart recommendation</p><button type="button" class="finance-icon-button" data-close-drawer aria-label="Close review">&times;</button></div>
          <h2 id="finance-drawer-title">{html.escape(rec_title)}</h2>
          <p id="finance-drawer-why" class="finance-drawer__why"><strong>Why:</strong> {html.escape(rec_why)}</p>
          <dl id="finance-drawer-facts" class="finance-recommendation-facts"></dl>
          <form id="finance-partial-form" class="finance-confirm-form" method="post" hidden>
            <label>Payment amount<input name="amount" inputmode="decimal" required placeholder="0.00"></label>
            <label>Payment date<input name="allocation_date" type="date"></label>
            <input name="idempotency_key" type="hidden">
            <div><button type="button" class="btn btn-secondary" data-close-drawer>Cancel</button><button type="submit" class="btn btn-primary">Confirm partial payment</button></div>
          </form>
          <form id="finance-installment-form" class="finance-confirm-form" method="post" hidden>
            <label>Installment amount<input name="amount" inputmode="decimal" required placeholder="0.00"></label>
            <label>Due date<input name="due_date" type="date" required></label>
            <input name="idempotency_key" type="hidden">
            <div><button type="button" class="btn btn-secondary" data-close-drawer>Cancel</button><button type="submit" class="btn btn-primary">Confirm installment</button></div>
          </form>
          <div id="finance-preview-actions" class="finance-drawer__actions"><a id="finance-drawer-source" class="btn btn-secondary" href="#" hidden>Open source</a><button type="button" class="btn btn-secondary" data-close-drawer>Close review</button></div>
          <p id="finance-preview-note" class="finance-preview-note">Review details before confirming. No bank payment is initiated.</p>
        </div>
      </aside>

      {income_review_drawer}

      <dialog id="finance-update-modal" class="finance-modal">
        <div class="finance-modal__head"><div><p class="finance-eyebrow">Sources and exceptions</p><h2>Update money</h2></div><button type="button" class="finance-icon-button" data-close-modal aria-label="Close update money">&times;</button></div>
        <form class="finance-dropzone" method="post" action="/admin/finances/upload" enctype="multipart/form-data">
          <strong>Upload bank CSV or QBO Open Invoices CSV</strong><span>Bank CSV creates actual cash history, not confirmed income. Eligible recurring deposits appear only as Expected. QBO Open Invoices supplies dated receivables.</span>
          <input id="finance-file-input" type="file" name="csv_file" accept=".csv"><label for="finance-file-input" class="btn btn-secondary btn-sm">Choose file</label>
          <input type="hidden" name="merge_mode" value="append"><button class="btn btn-primary btn-sm" type="submit">Upload and reconcile</button>
        </form>
        <div class="finance-source-row"><div><strong>ClickUp</strong><span>Connected source for planned AP/AR</span></div><form method="post" action="/admin/finances/sync-clickup"><button class="btn btn-secondary btn-sm" type="submit">Refresh</button></form></div>
        {reconciliation_shadow_html}
        <div class="finance-source-row"><div><strong>QuickBooks</strong><span>Refresh invoices for collections and actuals for payment matching. Bank CSV remains cash-on-hand truth.</span></div><div><a class="btn btn-secondary btn-sm" href="/admin/finances/qbo/connect">Connect</a><form method="post" action="/admin/finances/sync-qbo-invoices" style="display:inline"><button class="btn btn-secondary btn-sm" type="submit">Refresh receivables</button></form><form method="post" action="/admin/finances/sync-qbo-actuals" style="display:inline"><button class="btn btn-secondary btn-sm" type="submit">Refresh actuals</button></form><form method="post" action="/admin/finances/qbo/disconnect" style="display:inline" onsubmit="return confirm('Disconnect QuickBooks? Synced records remain, but no new QBO data will be pulled.');"><button class="btn btn-secondary btn-sm" type="submit">Disconnect</button></form></div></div>
        <div class="finance-source-row"><div><strong>Cash floor</strong><span>Reserve kept after required bills. Current: {cash_floor_display}</span></div><form class="finance-floor-form" method="post" action="/admin/finances/settings/cash-floor"><label class="sr-only" for="finance-cash-floor">Cash floor in dollars</label><input id="finance-cash-floor" name="cash_floor" inputmode="decimal" value="{cash_floor_input}" placeholder="Enter cash floor" required><button class="btn btn-secondary btn-sm" type="submit">Save floor</button></form></div>
        <div class="finance-source-row"><div><strong>Manual exception</strong><span>Add an obligation without changing source records.</span></div><div><a class="btn btn-secondary btn-sm" href="/admin/finances/ap/new">Add payable</a><a class="btn btn-secondary btn-sm" href="/admin/finances/ar/new">Add incoming</a></div></div>
        {inline_result}
      </dialog>

      <template id="finance-loading-state"><section class="finance-state-copy"><div class="finance-skeleton-grid"><i></i><i></i><i></i><i></i></div><p>Calculating forecast</p><p>Loading money queue</p></section></template>
      <template id="finance-error-state"><section class="finance-state-copy is-error"><strong>Finance data could not be loaded.</strong><p>Current page data was preserved. Retry the source update.</p></section></template>
      <template id="finance-import-error-state"><section class="finance-state-copy is-error"><strong>Import failed. No records were committed.</strong><p>The failed file, reason, and row-error report will appear here.</p><a href="#">Download row-error report</a></section></template>
      <p id="finance-live-region" class="sr-only" aria-live="polite"></p>
      <script id="finance-drawer-payloads" type="application/json">{drawer_json}</script>
    </main>

    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script>
    (() => {{
      const root = document.querySelector('.finance-control');
      const chartData = {chart_json};
      const queueEmpty = document.getElementById('finance-queue-empty');
      const queueScroll = document.querySelector('.finance-queue-scroll');
      const queueRows = [...document.querySelectorAll('[data-queue-tabs]')];
      const queueWindow = document.getElementById('finance-queue-window');
      const queuePageSize = document.getElementById('finance-queue-page-size');
      const queuePagination = document.getElementById('finance-queue-pagination');
      const queueRange = document.getElementById('finance-queue-range');
      const queuePageSummary = document.getElementById('finance-queue-page-summary');
      const queuePrevious = document.getElementById('finance-queue-previous');
      const queueNext = document.getElementById('finance-queue-next');
      const queueToday = new Date('{today.isoformat()}T00:00:00');
      let activeQueueFilter = 'needs-action';
      let activeQueuePage = 1;

      function rowMatchesWindow(row, filter) {{
        if (!row.dataset.queueDate) return true;
        const due = new Date(row.dataset.queueDate + 'T00:00:00');
        const difference = Math.round((due - queueToday) / 86400000);
        const days = Number(queueWindow.value);
        return filter === 'completed'
          ? true
          : filter === 'recent'
          ? difference <= 0 && difference >= -days
          : difference <= days;
      }}

      function matchingQueueRows(filter) {{
        return queueRows.filter(row =>
          row.dataset.queueTabs.split(',').includes(filter) && rowMatchesWindow(row, filter)
        );
      }}

      function updateQueueCounts() {{
        document.querySelectorAll('[data-queue-filter]').forEach(button => {{
          const total = matchingQueueRows(button.dataset.queueFilter).length;
          button.querySelector('span').textContent = String(total);
        }});
      }}

      function renderQueuePage() {{
        const matches = matchingQueueRows(activeQueueFilter);
        const pageSize = Number(queuePageSize.value);
        const pageCount = Math.max(1, Math.ceil(matches.length / pageSize));
        activeQueuePage = Math.min(activeQueuePage, pageCount);
        const start = (activeQueuePage - 1) * pageSize;
        const end = Math.min(start + pageSize, matches.length);
        const visibleRows = new Set(matches.slice(start, end));

        queueRows.forEach(row => {{ row.hidden = !visibleRows.has(row); }});
        queueEmpty.hidden = matches.length !== 0;
        queueScroll.hidden = matches.length === 0;
        queuePagination.hidden = matches.length === 0;
        queueRange.textContent = matches.length ? `${{start + 1}}-${{end}} of ${{matches.length}} results` : '0 results';
        queuePageSummary.textContent = `Page ${{activeQueuePage}} of ${{pageCount}}`;
        queuePrevious.disabled = activeQueuePage <= 1;
        queueNext.disabled = activeQueuePage >= pageCount;
        document.querySelectorAll('[data-queue-filter]').forEach(button => {{
          button.setAttribute('aria-pressed', String(button.dataset.queueFilter === activeQueueFilter));
        }});
      }}

      function resetQueue(filter = activeQueueFilter) {{
        activeQueueFilter = filter;
        activeQueuePage = 1;
        updateQueueCounts();
        renderQueuePage();
      }}

      document.querySelectorAll('[data-queue-filter]').forEach(button => button.addEventListener('click', () => resetQueue(button.dataset.queueFilter)));
      queueWindow.addEventListener('change', () => resetQueue());
      queuePageSize.addEventListener('change', () => resetQueue());
      queuePrevious.addEventListener('click', () => {{
        if (activeQueuePage > 1) {{ activeQueuePage -= 1; renderQueuePage(); document.getElementById('finance-queue').scrollIntoView({{block:'start'}}); }}
      }});
      queueNext.addEventListener('click', () => {{
        const pageCount = Math.max(1, Math.ceil(matchingQueueRows(activeQueueFilter).length / Number(queuePageSize.value)));
        if (activeQueuePage < pageCount) {{ activeQueuePage += 1; renderQueuePage(); document.getElementById('finance-queue').scrollIntoView({{block:'start'}}); }}
      }});
      resetQueue();

      document.getElementById('finance-smart-mode').addEventListener('change', event => {{
        root.dataset.smartMode = event.target.checked ? 'on' : 'off';
      }});
      const drawer = document.getElementById('finance-recommendation-drawer');
      const drawerPanel = drawer.querySelector('.finance-drawer__panel');
      const pageContent = document.getElementById('finance-page-content');
      const drawerPayloads = JSON.parse(document.getElementById('finance-drawer-payloads').textContent);
      const drawerEyebrow = document.getElementById('finance-drawer-eyebrow');
      const drawerTitle = document.getElementById('finance-drawer-title');
      const drawerWhy = document.getElementById('finance-drawer-why');
      const drawerFacts = document.getElementById('finance-drawer-facts');
      const drawerSource = document.getElementById('finance-drawer-source');
      const drawerActions = document.getElementById('finance-preview-actions');
      const drawerNote = document.getElementById('finance-preview-note');
      const liveRegion = document.getElementById('finance-live-region');
      const incomeDrawer = document.getElementById('finance-income-review-drawer');
      const incomeDrawerPanel = incomeDrawer.querySelector('.finance-drawer__panel');
      let drawerOpener = null;
      let incomeDrawerOpener = null;

      function renderDrawerPayload(payload) {{
        drawerEyebrow.textContent = payload.eyebrow || 'Review';
        drawerTitle.textContent = payload.title || 'Review details';
        drawerWhy.replaceChildren();
        const whyLabel = document.createElement('strong');
        whyLabel.textContent = 'Why: ';
        drawerWhy.append(whyLabel, document.createTextNode(payload.why || 'Review the source evidence.'));
        drawerFacts.replaceChildren();
        (payload.facts || []).forEach(fact => {{
          const row = document.createElement('div');
          const term = document.createElement('dt');
          const description = document.createElement('dd');
          term.textContent = fact[0];
          description.textContent = fact[1];
          row.append(term, description);
          drawerFacts.append(row);
        }});
        drawerSource.hidden = !payload.sourceUrl && !payload.sourceAction;
        drawerSource.textContent = payload.sourceLabel || 'Open source';
        drawerSource.dataset.sourceAction = payload.sourceAction || '';
        drawerSource.href = payload.sourceUrl || '#finance-update-modal';
        drawerNote.textContent = payload.note || 'Review details before confirming. No bank payment is initiated.';
      }}

      function savingsActionForm(payload, action, label, confirmation) {{
        const form = document.createElement('form');
        form.method = 'post';
        form.action = '/admin/finances/savings/' + encodeURIComponent(payload.savings.opportunity_key) + '/review';
        form.className = 'finance-confirm-form';
        const hidden = {{
          action,
          evidence_hash: payload.savings.evidence_hash,
          opportunity_json: JSON.stringify(payload.savings),
        }};
        Object.entries(hidden).forEach(([name, value]) => {{
          const input = document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = value; form.append(input);
        }});
        const note = document.createElement('p'); note.textContent = confirmation; form.append(note);
        const buttons = document.createElement('div');
        const cancel = document.createElement('button'); cancel.type = 'button'; cancel.className = 'btn btn-secondary'; cancel.textContent = 'Cancel'; cancel.addEventListener('click', () => openDrawer(payload, drawerOpener));
        const submit = document.createElement('button'); submit.type = 'submit'; submit.className = 'btn btn-primary'; submit.textContent = label;
        buttons.append(cancel, submit); form.append(buttons); return form;
      }}

      function renderSavingsActions(payload) {{
        drawerActions.replaceChildren();
        const create = (label, action, confirmation) => {{
          const button = document.createElement('button'); button.type = 'button'; button.className = 'btn btn-secondary'; button.textContent = label;
          button.addEventListener('click', () => {{
            drawerActions.replaceChildren(savingsActionForm(payload, action, label, confirmation));
          }});
          drawerActions.append(button);
        }};
        create('Keep for 90 days', 'keep', 'This hides this unchanged evidence for 90 days. It does not change cash or the forecast.');
        create('Dismiss for 90 days', 'dismiss', 'This hides this unchanged evidence for 90 days. It does not cancel or change the source charge.');
        create('Create ClickUp review task', 'follow_up', 'This creates one review task in the dedicated Finance review list. It does not create an AP/AR payable or change cash.');
        if (payload.savings.realization_ready) create('Confirm bank-verified saving', 'confirm_realized', 'A later posted bank charge is materially lower than the baseline. Confirm only after verifying this is a durable saving.');
      }}

      function setDrawer(open, opener = null) {{
        if (open) {{
          drawerOpener = opener || document.activeElement;
          drawer.setAttribute('aria-hidden', 'false');
          pageContent.inert = true;
          document.body.classList.add('finance-overlay-open');
          window.requestAnimationFrame(() => drawer.querySelector('[data-close-drawer]').focus());
        }} else {{
          drawer.setAttribute('aria-hidden', 'true');
          pageContent.inert = false;
          document.body.classList.remove('finance-overlay-open');
          const previousOpener = drawerOpener;
          drawerOpener = null;
          if (previousOpener && previousOpener.isConnected) previousOpener.focus();
        }}
      }}

      function setIncomeDrawer(open, opener = null) {{
        if (open) {{
          incomeDrawerOpener = opener || document.activeElement;
          incomeDrawer.setAttribute('aria-hidden', 'false');
          pageContent.inert = true;
          document.body.classList.add('finance-overlay-open');
          window.requestAnimationFrame(() => incomeDrawer.querySelector('[data-close-income-review]').focus());
        }} else {{
          incomeDrawer.setAttribute('aria-hidden', 'true');
          pageContent.inert = false;
          document.body.classList.remove('finance-overlay-open');
          const previousOpener = incomeDrawerOpener;
          incomeDrawerOpener = null;
          if (previousOpener && previousOpener.isConnected) previousOpener.focus();
        }}
      }}

      function openDrawer(payload, opener) {{
        document.getElementById('finance-partial-form').hidden = true;
        document.getElementById('finance-installment-form').hidden = true;
        document.getElementById('finance-preview-actions').hidden = false;
        renderDrawerPayload(payload);
        if (payload.savings) renderSavingsActions(payload); else drawerActions.replaceChildren(drawerSource, (() => {{ const close = document.createElement('button'); close.type='button'; close.className='btn btn-secondary'; close.dataset.closeDrawer=''; close.textContent='Close review'; close.addEventListener('click', () => setDrawer(false)); return close; }})());
        setDrawer(true, opener);
      }}

      document.querySelectorAll('[data-drawer-review]').forEach(button => button.addEventListener('click', () => {{
        const payload = drawerPayloads[button.dataset.drawerReview];
        if (payload) openDrawer(payload, button);
      }}));
      document.querySelectorAll('[data-savings-review]').forEach(button => button.addEventListener('click', () => {{
        const payload = drawerPayloads.savings[button.dataset.savingsReview];
        if (payload) openDrawer(payload, button);
      }}));
      document.querySelectorAll('[data-close-drawer]').forEach(button => button.addEventListener('click', () => setDrawer(false)));
      document.querySelectorAll('[data-open-income-review]').forEach(button => button.addEventListener('click', () => setIncomeDrawer(true, button)));
      document.querySelectorAll('[data-close-income-review]').forEach(button => button.addEventListener('click', () => setIncomeDrawer(false)));
      drawerSource.addEventListener('click', event => {{
        if (drawerSource.dataset.sourceAction !== 'update-money') return;
        event.preventDefault();
        setDrawer(false);
        const modal = document.getElementById('finance-update-modal');
        if (modal.showModal) modal.showModal(); else modal.setAttribute('open', '');
      }});
      document.addEventListener('keydown', event => {{
        if (drawer.getAttribute('aria-hidden') !== 'false') return;
        if (event.key === 'Escape') {{ event.preventDefault(); setDrawer(false); return; }}
        if (event.key !== 'Tab') return;
        const focusable = [...drawerPanel.querySelectorAll('a[href]:not([hidden]), button:not([disabled]):not([hidden]), input:not([disabled]):not([hidden]), select:not([disabled]):not([hidden]), textarea:not([disabled]):not([hidden]), [tabindex]:not([tabindex="-1"])')]
          .filter(element => element.offsetParent !== null);
        if (!focusable.length) {{ event.preventDefault(); drawerPanel.focus(); return; }}
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {{ event.preventDefault(); last.focus(); }}
        else if (!event.shiftKey && document.activeElement === last) {{ event.preventDefault(); first.focus(); }}
      }});
      document.addEventListener('keydown', event => {{
        if (incomeDrawer.getAttribute('aria-hidden') !== 'false') return;
        if (event.key === 'Escape') {{ event.preventDefault(); setIncomeDrawer(false); return; }}
        if (event.key !== 'Tab') return;
        const focusable = [...incomeDrawerPanel.querySelectorAll('button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])')]
          .filter(element => element.offsetParent !== null);
        if (!focusable.length) {{ event.preventDefault(); incomeDrawerPanel.focus(); return; }}
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {{ event.preventDefault(); last.focus(); }}
        else if (!event.shiftKey && document.activeElement === last) {{ event.preventDefault(); first.focus(); }}
      }});

      const savingsExpand = document.querySelector('[data-expand-savings]');
      if (savingsExpand) savingsExpand.addEventListener('click', () => {{
        const expanded = savingsExpand.getAttribute('aria-expanded') === 'true';
        document.querySelectorAll('[data-savings-extra="true"]').forEach(row => {{ row.hidden = expanded; }});
        savingsExpand.setAttribute('aria-expanded', String(!expanded));
        savingsExpand.textContent = expanded ? savingsExpand.dataset.collapsedLabel : 'Show top 3';
        if (!expanded) liveRegion.textContent = 'Expanded savings opportunities.';
      }});
      if (savingsExpand) savingsExpand.dataset.collapsedLabel = savingsExpand.textContent;
      document.querySelectorAll('[data-retry-savings]').forEach(button => button.addEventListener('click', () => window.location.reload()));

      document.querySelectorAll('[data-open-modal]').forEach(button => button.addEventListener('click', () => {{
        const modal = document.getElementById(button.dataset.openModal);
        if (modal.showModal) modal.showModal(); else modal.setAttribute('open', '');
      }}));
      document.querySelectorAll('[data-close-modal]').forEach(button => button.addEventListener('click', () => button.closest('dialog').close()));
      document.querySelectorAll('[data-preview-action]').forEach(button => button.addEventListener('click', () => {{
        const action = button.dataset.previewAction;
        const eventId = button.dataset.eventId;
        const party = button.dataset.party || 'this item';
        const partialForm = document.getElementById('finance-partial-form');
        const installmentForm = document.getElementById('finance-installment-form');
        const previewActions = document.getElementById('finance-preview-actions');
        const direction = button.dataset.direction === 'inflow' ? 'incoming cash' : 'cash outflow';
        const amount = '$' + Number(button.dataset.amount || 0).toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
        openDrawer({{
          eyebrow: 'Money queue review',
          title: action + ' - ' + party,
          why: 'Review this item and its cash effect before confirming any Finance record.',
          facts: [
            ['Amount', amount],
            ['Cash direction', direction],
            ['Action', action],
            ['Source evidence', 'Use the linked source or posted bank activity to verify this item.'],
            ['Downside', 'The obligation or expected receipt remains open until evidence confirms otherwise.']
          ],
          sourceUrl: button.dataset.sourceUrl || '',
          sourceAction: '',
          sourceLabel: button.dataset.sourceUrl ? 'Open source' : '',
          note: 'Finance records the decision. It does not initiate bank movement.'
        }}, button);
        partialForm.hidden = true;
        installmentForm.hidden = true;
        previewActions.hidden = false;
        if (eventId && action === 'Record partial payment') {{
          partialForm.action = '/admin/finances/actions/' + encodeURIComponent(eventId) + '/partial';
          partialForm.querySelector('[name="amount"]').value = '';
          partialForm.querySelector('[name="amount"]').placeholder = 'Up to ' + button.dataset.amount;
          partialForm.querySelector('[name="idempotency_key"]').value = window.crypto && window.crypto.randomUUID ? window.crypto.randomUUID() : String(Date.now());
          partialForm.hidden = false;
          previewActions.hidden = true;
        }}
        if (eventId && action === 'Split into installments') {{
          installmentForm.action = '/admin/finances/actions/' + encodeURIComponent(eventId) + '/installment';
          installmentForm.querySelector('[name="amount"]').value = '';
          installmentForm.querySelector('[name="amount"]').placeholder = 'Up to ' + button.dataset.amount;
          installmentForm.querySelector('[name="idempotency_key"]').value = window.crypto && window.crypto.randomUUID ? window.crypto.randomUUID() : String(Date.now());
          installmentForm.hidden = false;
          previewActions.hidden = true;
        }}
        document.querySelectorAll('.finance-row-menu[open]').forEach(menu => menu.removeAttribute('open'));
      }}));

      const status = document.getElementById('finance-chart-status');
      if (!window.Chart || !chartData.labels.length) {{ status.textContent = 'Forecast unavailable. Refresh finance data.'; return; }}
      new Chart(document.getElementById('finance-control-chart'), {{
        type: 'line',
        data: {{ labels: chartData.labels, datasets: [
          {{label:'Actual',data:chartData.actual,borderColor:'#2b3644',borderWidth:3,pointRadius:0,spanGaps:true}},
          {{label:'Committed',data:chartData.committed,borderColor:'#4f84c4',borderWidth:2.5,pointRadius:0,tension:.24}},
          {{label:'Expected',data:chartData.expected,borderColor:'#0f766e',borderWidth:2,borderDash:[7,4],pointRadius:0,tension:.24}},
          {{label:'Stress',data:chartData.stress,borderColor:'#b91c1c',borderWidth:2,borderDash:[3,4],pointRadius:0,tension:.18}},
          {{label:'Floor',data:chartData.labels.map(() => chartData.floor),borderColor:'rgba(161,98,7,.55)',borderWidth:1,borderDash:[2,4],pointRadius:0}}
        ]}},
        options: {{responsive:true,maintainAspectRatio:false,interaction:{{mode:'index',intersect:false}},plugins:{{legend:{{display:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{maxTicksLimit:8,color:'#7b8492',font:{{size:10}}}}}},y:{{grid:{{color:'rgba(43,54,68,.06)'}},ticks:{{maxTicksLimit:5,color:'#7b8492',callback:value => '$'+Math.round(value/1000)+'k'}}}}}}}}
      }});
      status.hidden = true;
    }})();
    </script>"""
    return _page_shell("Finance Control", "overview", body, flash=flash)
