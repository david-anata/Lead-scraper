"""Cashflow forecast engine — three pure functions, no DB or API calls.

All amounts are passed and returned as integer cents.  Callers convert to
Decimal / display strings at the boundary; the engine never sees floats.

Functions
---------
aggregate_weeks(events, starting_cash_cents, weeks, as_of_date)
    Groups cash events into weekly buckets and computes running cash position.

flag_risks(weeks, events, as_of_date)
    Scans week buckets and the raw event list for risk conditions.

apply_scenario(events, adjustments)
    Applies a list of amount/date overrides to a copy of the event list,
    then returns the modified events for re-running through aggregate_weeks.
"""

from __future__ import annotations

import statistics
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Data transfer objects (plain dataclasses — no ORM coupling)
# ---------------------------------------------------------------------------

@dataclass
class EventDTO:
    """Lightweight snapshot of a CashEvent row for engine processing."""
    id: str
    source: str                     # "manual" | "csv" | "clickup" | "recurring"
    event_type: str                 # "inflow" | "outflow"
    category: str
    name: str
    vendor_or_customer: str
    amount_cents: int               # always positive; event_type gives direction
    due_date: date | None
    status: str                     # "planned" | "pending" | "overdue" | "paid" | "posted" | "matched"
    confidence: str                 # "confirmed" | "estimated"
    matched_to_id: str | None = None
    recurring_rule: str = ""


@dataclass
class WeekBucket:
    """One week in the forecast window."""
    week_start: date
    week_end: date                  # week_start + 6 days (Mon–Sun)
    starting_cash_cents: int = 0
    inflow_cents: int = 0
    outflow_cents: int = 0
    net_cents: int = 0
    ending_cash_cents: int = 0
    events: list[EventDTO] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.week_start.strftime('%b %d')} – {self.week_end.strftime('%b %d, %Y')}"

    @property
    def is_negative(self) -> bool:
        return self.ending_cash_cents < 0


@dataclass
class RiskAlert:
    """A single flagged condition in the forecast."""
    severity: str                   # "critical" | "warning" | "info"
    alert_type: str                 # "negative_week" | "overdue" | "duplicate" | "outlier" | "large_outflow"
    title: str
    detail: str
    week_start: date | None = None
    event_ids: list[int] = field(default_factory=list)


@dataclass
class ScenarioAdjustment:
    """One override to apply in scenario modelling."""
    event_id: str
    new_amount_cents: int | None = None   # None = keep original
    new_due_date: date | None = None      # None = keep original
    remove: bool = False                  # True = exclude from forecast


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def aggregate_weeks(
    events: list[EventDTO],
    starting_cash_cents: int,
    weeks: int = 12,
    as_of_date: date | None = None,
) -> list[WeekBucket]:
    """Group events into weekly buckets and compute rolling cash position.

    Args:
        events:              All CashEvents to include in the forecast.
                             Pass only events with status != 'paid'/'cancelled'/'matched'
                             for forward-looking forecasts, or include 'posted'/'matched'
                             for actuals view.
        starting_cash_cents: Opening cash balance for the first week (from the
                             most recent bank CSV balance row).
        weeks:               How many weeks ahead to project (default 12).
        as_of_date:          The anchor date for the first week (defaults to today).

    Returns:
        A list of WeekBucket objects, one per week, in chronological order.
    """
    today = as_of_date or date.today()
    # Align to the start of the current week (Monday)
    monday = today - timedelta(days=today.weekday())

    buckets: list[WeekBucket] = []
    for i in range(weeks):
        week_start = monday + timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        buckets.append(WeekBucket(week_start=week_start, week_end=week_end))

    # Assign events to buckets by due_date
    bucket_by_start: dict[date, WeekBucket] = {b.week_start: b for b in buckets}

    for event in events:
        if event.due_date is None:
            continue
        # Skip cancelled and paid events — they should not affect the forecast
        if event.status in ("cancelled", "paid"):
            continue
        event_date = event.due_date if isinstance(event.due_date, date) else event.due_date.date()
        # Find the Monday of the week this event falls in
        event_monday = event_date - timedelta(days=event_date.weekday())
        bucket = bucket_by_start.get(event_monday)
        if bucket is None:
            continue  # outside the forecast window
        bucket.events.append(event)
        if event.event_type == "inflow":
            bucket.inflow_cents += event.amount_cents
        else:
            bucket.outflow_cents += event.amount_cents

    # Compute rolling net and ending cash
    running = starting_cash_cents
    for bucket in buckets:
        bucket.starting_cash_cents = running
        bucket.net_cents = bucket.inflow_cents - bucket.outflow_cents
        bucket.ending_cash_cents = running + bucket.net_cents
        running = bucket.ending_cash_cents

    return buckets


def flag_risks(
    weeks: list[WeekBucket],
    events: list[EventDTO],
    as_of_date: date | None = None,
    outlier_multiplier: float = 3.0,
    large_outflow_threshold_cents: int = 200_000,   # $2,000
    duplicate_window_days: int = 7,
) -> list[RiskAlert]:
    """Scan week buckets and the raw event list for risk conditions.

    Risk types detected:
        negative_week  — ending cash goes below zero
        overdue        — planned/pending event past its due_date with no match
        duplicate      — two outflows with same vendor + similar amount within
                         duplicate_window_days of each other
        outlier        — outflow amount > outlier_multiplier × median for category
        large_outflow  — single outflow exceeds large_outflow_threshold_cents

    Args:
        weeks:                    Output from aggregate_weeks().
        events:                   The same event list passed to aggregate_weeks(),
                                  used for overdue and duplicate detection.
        as_of_date:               Today's date for overdue comparison.
        outlier_multiplier:       Flag if amount > N × category median.
        large_outflow_threshold_cents: Flag any single outflow above this amount.
        duplicate_window_days:    Days window for duplicate vendor+amount check.

    Returns:
        List of RiskAlert objects, ordered by severity (critical first).
    """
    today = as_of_date or date.today()
    alerts: list[RiskAlert] = []

    # ── 1. Negative cash weeks ─────────────────────────────────────────────
    for bucket in weeks:
        if bucket.is_negative:
            alerts.append(RiskAlert(
                severity="critical",
                alert_type="negative_week",
                title=f"Negative cash: {bucket.label}",
                detail=(
                    f"Projected ending balance is "
                    f"–${abs(bucket.ending_cash_cents) / 100:,.2f}. "
                    f"Outflows ({_fmt(bucket.outflow_cents)}) exceed "
                    f"inflows ({_fmt(bucket.inflow_cents)}) + starting cash "
                    f"({_fmt(bucket.starting_cash_cents)})."
                ),
                week_start=bucket.week_start,
                event_ids=[e.id for e in bucket.events],
            ))

    # ── 2. Overdue obligations ─────────────────────────────────────────────
    overdue_statuses = {"planned", "pending", "overdue"}
    for event in events:
        if event.status not in overdue_statuses:
            continue
        if event.due_date is None:
            continue
        event_date = event.due_date if isinstance(event.due_date, date) else event.due_date.date()
        if event_date < today and event.matched_to_id is None:
            days_overdue = (today - event_date).days
            alerts.append(RiskAlert(
                severity="critical" if days_overdue > 7 else "warning",
                alert_type="overdue",
                title=f"Overdue: {event.name or event.vendor_or_customer}",
                detail=(
                    f"{_fmt(event.amount_cents)} was due "
                    f"{event_date.strftime('%b %d')} ({days_overdue} days ago). "
                    f"No matching bank transaction found."
                ),
                event_ids=[event.id],
            ))

    # ── 3. Duplicate obligations ───────────────────────────────────────────
    # Bucket by vendor first (O(n)), then compare within bucket (O(k²) where k is small)
    outflows = [e for e in events if e.event_type == "outflow" and e.due_date is not None]
    vendor_buckets: dict[str, list] = {}
    for e in outflows:
        key = (e.vendor_or_customer or "").lower().strip()
        if key:
            vendor_buckets.setdefault(key, []).append(e)

    seen: set[str] = set()
    for vendor_key, bucket in vendor_buckets.items():
        for i, a in enumerate(bucket):
            if a.id in seen:
                continue
            date_a = a.due_date if isinstance(a.due_date, date) else a.due_date.date()
            for b in bucket[i + 1:]:
                if b.id in seen:
                    continue
                date_b = b.due_date if isinstance(b.due_date, date) else b.due_date.date()
                if abs((date_a - date_b).days) > duplicate_window_days:
                    continue
                if a.amount_cents == 0:
                    continue
                ratio = abs(a.amount_cents - b.amount_cents) / a.amount_cents
                if ratio <= 0.10:
                    seen.add(b.id)
                    alerts.append(RiskAlert(
                        severity="warning",
                        alert_type="duplicate",
                        title=f"Possible duplicate: {a.vendor_or_customer}",
                        detail=(
                            f"Two {_fmt(a.amount_cents)} outflows to "
                            f"{a.vendor_or_customer} within {duplicate_window_days} days "
                            f"({date_a.strftime('%b %d')} and {date_b.strftime('%b %d')})."
                        ),
                        event_ids=[a.id, b.id],
                    ))

    # ── 4. Category outliers ───────────────────────────────────────────────
    by_category: dict[str, list[int]] = {}
    for event in outflows:
        by_category.setdefault(event.category, []).append(event.amount_cents)

    for category, amounts in by_category.items():
        if len(amounts) < 3:
            continue  # need at least 3 samples for meaningful median
        med = statistics.median(amounts)
        if med == 0:
            continue
        for event in outflows:
            if event.category != category:
                continue
            if event.amount_cents > outlier_multiplier * med:
                alerts.append(RiskAlert(
                    severity="warning",
                    alert_type="outlier",
                    title=f"Outlier expense: {event.name or event.vendor_or_customer}",
                    detail=(
                        f"{_fmt(event.amount_cents)} in {category} is "
                        f"{event.amount_cents / med:.1f}× the category median "
                        f"({_fmt(int(med))})."
                    ),
                    event_ids=[event.id],
                ))

    # ── 5. Large single outflows ───────────────────────────────────────────
    for event in outflows:
        if event.amount_cents >= large_outflow_threshold_cents:
            alerts.append(RiskAlert(
                severity="info",
                alert_type="large_outflow",
                title=f"Large outflow: {event.name or event.vendor_or_customer}",
                detail=(
                    f"{_fmt(event.amount_cents)} due "
                    f"{event.due_date.strftime('%b %d') if event.due_date else 'TBD'}."
                ),
                event_ids=[event.id],
            ))

    # Sort: critical → warning → info
    _severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: _severity_order.get(a.severity, 9))
    return alerts


def apply_scenario(
    events: list[EventDTO],
    adjustments: list[ScenarioAdjustment],
) -> list[EventDTO]:
    """Apply a set of overrides to a copy of the event list.

    Returns a new list — the originals are never mutated.  Pass the result
    back through aggregate_weeks() + flag_risks() to see the impact.

    Args:
        events:      The baseline event list from the DB.
        adjustments: List of ScenarioAdjustment objects describing overrides.

    Returns:
        Modified copy of the event list with adjustments applied.
    """
    adj_by_id: dict[str, ScenarioAdjustment] = {a.event_id: a for a in adjustments}
    result: list[EventDTO] = []
    for event in events:
        adj = adj_by_id.get(event.id)
        if adj is None:
            result.append(deepcopy(event))
            continue
        if adj.remove:
            continue  # exclude from scenario
        modified = deepcopy(event)
        if adj.new_amount_cents is not None:
            modified.amount_cents = adj.new_amount_cents
        if adj.new_due_date is not None:
            modified.due_date = adj.new_due_date
        result.append(modified)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fmt(cents: int) -> str:
    """Format integer cents as a dollar string for alert messages."""
    return f"${abs(cents) / 100:,.2f}"
