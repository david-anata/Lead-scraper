"""Business-day helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta


def coerce_to_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def is_business_day(value: date | datetime) -> bool:
    return coerce_to_date(value).weekday() < 5


def next_business_day(value: date | datetime) -> date:
    current = coerce_to_date(value)
    while not is_business_day(current):
        current += timedelta(days=1)
    return current


def business_days_between(start: date | datetime, end: date | datetime) -> int:
    """Count business days from ``start`` up to and including ``end``.

    Returns 0 when ``end`` is not after ``start``. Used wherever an elapsed
    figure is shown to an operator, so "overdue by N" means the same thing as
    the follow-up policies that are also expressed in business days.
    """

    first = coerce_to_date(start)
    last = coerce_to_date(end)
    if last <= first:
        return 0
    counted = 0
    current = first
    while current < last:
        current += timedelta(days=1)
        if is_business_day(current):
            counted += 1
    return counted


def add_business_days(value: date | datetime, days: int) -> date:
    current = coerce_to_date(value)
    if days <= 0:
        return next_business_day(current)

    added = 0
    while added < days:
        current += timedelta(days=1)
        if is_business_day(current):
            added += 1
    return current

