"""Follow-up dates for prospect decks.

A follow-up is three calendar days out, then the next weekday that is not
blocked. Blocked days are US federal holidays, the Friday after Thanksgiving,
and December 24 through January 1.

Thanksgiving 2026 is Thursday, November 26. A deck generated on Monday,
November 23 follows up on Monday, November 30.
"""

from __future__ import annotations

from datetime import date, timedelta


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The nth weekday in a month. Monday is 0. n is 1-based."""

    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return date(year, month, 1 + offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        probe = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        probe = date(year, month + 1, 1) - timedelta(days=1)
    while probe.weekday() != weekday:
        probe -= timedelta(days=1)
    return probe


def _observed(day: date) -> date:
    """Saturday holidays are observed Friday. Sunday holidays are observed Monday."""

    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def us_federal_holidays(year: int) -> set[date]:
    """US federal holidays for one year, with the weekend observance rule."""

    fixed = (
        date(year, 1, 1),
        date(year, 6, 19),
        date(year, 7, 4),
        date(year, 11, 11),
        date(year, 12, 25),
    )
    holidays = {_observed(day) for day in fixed}
    holidays.update(
        {
            _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
            _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
            _last_weekday(year, 5, 0),  # Memorial Day
            _nth_weekday(year, 9, 0, 1),  # Labor Day
            _nth_weekday(year, 10, 0, 2),  # Columbus Day
            _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        }
    )
    return holidays


def holidays_covering(start: date, end: date) -> set[date]:
    years = range(start.year, end.year + 1)
    blocked: set[date] = set()
    for year in years:
        blocked.update(us_federal_holidays(year))
    return blocked


def _day_after_thanksgiving(year: int) -> date:
    """Friday after the fourth Thursday in November."""

    return _nth_weekday(year, 11, 3, 4) + timedelta(days=1)


def _year_end_break(year: int) -> set[date]:
    """December 24 of ``year`` through January 1 of the next year, inclusive."""

    days: set[date] = set()
    cursor = date(year, 12, 24)
    end = date(year + 1, 1, 1)
    while cursor <= end:
        days.add(cursor)
        cursor += timedelta(days=1)
    return days


def follow_up_blocked_dates(start: date, end: date) -> set[date]:
    """Federal holidays, the Friday after Thanksgiving, and Dec 24 through Jan 1."""

    blocked = holidays_covering(start, end)
    for year in range(start.year - 1, end.year + 1):
        blocked.add(_day_after_thanksgiving(year))
        blocked.update(_year_end_break(year))
    return blocked


def next_follow_up_date(generated: date, *, offset_days: int = 3) -> date:
    """The deck follow-up date.

    Offset calendar days, then skip weekends, US federal holidays, the Friday
    after Thanksgiving, and December 24 through January 1.
    """

    follow_up = generated + timedelta(days=offset_days)
    blocked = follow_up_blocked_dates(follow_up, follow_up + timedelta(days=21))
    while follow_up.weekday() >= 5 or follow_up in blocked:
        follow_up += timedelta(days=1)
    return follow_up
