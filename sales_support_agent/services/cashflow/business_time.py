"""One business date for every Finance surface."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

FINANCE_TIMEZONE = ZoneInfo("America/Denver")


def operator_today() -> date:
    """Return the owner's Denver business date, never the server UTC date."""
    return datetime.now(FINANCE_TIMEZONE).date()

