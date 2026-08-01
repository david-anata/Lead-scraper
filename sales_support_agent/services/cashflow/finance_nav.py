"""The plain-language navigation for the replacement Finance experience."""

from __future__ import annotations

import html
from typing import Any

# The replacement Finance experience has five destinations.  Older routes stay
# available during the stabilization release, but they no longer compete with
# the owner's daily money workflow.
NAV_ITEMS = (
    ("today", "Today", "/admin/finances"),
    ("plan", "Cash plan", "/admin/finances/plan"),
    ("budget", "Budget & savings", "/admin/finances/budget"),
    ("review", "Review", "/admin/finances/review"),
    ("accounts", "Accounts & setup", "/admin/finances/accounts"),
)


def nav_counts() -> dict[str, int]:
    """How much work waits behind each page. Never raises: a broken count must
    not take the navigation down with it."""
    counts: dict[str, int] = {}
    try:
        from sales_support_agent.services.cashflow.bulk_resolve import list_review_items
        counts["review"] = int(list_review_items()["total"])
    except Exception:
        pass
    try:
        from sales_support_agent.services.cashflow.bookkeeping import bookkeeping_summary
        counts["bookkeeping"] = int(bookkeeping_summary()["needs_decision"])
    except Exception:
        pass
    try:
        from sales_support_agent.services.cashflow.bill_audit import run_bill_audit
        counts["audit"] = len(run_bill_audit())
    except Exception:
        pass
    try:
        from sales_support_agent.services.cashflow.collections import build_collections
        counts["collections"] = int(build_collections()["customer_count"])
    except Exception:
        pass
    try:
        # Only the predicted bills nobody has answered yet. Confirmed and
        # dismissed ones are decided, so badging them would never clear.
        from sales_support_agent.services.cashflow.bill_patterns import list_bill_patterns
        counts["whats_coming"] = int(list_bill_patterns()["counts"]["unreviewed"])
    except Exception:
        pass
    return counts


def render_finance_nav(active: str = "today", *, counts: dict[str, int] | None = None) -> str:
    """The navigation strip. Zero counts are hidden rather than shown as 0."""
    counts = counts if counts is not None else nav_counts()
    links = []
    for key, label, href in NAV_ITEMS:
        count = int(counts.get(key) or 0)
        badge = f'<span class="finance-nav-count">{count}</span>' if count else ""
        classes = "finance-nav-link" + (" is-active" if key == active else "")
        links.append(
            f'<a class="{classes}" href="{html.escape(href, quote=True)}">'
            f'{html.escape(label)}{badge}</a>'
        )
    return '<nav class="finance-nav" aria-label="Finance sections">' + "".join(links) + "</nav>"
