"""Finance navigation with live counts.

Eight tools were living inside one dialog, three of which produce hundreds of
rows. They belong on pages. The counts sit in the nav so the operator can see
where the work is without opening anything, and so a wrong number is visible
immediately rather than hidden behind a click.
"""

from __future__ import annotations

import html
from typing import Any

# (key, label, href). Order follows how often each is actually used.
NAV_ITEMS = (
    ("today", "Today", "/admin/finances"),
    ("review", "Review", "/admin/finances/review"),
    ("bookkeeping", "Bookkeeping", "/admin/finances/bookkeeping"),
    ("audit", "Bill audit", "/admin/finances/audit"),
    ("collections", "Who owes you", "/admin/finances/collections"),
    ("schedules", "Schedules", "/admin/finances/recurring"),
    # No apostrophe in the label: the strip escapes it, and a nav item nobody
    # can find by reading it is worse than slightly stiff wording.
    ("whats_coming", "What is coming", "/admin/finances/whats-coming"),
    ("setup", "Setup", "/admin/finances/setup"),
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
