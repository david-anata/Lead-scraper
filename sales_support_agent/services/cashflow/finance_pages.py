"""Standalone pages for the tools that outgrew the Update money dialog.

Bill audit, bookkeeping, collections and setup each get their own page with
their own scroll and their own URL. The rendering functions already exist in
overview.py and are reused verbatim here, so nothing renders twice and there is
no second copy to drift.
"""

from __future__ import annotations

from sales_support_agent.services.cashflow.finance_nav import render_finance_nav
from sales_support_agent.services.cashflow.overview import _page_shell


def _wrap(title: str, active: str, intro: str, body: str, flash: str = "") -> str:
    return _page_shell(
        title, active,
        f'{render_finance_nav(active)}<h1>{title}</h1><p class="page-sub">{intro}</p>{body}',
        flash=flash,
    )


def render_audit_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.overview import _render_bill_audit

    return _wrap(
        "Bill audit", "audit",
        "Where money is leaking. Internal transfers between your own accounts are "
        "never included, and every finding says why it qualified.",
        _render_bill_audit() or '<div class="card"><p style="margin:0">'
        "Nothing unusual. Bills look under control.</p></div>",
        flash=flash,
    )


def render_bookkeeping_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.overview import _render_bookkeeping

    return _wrap(
        "Bookkeeping", "bookkeeping",
        "Transactions file themselves; only real unknowns ask you. Nothing is "
        "pre-selected unless the merchant is recognised.",
        _render_bookkeeping() or '<div class="card"><p style="margin:0">'
        "No transactions to file yet.</p></div>",
        flash=flash,
    )


def render_collections_page(*, flash: str = "") -> str:
    from sales_support_agent.services.cashflow.overview import _render_collections

    return _wrap(
        "Who owes you", "collections",
        "Overdue customers, oldest first. One send per click; nothing goes out on "
        "its own and nothing sends in bulk.",
        _render_collections() or '<div class="card"><p style="margin:0">'
        "Nobody is past due right now.</p></div>",
        flash=flash,
    )


def render_setup_page(*, flash: str = "") -> str:
    """Things touched rarely: banks, accounts, vendors, trust, matching."""
    from sales_support_agent.services.cashflow.overview import (
        _render_match_review,
        _render_vendors_section,
    )

    sections = [
        _render_match_review(),
        _render_vendors_section(),
    ]
    body = (
        '<div class="card"><h2>Bank and accounts</h2>'
        '<p style="font-size:13px;color:#6b7a8d;margin:0">'
        'Connect or refresh a bank, and set which accounts count as spendable cash, '
        'from the Update money panel on the Today page.</p>'
        '<div class="action-row"><a class="btn btn-secondary" href="/admin/finances">'
        'Open Today</a></div></div>'
        + "".join(section for section in sections if section)
        + '<div class="card"><h2>Rebuild the schedule</h2>'
        '<p style="font-size:13px;color:#6b7a8d;margin:0">'
        'Turn your existing recurring items into schedules this app owns.</p>'
        '<div class="action-row"><a class="btn btn-secondary" href="/admin/finances/import">'
        'Open the importer</a></div></div>'
    )
    return _wrap(
        "Setup", "setup",
        "Things you touch rarely: payment matching, vendors, and rebuilding the schedule.",
        body, flash=flash,
    )
