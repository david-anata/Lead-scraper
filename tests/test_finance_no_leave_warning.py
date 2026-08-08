"""Finance pages must not block navigation with a leave-page warning.

It fired on staged changes that survive in the draft anyway, so it blocked
navigation to protect work that was never at risk. Worse, it fired on stale
drafts from earlier sessions, where the operator could see nothing unsaved and
had no way to clear it.
"""

from __future__ import annotations

import pathlib

REPO = pathlib.Path(__file__).resolve().parents[1]
FINANCE_SOURCES = [
    *(REPO / "sales_support_agent" / "services" / "cashflow").glob("*.py"),
    *(REPO / "sales_support_agent" / "static").glob("finance*.js"),
]


def test_no_finance_surface_registers_a_leave_page_warning():
    offenders = [
        path.relative_to(REPO).as_posix()
        for path in FINANCE_SOURCES
        if "beforeunload" in path.read_text(encoding="utf-8")
    ]

    assert offenders == [], (
        "these block navigation to protect staged changes that already survive "
        f"in the draft: {offenders}"
    )


def test_the_draft_recovery_notice_is_still_there():
    """Removing the warning must not remove the thing that made it unnecessary:
    the operator being told their staged changes came back."""
    budgeting = (REPO / "sales_support_agent" / "services" / "cashflow" / "budgeting.py").read_text()

    assert "unsaved change" in budgeting or "draft change" in budgeting
