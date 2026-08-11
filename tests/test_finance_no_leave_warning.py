"""Finance only warns while a browser change is not protected on the server."""

from __future__ import annotations

import pathlib

REPO = pathlib.Path(__file__).resolve().parents[1]
FINANCE_SOURCES = [
    *(REPO / "sales_support_agent" / "services" / "cashflow").glob("*.py"),
    *(REPO / "sales_support_agent" / "static").glob("finance*.js"),
]


def test_shared_workspace_warns_only_for_changes_not_confirmed_safe():
    workspace = (REPO / "sales_support_agent/static/finance-workspace.js").read_text(encoding="utf-8")

    assert 'window.addEventListener("beforeunload"' in workspace
    assert "state.unprotected" in workspace
    assert "state.saving" in workspace
    assert "state.saveFailed" in workspace
    assert "state.unprotected = false" in workspace


def test_the_draft_recovery_notice_is_still_there():
    """Removing the warning must not remove the thing that made it unnecessary:
    the operator being told their staged changes came back."""
    budgeting = (REPO / "sales_support_agent" / "services" / "cashflow" / "budgeting.py").read_text(encoding="utf-8")

    assert "unsaved change" in budgeting or "draft change" in budgeting


def test_every_finance_page_exposes_draft_status_review_and_discard():
    shell = (REPO / "sales_support_agent/services/cashflow/cashflow_helpers.py").read_text(encoding="utf-8")
    workspace = (REPO / "sales_support_agent/static/finance-workspace.js").read_text(encoding="utf-8")

    for wording in ("Draft protected", "Saving", "Save failed"):
        assert wording in workspace
    assert "Review and save" in shell
    assert "Discard draft" in shell
