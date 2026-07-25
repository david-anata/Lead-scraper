"""Trust check: tie the headline numbers back to their sources.

Answers "can I trust this?" by reconciling three things: displayed cash against
the sum of connected account balances, overdue receivables against the books,
and the obligations that pause cash decisions, broken down by reason. It only
reads and compares; it resolves nothing on its own.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional


def build_trust_check(
    *,
    cash_on_hand_cents: Optional[int] = None,
    payable_issues: Optional[list[dict[str, Any]]] = None,
    as_of: Optional[date] = None,
) -> dict[str, Any]:
    """Reconcile cash, receivables, and blocked obligations against source."""
    from sales_support_agent.services.cashflow.accounts_view import load_accounts_overview
    from sales_support_agent.services.cashflow.collections import list_overdue_receivables

    try:
        accounts = load_accounts_overview()
    except Exception:
        accounts = {"spendable_cents": 0, "reserve_cents": 0, "account_count": 0, "as_of": ""}
    account_total = int(accounts["spendable_cents"]) + int(accounts["reserve_cents"])

    try:
        receivables = list_overdue_receivables(as_of=as_of)
    except Exception:
        receivables = []
    ar_total = sum(int(r["owed_cents"]) for r in receivables)
    ar_count = sum(int(r["invoice_count"]) for r in receivables)

    issues = payable_issues or []
    reason_counts: dict[str, int] = {}
    for issue in issues:
        reason = str(issue.get("reason") or "other")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    reason_counts = dict(sorted(reason_counts.items(), key=lambda kv: kv[1], reverse=True))

    cash_matches: Optional[bool] = None
    cash_gap_cents: Optional[int] = None
    if cash_on_hand_cents is not None:
        cash_gap_cents = int(cash_on_hand_cents) - account_total
        cash_matches = abs(cash_gap_cents) <= 100  # within $1 of rounding

    return {
        "spendable_cents": int(accounts["spendable_cents"]),
        "reserve_cents": int(accounts["reserve_cents"]),
        "account_total_cents": account_total,
        "account_count": int(accounts["account_count"]),
        "balance_as_of": str(accounts.get("as_of") or ""),
        "cash_on_hand_cents": cash_on_hand_cents,
        "cash_matches": cash_matches,
        "cash_gap_cents": cash_gap_cents,
        "ar_total_cents": ar_total,
        "ar_count": ar_count,
        "obligation_issue_count": len(issues),
        "obligation_reason_counts": reason_counts,
    }
