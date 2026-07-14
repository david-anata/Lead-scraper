"""Renderer contract tests for the Finance Control V2 desktop page."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import patch

from sales_support_agent.services.cashflow.overview import render_cashflow_overview_page


TODAY = date.today()
BALANCE_AS_OF = TODAY.isoformat()


def _control_state(*, queue: list[dict] | None = None, balance_cents: int = 498_392) -> dict:
    return {
        "cash_position": {
            "cash_on_hand_cents": balance_cents,
            "balance_available": True,
            "incoming_confirmed_cents": 800_000,
            "incoming_expected_cents": 450_000,
            "required_out_cents": 1_100_000,
            "exposure_out_cents": 4_000_000,
            "safe_to_commit_cents": 0,
            "funding_gap_cents": 601_608,
            "floor_cents": 1_000_000,
        },
        "smart_brief": {
            "happening": "Confirmed income covers most required payments.",
            "broken": "One receipt needs a date.",
            "next": "Confirm the Acme receipt before scheduling rent.",
        },
        "forecast": {
            "labels": [BALANCE_AS_OF, (TODAY + timedelta(days=1)).isoformat()],
            "actual": [498_392, None],
            "committed": [498_392, 1_298_392],
            "expected": [498_392, 1_748_392],
            "stress": [498_392, -601_608],
            "floor_cents": 1_000_000,
        },
        "queue": {"items": queue or []},
        "recommendation": {
            "title": "Split rent into installments",
            "why": "The full payment breaches the cash floor.",
            "before_minimum_cash_cents": -260_000,
            "after_minimum_cash_cents": 1_040_000,
            "depends_on": "Confirmed Acme receipt",
            "confidence": "Medium",
            "limitations": "Six weeks of history",
            "downside": "The remaining rent stays overdue.",
            "action_label": "Create installment preview",
        },
    }


def _render(rows: list[dict], state: dict, *, balance_cents: int = 498_392) -> str:
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=rows),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(balance_cents, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=(rows, None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            return_value=state,
        ) as builder,
    ):
        page = asyncio.run(render_cashflow_overview_page())
    builder.assert_called_once_with(
        rows, balance_cents, BALANCE_AS_OF,
        smart_mode=True, settlement_annotations=None,
    )
    return page


def test_renderer_uses_control_builder_and_v2_scan_order() -> None:
    due = TODAY - timedelta(days=3)
    queue = [
        {
            "id": "rent-1",
            "event_type": "outflow",
            "party": "Studio Rent",
            "due_date": due.isoformat(),
            "open_amount_cents": 500_000,
            "action_label": "Protect cash",
            "needs_action": True,
        },
        {
            "id": "acme-1",
            "event_type": "inflow",
            "party": "Acme",
            "due_date": (TODAY + timedelta(days=2)).isoformat(),
            "open_amount_cents": 800_000,
            "confidence": "confirmed",
            "action_label": "Collect now",
            "needs_action": True,
        },
    ]
    page = _render([], _control_state(queue=queue))

    expected_order = ["Cash on hand", "Happening", "Cash trajectory", "Money queue"]
    positions = [page.index(label) for label in expected_order]
    assert positions == sorted(positions)
    assert "Incoming 14 days" in page
    assert "Required out 14 days" in page
    assert "Funding gap" in page
    assert "Committed" in page and "Expected" in page and "Stress" in page
    assert f"{due.strftime('%b %d, %Y')} - 3d late" in page


def test_unified_queue_actions_drawer_modal_and_states_are_present() -> None:
    queue = [
        {
            "id": "incoming-1",
            "event_type": "inflow",
            "party": "Acme",
            "due_date": (TODAY + timedelta(days=1)).isoformat(),
            "open_amount_cents": 800_000,
            "needs_action": True,
        },
        {
            "id": "payable-1",
            "event_type": "outflow",
            "party": "Rent",
            "due_date": (TODAY + timedelta(days=1)).isoformat(),
            "open_amount_cents": 500_000,
            "needs_action": True,
        },
    ]
    page = _render([], _control_state(queue=queue))

    for tab in ("Needs action", "Incoming", "Payables", "Recent"):
        assert tab in page
    assert "Mark received" in page
    assert "Record partial payment" in page
    assert 'id="finance-recommendation-drawer"' in page
    assert 'id="finance-update-modal"' in page
    assert "No bank payment is initiated" in page
    assert "Upload and reconcile" in page
    assert "Calculating forecast" in page
    assert "Finance data could not be loaded." in page
    assert "Import failed. No records were committed." in page
    assert "replace_range" not in page


def test_money_queue_has_page_size_range_and_navigation_controls() -> None:
    queue = [
        {
            "id": f"bill-{index}",
            "event_type": "outflow",
            "party": f"Vendor {index}",
            "due_date": (TODAY + timedelta(days=index % 20)).isoformat(),
            "open_amount_cents": 10_000 + index,
            "needs_action": True,
        }
        for index in range(12)
    ]
    page = _render([], _control_state(queue=queue))

    assert 'id="finance-queue-window"' in page
    assert 'id="finance-queue-page-size"' in page
    assert '<option value="25" selected>25</option>' in page
    assert '<option value="50">50</option>' in page
    assert '<option value="100">100</option>' in page
    assert 'role="group" aria-label="Money queue filters"' in page
    assert 'aria-pressed="true" data-queue-filter="needs-action"' in page
    assert 'role="tab"' not in page
    assert 'id="finance-queue-range"' in page
    assert 'id="finance-queue-previous"' in page
    assert 'id="finance-queue-page-summary"' in page
    assert 'id="finance-queue-next"' in page
    assert 'data-queue-date="' in page
    assert "renderQueuePage" in page
    assert "activeQueuePage = 1" in page


def test_zero_cash_snapshot_is_rendered_as_real_cash() -> None:
    page = _render([], _control_state(balance_cents=0), balance_cents=0)

    cash_section = page[page.index('aria-label="Cash position"'):page.index("Incoming 14 days")]
    assert "$0.00" in cash_section
    assert "Needs update" not in cash_section


def test_empty_queue_copy_keeps_exception_actions_available() -> None:
    page = _render([], _control_state(queue=[]))

    assert "No money decisions require attention in the selected window." in page
    assert "Add incoming" in page
    assert "Add payable" in page
    assert "Update money" in page


def test_bottom_review_guide_explains_cadence_reading_and_trust_rules() -> None:
    page = _render([], _control_state(queue=[]))

    queue_position = page.index('id="finance-queue"')
    guide_position = page.index('id="finance-review-guide"')
    drawer_position = page.index('id="finance-recommendation-drawer"')
    assert queue_position < guide_position < drawer_position

    guide = page[guide_position:drawer_position]
    assert "Run the money review in five minutes." in guide
    assert "Scan each workday" in guide
    assert "Mon + Fri" in guide
    assert "latest bank CSV for current cash" in guide
    assert "QBO open invoices for receivable balances" in guide
    assert "use manual entries only for exceptions" in guide
    assert "Cash on hand" in guide
    assert "Confirmed collections first" in guide
    assert "Overdue plus bills due in 14 days" in guide
    assert "configured cash floor" in guide
    assert "Clear Broken and Needs action first" in guide
    assert "never moves bank money" in guide
    assert "unpaid remainders stay open" in guide


def test_finance_stylesheet_is_cache_busted_by_release() -> None:
    with patch(
        "sales_support_agent.services.cashflow.cashflow_helpers._finance_css_version",
        return_value="release-123",
    ):
        page = _render([], _control_state(queue=[]))

    assert 'href="/static/finance.css?v=release-123"' in page


def test_renderer_falls_back_when_control_builder_fails() -> None:
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(0, "", ""),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=([], None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            side_effect=RuntimeError("control unavailable"),
        ),
    ):
        page = asyncio.run(render_cashflow_overview_page())

    assert "Needs update" in page
    assert "Low confidence" in page
    assert "Upload the latest bank CSV" in page
    assert "Money queue" in page


def test_queue_content_is_html_escaped() -> None:
    queue = [{
        "id": "unsafe",
        "event_type": "inflow",
        "party": "<script>alert(1)</script>",
        "due_date": (TODAY + timedelta(days=1)).isoformat(),
        "open_amount_cents": 100_00,
        "needs_action": True,
    }]
    page = _render([], _control_state(queue=queue))

    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page


def test_renderer_integrates_with_real_control_facade() -> None:
    rows = [
        {
            "id": "bank-balance",
            "record_kind": "transaction",
            "source": "csv",
            "event_type": "inflow",
            "category": "transfer",
            "amount_cents": 0,
            "account_balance_cents": 498_392,
            "due_date": BALANCE_AS_OF,
            "status": "posted",
            "confidence": "confirmed",
        },
        {
            "id": "rent",
            "record_kind": "obligation",
            "source": "clickup",
            "event_type": "outflow",
            "category": "rent",
            "vendor_or_customer": "Studio Rent",
            "amount_cents": 500_000,
            "due_date": (TODAY + timedelta(days=1)).isoformat(),
            "status": "planned",
            "confidence": "confirmed",
            "pay_priority": "must_pay",
            "flexibility": "fixed",
        },
    ]
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=rows),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(498_392, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=(rows, None),
        ),
    ):
        page = asyncio.run(render_cashflow_overview_page())

    assert "Studio Rent" in page
    assert "Cash trajectory" in page
    assert 'data-queue-filter="payables"' in page
