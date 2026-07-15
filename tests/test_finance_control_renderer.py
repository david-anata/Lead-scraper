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
        "trust_gate": {"ready": True, "summary": "Finance sources are current."},
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


def _render(
    rows: list[dict],
    state: dict,
    *,
    balance_cents: int = 498_392,
    balance_as_of: str = BALANCE_AS_OF,
    balance_source: str = "csv",
) -> str:
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=rows),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(balance_cents, balance_as_of, balance_source),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=(rows, None),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_finance_control_inputs",
            return_value=(None, None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            return_value=state,
        ) as builder,
    ):
        page = asyncio.run(render_cashflow_overview_page())
    builder.assert_called_once_with(
        rows, balance_cents, balance_as_of,
        smart_mode=True, settlement_annotations=None,
        income_decisions=None, source_connections=None,
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



def test_unconfigured_income_projection_precedes_generic_queue_cleanup() -> None:
    state = _control_state(queue=[])
    state["cash_position"]["incoming_confirmed_cents"] = 0
    state["cash_position"]["incoming_expected_cents"] = 0
    state["smart_brief"]["next"] = "Resolve missing amount."
    state["income_projection"] = {
        "status": "unconfigured",
        "posted_inflow_count": 165,
    }

    page = _render([], state)
    brief = page[page.index('aria-labelledby="smart-brief-title"'):page.index('aria-labelledby="trajectory-title"')]

    assert 'data-income-readiness="unconfigured"' in brief
    assert "Income forecast setup incomplete" in brief
    assert "Forecast income not configured." in brief
    assert "165 comparable posted bank inflows are actual history only." in brief
    assert "Review the CSV income projection first." in brief
    assert "Resolve missing amount." not in brief


def test_missing_cash_keeps_cash_verification_ahead_of_income_setup() -> None:
    state = _control_state(queue=[])
    state["cash_position"]["balance_available"] = False
    state["smart_brief"]["broken"] = "Current cash is missing."
    state["smart_brief"]["next"] = "Upload the latest bank CSV before reviewing anything else."
    state["income_projection"] = {
        "status": "unconfigured",
        "posted_inflow_count": 165,
    }

    page = _render([], state, balance_as_of="", balance_source="")
    brief = page[page.index('aria-labelledby="smart-brief-title"'):page.index('aria-labelledby="trajectory-title"')]

    assert "Current cash is missing." in brief
    assert "Upload the latest bank CSV before reviewing anything else." in brief
    assert "Income forecast setup incomplete" not in brief
    assert "Review the CSV income projection first." not in brief


def test_stale_cash_keeps_cash_verification_ahead_of_income_setup() -> None:
    stale_as_of = (TODAY - timedelta(days=4)).isoformat()
    state = _control_state(queue=[])
    state["smart_brief"]["broken"] = "Current cash is stale."
    state["smart_brief"]["next"] = "Upload a current bank CSV before reviewing anything else."
    state["income_projection"] = {
        "status": "unconfigured",
        "posted_inflow_count": 165,
    }

    page = _render([], state, balance_as_of=stale_as_of)
    brief = page[page.index('aria-labelledby="smart-brief-title"'):page.index('aria-labelledby="trajectory-title"')]

    assert "Current cash is stale." in brief
    assert "Upload a current bank CSV before reviewing anything else." in brief
    assert "Income forecast setup incomplete" not in brief
    assert "Review the CSV income projection first." not in brief


def test_income_card_and_trajectory_keep_income_sources_distinct() -> None:
    state = _control_state(queue=[])
    state["income_projection"] = {
        "status": "configured",
        "configured": True,
        "csv_trend_expected_cents": 300_000,
        "posted_inflow_count": 165,
    }

    page = _render([], state)
    incoming = page[page.index("Incoming 14 days"):page.index("Required out 14 days")]

    assert "$8,000" in incoming
    assert "Confirmed &middot; dated receivables" in incoming
    assert "Expected: $4,500" in incoming
    assert "$3,000 CSV trend" in incoming
    assert "$1,500 dated receivables" in incoming
    assert "Expected includes probability-weighted CSV recurring-deposit trends and dated receivables." in page
    assert "It is not committed cash." in page


def test_source_readiness_and_failed_trust_gate_precede_cash_decisions() -> None:
    state = _control_state(queue=[{
        "id": "rent-1", "event_type": "outflow", "party": "Studio Rent",
        "due_date": (TODAY + timedelta(days=1)).isoformat(), "open_amount_cents": 500_000,
        "action_label": "Pay now", "quick_actions": [
            {"action_type": "preview_cash_impact"}, {"action_type": "defer_or_change_date"},
            {"action_type": "mark_paid"},
        ],
    }])
    state["source_status"] = [
        {"source": "csv", "status": "ready", "detail": "Through today"},
        {"source": "clickup", "status": "stale", "detail": "Last sync failed"},
        {"source": "qbo", "status": "ready", "detail": "Open invoices current"},
    ]
    state["trust_gate"] = {
        "ready": False,
        "summary": "ClickUp plans are stale.",
        "failures": ["Planned AP and AR dates are not current."],
        "next_action": "Refresh ClickUp before making a cash decision.",
    }
    state["data_quality"] = {
        "quarantined_count": 2,
        "actionable_zero_obligation_count": 1,
    }

    page = _render([], state)

    positions = [
        page.index("Finance source readiness"),
        page.index('data-trust-ready="false"'),
        page.index('aria-label="Cash position"'),
    ]
    assert positions == sorted(positions)
    for label in ("Bank CSV", "ClickUp", "QBO"):
        assert label in page
    brief = page[page.index('aria-labelledby="smart-brief-title"'):page.index('aria-labelledby="trajectory-title"')]
    assert "Cash decisions are paused while finance source readiness is restored." in brief
    assert "Planned AP and AR dates are not current." in brief
    assert "Refresh ClickUp before making a cash decision." in brief
    assert "Review next action" in brief
    assert "Split rent into installments" not in page
    for label in ("Pay now", "Protect cash", "Defer / change date", "Mark paid"):
        assert label not in page
    assert "Review evidence" in page
    assert 'data-preview-action="Preview cash impact"' in page
    assert "CFO payment advice is unavailable until the trust gate passes." in page


def test_missing_trust_contract_fails_closed_and_reports_unavailable_cfo() -> None:
    state = _control_state(queue=[{
        "id": "rent-1", "event_type": "outflow", "party": "Studio Rent",
        "due_date": (TODAY + timedelta(days=1)).isoformat(), "open_amount_cents": 500_000,
        "action_label": "Protect cash",
    }])
    state.pop("trust_gate")

    page = _render([], state)

    assert 'data-trust-ready="false"' in page
    assert "CFO decision support unavailable" in page
    assert "Source readiness was not verified." in page
    assert "CFO payment advice is unavailable until the trust gate passes." in page
    assert "Protect cash" not in page
    assert "Pay now" not in page
    assert "Review evidence" in page


def test_renderer_fallback_fails_closed_and_source_statuses_are_not_ready() -> None:
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch("sales_support_agent.services.cashflow.overview._resolve_current_balance", return_value=(0, "", "")),
        patch("sales_support_agent.services.cashflow.overview._load_settlement_context", return_value=([], None)),
        patch("sales_support_agent.services.cashflow.control.build_finance_control", side_effect=RuntimeError("control unavailable")),
    ):
        page = asyncio.run(render_cashflow_overview_page())

    assert 'data-trust-ready="false"' in page
    assert "CFO decision support unavailable" in page
    assert "CFO payment advice is unavailable until the trust gate passes." in page
    assert "Bank CSV</strong><small>Status not reported.</small></div>\n            <span>Not reported</span>" in page
    assert "ClickUp</strong><small>Status not reported.</small></div>\n            <span>Ready</span>" not in page


def test_disconnected_or_failed_source_status_never_claims_ready() -> None:
    state = _control_state(queue=[])
    state["source_status"] = [
        {"source": "csv", "status": "disconnected", "detail": "Connection unavailable"},
        {"source": "clickup", "status": "ready", "ready": False, "detail": "Authorization failed"},
    ]

    page = _render([], state)

    assert "Connection unavailable</small></div>\n            <span>Disconnected</span>" in page
    assert "Authorization failed</small></div>\n            <span>Needs attention</span>" in page
    assert "Authorization failed</small></div>\n            <span>Ready</span>" not in page


def test_income_review_drawer_lists_pattern_evidence_and_decision_forms() -> None:
    state = _control_state(queue=[])
    state["income_projection"] = {
        "status": "inferred_review",
        "patterns": [{
            "pattern_key": "0123456789abcdef",
            "party": "Acme retainers",
            "decision": "review",
            "evidence": {
                "occurrence_dates": ["2026-05-01", "2026-06-01", "2026-07-01"],
                "median_cadence_days": 30,
                "projected_amount_cents": 320_000,
                "probability_bps": 5_000,
            },
        }],
    }

    page = _render([], state)
    incoming = page[page.index("Incoming 14 days"):page.index("Required out 14 days")]

    assert "Confirmed" in incoming
    assert "Expected" in incoming
    assert "Needs review" in incoming
    assert "$3,200" in incoming
    assert "Review income patterns" in incoming
    assert 'id="finance-income-review-drawer"' in page
    assert "May 01, 2026, Jun 01, 2026, Jul 01, 2026" in page
    assert "Every 30 days" in page
    assert "$3,200.00" in page
    assert "50%" in page
    action = 'action="/admin/finances/income-patterns/0123456789abcdef/decision"'
    assert page.count(action) == 3
    for value in ("track_expected", "one_time", "exclude"):
        assert f'name="decision" value="{value}"' in page
    for label in ("Track as expected", "One-time", "Exclude"):
        assert label in page


def test_inferred_queue_rows_expose_explain_action() -> None:
    queue = [{
        "id": "trend-income-1",
        "event_type": "inflow",
        "party": "Acme",
        "due_date": (TODAY + timedelta(days=2)).isoformat(),
        "open_amount_cents": 250_000,
        "trend_inferred": True,
    }]

    page = _render([], _control_state(queue=queue))

    assert 'data-preview-action="Explain"' in page


def test_income_review_drawer_has_loading_empty_and_error_safe_copy() -> None:
    expected = {
        "loading": "Income pattern review is loading.",
        "ready": "No income patterns need review.",
        "error": "Income pattern review is unavailable.",
    }
    for status, copy in expected.items():
        state = _control_state(queue=[])
        state["income_projection"] = {"status": status, "patterns": []}
        assert copy in _render([], state)


def test_renderer_passes_loaded_decisions_and_connections_to_control() -> None:
    decisions = [{"pattern_key": "0123456789abcdef", "decision": "exclude"}]
    connections = [{"source": "qbo", "status": "ready"}]
    state = _control_state(queue=[])
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch("sales_support_agent.services.cashflow.overview._resolve_current_balance", return_value=(498_392, BALANCE_AS_OF, "csv")),
        patch("sales_support_agent.services.cashflow.overview._load_settlement_context", return_value=([], None)),
        patch("sales_support_agent.services.cashflow.overview._load_finance_control_inputs", return_value=(decisions, connections)),
        patch("sales_support_agent.services.cashflow.control.build_finance_control", return_value=state) as builder,
    ):
        asyncio.run(render_cashflow_overview_page(settings=object()))

    builder.assert_called_once_with(
        [], 498_392, BALANCE_AS_OF,
        smart_mode=True,
        settlement_annotations=None,
        income_decisions=decisions,
        source_connections=connections,
    )

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
    assert 'action="/admin/finances/settings/cash-floor"' in page
    assert "Save floor" in page


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


def test_update_money_keeps_qbo_receivables_separate_from_cash_truth() -> None:
    page = _render([], _control_state(queue=[]))

    assert 'action="/admin/finances/sync-qbo-invoices"' in page
    assert "Refresh receivables" in page
    assert "Bank CSV remains the source of cash on hand." in page


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
    assert "latest bank CSV for current cash and actual history" in guide
    assert "Eligible recurring deposits can appear only as Expected" in guide
    assert "CSV does not create confirmed income" in guide
    assert "QBO open invoices for dated receivable balances" in guide
    assert "use manual entries only for exceptions" in guide
    assert "Cash on hand" in guide
    assert "Confirmed means a dated receivable" in guide
    assert "weighted CSV trends and unconfirmed dated receivables" in guide
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
