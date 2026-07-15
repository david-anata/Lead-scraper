"""Read-only Savings Opportunities renderer contract."""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from sales_support_agent.services.cashflow.overview import render_cashflow_overview_page


TODAY = date.today()
BALANCE_AS_OF = TODAY.isoformat()


def _control_state() -> dict:
    return {
        "cash_position": {
            "cash_on_hand_cents": 500_000,
            "balance_available": True,
            "incoming_confirmed_cents": 200_000,
            "incoming_expected_cents": 100_000,
            "required_out_cents": 300_000,
            "exposure_out_cents": 0,
            "safe_to_commit_cents": 0,
            "funding_gap_cents": 600_000,
            "floor_cents": 1_000_000,
        },
        "smart_brief": {
            "happening": "Cash and bills are current.",
            "broken": "The floor is at risk.",
            "next": "Review the required payments.",
        },
        "forecast": {
            "labels": [BALANCE_AS_OF, (TODAY + timedelta(days=1)).isoformat()],
            "actual": [500_000, None],
            "committed": [500_000, 200_000],
            "expected": [500_000, 300_000],
            "stress": [500_000, -100_000],
            "floor_cents": 1_000_000,
        },
        "queue": {"items": []},
        "recommendation": {
            "title": "Protect the cash floor",
            "why": "Required payments exceed current cash.",
            "before_minimum_cash_cents": -100_000,
            "after_minimum_cash_cents": 100_000,
            "depends_on": "Posted bank cash",
            "confidence": "High",
            "limitations": "No forecast mutation",
            "downside": "A payment remains open.",
            "action_label": "Review required payments",
        },
    }


def _opportunity(index: int, **overrides: object) -> dict:
    result = {
        "key": f"save-{index}",
        "display_name": f"Vendor {index}",
        "reason": f"{index + 3} comparable posted charges",
        "next_expected": f"Jul {20 + index}, 2026",
        "potential": f"${40 + index}/month",
        "horizon": "monthly",
        "one_time_cents": None,
        "monthly_cents": (40 + index) * 100,
        "annual_cents": (40 + index) * 1200,
        "observed_90d_cents": None,
        "confidence": "High",
        "freshness": "Bank CSV through Jul 14",
        "evidence": [f"2026-06-{10 + index:02d} ${(40 + index):.2f} - Posted outflow"],
        "calculation": f"Median posted charge is ${40 + index}.",
        "cash_effect_cents": (40 + index) * 100,
        "limitations": "Contract terms and usage are unknown.",
        "downside": "The service may support active work.",
        "source_url": f"/admin/finances/source/{index}",
        "protected": False,
        "conflicted": False,
    }
    result.update(overrides)
    return result


def _render(savings: dict) -> str:
    with (
        patch.dict("os.environ", {"FINANCE_SAVINGS_MODE": "live"}),
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(500_000, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=([], None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            return_value=_control_state(),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_savings_renderer_state",
            return_value=savings,
        ),
    ):
        return asyncio.run(render_cashflow_overview_page())


def test_savings_release_gate_defaults_to_live() -> None:
    with patch.dict("os.environ", {}, clear=True):
        page = _render_with_release_default(
            {"status": "ready", "opportunities": [_opportunity(0)], "total_count": 1}
        )

    assert "Savings checks are validating in shadow mode." not in page
    assert 'data-savings-review="save-0"' in page


def test_cash_floor_load_failure_hides_safe_to_commit_and_savings() -> None:
    with (
        patch.dict("os.environ", {"FINANCE_SAVINGS_MODE": "live"}),
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(500_000, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=([], []),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            side_effect=RuntimeError("floor unavailable"),
        ),
    ):
        page = asyncio.run(render_cashflow_overview_page())

    assert "Cash floor unavailable" in page
    assert "Current: Unavailable" in page
    assert "Savings review is unavailable." in page
    assert 'data-savings-review="' not in page


def _render_with_release_default(savings: dict) -> str:
    with (
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=[]),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(500_000, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=([], None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            return_value=_control_state(),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_savings_renderer_state",
            return_value=savings,
        ),
    ):
        return asyncio.run(render_cashflow_overview_page())


def test_savings_section_follows_money_queue_and_precedes_guide() -> None:
    page = _render({"status": "ready", "opportunities": [_opportunity(0)], "total_count": 1})

    queue_position = page.index('id="finance-queue"')
    savings_position = page.index('id="finance-savings"')
    guide_position = page.index('id="finance-review-guide"')
    assert queue_position < savings_position < guide_position
    assert "Potential only" in page
    assert "Potential &middot; not realized" in page
    assert "This scenario is not applied to the Finance forecast." in page


def test_savings_rows_show_top_three_then_expand_in_place_to_ten() -> None:
    opportunities = [_opportunity(index) for index in range(10)]
    page = _render({"status": "ready", "opportunities": opportunities, "total_count": 14})
    savings = page[page.index('id="finance-savings"'):page.index('id="finance-review-guide"')]

    assert savings.count('data-savings-review="') == 10
    assert savings.count('data-savings-extra="true" hidden') == 7
    assert "Show 10 of 14" in savings
    assert "Showing the strongest 10 of 14 opportunities." in savings
    assert "data-expand-savings" in savings
    assert "Show top 3" in page


def test_savings_horizons_remain_separate() -> None:
    opportunities = [
        _opportunity(0),
        _opportunity(
            1,
            potential="$125 one-time",
            horizon="one-time",
            monthly_cents=None,
            annual_cents=None,
            one_time_cents=12_500,
        ),
        _opportunity(
            2,
            potential="$90/90 days",
            horizon="90-day",
            monthly_cents=None,
            annual_cents=None,
            observed_90d_cents=9_000,
        ),
    ]
    page = _render({"status": "ready", "opportunities": opportunities, "total_count": 3})

    assert "Up to $40/month recurring" in page
    assert "$125 one-time" in page
    assert "$90 observed in 90 days" in page
    assert "$255" not in page


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"status": "empty", "opportunities": [], "total_count": 0}, "No evidence-backed savings opportunities need review."),
        ({"status": "loading", "opportunities": [], "total_count": 0}, "Finding savings opportunities."),
        ({"status": "insufficient_history", "opportunities": [], "total_count": 0}, "Upload at least 90 days with three comparable charges."),
        ({"status": "error", "opportunities": [], "total_count": 0}, "Savings review is unavailable."),
    ],
)
def test_savings_section_isolates_non_ready_states(state: dict, expected: str) -> None:
    page = _render(state)

    assert expected in page
    assert "Cash trajectory" in page
    assert "Money queue" in page


def test_stale_savings_retains_evidence_but_suppresses_cash_effect() -> None:
    page = _render({"status": "stale", "opportunities": [_opportunity(0)], "total_count": 1})

    assert "Estimates are stale." in page
    assert "Cash impact is unavailable until sources are refreshed." in page
    assert "Unavailable until cash is current." in page
    assert "Bank CSV through Jul 14" in page


def test_smart_off_placeholder_and_read_only_drawer_accessibility_are_wired() -> None:
    page = _render({"status": "ready", "opportunities": [_opportunity(0)], "total_count": 1})

    assert 'class="finance-savings__off smart-off-only"' in page
    assert "Turn on Smart mode" in page
    assert 'role="dialog" aria-modal="true" tabindex="-1"' in page
    assert "pageContent.inert = true" in page
    assert "event.key === 'Escape'" in page
    assert "drawerOpener" in page
    assert "previousOpener.focus()" in page
    assert "drawerFacts.replaceChildren()" in page
    assert 'id="finance-drawer-source"' in page
    assert "Potential only. Finance records review decisions but never cancels a service or changes the forecast." in page
    assert "Create ClickUp review task" in page
    assert 'id="finance-live-region"' in page


def test_savings_content_is_html_escaped() -> None:
    page = _render({
        "status": "ready",
        "opportunities": [_opportunity(0, display_name="<script>alert(1)</script>")],
        "total_count": 1,
    })

    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
    assert "\\u003cscript>alert(1)\\u003c/script>" in page


def test_renderer_consumes_the_deterministic_savings_engine_contract() -> None:
    rows = []
    for index in range(4):
        posted = TODAY - timedelta(days=30 * (3 - index))
        rows.append({
            "id": f"design-{index}",
            "source_id": f"design-{index}",
            "record_kind": "transaction",
            "source": "csv",
            "status": "posted",
            "event_type": "outflow",
            "category": "software",
            "vendor_or_customer": "Design Tool",
            "amount_cents": 24_900,
            "due_date": posted.isoformat(),
        })
    rows.append({
        "id": "design-obligation",
        "record_kind": "obligation",
        "source": "clickup",
        "event_type": "outflow",
        "category": "software",
        "vendor_or_customer": "Design Tool",
        "amount_cents": 24_900,
        "due_date": TODAY.isoformat(),
        "pay_priority": "can_hold",
    })

    with (
        patch.dict("os.environ", {"FINANCE_SAVINGS_MODE": "live"}),
        patch("sales_support_agent.services.cashflow.overview.list_obligations", return_value=rows),
        patch(
            "sales_support_agent.services.cashflow.overview._resolve_current_balance",
            return_value=(500_000, BALANCE_AS_OF, "csv"),
        ),
        patch(
            "sales_support_agent.services.cashflow.overview._load_settlement_context",
            return_value=(rows, None),
        ),
        patch(
            "sales_support_agent.services.cashflow.control.build_finance_control",
            return_value=_control_state(),
        ),
    ):
        page = asyncio.run(render_cashflow_overview_page())

    assert 'data-savings-state="ready"' in page
    assert "Design Tool" in page
    assert "$249/month" in page
    assert "Bank CSV through" in page
    assert "Open bank source" in page
