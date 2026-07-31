"""Trust and presentation contracts for the replacement Finance experience."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

from sales_support_agent.services.cashflow.money_brief import (
    build_finance_brief,
    render_calculation_page,
    render_cash_plan_page,
    render_money_brief_page,
)
from sales_support_agent.services.cashflow.review_page import render_review_preview


def _state(*, trust_ready: bool = True) -> dict:
    return {
        "cash": {
            "cash_on_hand_cents": 2_661_227,
            "balance_available": True,
            "incoming_confirmed_cents": 2_764_500,
            "incoming_expected_cents": 205_600,
            "required_out_cents": 1_673_200,
            "expected_out_cents": 411_200,
            "floor_cents": 1_000_000,
        },
        "trust_gate": {
            "ready": trust_ready,
            "issues": [] if trust_ready else ["One payment has conflicting evidence."],
        },
        "data_quality": {
            "summary": "185 records excluded; 0 open items need amounts.",
        },
        "collections": {
            "next_collection": {"party": "Divi Energy"},
        },
    }


def _brief(*, trust_ready: bool = True):
    state = _state(trust_ready=trust_ready)
    with (
        patch(
            "sales_support_agent.services.cashflow.money_brief._build_renderer_state",
            return_value=({}, {}, False),
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._normalise_renderer_state",
            return_value=state,
        ),
        patch(
            "sales_support_agent.services.cashflow.bulk_resolve.list_review_items",
            return_value={"total": 1 if not trust_ready else 0, "groups": []},
        ),
    ):
        return build_finance_brief(
            rows=[],
            balance_cents=2_661_227,
            balance_as_of="2026-07-30",
            balance_source="plaid",
        )


def test_brief_keeps_evidence_classes_separate_and_uses_plain_formulas() -> None:
    brief = _brief()
    assert brief.amount("cash").evidence_class == "verified"
    assert brief.amount("confirmed_in").evidence_class == "confirmed"
    assert brief.amount("expected_in").evidence_class == "expected"
    assert [item.cents for item in brief.outlooks] == [
        576_827,   # cash - confirmed out - expected out
        3_341_327, # conservative + confirmed incoming
        3_546_927, # likely + expected incoming
    ]
    assert brief.outlooks[0].formula == "Verified cash − confirmed out − expected out"


def test_same_evidence_produces_same_calculation_id() -> None:
    assert _brief().calculation_id == _brief().calculation_id


def test_live_brief_uses_spendable_plaid_cash_not_reserves() -> None:
    from sales_support_agent.services.cashflow.money_brief import load_finance_brief

    with (
        patch(
            "sales_support_agent.services.cashflow.money_brief.list_obligations",
            return_value=[],
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._load_settlement_context",
            return_value=([], []),
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._resolve_current_balance",
            return_value=(2_554_402, "2026-07-30", "plaid"),
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief.load_accounts_overview",
            return_value={
                "spendable_cents": 2_552_539,
                "reserve_cents": 1_863,
                "account_count": 4,
                "as_of": "2026-07-30",
            },
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._load_finance_control_inputs",
            return_value=(None, None),
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._build_renderer_state",
            return_value=({}, {}, False),
        ),
        patch(
            "sales_support_agent.services.cashflow.money_brief._normalise_renderer_state",
            return_value=_state(),
        ) as normalise,
    ):
        normalise.return_value["cash"]["cash_on_hand_cents"] = 2_552_539
        brief = load_finance_brief(object())

    assert brief.amount("cash").cents == 2_552_539


def test_today_is_a_five_number_brief_without_old_dashboard_surfaces() -> None:
    page = render_money_brief_page(_brief(trust_ready=False))
    for label in (
        "Verified cash now",
        "Confirmed money in",
        "Expected money in",
        "Confirmed money out",
        "Expected money out",
    ):
        assert label in page
    assert "Three honest possibilities" in page
    assert "One payment has conflicting evidence." in page
    assert "Money queue" not in page
    assert "Smart brief" not in page
    assert "Savings opportunities" not in page
    assert "finance-recommendation-drawer" not in page


def test_excluded_history_does_not_promise_an_empty_review_queue() -> None:
    brief = replace(_brief(trust_ready=False), review_count=0)
    page = render_money_brief_page(brief)
    assert "Calculated with exclusions" in page
    assert "No daily review cases" in page
    assert ">Open Review<" not in page


def test_calculation_page_explains_sources_and_rules_without_actions() -> None:
    page = render_calculation_page(_brief())
    assert "See exactly how it was calculated" in page
    assert "Plaid connected bank" in page
    assert "Verified cash − confirmed out − expected out" in page
    assert "Nothing on this page changes your bank, books, or forecast." in page
    assert "<form" not in page


def test_cash_plan_is_explicitly_read_only() -> None:
    page = render_cash_plan_page(_brief())
    assert "Plan without changing your books" in page
    assert "Read-only planning" in page
    assert "These scenarios do not edit Plaid, QuickBooks, schedules, bills, or invoices." in page


def test_review_preview_is_a_full_page_and_explicitly_does_not_save() -> None:
    page = render_review_preview(
        {
            "action": "no_action_needed",
            "action_label": "No longer owed",
            "eligible": [{"id": "evt-1", "name": "Old vendor bill", "amount_cents": 12500}],
            "eligible_count": 1,
            "amount_cents": 12500,
            "skipped_count": 0,
            "skipped_protected": [],
        }
    )
    assert "Check this answer before saving" in page
    assert "Nothing has changed yet" in page
    assert "Cancel — change nothing" in page
    assert "Confirm no longer owed" in page
    assert "finance-recommendation-drawer" not in page
