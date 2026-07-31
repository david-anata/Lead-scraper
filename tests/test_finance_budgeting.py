"""Budget and high spending review contracts."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from sales_support_agent.services.cashflow import budgeting


def _row(
    row_id: str,
    source: str,
    occurred: str,
    amount: int,
    *,
    category: str = "software",
    merchant: str = "Tool Co",
) -> dict:
    return {
        "id": row_id,
        "record_kind": "transaction",
        "source": source,
        "event_type": "outflow",
        "status": "posted",
        "amount_cents": amount,
        "due_date": occurred,
        "category": category,
        "vendor_or_customer": merchant,
    }


def test_budget_uses_one_canonical_source_and_does_not_double_count() -> None:
    rows = []
    for source in ("plaid", "qbo_bank", "csv"):
        rows.extend(
            _row(f"{source}-{month}", source, f"2026-{month}-10", 10_000)
            for month in ("01", "02", "03", "04", "05", "06", "07")
        )
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 20))
    assert view["source"] == "plaid"
    assert view["transaction_count"] == 7
    assert view["comparison_months"] == [
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"
    ]
    assert len(view["monthly_totals"]) == 6
    software = view["categories"][0]
    assert software["average_cents"] == 10_000
    assert software["target_cents"] == 8_500
    assert software["potential_saving_cents"] == 7_000
    assert software["recurring_saving_cents"] == 1_500


def test_budget_excludes_uncategorized_internal_share_transfers() -> None:
    rows = [
        _row(
            "transfer-1",
            "plaid",
            "2026-07-10",
            25_000_000,
            category="uncategorized",
            merchant="Withdrawal Trans, To Share 58",
        ),
        _row(
            "fee-1",
            "plaid",
            "2026-07-11",
            800,
            category="fees",
            merchant="Service fee",
        ),
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 30))

    assert [item["key"] for item in view["categories"]] == ["fees"]
    assert view["totals"]["current_cents"] == 800


def test_budget_excludes_credit_card_payments_and_named_internal_withdrawals() -> None:
    rows = [
        _row(
            "chase-1",
            "plaid",
            "2026-07-10",
            2_000_00,
            category="uncategorized",
            merchant="Payment to Chase",
        ),
        _row(
            "jpm-chase-1",
            "plaid",
            "2026-07-10",
            2_000_00,
            category="other",
            merchant="Payment to JPMorganChase",
        ),
        _row(
            "internal-1",
            "plaid",
            "2026-07-11",
            4_000_00,
            category="other",
            merchant="Home banking Withdrawal - Anata LLC",
        ),
        _row(
            "a2a-1",
            "plaid",
            "2026-07-11",
            5_000_00,
            category="other",
            merchant="Withdrawal Home A2A Transfer: ****5196",
        ),
        _row(
            "software-1",
            "plaid",
            "2026-07-12",
            2_000,
            category="software",
            merchant="Anthropic",
        ),
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))

    assert [item["key"] for item in view["categories"]] == ["software"]


def test_budget_recategorizes_existing_uncategorized_plaid_rows() -> None:
    rows = [
        _row(
            f"openai-{month}",
            "plaid",
            f"2026-{month}-10",
            20_000,
            category="uncategorized",
            merchant="OPENAI ChatGPT",
        )
        for month in ("01", "02", "03", "04", "05", "06", "07")
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))

    assert [item["key"] for item in view["categories"]] == ["software"]
    assert view["categories"][0]["potential_saving_cents"] == 3_000
    assert view["categories"][0]["recurring_saving_cents"] == 3_000


def test_budget_protects_ondeck_debt_from_savings_targets() -> None:
    rows = [
        _row(
            f"ondeck-{month}",
            "plaid",
            f"2026-{month}-10",
            50_000,
            category="uncategorized",
            merchant="OnDeck Capital",
        )
        for month in ("01", "02", "03", "04", "05", "06", "07")
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    debt = view["categories"][0]

    assert debt["key"] == "debt"
    assert debt["protected"] is True
    assert debt["recurring_saving_cents"] == 0


def test_protected_costs_are_not_given_a_cut_target() -> None:
    rows = [
        _row(f"rent-{month}", "plaid", f"2026-{month}-01", 200_000, category="rent")
        for month in ("01", "02", "03", "04", "05", "06", "07")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    rent = view["categories"][0]
    assert rent["protected"] is True
    assert rent["target_cents"] == rent["average_cents"]
    assert rent["potential_saving_cents"] == 0
    assert rent["recurring_saving_cents"] == 0


def test_six_month_review_detects_a_rising_category_trend() -> None:
    amounts = {
        "01": 10_000,
        "02": 10_000,
        "03": 10_000,
        "04": 20_000,
        "05": 20_000,
        "06": 20_000,
        "07": 20_000,
    }
    rows = [
        _row(f"software-{month}", "plaid", f"2026-{month}-10", amount)
        for month, amount in amounts.items()
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    software = view["categories"][0]

    assert software["earlier_average_cents"] == 10_000
    assert software["recent_average_cents"] == 20_000
    assert software["trend_direction"] == "up"
    assert software["trend_bps"] == 10_000
    assert software["recurring_saving_cents"] == 2_250


def test_llm_can_only_prioritize_deterministic_categories(monkeypatch) -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", 10_000)
        for month in ("01", "02", "03", "04", "05", "06", "07")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    stored: dict = {}
    monkeypatch.setattr(budgeting, "load_budget_view", lambda: view)
    monkeypatch.setattr(budgeting, "kv_get_json", lambda _key: None)
    monkeypatch.setattr(budgeting, "kv_set_json", lambda _key, value: stored.update(value))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    monkeypatch.setattr(
        budgeting,
        "_call_anthropic",
        lambda *_args: {
            "summary": "Cut $999 of unused software first.",
            "recommendations": [
                {
                    "category_key": "software",
                    "headline": "Audit 100 software seats",
                    "reason": "Spending is $999 above target.",
                    "next_action": "Cancel 20 unused seats.",
                    "confidence": "high",
                },
                {
                    "category_key": "invented",
                    "headline": "Invented",
                    "reason": "Invented",
                    "next_action": "Invented",
                    "confidence": "high",
                },
                {
                    "category_key": "software",
                    "headline": "Duplicate software advice",
                    "reason": "Duplicate software advice",
                    "next_action": "Duplicate software advice",
                    "confidence": "medium",
                },
            ],
        },
    )
    result = budgeting.run_budget_review(SimpleNamespace(), force=True)
    assert len(result["recommendations"]) == 1
    assert result["recommendations"][0]["potential_saving_cents"] == 1_500
    assert result["recommendations"][0]["recurring_saving_cents"] == 1_500
    assert result["summary"].startswith(
        "The six-month review supports $15.00 in recurring monthly savings targets"
    )
    assert result["recommendations"][0]["headline"] == "Review software commitments"
    assert "$999" not in result["recommendations"][0]["reason"]
    assert result["recommendations"][0]["next_action"].startswith(
        "Confirm which software costs"
    )
    assert stored["calculation_id"] == view["calculation_id"]


def test_budget_page_is_explicitly_advisory_and_explainable() -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", 10_000)
        for month in ("01", "02", "03", "04", "05", "06", "07")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    page = budgeting.render_budget_page(
        view, {"status": "empty", "recommendations": []}
    )
    assert "Stop the monthly cash leak" in page
    assert "Six-month monthly average" in page
    assert "Recurring savings target" in page
    assert "What operating spending is doing" in page
    assert "What should we cut or renegotiate?" in page
    assert "Run high spending review" in page
    assert "planning targets, not changes to your bank or books" in page
    assert "Mirrored sources and internal transfers are excluded" in page
