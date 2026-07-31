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
            [
                _row(f"{source}-apr", source, "2026-04-10", 10_000),
                _row(f"{source}-may", source, "2026-05-10", 10_000),
                _row(f"{source}-jun", source, "2026-06-10", 10_000),
                _row(f"{source}-jul", source, "2026-07-10", 10_000),
            ]
        )
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 20))
    assert view["source"] == "plaid"
    assert view["transaction_count"] == 4
    software = view["categories"][0]
    assert software["average_cents"] == 10_000
    assert software["target_cents"] == 8_500
    assert software["potential_saving_cents"] == 1_500


def test_protected_costs_are_not_given_a_cut_target() -> None:
    rows = [
        _row(f"rent-{month}", "plaid", f"2026-{month}-01", 200_000, category="rent")
        for month in ("04", "05", "06", "07")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    rent = view["categories"][0]
    assert rent["protected"] is True
    assert rent["target_cents"] == rent["average_cents"]
    assert rent["potential_saving_cents"] == 0


def test_llm_can_only_prioritize_deterministic_categories(monkeypatch) -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", 10_000)
        for month in ("04", "05", "06", "07")
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
            "summary": "Cut unused software first.",
            "recommendations": [
                {
                    "category_key": "software",
                    "headline": "Audit software seats",
                    "reason": "Posted software spending is stable.",
                    "next_action": "List owners and cancel unused seats.",
                    "confidence": "high",
                },
                {
                    "category_key": "invented",
                    "headline": "Invented",
                    "reason": "Invented",
                    "next_action": "Invented",
                    "confidence": "high",
                },
            ],
        },
    )
    result = budgeting.run_budget_review(SimpleNamespace(), force=True)
    assert len(result["recommendations"]) == 1
    assert result["recommendations"][0]["potential_saving_cents"] == 1_500
    assert stored["calculation_id"] == view["calculation_id"]


def test_budget_page_is_explicitly_advisory_and_explainable() -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", 10_000)
        for month in ("04", "05", "06", "07")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    page = budgeting.render_budget_page(
        view, {"status": "empty", "recommendations": []}
    )
    assert "Stop the monthly cash leak" in page
    assert "Possible EOM cash improvement" in page
    assert "What and where can we save?" in page
    assert "Run high spending review" in page
    assert "planning targets, not changes to your bank or books" in page
    assert "Mirrored sources are excluded" in page
