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
    assert view["earliest_date"]
    assert view["latest_date"]
    assert view["coverage_days"] >= 1
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


def test_budget_protects_generic_checks_from_the_vendor_trim_list() -> None:
    rows = [
        _row(
            "payroll-check-1", "plaid", "2026-07-11", 125_000,
            category="uncategorized", merchant="Check #1842",
        ),
        _row(
            "software-1", "plaid", "2026-06-12", 2_000,
            category="software", merchant="Anthropic",
        ),
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    by_key = {item["key"]: item for item in view["categories"]}

    assert by_key["manual_check"]["protected"] is True
    assert [item["display_name"] for item in view["trim_items"]] == ["Anthropic"]


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


def test_budget_protects_dominion_and_classifies_generic_intuit_as_software() -> None:
    rows = []
    for month in ("01", "02", "03", "04", "05", "06", "07"):
        rows.extend(
            [
                _row(
                    f"dominion-{month}",
                    "plaid",
                    f"2026-{month}-10",
                    20_000,
                    category="uncategorized",
                    merchant="Dominion Energy",
                ),
                _row(
                    f"intuit-{month}",
                    "plaid",
                    f"2026-{month}-12",
                    10_000,
                    category="uncategorized",
                    merchant="Intuit",
                ),
            ]
        )

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    by_key = {item["key"]: item for item in view["categories"]}

    assert by_key["utilities"]["protected"] is True
    assert by_key["utilities"]["recurring_saving_cents"] == 0
    assert by_key["software"]["protected"] is False
    assert by_key["software"]["recurring_saving_cents"] == 1_500


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
    assert software["recurring_saving_cents"] == 3_000


def test_deep_review_finds_a_rising_vendor() -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", amount, merchant="Seat Tool")
        for month, amount in {
            "01": 10_000, "02": 10_000, "03": 10_000,
            "04": 20_000, "05": 20_000, "06": 20_000,
        }.items()
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    finding = view["investigations"][0]
    assert finding["kind"] == "rising_vendor"
    assert finding["merchant"] == "Seat Tool"
    assert finding["monthly_review_cents"] == 10_000


def test_deep_review_finds_new_recurring_and_duplicate_looking_spend() -> None:
    rows = [
        _row("new-04", "plaid", "2026-04-10", 8_000, merchant="New Tool"),
        _row("new-05", "plaid", "2026-05-10", 8_000, merchant="New Tool"),
        _row("dup-1", "plaid", "2026-06-12", 4_000, merchant="Double Bill"),
        _row("dup-2", "plaid", "2026-06-12", 4_000, merchant="Double Bill"),
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    by_kind = {item["kind"]: item for item in view["investigations"]}
    assert by_kind["new_recurring"]["monthly_review_cents"] == 5_333
    assert by_kind["duplicate_looking"]["one_time_review_cents"] == 4_000


def test_trim_list_includes_every_controllable_vendor_ranked_by_spend() -> None:
    rows = [
        _row("elementor-1", "plaid", "2026-05-10", 9_900, merchant="Elementor"),
        _row("elementor-2", "plaid", "2026-06-10", 9_900, merchant="Elementor"),
        _row("small-1", "plaid", "2026-03-10", 2_000, merchant="Small Tool"),
        _row("rent-1", "plaid", "2026-03-01", 200_000, merchant="Building", category="rent"),
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))

    assert [item["display_name"] for item in view["trim_items"]] == ["Elementor", "Small Tool"]
    assert view["trim_items"][0]["cadence"] == "irregular"
    assert view["trim_items"][0]["monthly_potential_cents"] == 0
    assert view["trim_items"][1]["cadence"] == "one_time"
    assert view["trim_items"][1]["monthly_potential_cents"] == 0
    assert len(view["trim_items"][0]["opportunity_key"]) == 64
    assert view["trim_items"][0]["review_state"] == "unknown"


def test_one_time_purchase_does_not_create_recurring_savings() -> None:
    rows = [
        _row("one-time-1", "plaid", "2026-06-10", 600_000, merchant="One Time Build"),
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))

    assert view["trim_items"][0]["cadence"] == "one_time"
    assert view["categories"][0]["recurring_average_cents"] == 0
    assert view["categories"][0]["recurring_saving_cents"] == 0


def test_stopped_vendor_moves_to_inactive_history_and_not_recurring_savings() -> None:
    rows = [
        _row("old-1", "plaid", "2026-01-10", 9_900, merchant="Cancelled Tool"),
        _row("old-2", "plaid", "2026-02-10", 9_900, merchant="Cancelled Tool"),
    ]

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))

    assert view["trim_items"][0]["cadence"] == "inactive"
    assert view["trim_items"][0]["monthly_potential_cents"] == 0
    assert view["categories"][0]["recurring_average_cents"] == 0
    assert view["categories"][0]["recurring_saving_cents"] == 0


def test_recurring_savings_exclude_reductions_already_reflected_this_month() -> None:
    rows = [
        _row(f"software-{month}", "plaid", f"2026-{month}-10", 20_000)
        for month in ("01", "02", "03", "04", "05", "06")
    ]
    rows.append(_row("software-07", "plaid", "2026-07-10", 5_000))

    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    software = view["categories"][0]

    assert software["historical_reduction_cents"] == 3_000
    assert software["projected_cents"] < software["target_cents"]
    assert software["recurring_saving_cents"] == 0


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
    assert "Potential monthly savings" in page
    assert "Cancellation in progress" in page
    assert "Bank-verified monthly savings" in page
    assert "Possible EOM improvement" in page
    assert "What operating spending is doing" in page
    assert "What should we cut or renegotiate?" in page
    assert "Where the deeper savings may be hiding" in page
    assert "Monthly trim brief" in page
    assert "What may improve month-end cash" in page
    assert "Potential and in-progress cuts remain scenarios" in page
    assert "Decide what stays and what goes" in page
    assert "Needed" in page and "Unknown" in page and "Investigate" in page and "Waste" in page
    assert "Save all changes" in page
    assert "Discard draft" in page
    assert "localStorage" in page
    assert "beforeunload" in page
    assert "Recovered ${restored} unsaved change" in page
    assert "has-unsaved-changes" in page
    assert 'data-trim-batch-form' in page
    assert "save immediately" not in page
    assert "Run high spending review" in page
    assert "planning targets, not changes to your bank or books" in page
    assert "Mirrored sources and internal transfers are excluded" in page


def test_budget_page_defaults_to_five_highest_impact_unresolved_vendors() -> None:
    rows = []
    for vendor_number in range(20):
        for month in ("04", "05", "06"):
            rows.append(_row(
                f"vendor-{vendor_number}-{month}", "plaid", f"2026-{month}-10",
                (vendor_number + 1) * 1_000, merchant=f"Vendor {vendor_number}",
            ))
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    view["trim_items"][0]["review_state"] = "needed"

    page = budgeting.render_budget_page(
        view, {"status": "empty", "recommendations": []}
    )

    assert 'data-trim-filter="needs_decision" class="is-active"' in page
    assert "Needs decision <strong>5</strong>" in page
    assert "Saved keep decisions leave this queue" in page


def test_recurring_classifier_quarantines_probable_duplicates() -> None:
    rows = [
        _row("tool-04", "plaid", "2026-04-10", 9_900, merchant="Tool Co"),
        _row("tool-05", "plaid", "2026-05-10", 9_900, merchant="Tool Co"),
        _row("tool-06-a", "plaid", "2026-06-10", 9_900, merchant="Tool Co"),
        _row("tool-06-b", "plaid", "2026-06-10", 9_900, merchant="Tool Co"),
    ]

    item = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))["trim_items"][0]

    assert item["cadence"] == "uncertain"
    assert item["probable_duplicate_cents"] == 9_900
    assert item["monthly_potential_cents"] == 0


def test_annual_cost_requires_an_annual_comparison_window() -> None:
    rows = [
        _row("annual-2025", "plaid", "2025-06-15", 120_000, merchant="Annual Tool"),
        _row("annual-2026", "plaid", "2026-06-15", 120_000, merchant="Annual Tool"),
    ]

    item = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))["trim_items"][0]

    assert item["cadence"] == "annual"
    assert item["monthly_potential_cents"] == 10_000
    assert item["evidence_dates"] == ["2025-06-15", "2026-06-15"]


def test_vendor_review_is_full_page_with_posted_evidence() -> None:
    rows = [
        _row(f"tool-{month}", "plaid", f"2026-{month}-10", 9_900, merchant="Tool Co")
        for month in ("04", "05", "06")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    item = view["trim_items"][0]
    item["transactions"][0]["account"] = "plaid-account-token-that-must-not-render"

    page = budgeting.render_budget_vendor_page(view, item["opportunity_key"])

    assert "See the bank evidence, then take one clear next step" in page
    assert "Bank description" in page
    assert "Connected bank account" in page
    assert "plaid-account-token-that-must-not-render" not in page
    assert '<option value="unknown" selected>Unknown</option>' in page
    assert "Save this decision" in page
    assert "side drawer" not in page


def test_monthly_brief_omits_zero_value_nonrecurring_waste() -> None:
    rows = [
        _row("one-off", "plaid", "2026-06-10", 9_900, merchant="Past One Off")
    ]
    view = budgeting.build_budget_view(rows, as_of=date(2026, 7, 31))
    item = view["trim_items"][0]
    item["review_state"] = "waste"

    page = budgeting.render_budget_page(
        view, {"status": "empty", "recommendations": []}
    )

    assert "$0.00 per month awaits cancellation work" not in page
