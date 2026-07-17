from datetime import date

from sqlalchemy import create_engine, text

from sales_support_agent.services.cashflow.reconciliation import (
    build_reconciliation_shadow,
    persist_reconciliation_shadow,
)
from sales_support_agent.models.database import init_database, create_session_factory
from sales_support_agent.services.cashflow.overview import (
    _reconciliation_shadow_html,
    _source_readiness_html,
)
from sales_support_agent.services.cashflow.control import build_finance_control_state


def _clickup_row(
    identifier: str,
    name: str,
    due_date: date,
    *,
    status: str = "overdue",
    recurring_rule: str = "biweekly",
) -> dict:
    return {
        "id": identifier,
        "source": "clickup",
        "record_kind": "obligation",
        "event_type": "outflow",
        "category": "payroll",
        "name": name,
        "vendor_or_customer": name,
        "amount_cents": 500_000,
        "due_date": due_date,
        "recurring_rule": recurring_rule,
        "status": status,
    }


def test_shadow_marks_older_open_recurring_occurrence_for_reconciliation_without_claiming_payment() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5)),
            _clickup_row("payroll-jul", "Payroll 5th", date(2026, 6, 19)),
        ],
        as_of=date(2026, 7, 15),
    )

    assert report["mode"] == "shadow"
    assert report["candidate_superseded_count"] == 1
    assert report["forecast_excluded_ids"] == ["payroll-jun"]
    assert report["supersession_review_count"] == 1
    assert report["review_records"][0]["id"] == "payroll-jun"
    assert report["review_records"][0]["later_occurrence_id"] == "payroll-jul"
    assert report["review_records"][0]["candidate_state"] == "recurrence_continuity_review"


def test_shadow_excludes_historical_schedule_residue_without_treating_it_as_payment_proof() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("benefits-jul", "Select Benefits", date(2026, 7, 4), recurring_rule="monthly"),
            _clickup_row("benefits-aug", "Select Benefits", date(2026, 8, 4), recurring_rule="monthly"),
        ],
        as_of=date(2026, 7, 15),
    )

    assert report["candidate_superseded_count"] == 1
    assert report["forecast_excluded_ids"] == ["benefits-jul"]
    assert report["supersession_review_count"] == 1
    assert report["review_records"][0]["id"] == "benefits-jul"
    assert report["review_records"][0]["candidate_state"] == "recurrence_continuity_review"


def test_shadow_omits_quarantined_clickup_duplicate() -> None:
    report = build_reconciliation_shadow(
        [
            {
                **_clickup_row("von-old", "Fulfillment Pay - Von", date(2026, 6, 22)),
                "source_status": "probable_duplicate",
                "match_status": "duplicate",
            },
            _clickup_row("von-current", "Fulfillment Pay - Von", date(2026, 7, 20)),
        ],
        as_of=date(2026, 7, 15),
    )

    assert report["supersession_review_count"] == 0
    assert report["review_records"] == []


def test_shadow_keeps_skipped_recurring_period_in_review() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("benefits-jun", "Select Benefits", date(2026, 6, 4)),
            _clickup_row("benefits-aug", "Select Benefits", date(2026, 8, 4)),
        ],
        as_of=date(2026, 8, 5),
    )

    assert report["candidate_superseded_count"] == 1
    assert report["supersession_review_count"] == 1
    assert report["review_records"][0]["candidate_state"] == "supersession_needs_review"


def test_shadow_infers_sustained_clickup_recurrence_when_cadence_metadata_is_missing() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("payroll-mar", "Payroll 5th", date(2026, 3, 5), recurring_rule=""),
            _clickup_row("payroll-mar-2", "Payroll 5th", date(2026, 3, 19), recurring_rule=""),
            _clickup_row("payroll-apr", "Payroll 5th", date(2026, 4, 2), recurring_rule=""),
            _clickup_row("payroll-apr-2", "Payroll 5th", date(2026, 4, 16), recurring_rule=""),
        ],
        as_of=date(2026, 4, 20),
    )

    assert report["candidate_superseded_count"] == 3
    assert report["forecast_excluded_ids"] == ["payroll-apr", "payroll-mar", "payroll-mar-2"]
    assert all(item["recurrence_inferred"] for item in report["review_records"])


def test_shadow_does_not_infer_a_recurring_schedule_from_two_similar_bills() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("one", "Special vendor charge", date(2026, 6, 1), recurring_rule=""),
            _clickup_row("two", "Special vendor charge", date(2026, 6, 15), recurring_rule=""),
        ],
        as_of=date(2026, 6, 20),
    )

    assert report["candidate_superseded_count"] == 0
    assert report["review_records"] == []


def test_shadow_does_not_mark_terminal_or_nonrecurring_rows_as_superseded() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5), status="completed"),
            _clickup_row("payroll-jul", "Payroll 5th", date(2026, 7, 5)),
            {
                **_clickup_row("rent-jun", "Rent", date(2026, 6, 5)),
                "recurring_rule": "",
            },
            {
                **_clickup_row("rent-jul", "Rent", date(2026, 7, 5)),
                "recurring_rule": "",
            },
        ],
        as_of=date(2026, 7, 15),
    )

    assert report["candidate_superseded_count"] == 0


def test_source_readiness_exposes_reconciliation_delta_without_claiming_settlement() -> None:
    rendered = _source_readiness_html(
        [],
        {
            "mode": "shadow",
            "candidate_superseded_count": 0,
            "candidate_superseded_cents": 0,
            "supersession_review_count": 2,
        },
    )

    assert "Reconciliation" in rendered
    assert "settlement evidence" in rendered


def test_reconciliation_review_names_candidates_without_offering_a_release_action() -> None:
    rendered = _reconciliation_shadow_html({
        "mode": "shadow",
        "candidate_superseded_count": 0,
        "supersession_review_count": 1,
        "review_records": [{
            "name": "Payroll 5th",
            "due_date": "2026-06-05",
            "later_due_date": "2026-07-05",
            "amount_cents": 500_000,
            "candidate_state": "recurrence_continuity_review",
        }],
    })

    assert "Payroll 5th" in rendered
    assert "2026-06-05" in rendered
    assert "2026-07-05" in rendered
    assert "$5,000" in rendered
    assert "settlement" in rendered
    assert "Review continuity" in rendered


def test_finance_control_excludes_historical_recurring_residue_from_required_cash() -> None:
    rows = [
        {
            **_clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5)),
            "account_balance_cents": None,
        },
        {
            **_clickup_row("payroll-jul", "Payroll 5th", date(2026, 6, 19)),
            "account_balance_cents": None,
        },
        {
            "id": "bank-balance",
            "source": "csv",
            "record_kind": "transaction",
            "event_type": "inflow",
            "amount_cents": 0,
            "due_date": date(2026, 7, 15),
            "status": "posted",
            "account_balance_cents": 1_000_000,
        },
    ]

    state = build_finance_control_state(rows, as_of=date(2026, 7, 15))

    assert state["metrics"]["required_outgoing_cents"] == 500_000
    assert state["reconciliation_shadow"]["candidate_superseded_count"] == 1
    assert state["reconciliation_shadow"]["supersession_review_count"] == 1
    historical = next(item for item in state["queue"]["items"] if item["id"] == "payroll-jun")
    assert historical["group"] == "reconcile_history"
    assert historical["open_amount_cents"] == 500_000


def test_shadow_report_persistence_is_idempotent_and_does_not_touch_cash_events() -> None:
    factory = create_session_factory("sqlite:///:memory:")
    init_database(factory)
    engine = factory.kw["bind"]
    report = build_reconciliation_shadow(
        [
            _clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5)),
            _clickup_row("payroll-jul", "Payroll 5th", date(2026, 6, 19)),
        ],
        as_of=date(2026, 7, 15),
    )

    first_id = persist_reconciliation_shadow(engine, report)
    second_id = persist_reconciliation_shadow(engine, report)

    assert first_id == second_id
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM finance_reconciliation_reports")).scalar_one() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM cash_events")).scalar_one() == 0
