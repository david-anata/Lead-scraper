from datetime import date

from sales_support_agent.services.cashflow.reconciliation import build_reconciliation_shadow
from sales_support_agent.services.cashflow.overview import _source_readiness_html
from sales_support_agent.services.cashflow.control import build_finance_control_state


def _clickup_row(identifier: str, name: str, due_date: date, *, status: str = "overdue") -> dict:
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
        "recurring_rule": "biweekly",
        "status": status,
    }


def test_shadow_identifies_older_open_recurring_occurrence_without_releasing_it() -> None:
    report = build_reconciliation_shadow(
        [
            _clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5)),
            _clickup_row("payroll-jul", "Payroll 5th", date(2026, 7, 5)),
        ],
        as_of=date(2026, 7, 15),
    )

    assert report["mode"] == "shadow"
    assert report["candidate_superseded_count"] == 1
    assert report["candidate_superseded_cents"] == 500_000
    assert report["candidates"][0]["id"] == "payroll-jun"
    assert report["candidates"][0]["later_occurrence_id"] == "payroll-jul"


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


def test_source_readiness_exposes_shadow_delta_without_changing_finance_values() -> None:
    rendered = _source_readiness_html(
        [],
        {
            "mode": "shadow",
            "candidate_superseded_count": 2,
            "candidate_superseded_cents": 750_000,
        },
    )

    assert "Reconciliation" in rendered
    assert "Cash is unchanged" in rendered
    assert "$7,500" in rendered


def test_finance_control_exposes_shadow_report_without_changing_required_cash() -> None:
    rows = [
        {
            **_clickup_row("payroll-jun", "Payroll 5th", date(2026, 6, 5)),
            "account_balance_cents": None,
        },
        {
            **_clickup_row("payroll-jul", "Payroll 5th", date(2026, 7, 5)),
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

    assert state["metrics"]["required_outgoing_cents"] == 1_000_000
    assert state["reconciliation_shadow"]["candidate_superseded_count"] == 1
