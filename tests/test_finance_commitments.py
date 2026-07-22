from datetime import date

import pytest

from sales_support_agent.services.cashflow.commitments import (
    preview_transition,
    validate_commitment_fields,
)


def test_protected_commitment_requires_approval_by_default():
    result = validate_commitment_fields({
        "commitment_type": "payroll",
        "amount_cents": 100_000,
        "due_date": date(2026, 8, 5),
    })
    assert result["approval_status"] == "pending"


def test_llm_or_form_cannot_supply_unknown_workflow_state():
    with pytest.raises(ValueError, match="Unsupported workflow status"):
        validate_commitment_fields({"workflow_status": "paid_by_ai", "amount_cents": 1})


def test_transition_preview_never_claims_to_move_money():
    preview = preview_transition(
        {"id": "c1", "workflow_status": "draft", "commitment_type": "payable"},
        "needs_review",
    )
    assert preview["changes_cash"] is False
    assert preview["requires_confirmation"] is True


def test_protected_commitment_cannot_be_scheduled_without_approval():
    with pytest.raises(ValueError, match="explicit approval"):
        preview_transition(
            {
                "id": "payroll-1",
                "workflow_status": "approved",
                "commitment_type": "payroll",
                "approval_status": "pending",
            },
            "scheduled",
        )


def test_bank_verified_requires_settlement_evidence():
    with pytest.raises(ValueError, match="posted settlement evidence"):
        preview_transition(
            {
                "id": "bill-1",
                "workflow_status": "scheduled",
                "commitment_type": "payable",
                "settlement_evidence_available": False,
            },
            "bank_verified",
        )
