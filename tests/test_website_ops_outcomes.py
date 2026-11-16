from sales_support_agent.services.website_ops_outcomes import (
    ACTIVE_OUTCOME,
    FAILED_OUTCOME,
    NO_OPPORTUNITY_OUTCOME,
    VERIFIED_OUTCOME,
    classify_run_outcome,
    in_progress_without_production_delta,
    persisted_run_status,
)


def test_verified_production_change_requires_durable_evidence() -> None:
    outcome = classify_run_outcome(
        {
            "executed_actions": [
                {
                    "verification_status": "verified",
                    "commit_sha": "abc123",
                    "production_url": "https://anatainc.com/blog/example",
                }
            ],
            "operations_summary": {"executed_actions": 1},
        }
    )
    assert outcome["status"] == VERIFIED_OUTCOME
    assert outcome["production_delta_count"] == 1
    assert outcome["last_stage"] == "production_verification"


def test_completed_execution_without_production_evidence_fails_outcome() -> None:
    outcome = classify_run_outcome(
        {
            "executed_actions": [{"verification_status": "pending", "commit_sha": "abc123"}],
            "operations_summary": {"executed_actions": 1},
        }
    )
    assert outcome["status"] == FAILED_OUTCOME
    assert outcome["failure_stage"] == "production_verification"
    assert outcome["production_delta_count"] == 0


def test_qualified_queue_reports_active_work_instead_of_false_success() -> None:
    outcome = classify_run_outcome(
        {
            "action_queue": [{"status": "approved", "action_type": "meta_update"}],
            "operations_summary": {"auto_ready_actions": 1},
        }
    )
    assert outcome["status"] == ACTIVE_OUTCOME
    assert outcome["production_delta_count"] == 0


def test_in_progress_without_production_delta_is_not_a_failure() -> None:
    outcome = classify_run_outcome(
        {
            "action_queue": [{"status": "approved", "action_type": "meta_update"}],
            "operations_summary": {
                "auto_ready_actions": 329,
                "candidate_states": {"validated": 329},
            },
        }
    )
    assert in_progress_without_production_delta(outcome)
    assert outcome["production_delta_count"] == 0
    assert "no production delta yet" in outcome["actual_output"]
    assert (
        persisted_run_status(mode="daily", outcome=outcome, ok=True)
        == "work_in_progress"
    )


def test_execution_without_verified_delta_stays_a_failure() -> None:
    outcome = {
        "status": FAILED_OUTCOME,
        "production_delta_count": 0,
        "summary": "No verified production delta.",
    }
    assert persisted_run_status(mode="daily", outcome=outcome, ok=False) == "failed_outcome"


def test_verified_delta_persists_as_success() -> None:
    outcome = {
        "status": VERIFIED_OUTCOME,
        "production_delta_count": 1,
    }
    assert persisted_run_status(mode="daily", outcome=outcome, ok=True) == "succeeded"


def test_empty_run_records_truthful_no_opportunity() -> None:
    outcome = classify_run_outcome({"operations_summary": {}})
    assert outcome["status"] == NO_OPPORTUNITY_OUTCOME
    assert "No qualified opportunity" in outcome["summary"]
