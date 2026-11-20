from sales_support_agent.services.website_ops_outcomes import (
    ACTIVE_OUTCOME,
    FAILED_OUTCOME,
    NO_OPPORTUNITY_OUTCOME,
    VERIFIED_OUTCOME,
    classify_run_outcome,
    persisted_run_disposition,
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


def test_large_in_progress_queue_without_a_production_delta_is_not_a_failure() -> None:
    queue = [
        {"status": "approved", "action_type": "meta_update"}
        for _ in range(329)
    ]
    outcome = classify_run_outcome(
        {
            "action_queue": queue,
            "executed_actions": [],
            "operations_summary": {
                "auto_ready_actions": 329,
                "executed_actions": 0,
                "candidate_states": {"queued": 329},
            },
        }
    )
    assert outcome["status"] == ACTIVE_OUTCOME
    assert outcome["status"] != FAILED_OUTCOME
    assert outcome["production_delta_count"] == 0
    assert "329" in outcome["actual_output"]
    assert outcome["failure_stage"] == ""


def test_persisted_disposition_keeps_daily_in_progress_work_as_non_failure() -> None:
    disposition = persisted_run_disposition(
        mode="daily",
        outcome={
            "status": "work_in_progress",
            "summary": "329 active qualified item(s); no production delta yet.",
            "production_delta_count": 0,
            "resume_required": True,
        },
        ok=True,
    )
    assert disposition["status"] == ACTIVE_OUTCOME
    assert disposition["failure"] is False
    assert disposition["successful_day"] is False
    assert disposition["last_error"] == ""
    assert disposition["recovery_status"] == "resume_next_pulse"


def test_empty_run_records_truthful_no_opportunity() -> None:
    outcome = classify_run_outcome({"operations_summary": {}})
    assert outcome["status"] == NO_OPPORTUNITY_OUTCOME
    assert "No qualified opportunity" in outcome["summary"]
