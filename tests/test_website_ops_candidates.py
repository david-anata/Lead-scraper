import json
from pathlib import Path
from types import SimpleNamespace

from sales_support_agent.services.website_ops import render_candidates_page
from sales_support_agent.services.website_ops_candidates import (
    build_candidates,
    candidate_summary,
    load_candidate_ledger,
    persist_candidate_ledger,
    select_bounded_actions,
)


def _report() -> dict:
    return {
        "crawl_verification": {
            "records": [
                {
                    "url": "https://anatainc.com/services",
                    "warning_results": [
                        {
                            "report": "response_codes_internal_client_error_(4xx)",
                            "verdict": "confirmed",
                            "crawler_evidence": "404",
                            "reason": "Production still returns 404.",
                        },
                        {
                            "report": "canonicals_missing",
                            "verdict": "disproved",
                            "crawler_evidence": "No canonical in old crawl.",
                            "reason": "Rendered production has a canonical.",
                        },
                    ],
                }
            ]
        },
        "query_intelligence": {
            "article_pipeline": {"status": "waiting_for_distinct_week"},
            "clusters": [
                {
                    "cluster_id": "amazon-ads",
                    "owner_url": "https://anatainc.com/services/amazon-advertising",
                    "ownership_status": "assigned",
                    "quality_status": "eligible",
                    "validation_status": "validated",
                    "intent": "commercial",
                },
                {
                    "cluster_id": "shipping-guide",
                    "owner_url": "https://anatainc.com/services/ecommerce-fulfillment",
                    "ownership_status": "assigned",
                    "quality_status": "eligible",
                    "validation_status": "hypothesis",
                    "intent": "informational",
                },
            ],
        },
        "action_queue": [
            {
                "feedback_id": "meta-1",
                "action_type": "meta_title_update",
                "page_url": "https://anatainc.com/services/amazon-advertising",
                "execution_eligibility": "auto_execute",
                "reason": "Exact intent correction.",
            }
        ],
        "executed_actions": [],
    }


def test_candidate_projection_has_one_non_overlapping_state_per_candidate() -> None:
    candidates = build_candidates(_report())
    summary = candidate_summary(candidates)

    assert len(candidates) == 5
    assert sum(summary["by_state"].values()) == 5
    assert summary["by_state"]["validated"] == 2
    assert summary["by_state"]["disproved"] == 1
    assert summary["by_state"]["observed"] == 1
    assert summary["by_state"]["queued"] == 1
    assert summary["ready_candidates"] == 1
    assert {item["lane_id"] for item in candidates} >= {
        "broken_internal_links",
        "canonical_sitemap",
        "metadata",
        "new_article",
    }


def test_candidate_ledger_appends_only_real_transitions(tmp_path: Path) -> None:
    first = build_candidates(_report())
    persist_candidate_ledger(tmp_path, candidates=first, run_id="run-1")
    persist_candidate_ledger(tmp_path, candidates=first, run_id="run-2")

    transitions_path = tmp_path / "candidates" / "transitions.jsonl"
    initial_transitions = transitions_path.read_text(encoding="utf-8").splitlines()
    assert len(initial_transitions) == len(first)

    changed = [dict(item) for item in first]
    changed[0]["state"] = "completed"
    changed[0]["state_reason"] = "Verified in production."
    persist_candidate_ledger(tmp_path, candidates=changed, run_id="run-3")

    transitions = [
        json.loads(line)
        for line in transitions_path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(transitions) == len(first) + 1
    assert transitions[-1]["from_state"] != transitions[-1]["to_state"]
    assert transitions[-1]["run_id"] == "run-3"
    assert load_candidate_ledger(tmp_path)["summary"]["total_candidates"] == len(first)


def test_candidate_page_exposes_drilldowns_and_executor_truth(tmp_path: Path) -> None:
    candidates = build_candidates(_report())
    persist_candidate_ledger(tmp_path, candidates=candidates, run_id="run-1")

    html = render_candidates_page(
        SimpleNamespace(website_ops_root=tmp_path),
        state_filter="validated",
    )

    assert "Candidate ledger" in html
    assert "Counts are non-overlapping candidate states" in html
    assert "Action-lane coverage" in html
    assert "Not Automated" in html
    assert "2 shown" in html
    assert "/admin/website-ops/candidates?state=validated" in html


def test_lane_budgets_and_target_locks_bound_execution() -> None:
    actions = [
        {
            "feedback_id": f"meta-{index}",
            "action_type": "meta_title_update",
            "page_url": f"https://anatainc.com/page-{index}",
            "execution_eligibility": "auto_execute",
        }
        for index in range(11)
    ]
    actions.append(
        {
            "feedback_id": "same-page-description",
            "action_type": "meta_description_update",
            "page_url": "https://anatainc.com/page-0",
            "execution_eligibility": "auto_execute",
        }
    )
    actions.append(
        {
            "feedback_id": "faq-1",
            "action_type": "inject_faq_block",
            "page_url": "https://anatainc.com/page-faq",
            "execution_eligibility": "auto_execute",
        }
    )

    selected, deferred = select_bounded_actions(actions)

    assert len(selected) == 10
    assert all(item["lane_id"] == "metadata" for item in selected)
    assert any("run budget" in item["execution_reason"] for item in deferred)
    assert any("complete production executor" in item["execution_reason"] for item in deferred)


def test_lane_budget_never_promotes_an_unapproved_action() -> None:
    selected, deferred = select_bounded_actions(
        [
            {
                "feedback_id": "meta-review",
                "action_type": "meta_title_update",
                "page_url": "https://anatainc.com/services/amazon-advertising",
                "execution_eligibility": "approval_required",
            }
        ]
    )

    assert selected == []
    assert deferred[0]["execution_eligibility"] == "approval_required"
    assert "eligibility gate" in deferred[0]["execution_reason"]


def test_completed_action_replaces_its_queued_candidate_state() -> None:
    report = _report()
    report["executed_actions"] = [
        {
            "feedback_id": "meta-1",
            "action_type": "meta_title_update",
            "page_url": "https://anatainc.com/services/amazon-advertising",
            "verification_status": "verified",
            "message": "Verified in production.",
        }
    ]

    candidates = build_candidates(report)
    action_candidates = [
        item
        for item in candidates
        if item["source_type"] == "action" and item["source_key"] == "meta-1"
    ]

    assert len(action_candidates) == 1
    assert action_candidates[0]["state"] == "completed"
