from sales_support_agent.services.website_ops import _build_operations_summary
from sales_support_agent.services.website_ops_vendor.core import (
    render_daily_report_html,
    render_daily_report_markdown,
)


def _report() -> dict:
    return {
        "title": "Website Ops Daily Report",
        "date": "2026-07-28",
        "generated_at": "2026-07-28T14:00:00+00:00",
        "scope": "production marketing",
        "status": "healthy",
        "pages_reviewed": 47,
        "pages_healthy": 47,
        "pages_with_issues": 0,
        "issues_found": 0,
        "issue_counts_by_priority": {"P0": 0, "P1": 0, "P2": 0, "P3": 0},
        "pages": [],
        "issues": [],
        "recommendations": [],
        "action_queue": [
            {
                "action_type": "meta_title_update",
                "page_url": "https://anatainc.com/services/amazon-advertising",
                "execution_eligibility": "auto_execute",
            }
        ],
        "content_tasks": [
            {
                "action_type": "inject_faq_block",
                "page_url": "https://anatainc.com/services/amazon-advertising",
            }
        ],
        "executed_actions": [],
        "crawl_verification": {
            "summary": {
                "confirmed_warnings": 1,
                "pending_warnings": 8,
                "disproved_warnings": 31,
                "noise_warnings": 3,
            }
        },
        "query_intelligence": {
            "summary": {
                "total_clusters": 12,
                "validated_clusters": 2,
                "hypothesis_clusters": 10,
                "ownership_conflicts": 0,
            },
            "article_pipeline": {
                "status": "waiting_for_distinct_week",
                "message": "1 of 2 distinct ISO-week evidence cycles complete.",
            },
        },
    }


def test_operations_summary_explains_throughput_and_deferrals() -> None:
    summary = _build_operations_summary(_report())

    assert summary["observed_candidates"] == 21
    assert summary["validated_candidates"] == 3
    assert summary["auto_ready_actions"] == 1
    assert summary["executed_actions"] == 0
    assert any(item["count"] == 31 for item in summary["deferred_reasons"])
    assert any(item["status"] == "not_automated" for item in summary["execution_coverage"])


def test_report_artifacts_surface_candidate_funnel_and_reasons() -> None:
    report = _report()
    report["operations_summary"] = _build_operations_summary(report)

    markdown = render_daily_report_markdown(report)
    html = render_daily_report_html(report)

    for output in (markdown, html):
        assert "Improvement Funnel" in output
        assert "Why Other Work Did Not Run" in output
        assert "Autonomous Coverage" in output
        assert "31" in output
        assert "waiting" in output.lower()


def test_operations_summary_prefers_non_overlapping_durable_candidate_states() -> None:
    report = _report()
    report["candidate_ledger"] = {
        "summary": {
            "total_candidates": 5,
            "ready_candidates": 1,
            "by_state": {
                "validated": 2,
                "queued": 1,
                "disproved": 1,
                "observed": 1,
                "completed": 0,
            },
        },
        "lanes": [
            {
                "lane_id": "metadata",
                "label": "Metadata corrections",
                "executor_status": "autonomous",
                "candidate_count": 3,
                "run_budget": 10,
                "concurrency": 2,
            }
        ],
        "candidates": [{"candidate_id": "present"}],
    }

    summary = _build_operations_summary(report)

    assert summary["count_basis"] == "durable_candidates"
    assert summary["observed_candidates"] == 5
    assert summary["validated_candidates"] == 2
    assert summary["auto_ready_actions"] == 1
    assert summary["candidate_states"]["disproved"] == 1
