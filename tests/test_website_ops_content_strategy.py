from datetime import date
from pathlib import Path
from types import SimpleNamespace

from sales_support_agent.services.website_ops import render_content_strategy_page
from sales_support_agent.services.website_ops_content_strategy import (
    build_content_strategy,
    load_content_strategy,
    persist_content_strategy,
)


def _intelligence(*, cycles: int = 1, sources: int = 2) -> dict:
    cited = [
        {"title": f"Source {index}", "url": f"https://example{index}.com/guide"}
        for index in range(sources)
    ]
    return {
        "summary": {"weekly_validation_cycles": cycles},
        "clusters": [
            {
                "cluster_id": "ppc-structure",
                "label": "how to structure an amazon ppc account",
                "normalized_query": "how to structure an amazon ppc account",
                "validation_status": "validated",
                "quality_status": "eligible",
                "ownership_status": "assigned",
                "intent": "informational",
                "owner_url": "https://anatainc.com/services/amazon-advertising",
                "alignment": {"composite": 0.2},
                "citation": {"cited_urls": cited},
            },
            {
                "cluster_id": "amazon-advertising",
                "label": "amazon advertising agency",
                "validation_status": "validated",
                "quality_status": "eligible",
                "ownership_status": "assigned",
                "intent": "commercial",
                "owner_url": "https://anatainc.com/services/amazon-advertising",
                "alignment": {"composite": 0.7},
                "citation": {"cited_urls": []},
            },
        ],
    }


def test_strategy_schedules_source_qualified_article_for_next_week() -> None:
    strategy = build_content_strategy(
        _intelligence(cycles=1, sources=2),
        today=date(2026, 7, 28),
    )

    article = next(
        item for item in strategy["briefs"] if item["content_type"] == "New article"
    )
    assert article["stage"] == "scheduled"
    assert article["earliest_publish_date"] == "2026-08-03"
    assert strategy["weekly_article_budget"] == 1
    assert strategy["summary"]["scheduled_for_validation"] == 1


def test_strategy_marks_two_week_article_ready() -> None:
    strategy = build_content_strategy(
        _intelligence(cycles=2, sources=2),
        today=date(2026, 8, 3),
    )
    article = next(
        item for item in strategy["briefs"] if item["content_type"] == "New article"
    )

    assert article["stage"] == "ready"
    assert strategy["summary"]["ready_to_publish"] == 1
    assert "publish" in article["next_operation"].lower()


def test_strategy_keeps_source_research_actionable() -> None:
    strategy = build_content_strategy(
        _intelligence(cycles=2, sources=1),
        today=date(2026, 8, 3),
    )
    article = next(
        item for item in strategy["briefs"] if item["content_type"] == "New article"
    )

    assert article["stage"] == "researching"
    assert article["source_count"] == 1
    assert "two authoritative" in article["next_operation"]


def test_strategy_persists_and_page_exposes_the_operating_program(
    tmp_path: Path,
) -> None:
    strategy = build_content_strategy(
        _intelligence(cycles=1, sources=2),
        today=date(2026, 7, 28),
    )
    persist_content_strategy(tmp_path, strategy)
    assert load_content_strategy(tmp_path)["summary"]["total_briefs"] == 2

    page = render_content_strategy_page(
        SimpleNamespace(website_ops_root=tmp_path),
    )

    assert "Content strategy and publishing program" in page
    assert "This week’s operating plan" in page
    assert "1 article / week" in page
    assert "how to structure an amazon ppc account" in page
    assert "Revalidate in the next distinct ISO week" in page
    assert "/admin/website-ops/strategy?stage=scheduled" in page
