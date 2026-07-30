from types import SimpleNamespace

from sales_support_agent.services.website_ops_article_engine import (
    _claim_daily_article_slot,
    _eligible_editorial_seed,
    article_generation_progress,
)
from sales_support_agent.api.website_ops_jobs_router import WEBSITE_OPS_PULSE_HOURS


def test_daily_article_quota_tracks_eight_topic_target_and_pillars(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)

    assert _claim_daily_article_slot(settings, "topic-one", "Shipping OS") is True
    assert _claim_daily_article_slot(settings, "topic-one") is False
    assert _claim_daily_article_slot(settings, "topic-two", "Shipping OS") is True

    progress = article_generation_progress(settings)
    assert progress["daily_minimum"] == 8
    assert progress["daily_target"] == 8
    assert progress["generated_today"] == 2
    assert progress["remaining_to_minimum"] == 6
    assert progress["remaining_to_target"] == 6
    assert progress["pillar_counts"]["Shipping OS"] == 2

    for index in range(3, 9):
        assert _claim_daily_article_slot(settings, f"topic-{index}") is True
    assert _claim_daily_article_slot(settings, "topic-nine") is False


def test_editorial_backlog_supplies_distinct_topics_when_query_gaps_are_empty() -> None:
    first = _eligible_editorial_seed(set())
    assert first is not None
    assert first["source_kind"] == "editorial_backlog"
    assert first["normalized_query"] == "how to calculate amazon tacos"

    second = _eligible_editorial_seed({first["cluster_id"]})
    assert second is not None
    assert second["cluster_id"] != first["cluster_id"]
    assert second["owner_url"].startswith("https://anatainc.com/services/")


def test_editorial_backlog_can_select_a_required_service_pillar() -> None:
    seed = _eligible_editorial_seed(set(), pillar="Shipping OS")
    assert seed is not None
    assert seed["pillar"] == "Shipping OS"


def test_scheduler_has_one_daily_pulse_for_each_required_article() -> None:
    assert WEBSITE_OPS_PULSE_HOURS == (8, 9, 10, 11, 12, 13, 14, 15)
