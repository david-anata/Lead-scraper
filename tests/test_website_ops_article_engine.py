from types import SimpleNamespace

from sales_support_agent.services.website_ops_article_engine import (
    _claim_daily_article_slot,
    _eligible_editorial_seed,
    article_generation_progress,
)


def test_daily_article_quota_tracks_minimum_and_three_topic_target(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)

    assert _claim_daily_article_slot(settings, "topic-one") is True
    assert _claim_daily_article_slot(settings, "topic-one") is False
    assert _claim_daily_article_slot(settings, "topic-two") is True

    progress = article_generation_progress(settings)
    assert progress["daily_minimum"] == 2
    assert progress["daily_target"] == 3
    assert progress["generated_today"] == 2
    assert progress["remaining_to_minimum"] == 0
    assert progress["remaining_to_target"] == 1

    assert _claim_daily_article_slot(settings, "topic-three") is True
    assert _claim_daily_article_slot(settings, "topic-four") is False


def test_editorial_backlog_supplies_distinct_topics_when_query_gaps_are_empty() -> None:
    first = _eligible_editorial_seed(set())
    assert first is not None
    assert first["source_kind"] == "editorial_backlog"
    assert first["normalized_query"] == "how to calculate amazon tacos"

    second = _eligible_editorial_seed({first["cluster_id"]})
    assert second is not None
    assert second["cluster_id"] != first["cluster_id"]
    assert second["owner_url"].startswith("https://anatainc.com/services/")
