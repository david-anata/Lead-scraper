from types import SimpleNamespace

from sales_support_agent.services.website_ops_article_engine import (
    _claim_daily_article_slot,
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

