from types import SimpleNamespace
import json
from datetime import datetime, timezone
from unittest import mock

from sales_support_agent.services.website_ops_article_engine import (
    OFFICIAL_RESEARCH_DOMAINS,
    _claim_daily_article_slot,
    _eligible_editorial_seed,
    _historical_cluster_ids,
    article_batch_size,
    article_generation_progress,
    build_article_action,
    release_daily_article_slot,
    _request_article,
)
from sales_support_agent.api.website_ops_jobs_router import WEBSITE_OPS_PULSE_HOURS
from sales_support_agent.services.website_ops_autonomy import (
    website_ops_content_execution_mode,
)


def test_codex_owns_content_execution_by_default() -> None:
    with mock.patch.dict("os.environ", {}, clear=True):
        assert website_ops_content_execution_mode() == "codex"


def test_api_content_execution_requires_explicit_opt_in() -> None:
    with mock.patch.dict(
        "os.environ", {"WEBSITE_OPS_CONTENT_EXECUTION_MODE": "api"}, clear=True
    ):
        assert website_ops_content_execution_mode() == "api"


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


def test_openai_research_is_constrained_to_official_domains() -> None:
    response = mock.Mock()
    response.json.return_value = {"output_text": "{}"}
    with mock.patch(
        "sales_support_agent.services.website_ops_article_engine.citation_config",
        return_value=SimpleNamespace(
            api_key="key",
            provider="openai",
            model="gpt-5",
        ),
    ), mock.patch(
        "sales_support_agent.services.website_ops_article_engine.requests.post",
        return_value=response,
    ) as post:
        _request_article(settings=SimpleNamespace(), prompt="research")

    tool = post.call_args.kwargs["json"]["tools"][0]
    assert tool["filters"]["allowed_domains"] == OFFICIAL_RESEARCH_DOMAINS


def test_anthropic_research_is_constrained_to_official_domains() -> None:
    response = mock.Mock()
    response.json.return_value = {"content": [{"type": "text", "text": "{}"}]}
    with mock.patch(
        "sales_support_agent.services.website_ops_article_engine.citation_config",
        return_value=SimpleNamespace(
            api_key="key",
            provider="anthropic",
            model="claude-sonnet",
        ),
    ), mock.patch(
        "sales_support_agent.services.website_ops_article_engine.requests.post",
        return_value=response,
    ) as post:
        _request_article(settings=SimpleNamespace(), prompt="research")

    tool = post.call_args.kwargs["json"]["tools"][0]
    assert tool["allowed_domains"] == OFFICIAL_RESEARCH_DOMAINS


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


def test_article_batch_size_catches_up_as_daily_deadline_approaches(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    assert article_batch_size(
        settings,
        local_now=datetime(2026, 8, 1, 8, tzinfo=timezone.utc),
    ) == 1
    assert article_batch_size(
        settings,
        local_now=datetime(2026, 8, 1, 13, tzinfo=timezone.utc),
    ) == 3

    for index in range(5):
        assert _claim_daily_article_slot(settings, f"published-{index}") is True
    assert article_batch_size(
        settings,
        local_now=datetime(2026, 8, 1, 14, tzinfo=timezone.utc),
    ) == 2


def test_daily_generation_fills_each_service_pillar_before_extra_topics(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    generated = 0

    def generate_article(**_kwargs):
        nonlocal generated
        generated += 1
        return {
            "slug": f"balanced-topic-{generated}",
            "title": f"Balanced topic {generated}",
            "content": {},
        }

    for _ in range(8):
        action = build_article_action(
            settings=settings,
            query_intelligence={"clusters": []},
            requester=generate_article,
        )
        assert action is not None

    progress = article_generation_progress(settings)
    assert progress["generated_today"] == 8
    assert set(progress["pillar_counts"].values()) == {2}


def test_topic_history_prevents_republishing_an_old_daily_claim(tmp_path) -> None:
    history = tmp_path / "content-strategy" / "article-generation"
    history.mkdir(parents=True)
    (history / "2026-07-28.json").write_text(
        '{"cluster_ids":["already-published"]}',
        encoding="utf-8",
    )
    settings = SimpleNamespace(website_ops_root=tmp_path)

    assert _historical_cluster_ids(settings) == {"already-published"}


def test_topic_history_reconciles_existing_production_article_feedback(tmp_path) -> None:
    feedback = tmp_path / "feedback"
    feedback.mkdir(parents=True)
    (feedback / "existing.json").write_text(
        json.dumps(
            {
                "status": "error",
                "suggested_action_type": "publish_blog_article",
                "suggested_action_value": json.dumps(
                    {
                        "slug": "existing-article",
                        "evidenceId": "existing-production-topic",
                    }
                ),
                "execution_error": "Generated article slug already exists.",
            }
        ),
        encoding="utf-8",
    )
    settings = SimpleNamespace(website_ops_root=tmp_path)

    assert "existing-production-topic" in _historical_cluster_ids(settings)


def test_topic_history_does_not_suppress_retryable_article_failure(tmp_path) -> None:
    feedback = tmp_path / "feedback"
    feedback.mkdir(parents=True)
    (feedback / "retry.json").write_text(
        json.dumps(
            {
                "status": "error",
                "action_type": "publish_blog_article",
                "action_value": json.dumps(
                    {"slug": "retry-me", "evidenceId": "retryable-topic"}
                ),
                "execution_error": "Production verification timed out.",
            }
        ),
        encoding="utf-8",
    )
    settings = SimpleNamespace(website_ops_root=tmp_path)

    assert "retryable-topic" not in _historical_cluster_ids(settings)


def test_failed_generation_does_not_consume_daily_topic_slot(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)

    def fail_generation(**_kwargs):
        raise RuntimeError("provider timeout")

    try:
        build_article_action(
            settings=settings,
            query_intelligence={"clusters": []},
            requester=fail_generation,
        )
    except RuntimeError:
        pass

    assert article_generation_progress(settings)["generated_today"] == 0


def test_failed_publication_releases_generated_topic_slot(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    assert _claim_daily_article_slot(settings, "retry-topic", "Shipping OS") is True

    assert release_daily_article_slot(settings, "retry-topic") is True
    progress = article_generation_progress(settings)
    assert progress["generated_today"] == 0
    assert progress["pillar_counts"]["Shipping OS"] == 0
    assert _claim_daily_article_slot(settings, "retry-topic", "Shipping OS") is True


def test_malformed_json_generation_is_retried_before_claiming_slot(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    attempts = []

    def retry_generation(**kwargs):
        attempts.append(kwargs["prompt"])
        if len(attempts) < 3:
            raise json.JSONDecodeError("bad object", "{", 1)
        return {"slug": "recovered-article", "title": "Recovered article", "content": {}}

    action = build_article_action(
        settings=settings,
        query_intelligence={"clusters": []},
        requester=retry_generation,
    )

    assert action is not None
    assert len(attempts) == 3
    assert "previous response was malformed JSON" in attempts[1]
    assert article_generation_progress(settings)["generated_today"] == 1


def test_published_registry_identity_is_excluded_from_new_generation(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    with (
        mock.patch(
            "sales_support_agent.services.website_ops_github.github_metadata_is_configured",
            return_value=True,
        ),
        mock.patch(
            "sales_support_agent.services.website_ops_github.load_generated_article_identities",
            return_value={
                "evidence_ids": {
                    "editorial-amazon-ppc-structure",
                    "editorial-amazon-tacos",
                    "editorial-amazon-listing-audit",
                },
                "primary_intents": {
                    "how to structure amazon ppc campaigns",
                    "how to calculate amazon tacos",
                    "how to audit an amazon product listing",
                },
                "slugs": set(),
            },
        ),
    ):
        action = build_article_action(
            settings=settings,
            query_intelligence={"clusters": []},
            requester=lambda **_kwargs: {
                "slug": "next-distinct-topic",
                "title": "Next distinct topic",
                "content": {},
            },
        )

    assert action is not None
    article = json.loads(action["action_value"])
    assert article["evidenceId"] not in {
        "editorial-amazon-ppc-structure",
        "editorial-amazon-tacos",
        "editorial-amazon-listing-audit",
    }
