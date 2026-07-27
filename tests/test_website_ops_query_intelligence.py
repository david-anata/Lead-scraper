from types import SimpleNamespace

from sales_support_agent.services.website_ops_query_intelligence import (
    build_clusters,
    build_query_intelligence,
    build_recommendations,
    collect_query_observations,
    normalize_query,
    record_outcomes,
    run_citation_harness,
)


def _page(
    url: str = "https://anatainc.com/services/amazon-ppc-management",
    *,
    impressions: float = 42,
) -> dict:
    return {
        "page_url": url,
        "page_title": "Amazon PPC Management | Anata",
        "aeo": {
            "answer_readiness": "ready",
            "observed_queries": [
                {
                    "query": "amazon ppc agency",
                    "clicks": 3,
                    "impressions": impressions,
                    "source": "observed",
                }
            ],
            "observed_customer_questions": [],
            "simulated_coverage_prompts": [
                {
                    "facet": "comparison",
                    "prompt": "How should a buyer compare Amazon PPC management options?",
                    "source": "simulated",
                }
            ],
        },
    }


def test_normalization_preserves_raw_semantics_without_stopwords() -> None:
    assert normalize_query("What is the Best Ecommerce Advertising Agency?") == (
        "commerce ads agency"
    )


def test_observed_and_simulated_evidence_remain_distinct_but_cluster() -> None:
    records = collect_query_observations([_page()])
    assert {item["evidence_class"] for item in records} == {
        "observed_search",
        "simulated",
    }
    assert len({item["cluster_id"] for item in records}) == 1
    assert {item["raw_query"] for item in records} == {
        "amazon ppc agency",
        "How should a buyer compare Amazon PPC management options?",
    }


def test_two_independent_signals_validate_one_owner() -> None:
    page = _page()
    records = collect_query_observations([page])
    clusters = build_clusters(records, [page])
    assert clusters[0]["validation_status"] == "validated"
    assert clusters[0]["owner_url"] == page["page_url"]
    assert clusters[0]["ownership_status"] == "assigned"


def test_search_operator_query_is_audited_but_cannot_validate() -> None:
    page = _page()
    page["aeo"]["observed_queries"] = [
        {
            "query": '"amazon ppc" -site:reddit.com -site:youtube.com',
            "clicks": 0,
            "impressions": 2,
            "source": "observed",
        }
    ]
    records = collect_query_observations([page])
    operator_record = next(
        item for item in records if item["evidence_class"] == "observed_search"
    )
    clusters = build_clusters(records, [page])

    assert operator_record["quality_status"] == "quarantined"
    assert operator_record["raw_query"].startswith('"amazon ppc"')
    assert not any(item["validation_status"] == "validated" for item in clusters)
    quarantined = next(
        item for item in clusters if item["quality_status"] == "quarantined"
    )
    assert quarantined["observed_impressions"] == 0
    assert "Search-operator query" in quarantined["quality_reasons"][0]


def test_brand_lookalike_observation_does_not_validate_anata_prompt() -> None:
    page = {
        "page_url": "https://anatainc.com/careers",
        "page_title": "Careers | Anata",
        "aeo": {
            "answer_readiness": "ready",
            "observed_queries": [
                {
                    "query": "anaconda careers",
                    "clicks": 0,
                    "impressions": 8,
                    "source": "observed",
                }
            ],
            "observed_customer_questions": [],
            "simulated_coverage_prompts": [
                {
                    "facet": "brand",
                    "prompt": "What is Anata careers?",
                    "source": "simulated",
                }
            ],
        },
    }
    records = collect_query_observations([page])
    clusters = build_clusters(records, [page])

    assert len({item["cluster_id"] for item in records}) == 2
    assert not any(item["validation_status"] == "validated" for item in clusters)


def test_observed_overlap_creates_cannibalization_conflict() -> None:
    primary = _page(impressions=42)
    competing = _page(
        "https://anatainc.com/services/amazon-advertising",
        impressions=8,
    )
    records = collect_query_observations([primary, competing])
    clusters = build_clusters(records, [primary, competing])
    validated = [item for item in clusters if item["validation_status"] == "validated"]
    assert validated[0]["owner_url"] == primary["page_url"]
    assert validated[0]["ownership_status"] == "conflict"
    assert competing["page_url"] in validated[0]["conflict_urls"]


def test_citation_harness_records_fanout_and_anata_citation(tmp_path) -> None:
    settings = SimpleNamespace(
        website_ops_root=tmp_path,
        openai_api_key="test-key",
    )
    cluster = {
        "cluster_id": "cluster-1",
        "label": "amazon ppc agency",
        "validation_status": "validated",
        "ownership_status": "assigned",
    }

    def requester(**_kwargs):
        return {
            "output": [
                {
                    "type": "web_search_call",
                    "action": {
                        "type": "search",
                        "queries": [
                            "amazon ppc agency comparison",
                            "amazon advertising management proof",
                        ],
                    },
                },
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "Anata is one option.",
                            "annotations": [
                                {
                                    "type": "url_citation",
                                    "url": "https://anatainc.com/services/amazon-ppc-management",
                                    "title": "Amazon PPC Management",
                                }
                            ],
                        }
                    ],
                },
            ]
        }

    results = run_citation_harness(
        settings=settings,
        clusters=[cluster],
        run_mode="weekly",
        requester=requester,
    )
    assert results[0]["status"] == "cited"
    assert results[0]["retrieval_used"] is True
    assert len(results[0]["fanout_queries"]) == 2
    assert results[0]["anata_cited"] is True


def test_missing_citation_provider_is_unavailable_not_zero(tmp_path) -> None:
    settings = SimpleNamespace(
        website_ops_root=tmp_path,
        openai_api_key="",
    )
    results = run_citation_harness(
        settings=settings,
        clusters=[
            {
                "cluster_id": "cluster-1",
                "label": "amazon ppc agency",
                "validation_status": "validated",
                "ownership_status": "assigned",
            }
        ],
        run_mode="weekly",
    )
    assert results[0]["status"] == "unavailable"
    assert results[0]["cited_urls"] == []
    assert "citation_count" not in results[0]


def test_recommendations_are_shadowed_until_two_weekly_cycles() -> None:
    cluster = {
        "cluster_id": "cluster-1",
        "label": "amazon ppc agency",
        "intent": "commercial",
        "validation_status": "validated",
        "ownership_status": "assigned",
        "owner_url": "https://anatainc.com/services/amazon-ppc-management",
        "evidence_classes": ["observed_search", "simulated"],
        "alignment": {"title_alignment": 0.2, "answer_coverage": 0.6},
    }
    shadow = build_recommendations(
        clusters=[cluster],
        decision_data_ready=True,
        weekly_validation_cycles=1,
    )
    eligible = build_recommendations(
        clusters=[cluster],
        decision_data_ready=True,
        weekly_validation_cycles=2,
    )
    assert shadow[0]["execution_status"] == "shadow"
    assert eligible[0]["execution_status"] == "eligible"


def test_build_persists_snapshot_and_immutable_logs(tmp_path) -> None:
    settings = SimpleNamespace(
        website_ops_root=tmp_path,
        openai_api_key="",
    )
    first = build_query_intelligence(
        settings=settings,
        page_insights=[_page()],
        decision_data_ready=True,
        run_mode="daily",
    )
    second = build_query_intelligence(
        settings=settings,
        page_insights=[_page()],
        decision_data_ready=True,
        run_mode="daily",
    )
    root = tmp_path / "query_intelligence"
    assert first["summary"]["validated_clusters"] == 1
    assert second["summary"]["validated_clusters"] == 1
    assert (root / "snapshot.json").exists()
    assert len((root / "query_observations.jsonl").read_text().splitlines()) == 2


def test_outcome_learning_records_association_not_causation(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    page = _page()
    page["search_console"] = {
        "impressions": 42,
        "clicks": 3,
        "ctr": 3 / 42,
    }
    page["ga4"] = {"sessions": 10, "lead_conversions": 1}
    first = record_outcomes(
        settings=settings,
        page_insights=[page],
        decision_data_ready=True,
        run_mode="weekly",
    )
    page["search_console"]["clicks"] = 4
    page["ga4"]["lead_conversions"] = 2
    second = record_outcomes(
        settings=settings,
        page_insights=[page],
        decision_data_ready=True,
        run_mode="weekly",
    )
    assert first[0]["association_only"] is True
    assert second[0]["deltas_from_previous_observation"]["clicks"] == 1
    assert second[0]["deltas_from_previous_observation"]["lead_conversions"] == 1
    assert "does not claim" in second[0]["method_note"]


def test_outcomes_are_unavailable_when_decision_data_is_blocked(tmp_path) -> None:
    settings = SimpleNamespace(website_ops_root=tmp_path)
    outcomes = record_outcomes(
        settings=settings,
        page_insights=[_page()],
        decision_data_ready=False,
        run_mode="daily",
    )
    assert outcomes[0]["status"] == "unavailable"
    assert outcomes[0]["association_only"] is True
