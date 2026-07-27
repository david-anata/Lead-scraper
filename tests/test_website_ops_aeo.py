from sales_support_agent.services.website_ops_aeo import (
    build_aeo_assessment,
    simulated_fanout,
)


def test_fanout_is_always_labeled_simulated() -> None:
    prompts = simulated_fanout({"h1": ["Amazon PPC Management"]})
    assert {item["source"] for item in prompts} == {"simulated"}
    assert {item["facet"] for item in prompts} >= {"comparison", "proof", "risk"}


def test_service_fanout_uses_url_intent_instead_of_marketing_tagline() -> None:
    prompts = simulated_fanout(
        {
            "url": "https://anatainc.com/services/amazon-advertising",
            "h1": ["Every Amazon ad dollar, working toward one plan."],
            "title": "Amazon Advertising | Anata Inc.",
        }
    )

    assert {item["facet"] for item in prompts} >= {"comparison", "proof", "risk"}
    assert all("Amazon Advertising" in item["prompt"] for item in prompts)
    assert all("Every Amazon ad dollar" not in item["prompt"] for item in prompts)


def test_tool_fanout_uses_task_specific_questions() -> None:
    prompts = simulated_fanout(
        {
            "url": "https://anatainc.com/tools/amazon-profit-calculator",
            "h1": ["Know the margin before you make the move."],
        }
    )

    assert {item["facet"] for item in prompts} == {
        "purpose",
        "inputs",
        "process",
        "interpretation",
    }
    assert prompts[0]["prompt"] == "What does the Amazon Profit Calculator help calculate?"


def test_company_fanout_is_limited_and_navigational() -> None:
    prompts = simulated_fanout(
        {
            "url": "https://anatainc.com/about",
            "h1": ["An operations company that runs the store."],
        }
    )

    assert len(prompts) == 2
    assert {item["facet"] for item in prompts} == {"brand", "next-step"}
    assert all("Anata ecommerce operations company" in item["prompt"] for item in prompts)


def test_assessment_keeps_observed_and_simulated_evidence_separate() -> None:
    result = build_aeo_assessment(
        {
            "url": "https://anatainc.com/services/amazon-ppc-management",
            "final_url": "https://anatainc.com/services/amazon-ppc-management",
            "status_code": 200,
            "noindex": False,
            "response_error": "",
            "title": "Amazon PPC Management | Anata",
            "meta_description": "Amazon advertising operations connected to catalog and profit signals.",
            "h1": ["Amazon PPC Management"],
            "h2": ["Who this service is for", "How we run it", "Questions, answered"],
            "text_length": 2200,
        },
        gsc={"top_queries": [{"query": "amazon ppc agency", "impressions": 42, "clicks": 3}]},
        customer_questions=[{"question": "Who owns bid changes?", "frequency": 2}],
    )
    assert result["technical_eligibility"] == "eligible"
    assert result["answer_readiness"] == "ready"
    assert result["observed_queries"][0]["source"] == "observed"
    assert result["observed_customer_questions"][0]["source"] == "observed"
    assert result["simulated_coverage_prompts"][0]["source"] == "simulated"


def test_technical_failure_blocks_eligibility_before_aeo_advice() -> None:
    result = build_aeo_assessment(
        {
            "url": "https://anatainc.com/services/example",
            "final_url": "https://anatainc.com/services/example",
            "status_code": 503,
            "noindex": True,
            "h1": [],
            "h2": [],
            "text_length": 0,
        }
    )
    assert result["technical_eligibility"] == "blocked"
    assert len(result["technical_blockers"]) == 2
    assert result["answer_readiness"] == "needs-work"
