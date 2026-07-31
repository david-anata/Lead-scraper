from sales_support_agent.services.website_ops_editorial_quality import (
    contextual_evidence_errors,
    repair_deterministic_article_defects,
)


def test_contextual_evidence_requires_two_sections_sources_and_internal_routes() -> None:
    sources = [
        {"title": "One", "url": "https://example.com/one"},
        {"title": "Two", "url": "https://example.org/two"},
    ]
    sections = [
        {
            "citations": [{"title": "One", "href": "https://example.com/one"}],
            "internalLinks": [{"title": "Service", "href": "/services/ecommerce-marketing"}],
        },
        {
            "citations": [{"title": "Two", "href": "https://example.org/two"}],
            "internalLinks": [{"title": "Guide", "href": "/guides/amazon-advertising"}],
        },
    ]
    assert contextual_evidence_errors(sections=sections, sources=sources) == []


def test_contextual_evidence_rejects_footer_only_links_and_uncited_sources() -> None:
    errors = contextual_evidence_errors(
        sections=[{"citations": [], "internalLinks": []}],
        sources=[
            {"title": "One", "url": "https://example.com/one"},
            {"title": "Two", "url": "https://example.org/two"},
        ],
    )
    assert any("Contextual citations" in error for error in errors)
    assert any("Contextual internal links" in error for error in errors)


def test_deterministic_article_repair_fixes_description_and_em_dash() -> None:
    article, repairs = repair_deterministic_article_defects(
        {
            "description": "This description is intentionally much too long " * 5,
            "content": {"sections": [{"paragraphs": ["One\u2014two"]}]},
        }
    )

    assert 50 <= len(article["description"]) <= 155
    assert "\u2014" not in str(article)
    assert len(repairs) == 2
