from sales_support_agent.services.website_ops_inventory import build_production_inventory
from sales_support_agent.services.website_ops_portfolio import build_daily_action_portfolio


def test_production_inventory_joins_sources_and_separates_nonproduction() -> None:
    inventory = build_production_inventory(
        sitemap_urls=["https://anatainc.com/", "https://anatainc.com/blog/guide"],
        crawl_inventory={
            "records": [
                {
                    "url": "https://anatainc.com/blog/guide/",
                    "status_code": 200,
                    "inlinks": 0,
                    "crawl_depth": 4,
                    "title": "Guide",
                    "h1": "Guide",
                    "source_reports": ["internal_all.csv", "links_all.csv"],
                    "warnings": [{"report": "orphan_pages.csv"}],
                },
                {"url": "https://preview.vercel.app/blog/guide", "status_code": 200},
            ]
        },
        indexing_inventory={"records": [{"url": "https://anatainc.com/blog/guide"}]},
        intent_coverage={
            "records": [
                {
                    "url": "https://anatainc.com/blog/guide",
                    "primary_intent": "ecommerce guide",
                    "intent_type": "informational",
                }
            ]
        },
    )
    assert inventory["summary"]["known_production_urls"] == 2
    guide = next(item for item in inventory["records"] if item["url"].endswith("/blog/guide"))
    assert guide["in_sitemap"] is True
    assert guide["in_indexing_export"] is True
    assert guide["in_intent_map"] is True
    assert guide["orphan_candidate"] is True
    assert guide["deep_candidate"] is True


def test_action_portfolio_never_fills_slots_with_unqualified_work() -> None:
    portfolio = build_daily_action_portfolio(
        action_queue=[
            {
                "feedback_id": "one",
                "page_url": "https://anatainc.com/platform/shipping",
                "action_type": "meta_update",
                "execution_eligibility": "auto_execute",
                "priority": "high",
            },
            {
                "feedback_id": "two",
                "page_url": "https://anatainc.com/blog/draft",
                "action_type": "publish_blog_article",
                "execution_eligibility": "approval_required",
            },
        ],
        candidate_ledger={
            "candidates": [
                {"state": "verifying", "lane_id": "internal_links"},
                {"state": "unsupported", "lane_id": "structured_data"},
            ]
        },
    )
    assert portfolio["qualified_action_count"] == 1
    assert portfolio["remaining_slots"] == 7
    assert portfolio["status"] == "underfilled"
    assert portfolio["qualified_actions"][0]["service_pillar"] == "Shipping OS"
    assert {item["state"] for item in portfolio["empty_slot_reasons"]} == {
        "unsupported",
        "verifying",
    }
