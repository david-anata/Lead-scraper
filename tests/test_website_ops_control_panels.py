from sales_support_agent.services.website_ops_control_panels import (
    render_daily_portfolio_panel,
    render_production_inventory_panel,
)


def test_daily_portfolio_panel_explains_unfilled_slots() -> None:
    panel = render_daily_portfolio_panel(
        {
            "qualified_action_count": 1,
            "remaining_slots": 7,
            "daily_action_target": 8,
            "status": "underfilled",
            "truthful_summary": "1 of 8 daily slots contains qualified work.",
            "qualified_actions": [
                {
                    "page_title": "Shipping metadata",
                    "page_url": "https://anatainc.com/platform/shipping",
                    "action_type": "meta_update",
                    "service_pillar": "Shipping OS",
                }
            ],
            "empty_slot_reasons": [
                {
                    "state": "verifying",
                    "count": 2,
                    "reason": "Production evidence is still being verified.",
                }
            ],
            "next_operation": "Verify the next candidate.",
        }
    )
    assert "1 of 8 qualified actions" in panel
    assert "Why slots are empty" in panel
    assert "Shipping OS" in panel


def test_inventory_panel_labels_missing_exports_without_claiming_completion() -> None:
    panel = render_production_inventory_panel(
        {
            "summary": {"known_production_urls": 4, "urls_missing_intent_owner": 1},
            "evidence_coverage": {
                "status_codes": "available",
                "javascript_rendering": "missing_export",
            },
        }
    )
    assert "4" in panel
    assert "1 export gaps" in panel
    assert "Javascript Rendering" in panel
