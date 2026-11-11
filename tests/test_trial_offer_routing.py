"""Trial Offer takes one configured signal and never the unmatched leftovers."""

from datetime import datetime, timezone

import outbound_instantly_push as push
import outbound_recipes as recipes


NOW = datetime(2026, 7, 29, tzinfo=timezone.utc)


def _store(**extra):
    store = {
        "name": "brand.com",
        "merchant_name": "Brand",
        "platform": "shopify",
        "country_code": "US",
        "categories": "/Beauty & Fitness/Face & Body Care",
        "sales_channels": [],
        "apps": [],
    }
    store.update(extra)
    return store


def test_app_filters_use_the_numeric_platform_id():
    assert recipes.filter_app_id("shopify.rebuy") == "1.rebuy"
    assert recipes.filter_app_id("1.rebuy") == "1.rebuy"
    assert all(
        recipes.filter_app_id(token).startswith("1.")
        and not recipes.filter_app_id(token).startswith("shopify.")
        for token in recipes.GROWTH_APP_TOKENS
    )


def test_default_trial_signal_is_churned_tool_and_can_be_switched():
    assert recipes.active_trial_signal(None) == "churned_tool"
    assert recipes.active_trial_signal({"trial_offer.active_signal": "category"}) == "category"
    assert "catalog" not in recipes.setting(None, "trial_offer.signal_priority")


def test_churned_tool_qualifies_only_when_it_is_the_active_signal():
    store = _store(apps=[{
        "platform": "shopify", "token": "rebuy",
        "removed_at": "2026-07-10T00:00:00Z",
    }])
    assert recipes.qualifying_trial_signal(store, NOW, None) == "churned_tool"
    assert recipes.qualifying_trial_signal(
        store, NOW, {"trial_offer.active_signal": "category"},
    ) == "category"


def test_v2_campaigns_claim_a_store_before_trial_offer():
    store = _store(
        sales_channels=["Amazon"],
        apps=[{"platform": "shopify", "token": "rebuy", "removed_at": "2026-07-10T00:00:00Z"}],
    )
    lead = {"signals": []}
    recipes.annotate_trial_signal(lead, store, now=NOW, settings=None)
    assert "trial_signal" not in lead
    campaign, status, route = push.resolve_recipe_campaign(
        "churned_tool",
        ["route:amz_on"],
    )
    assert route == "amz_on" and status == "routed"
    assert campaign == push.RECIPE_CAMPAIGN_MAP["amz_on"]


def test_unmatched_baseline_stays_queued_and_a_recorded_signal_goes_to_trial():
    queued_id, queued_status, queued_route = push.resolve_recipe_campaign(
        "icp_baseline", [],
    )
    assert queued_id == "" and queued_status == "queued" and queued_route == ""
    campaign, status, route = push.resolve_recipe_campaign(
        "icp_baseline", ["trial_offer:churned_tool"],
    )
    assert status == "routed" and route == "trial_offer"
    assert campaign == push.LEGACY_TRIAL_CAMPAIGN_ID
    plan = push.dry_run_route_plan([
        {"recipe": "icp_baseline", "signals": []},
        {"recipe": "social_surge", "signals": []},
        {"recipe": "churned_tool", "signals": ["trial_offer:churned_tool"]},
    ])
    assert plan["legacy_or_hold_targets"] == 0
    assert plan["queued"] == 2
    assert plan["routed"].get(push.LEGACY_TRIAL_CAMPAIGN_ID) == 1


def test_a_trial_push_without_a_signal_is_still_refused():
    bare = push.push_leads([{"email": "a@b.com"}], push.LEGACY_TRIAL_CAMPAIGN_ID, dry_run=False)
    assert bare.created == 0 and bare.reason == push.TRIAL_CLOSED_REASON
    qualified = push.push_leads(
        [{"email": "a@b.com", "trial_signal": "churned_tool"}],
        push.LEGACY_TRIAL_CAMPAIGN_ID,
        dry_run=True,
    )
    assert qualified.reason != push.TRIAL_CLOSED_REASON
