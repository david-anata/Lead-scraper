"""Openers (rules enforced in code) and the Instantly push (safety defaults)."""

import outbound_instantly_push as push
import outbound_openers as op


def setup_function():
    op._OPENERS_BY_DOMAIN.clear()
    op._ANGLES_BY_DOMAIN.clear()

LEAD = {"brand": "Huppy", "domain": "huppy.com", "niche": "oral care",
        "reason": "launched a new product line", "recipe": "new_growth_app", "tier": "A",
        "categories": ["Health & Beauty"]}
CONTACT = {"email": "Ann@Huppy.com", "first_name": "Ann", "last_name": "Lee", "title": "Founder"}
IDEA = (
    "Huppy's oral care line is easy to picture, and a small set would give "
    "a new shopper one clear way to try it."
)
TABS = (
    "Huppy's toothpaste tabs are easy to picture, and a small set would give "
    "a new shopper one clear way to try them."
)
ON_AMAZON = (
    "Huppy's toothpaste tabs are already on Amazon, and a small set would "
    "make the first choice obvious."
)


def test_clean_replaces_dashes_with_hyphens():
    assert "—" not in op.clean("Nice work — really")
    assert op.clean("Nice work — really") == "Nice work, really"


def test_validate_rejects_numbers_links_cliches_and_length():
    assert not op.validate("You grew 40% last year.")[0]
    assert not op.validate("See huppy.com/x https://x.co")[0]
    assert not op.validate("Hope this finds you well, Ann.")[0]
    assert not op.validate(" ".join(["word"] * 40))[0]
    assert not op.validate("")[0]
    assert not op.validate("Huppy sells toothpaste tabs.")[0]
    assert op.validate(TABS)[0]
    assert not op.validate("Is Amazon on your radar?")[0]
    assert not op.validate("They have social momentum.")[0]
    assert not op.validate("Look at apparel_accessories next.")[0]
    assert not op.validate("Nice work — really")[0]


def test_ai_line_used_when_valid():
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: IDEA)
    assert src == "ai" and "Huppy" in line


def test_bad_ai_line_falls_back():
    lead = {**LEAD, "domain": "bad-ai.com", "template_fallback": True, "store_profile": {
        "most_recently_published_product": {"title": "Toothpaste tabs"},
    }}
    line, src = op.write_opener(lead, CONTACT, llm=lambda p: "You grew 300% - wow!")
    assert src == "fallback"
    assert "toothpaste tabs" in line.lower()
    ok, _ = op.validate(line)
    assert ok


def test_ai_exception_falls_back_without_raising():
    def boom(p):
        raise RuntimeError("down")
    lead = {**LEAD, "domain": "boom.com", "template_fallback": True, "store_profile": {
        "description": "Mint toothpaste tabs for daily brushing",
    }}
    line, src = op.write_opener(lead, CONTACT, llm=boom)
    assert src == "fallback" and line
    assert "300" not in line


def test_no_key_no_llm_uses_fallback(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    lead = {**LEAD, "domain": "nokey.com", "template_fallback": True, "store_profile": {
        "description": "Mint toothpaste tabs for daily brushing",
    }}
    line, src = op.write_opener(lead, CONTACT)
    assert src == "fallback" and "tabs" in line.lower()


def test_fallback_never_contains_digits_even_if_reason_does():
    line = op.fallback_line("Huppy", "oral care", "grew 40% in a month", product="toothpaste tabs")
    assert "40" not in line and "toothpaste tabs" in line and op.validate(line)[0]


def test_build_lead_maps_instantly_fields():
    contact = {**CONTACT, "brand_angle": "Anata helps oral care brands grow."}
    lead = push.build_lead(contact, LEAD, "Hello line")
    assert lead["email"] == "ann@huppy.com"
    assert lead["company_name"] == "Huppy" and lead["personalization"] == "Hello line"
    assert lead["custom_variables"]["category"] == "health & beauty"
    assert lead["custom_variables"]["brandAngle"] == "Anata helps oral care brands grow."
    assert "—" not in lead["custom_variables"]["brandAngle"]


def test_push_is_dry_run_by_default_and_calls_nothing():
    calls = []
    out = push.push_leads([{"email": "a@b.com"}], "new-campaign",
                          post=lambda *a, **k: calls.append(1))
    assert out.dry_run and not calls


def test_push_refuses_protected_campaign(monkeypatch):
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    monkeypatch.setenv("OUTBOUND_PROTECTED_CAMPAIGN_IDS", "4f118329-c07e-4279-9c08-12059f2a49f4")
    calls = []
    out = push.push_leads([{"email": "a@b.com"}], "4f118329-c07e-4279-9c08-12059f2a49f4",
                          dry_run=False, key="k" * 20, post=lambda *a, **k: calls.append(1))
    assert not calls and "protected" in out.reason.lower()


def test_push_needs_env_flag_even_when_dry_run_false(monkeypatch):
    monkeypatch.delenv("OUTBOUND_PUSH_ENABLED", raising=False)
    calls = []
    out = push.push_leads([{"email": "a@b.com"}], "new-campaign", dry_run=False,
                          key="k" * 20, post=lambda *a, **k: calls.append(1))
    assert not calls and out.dry_run


def test_push_sends_chunks_when_enabled(monkeypatch):
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    sent = []

    class R:
        status_code = 200
        def json(self):
            return {"created_leads": [1] * len(sent[-1]), "skipped_count": 0}

    def post(url, headers=None, json=None, timeout=None):
        sent.append(json["leads"])
        assert json["campaign_id"] == "new-campaign"
        assert json["skip_if_in_workspace"] is True
        return R()

    leads = [{"email": f"a{i}@b.com"} for i in range(250)]
    out = push.push_leads(leads, "new-campaign", dry_run=False, key="k" * 20, post=post)
    assert [len(c) for c in sent] == [100, 100, 50]
    assert out.created == 250 and not out.dry_run


def test_push_error_hides_key(monkeypatch):
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    secret = "inst_secret_key_12345"

    def post(*a, **k):
        raise RuntimeError(f"boom {secret}")

    out = push.push_leads([{"email": "a@b.com"}], "new-campaign", dry_run=False, key=secret, post=post)
    assert out.errors and all(secret not in e for e in out.errors)


def test_fallback_never_leaks_internal_reason():
    line = op.fallback_line(
        "Basin", "bath", "They fit our ideal customer profile", product="bath towels",
    )
    assert "ideal customer" not in line.lower() and "Basin" in line and op.validate(line)[0]


def test_template_tics_rejected():
    assert not op.validate("I noticed Basin is building something special. Curious?")[0]


def test_retry_once_then_accept():
    lines = iter(["I noticed Huppy is building something special.", IDEA])
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: next(lines))
    assert src == "ai" and line == IDEA


def test_category_slugs_become_plain_words_and_product_wins():
    slug_line = op.fallback_line("Huppy", "apparel_accessories")
    assert slug_line == ""
    assert "apparel_accessories" not in slug_line
    assert op.plain_category("food_beverage") == "food and beverage"
    assert op.plain_category("beauty_wellness") == "beauty and wellness"
    assert "?" not in slug_line
    product_line = op.fallback_line("Huppy", "apparel_accessories", product="toothpaste tabs")
    assert "toothpaste tabs" in product_line
    assert "apparel_accessories" not in product_line
    signal = op.fallback_line("Huppy", "", recipe="plan_upgrade")
    assert signal == ""
    assert "upgraded" not in signal
    assert "?" not in signal


def test_one_opener_per_company_is_cached_by_domain():
    calls = []

    def llm(prompt):
        calls.append(prompt)
        return TABS

    lead = {**LEAD, "amz_product": "toothpaste tabs", "amz_confidence": "high"}
    first = op.write_opener(lead, CONTACT, llm=llm)
    second = op.write_opener(lead, {"title": "CMO", "email": "other@huppy.com"}, llm=llm)
    assert first == second
    assert first[1] == "ai"
    assert len(calls) == 1
    assert "toothpaste tabs" in calls[0]


def test_unconfirmed_amazon_product_is_not_used():
    prompts = []

    def llm(prompt):
        prompts.append(prompt)
        return ON_AMAZON

    described = {**LEAD, "template_fallback": True, "store_profile": {"description": "Mint toothpaste tabs for daily brushing"}}
    for conf in ("", "low", "medium"):
        op._OPENERS_BY_DOMAIN.clear()
        prompts.clear()
        line, source = op.write_opener(
            {**described, "domain": f"{conf or 'blank'}.example", "amz_product": "toothpaste tabs", "amz_confidence": conf},
            CONTACT, llm=llm,
        )
        assert "amazon" not in line.lower()
        assert source == "fallback"
        assert "Amazon product" not in prompts[0]


def test_absent_brand_never_claims_amazon():
    line, _ = op.write_opener({**LEAD, "amz_product": "tabs", "amazon_confidence": "high", "amazon_absent": True}, CONTACT, llm=lambda p: "Huppy is on Amazon.")
    assert "amazon" not in line.lower()


def test_confirmed_amazon_product_allows_the_claim():
    line, source = op.write_opener(
        {**LEAD, "domain": "confirmed-tabs.com", "recipe": "amz_on",
         "amz_product": "toothpaste tabs", "amz_confidence": "high"},
        CONTACT, llm=lambda p: ON_AMAZON,
    )
    assert source == "ai" and "Amazon" in line


def test_opener_is_empty_when_nothing_solid():
    bare = {"brand": "Huppy", "domain": "empty.com"}
    line, source = op.write_opener(bare, CONTACT, llm=lambda p: "Huppy sells to consumers online.")
    assert line == "" and source == "none"
    assert op.fallback_line("Huppy") == ""


def test_brand_angle_uses_a_known_fact_and_not_an_unconfirmed_amazon():
    lead = {**LEAD, "domain": "angle-fact.com", "template_fallback": True, "store_profile": {
        "description": "Mint toothpaste tabs for daily brushing",
    }}
    angle, source = op.write_brand_angle(lead, CONTACT, llm=lambda p: "Anata helps Huppy grow on Amazon.")
    assert source == "fallback"
    assert "amazon" not in angle.lower()
    assert "—" not in angle and " - " not in angle
    assert angle and not angle.lower().startswith("anata helps")


def test_brand_angle_names_amazon_only_when_the_product_is_confirmed():
    lead = {
        **LEAD, "domain": "tabs.com", "recipe": "amz_on",
        "amz_product": "toothpaste tabs", "amz_confidence": "high",
    }
    angle, source = op.write_brand_angle(lead, CONTACT, llm=lambda p: ON_AMAZON)
    assert source == "ai" and "Amazon" in angle and "toothpaste tabs" in angle


def test_brand_angle_falls_back_to_a_category_line():
    lead = {"brand": "Huppy", "domain": "cat.com", "niche": "oral care", "categories": ["oral care"]}
    angle, source = op.write_brand_angle(lead, CONTACT, llm=lambda p: "Anata helps Huppy dominate retail.")
    assert source == "none"
    assert angle == ""


def test_brand_angle_uses_scraped_product_without_calling_amazon():
    html = '<meta property="og:title" content="Toothpaste tabs">'
    lead = {"brand": "Huppy", "domain": "site.com", "site_html": html, "template_fallback": True}
    line, _source = op.write_opener(lead, CONTACT, llm=lambda _p: "Huppy is big on Amazon.")
    angle, _angle_source = op.write_brand_angle(lead, CONTACT)
    assert "amazon" not in line.lower()
    assert "toothpaste tabs" in line.lower()
    assert "amazon" not in angle.lower()
    assert angle and angle != line


def test_brand_angle_is_empty_without_a_category():
    angle, source = op.write_brand_angle({"brand": "Huppy", "domain": "none.com"}, CONTACT)
    assert angle == "" and source == "none"


def test_category_filler_urls_and_backend_are_not_sendable():
    filler = op.opener_hold_reason(
        "Kalm sells food & drink.",
        {"brand": "Kalm", "categories": "Food & Drink"},
    )
    assert "category filler" in filler
    assert "url" in op.opener_hold_reason("See https://kalm.co/salsa today.", {"brand": "Kalm"})
    assert "backend" in op.opener_hold_reason(
        "Kalm recently upgraded their store plan.", {"brand": "Kalm"},
    )
    assert op.opener_hold_reason(
        "Kalm's Green Chile Salsa could be sold as a small set a new shopper can try.",
        {"brand": "Kalm", "categories": "Food & Drink"},
    ) == ""
    assert op.angle_is_filler("Anata helps food & drink brands grow.")
    opener, angle = op.shopper_lines({
        "brand": "Kalm",
        "domain": "food.com",
        "categories": "Food & Drink",
        "store_profile": {
            "most_recently_published_product": {"title": "Green Chile Salsa"},
            "description": "Roasted green chile salsa",
            "plan": "Shopify Plus",
            "apps": ["Klaviyo"],
            "store_locator_page": "https://food.com/pages/stores",
        },
    })
    assert "green chile salsa" in opener.lower()
    assert "http" not in opener.lower() and "klaviyo" not in opener.lower()
    assert "shopify" not in opener.lower() and "shopify" not in angle.lower()


def test_friendly_category():
    assert push.friendly_category("/Business & Industrial/Agriculture & Forestry, /Home & Garden/Gardening & Landscaping") == "gardening & landscaping"
    assert push.friendly_category("/Apparel") == "apparel"
    assert push.friendly_category(["/Apparel/Clothing Accessories"]) == "clothing accessories"
    assert push.friendly_category("") == ""


def test_live_campaign_not_protected_by_default(monkeypatch):
    monkeypatch.delenv("OUTBOUND_PROTECTED_CAMPAIGN_IDS", raising=False)
    assert push.LIVE_CAMPAIGN_ID not in push.protected_campaigns()


def test_configured_campaign_id_does_not_fall_back(monkeypatch):
    monkeypatch.delenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", raising=False)
    assert push.configured_campaign_id() == ""
    monkeypatch.setenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", "  campaign-1  ")
    assert push.configured_campaign_id() == "campaign-1"


def test_recipe_campaign_map_is_fixed_and_does_not_use_the_retired_campaign():
    assert push.RECIPE_CAMPAIGN_MAP == {
        "amz_on": "8e0df427-1f18-43ac-b0c6-e8ea787123d7",
        "amz_off": "138a162d-6654-4916-9959-20c11e31214b",
        "plan_upgrade": "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72",
        "replatform": "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
        "replatformed": "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
        "abby_reengage": "907d32fd-d6db-4ce6-8edc-485b78cce120",
    }
    assert push.LIVE_CAMPAIGN_ID not in push.RECIPE_CAMPAIGN_MAP.values()
    assert "redo_partner" in push.PUSH_EXCLUDED_RECIPES


def test_replatformed_routes_to_the_same_campaign_as_replatform():
    campaign = "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c"
    assert push.RECIPE_CAMPAIGN_MAP["replatformed"] == campaign
    assert push.campaign_for_recipe("replatformed", "caller") == campaign
    assert push.campaign_for_recipe("replatformed") == push.campaign_for_recipe("replatform")


def test_campaign_for_recipe_ignores_the_default_and_the_env(monkeypatch):
    monkeypatch.setenv("OUTBOUND_INSTANTLY_CAMPAIGN_ID", push.LEGACY_TRIAL_CAMPAIGN_ID)
    assert push.campaign_for_recipe("plan_upgrade", "caller") == push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]
    assert push.campaign_for_recipe("social_surge", "caller") == ""
    assert push.campaign_for_recipe("  amz_off  ") == push.RECIPE_CAMPAIGN_MAP["amz_off"]
    assert push.campaign_for_recipe("icp_baseline") == ""
    assert push.campaign_for_recipe("icp_baseline", push.LEGACY_TRIAL_CAMPAIGN_ID) == ""
    assert push.campaign_for_recipe("abby_reengage", "") == push.RECIPE_CAMPAIGN_MAP["abby_reengage"]
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in push.RECIPE_CAMPAIGN_MAP.values()


def test_push_leads_refuses_trial_offer_even_on_a_dry_run():
    out = push.push_leads([{"email": "a@b.com"}], push.LEGACY_TRIAL_CAMPAIGN_ID, dry_run=False)
    assert out.created == 0 and out.reason == push.TRIAL_CLOSED_REASON
    held = push.push_leads(
        [{"email": "a@b.com"}], "71add6d5-0096-52df-a6e2-66135fbe22e4", dry_run=True,
    )
    assert held.created == 0 and held.reason == push.TRIAL_CLOSED_REASON


def test_route_tokens_come_only_from_store_fields():
    now = __import__("datetime").datetime(2026, 11, 6, tzinfo=__import__("datetime").timezone.utc)
    plus = {
        "plan": "Shopify Plus",
        "last_plan_change_at": "2026-09-01T00:00:00Z",
        "sales_channels": ["Amazon", "Etsy"],
    }
    assert push.ROUTE_PLAN_UPGRADE in push.route_tokens_for_store(plus, now=now)
    assert push.ROUTE_AMZ_ON in push.route_tokens_for_store(plus, now=now)
    assert push.ROUTE_AMZ_OFF not in push.route_tokens_for_store(plus, now=now)
    assert push.route_tokens_for_store({"plan": "Shopify Plus"}, now=now) == []
    assert push.route_tokens_for_store({"sales_channels": []}, now=now) == []
    assert push.route_tokens_for_store({"sales_channels": ["Etsy"]}, now=now) == [push.ROUTE_AMZ_OFF]
    moved = {
        "last_platform": "woocommerce",
        "last_platform_change_at": "2026-08-01T00:00:00Z",
    }
    assert push.route_tokens_for_store(moved, now=now) == [push.ROUTE_REPLATFORM]
    assert push.route_tokens_for_store({"last_platform": "wix", "last_platform_change_at": "2026-08-01T00:00:00Z"}, now=now) == []


def test_dry_run_keeps_held_fillers_off_trial_offer_unless_a_signal_matches():
    """Production-shaped routing. Nothing is sent."""

    blocked = {"icp_baseline", "social_surge", "churned_tool", "new_growth_app"}
    plan = dry = push.dry_run_route_plan(
        [
            {"recipe": "amz_on", "signals": []},
            {"recipe": "icp_baseline", "signals": []},
            {"recipe": "social_surge", "signals": ["Fits our ICP, thin buying signals"]},
            {"recipe": "icp_baseline", "signals": [push.ROUTE_PLAN_UPGRADE, push.ROUTE_AMZ_ON]},
            {"recipe": "new_growth_app", "signals": [push.ROUTE_AMZ_ON, push.ROUTE_AMZ_OFF]},
            {"recipe": "churned_tool", "signals": ["Upgraded their store plan recently"]},
            {"recipe": "", "signals": []},
        ],
        campaign_map=dict(push.RECIPE_CAMPAIGN_MAP),
        blocked=blocked,
    )
    assert plan["legacy_or_hold_targets"] == 0
    assert push.LEGACY_TRIAL_CAMPAIGN_ID not in plan["routed"]
    assert not set(plan["routed"]).intersection(push.HOLD_INGEST_CAMPAIGN_IDS)
    assert plan["routed"][push.RECIPE_CAMPAIGN_MAP["amz_on"]] == 1
    assert plan["routed"][push.RECIPE_CAMPAIGN_MAP["plan_upgrade"]] == 2
    assert plan["blocked"] == 3
    assert plan["queued"] == 1
    assert dry["routed_contacts"] == 3
    # A hold row flipped back to active still must not receive the filler recipe.
    pointed = dict(push.RECIPE_CAMPAIGN_MAP)
    pointed["icp_baseline"] = "71add6d5-0096-52df-a6e2-66135fbe22e4"
    held_open = push.dry_run_route_plan(
        [{"recipe": "icp_baseline", "signals": []}],
        campaign_map=pointed,
        blocked=set(),
    )
    assert held_open["legacy_or_hold_targets"] == 0
    assert held_open["queued"] == 1
    assert held_open["routed"] == {}


class _R:
    def __init__(self, code, data):
        self.status_code, self._d = code, data

    def json(self):
        return self._d


def test_domain_in_instantly_matches_company_not_substring():
    hit = lambda *a, **k: _R(200, {"items": [{"email": "laura@basin.com", "company_domain": "basin.com"}]})  # noqa: E731
    near = lambda *a, **k: _R(200, {"items": [{"email": "x@mybasin.com", "company_domain": "mybasin.com"}]})  # noqa: E731
    assert push.domain_in_instantly("basin.com", key="k" * 20, post=hit) is True
    assert push.domain_in_instantly("basin.com", key="k" * 20, post=near) is False


def test_domain_in_instantly_treats_a_store_host_as_the_brand():
    hit = lambda *a, **k: _R(200, {"items": [{  # noqa: E731
        "email": "a@ca.petlibro.com", "company_domain": "ca.petlibro.com",
        "website": "https://ca.petlibro.com"}]})
    assert push.domain_in_instantly("petlibro.com", key="k" * 20, post=hit) is True
    seen = {}

    def post(url, **kw):
        seen["body"] = kw.get("json")
        return _R(200, {"items": []})

    assert push.domain_in_instantly("ca.petlibro.com", key="k" * 20, post=post) is False
    assert seen["body"]["search"] == "petlibro.com"


def test_domain_in_instantly_failure_is_none():
    assert push.domain_in_instantly("a.com", key="k" * 20, post=lambda *a, **k: _R(500, {})) is None
    def boom(*a, **k):
        raise RuntimeError("down")
    assert push.domain_in_instantly("a.com", key="k" * 20, post=boom) is None
    assert push.domain_in_instantly("a.com", key="") is None
