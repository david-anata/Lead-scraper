"""Dedupe, contact quality, US-only sourcing, and gated personalization."""

from types import SimpleNamespace

import outbound_campaigns as campaigns
import outbound_contacts as contacts
import outbound_openers as openers
import outbound_pipeline as pipeline
import outbound_reengage as reengage


def test_title_rank_drops_associate_and_non_buyers_and_keeps_founders():
    assert contacts.title_rank("E-commerce Merchandise Associate") is None
    assert contacts.title_rank("Marketing & Support Associate") is None
    assert contacts.title_rank("Product Owner, Requirements & UX Engineering/Design") is None
    assert contacts.title_rank("Graphic Design & E-Commerce Manager") is None
    assert contacts.title_rank("VP of Engineering") is None
    assert contacts.title_rank("Head of HR") is None
    assert contacts.title_rank("Founder") == 0
    assert contacts.title_rank("Founder & Designer") == 0
    assert contacts.title_rank("Brand Owner") == 0
    assert contacts.title_rank("Head of Ecommerce") == 1
    assert contacts.title_rank("Ecommerce Manager") == 1
    assert contacts.title_rank("Amazon Manager") == 1


def test_enrich_rechecks_the_saved_title_so_associate_cannot_slip_through():
    """Search said Ecommerce Manager. The enriched title is the one we keep."""

    search = [{"person": {"person_id": "p1", "current_job_title": "Ecommerce Manager"}}]
    enrich = {
        "p1": (200, {
            "error": False,
            "person": {
                "person_id": "p1", "first_name": "Marla", "last_name": "Lee",
                "current_job_title": "E-commerce Merchandise Associate",
                "email": {"status": "VERIFIED", "email": "marla@bbqing.com"},
            },
        }),
    }

    def post(url, *, key, body, timeout=25):
        if url == contacts.PROSPEO_SEARCH_URL:
            return 200, {"error": False, "results": search}
        return enrich[body["data"]["person_id"]]

    found = contacts.find_contacts("bbqing.com", api_key="k" * 20, post=post, sleep=lambda _s: None)
    assert found.contacts == []
    assert "title" in found.reason


def test_role_mailbox_catches_variants_and_leaves_personal_names():
    blocked = (
        "press@brand.com", "pr@brand.com", "media@brand.com", "wholesale@brand.com",
        "care@brand.com", "shop@brand.com", "store@brand.com", "partnerships@brand.com",
        "billing@brand.com", "returns@brand.com", "careers@brand.com", "jobs@brand.com",
        "accounts@brand.com", "sales1@brand.com", "info.us@brand.com", "hello-team@brand.com",
        "customer.service@brand.com",
    )
    for address in blocked:
        assert contacts.is_role_mailbox(address), address
    for address in ("priya@brand.com", "jane.smith@brand.com", "hillary@brand.com", "storey@brand.com"):
        assert not contacts.is_role_mailbox(address), address
    assert not contacts.acceptable_first_name("")
    assert not contacts.acceptable_first_name("there")
    assert contacts.acceptable_first_name("Marla")


def test_list_domains_query_forces_us_even_when_the_caller_drops_it():
    params = pipeline.build_storeleads_query(extra_params={
        "f:cc": "AU,GB,CA", "f:cc:op": "or", "f:p": "shopify",
    })
    assert params["f:cc"] == "US"
    assert params["f:cc:op"] == "or"
    widened = campaigns.resolve_filters({"f:cc": "US,GB,CA,AU", "f:p": "shopify", "f:ds": "Active"})
    assert widened["f:cc"] == "US"
    assert campaigns.resolve_filters(None) == {}
    assert campaigns.resolve_filters({}) == {}


def test_company_fit_rejects_non_us_and_retailers():
    base = {
        "country_code": "US", "estimated_sales_yearly": 5_000_000_00,
        "categories": "Beauty", "merchant_name": "Glow", "tags": "",
    }
    assert pipeline.passes_company_fit(base)
    assert not pipeline.passes_company_fit({**base, "country_code": "AU"})
    assert not pipeline.passes_company_fit({**base, "country_code": "CA"})
    assert not pipeline.passes_company_fit({**base, "country_code": "GB"})
    assert not pipeline.passes_company_fit({**base, "country_code": "EE"})
    assert not pipeline.passes_company_fit({**base, "country_code": ""})
    assert not pipeline.passes_company_fit({**base, "brands_page": "https://brand.example/pages/brands"})


class _Response:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


def test_topup_moves_one_record_and_does_not_copy_a_negative_or_a_role(monkeypatch):
    monkeypatch.setenv("OUTBOUND_REENGAGE_TOPUP_ENABLED", "true")
    monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
    calls = []

    def post(url, **kwargs):
        body = kwargs.get("json") or {}
        calls.append((url, body))
        if url.endswith("/leads/list"):
            campaign = body.get("campaign")
            if campaign == reengage.TARGET_CAMPAIGN_ID:
                return _Response(200, {"items": []})
            return _Response(200, {"items": [
                {"id": "keep", "email": "done@brand.com", "status": 3, "first_name": "Done",
                 "company_name": "Brand", "campaign": campaign},
                {"id": "old", "email": "done@brand.com", "status": 3, "first_name": "Done",
                 "campaign": "somewhere-else"},
                {"id": "nope", "email": "nope@brand.com", "status": 3, "first_name": "No",
                 "lt_interest_status": -1, "campaign": campaign},
                {"id": "role", "email": "sales1@brand.com", "status": 3, "first_name": "Sales",
                 "campaign": campaign},
                {"id": "blank", "email": "noname@brand.com", "status": 3, "first_name": "",
                 "campaign": campaign},
                {"id": "assoc", "email": "marla@bbqing.com", "status": 3, "first_name": "Marla",
                 "title": "E-commerce Merchandise Associate", "campaign": campaign},
            ]})
        return _Response(200, {"id": "job"})

    from datetime import datetime
    from zoneinfo import ZoneInfo

    monday = datetime(2026, 11, 9, 7, tzinfo=ZoneInfo("America/Denver"))
    result = reengage.run_weekly_topup(
        post=post,
        now=monday,
        lookup_countries=lambda domains: {domain: "US" for domain in domains},
    )
    assert result["moved"] == 1
    assert result["added"] == 0
    move = next(body for url, body in calls if url == reengage.MOVE_URL)
    assert move["copy_leads"] is False
    assert move["ids"] == ["keep"]
    assert move["to_campaign_id"] == reengage.TARGET_CAMPAIGN_ID
    assert all(body.get("skip_if_in_workspace") is not False for _url, body in calls)
    assert "negative or unsubscribed" in result["rejected"]
    assert "role mailbox" in result["rejected"]
    assert "missing first name" in result["rejected"]
    assert "title" in result["rejected"]


def test_topup_plan_skips_a_person_who_bounced_on_another_record():
    plan = reengage.plan_reengage_actions(
        [{"id": "newish", "email": "ada@brand.com", "first_name": "Ada", "last_name": "", "company_name": "Brand"}],
        {"ada@brand.com": [
            {"id": "newish", "email": "ada@brand.com", "status": 3, "campaign": "old"},
            {"id": "bounced", "email": "ada@brand.com", "status": -1, "campaign": "older"},
        ]},
    )
    assert plan["moves"] == []
    assert plan["adds"] == []
    assert plan["rejected"]["bounced"] == 1


def test_topup_adds_only_when_no_record_exists_and_skips_workspace_dupes():
    body = reengage.move_body(["abc"], reengage.TARGET_CAMPAIGN_ID)
    assert body["copy_leads"] is False
    plan = reengage.plan_reengage_actions(
        [{"id": "", "email": "new@brand.com", "first_name": "New", "last_name": "Person", "company_name": "Brand"}],
        {},
    )
    assert plan["adds"][0]["email"] == "new@brand.com"
    assert plan["moves"] == []


def test_personalization_stays_off_until_the_flag_and_then_stays_factual(monkeypatch):
    lead = {
        "brand": "RICH Hair Care",
        "domain": "richhaircare.com",
        "recipe": "amz_on",
        "categories": "Beauty",
        "store_profile": {
            "sales_channels": ["Amazon"],
            "apps": ["Klaviyo"],
            "technologies": ["Facebook Pixel", "Google Ads"],
            "plan": "Shopify",
            "product_count": 14,
            "city": "Philadelphia",
            "store_locator_page": "https://richhaircare.com/pages/stores",
            "last_platform": "woocommerce",
            "last_platform_change_at": "2026-09-06T00:00:00Z",
            "contact_info": [{"type": "tiktok", "followers": 1000, "followers_30d": 50}],
        },
    }
    monkeypatch.delenv(openers.RICH_PERSONALIZATION_FLAG, raising=False)
    before_facts = openers.collect_facts(lead)
    assert before_facts.rich_phrases == ()
    before, _source = openers.write_opener(lead)
    assert "Klaviyo" not in before
    after_lead = {**lead, "rich_personalization": True}
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    after, source = openers.write_opener(after_lead)
    angle, _angle_source = openers.write_brand_angle(after_lead)
    assert source == "fallback"
    assert "Klaviyo" in after
    assert "Amazon" in after
    assert "is based in Philadelphia" in openers.collect_facts(after_lead).rich_phrases
    for line in (after, angle):
        assert "—" not in line and "–" not in line
        assert not any(char.isdigit() for char in line)
    assert "fourteen" in after or "Klaviyo" in after


def test_non_us_profile_is_not_required_for_the_flag_off_opener():
    store = {
        "name": "coziya-outdoor.com", "merchant_name": "Coziya",
        "country_code": "EE", "categories": "Home",
        "estimated_sales_yearly": 5_000_000_00, "tags": "",
    }
    assert not pipeline.passes_company_fit(store)
    profile = pipeline.store_profile(store)
    assert profile["country_code"] == "EE"
    assert "most_recently_published_product" in profile
