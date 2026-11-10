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
    assert pipeline.passes_company_fit({**base, "country_code": ""})
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
        calls.append((url, body, kwargs.get("headers") or {}))
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
    move_call = next(item for item in calls if item[0] == reengage.MOVE_URL)
    move = move_call[1]
    assert move_call[2].get("Authorization") == "Bearer test-key"
    assert move["copy_leads"] is False
    assert move["ids"] == ["keep"]
    assert move["to_campaign_id"] == reengage.TARGET_CAMPAIGN_ID
    assert all(body.get("skip_if_in_workspace") is not False for _url, body, _headers in calls)
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
    angle, angle_source = openers.write_brand_angle(after_lead)
    assert source == "none" and angle_source == "none"
    assert after == "" and angle == ""
    assert "is based in Philadelphia" in openers.store_fact_phrases(after_lead)
    for line in (after, angle):
        assert "—" not in line and "–" not in line
        assert not any(char.isdigit() for char in line)
        assert "http" not in line.lower() and "www." not in line.lower()


def test_rich_lines_sound_like_a_person_and_never_lead_with_a_negative(monkeypatch):
    monkeypatch.delenv(openers.RICH_PERSONALIZATION_FLAG, raising=False)
    off_amazon = {
        "brand": "North Trampoline",
        "domain": "northtrampoline.com",
        "recipe": "amz_off",
        "categories": "sports",
        "amazon_absent": True,
        "rich_personalization": True,
        "store_profile": {
            "store_locator_page": "https://northtrampoline.com/pages/stores",
            "apps": [{"name": "Klaviyo: Email Marketing"}],
            "technologies": ["Facebook Pixel"],
        },
    }
    opener, opener_source = openers.write_opener(off_amazon)
    angle, angle_source = openers.write_brand_angle(off_amazon)
    assert opener == "" and angle == ""
    assert opener_source == "none" and angle_source == "none"

    thin = {
        "brand": "North Trampoline",
        "domain": "northtrampoline.com",
        "recipe": "amz_off",
        "categories": "sports",
        "amazon_absent": True,
        "rich_personalization": True,
        "store_profile": {},
    }
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    thin_line, _source = openers.write_opener(thin)
    thin_angle, _angle_source = openers.write_brand_angle(thin)
    assert thin_line == ""
    assert thin_angle == ""
    for line in (opener, angle, thin_line, thin_angle):
        assert "—" not in line and "–" not in line
        assert not any(char.isdigit() for char in line)
        assert "http" not in line.lower() and "www." not in line.lower()

    ads_lead = {
        "brand": "Wove Made",
        "domain": "wovemade.com",
        "recipe": "replatformed",
        "categories": "/Toys & Hobbies/Arts & Crafts",
        "signals": ["They moved platforms in the last 90 days", "Runs Meta and Google ads"],
        "rich_personalization": True,
        "store_profile": {"apps": ["Affirm pay over time messaging"]},
    }
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    ads_line, _source = openers.write_opener(ads_lead)
    ads_angle, _angle_source = openers.write_brand_angle(ads_lead)
    assert "meta" not in ads_line.lower() and "google" not in ads_line.lower()
    assert "moved" not in ads_line.lower() and "platform" not in ads_line.lower()
    assert "platform" not in ads_angle.lower()
    assert not ads_angle.lower().startswith("anata helps")

    quiet = {
        "brand": "Krazy Kreations",
        "domain": "krazykreations.us",
        "recipe": "replatformed",
        "categories": "/Toys & Hobbies/Arts & Crafts",
        "signals": ["No ad pixel found"],
        "rich_personalization": True,
        "store_profile": {},
    }
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    quiet_line, _source = openers.write_opener(quiet)
    assert "pixel" not in quiet_line.lower()
    assert "moved" not in quiet_line.lower() and "upgraded" not in quiet_line.lower()


def test_a_lone_product_is_kept_and_a_review_app_is_not_called_reviews(monkeypatch):
    monkeypatch.delenv(openers.RICH_PERSONALIZATION_FLAG, raising=False)
    product_only = {
        "brand": "Example",
        "domain": "example-soap.com",
        "rich_personalization": True,
        "store_profile": {
            "most_recently_published_product": {
                "title": "Cedar Soap",
                "published_at": "2026-10-02T00:00:00Z",
            },
        },
    }
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    line, source = openers.write_opener(product_only)
    angle, _angle_source = openers.write_brand_angle(product_only)
    facts = openers.collect_facts(product_only)
    assert "Cedar Soap" in facts.rich_phrases[0]
    assert source == "none"
    assert line == "" and angle == ""

    installed = {
        "brand": "Example",
        "domain": "example-yotpo.com",
        "categories": "hair care",
        "rich_personalization": True,
        "store_profile": {"apps": ["Yotpo"]},
    }
    openers._OPENERS_BY_DOMAIN.clear()
    openers._ANGLES_BY_DOMAIN.clear()
    app_line, _source = openers.write_opener(installed)
    app_angle, _angle_source = openers.write_brand_angle(installed)
    assert "yotpo" not in app_line.lower()
    assert "talking" not in app_line.lower()
    assert not app_angle.lower().startswith("anata helps")
    for text in (line, angle, app_line, app_angle):
        assert "—" not in text and "–" not in text
        assert not any(char.isdigit() for char in text)


def test_style_validator_rejects_digits_dashes_urls_tech_and_low_ratings():
    from outbound_personalization import style_violation

    assert style_violation(
        "Parents leave thousands of reviews and then look for the same product on Amazon."
    ) == ""
    assert style_violation("Too short.") == "too short"
    assert style_violation("A line with  a doubled space in it.") == "repr or placeholder"
    assert style_violation("The catalog is ['apparel'] and nothing else.") == "repr or placeholder"
    assert style_violation("The value was None and that cannot send.") == "repr or placeholder"
    assert style_violation("Hello {{firstName}} this line is long enough to count.") == "repr or placeholder"
    assert style_violation("Plans start at $99.") == "digit"
    assert style_violation("A smart bottle — with coaching.") == "em dash"
    assert style_violation("See https://brand.com/pages for it.") == "url"
    assert style_violation("Anata helps Joybees grow.", role="angle") == "anata helps"
    assert style_violation(
        "They upgraded to Shopify Plus.", recipe="plan_upgrade",
    ) == "tech or backend change"
    assert style_violation(
        "They moved off WooCommerce.", recipe="replatform",
    ) == "tech or backend change"
    assert style_violation("Reviews average three stars.") == "low rating"
    assert style_violation(
        "Reviews average close to five stars.", high_rating=True,
    ) == ""
    assert style_violation(
        "Reviews average close to five stars.", high_rating=False,
    ) == "low rating"


def test_raw_storeleads_files_produce_no_line_without_a_model(monkeypatch):
    import json
    from pathlib import Path

    import outbound_personalization as personalization
    from outbound_pipeline import country_eligibility

    monkeypatch.setattr(
        "sales_support_agent.services.llm_client.llm_available", lambda **_kwargs: False,
    )
    recipes = {
        "lovevery.com": "amz_on", "chewrevgum.com": "amz_on",
        "hazelicious.com": "amz_on", "pureandcimple.com": "amz_on",
        "rivieras.com": "amz_off", "glo910.com": "amz_off",
        "water.io": "amz_off", "bbqing.com": "amz_off",
        "benibeca.com": "plan_upgrade", "drinkhappyviking.com": "plan_upgrade",
        "joybees.com": "plan_upgrade", "casekoo.com": "plan_upgrade",
        "barbellvoodoo.com": "replatform", "wovemade.com": "replatform",
        "krazykreations.us": "replatform", "repriswines.com": "replatform",
        "jaxkelly.com": "abby_reengage", "matatraders.com": "abby_reengage",
    }
    non_us = {
        "rivieras.com": "FR", "glo910.com": "ES",
        "benibeca.com": "MX", "bbqing.com": "CA",
    }
    folder = Path("tests/fixtures/sl_raw")
    files = sorted(folder.glob("*.json"))
    assert len(files) == 18
    for path in files:
        payload = json.loads(path.read_text())
        store = payload["domain"]
        domain = path.stem
        lead = {
            "domain": domain,
            "brand": store.get("merchant_name") or domain,
            "recipe": recipes[domain],
            "categories": store.get("categories"),
            "store": store,
            "rich_personalization": True,
        }
        opener, angle, source, reason = personalization.generate(lead, llm=None, api_key="")
        assert opener == "" and angle == "" and source == "none", domain
        assert reason, domain
        blob = " ".join(personalization.fact_sheet(lead).facts)
        assert "['" not in blob and "{'" not in blob and "[" not in blob, domain
        allowed, country_reason = country_eligibility(store)
        if domain in non_us:
            assert allowed is False, domain
            assert non_us[domain] in country_reason
            assert non_us[domain] in reason or reason in {"multi-brand retailer", "restricted category"}
        if domain == "repriswines.com":
            assert reason == "restricted category"
        if domain == "bbqing.com":
            assert personalization.fact_sheet(lead).blocked == "multi-brand retailer"


def test_backfill_apply_keeps_the_email_and_matches_a_domain_to_a_campaign_lead():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "backfill_reengage", "scripts/backfill_reengage_personalization.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    targets = module._targets_from(["ada@brand.com", "other.com"])
    assert targets[0] == {"email": "ada@brand.com", "domain": "brand.com"}
    assert targets[1]["email"] == ""
    patched: list[tuple[str, str]] = []
    result = module._apply(
        [
            {"email": "ada@brand.com", "domain": "brand.com", "us": True,
             "opener": "Ada sells soap.", "brand_angle": "Anata helps Ada grow."},
            {"email": "", "domain": "other.com", "us": True,
             "opener": "Other sells soap.", "brand_angle": "Anata helps Other grow."},
        ],
        "camp",
        campaign_leads=[{"email": "sam@other.com"}],
        find_lead=lambda email: [{"id": "id-" + email, "email": email}],
        patch_lead=lambda lead_id, row: patched.append((lead_id, row["email"])) or 200,
    )
    assert result["ok"] is True
    assert result["updated"] == 2
    assert {email for _lead_id, email in patched} == {"ada@brand.com", "sam@other.com"}


def _foreign_storefront(name: str, apex: str, **hq: str) -> dict:
    """A US host for a brand whose headquarters are somewhere else.

    country_code US, ships_to_countries, and cluster_domains are the signals
    that used to let the brand through. They are not headquarters.
    """

    return {
        "name": name,
        "merchant_name": apex.split(".")[0],
        "platform": "shopify",
        "country_code": "US",
        "cluster_best_ranked": apex,
        "cluster_domains": [apex, name],
        "ships_to_countries": ["US", "CA", "GB"],
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "/Apparel/Footwear",
        "tags": "",
        "contact_info": [{"type": "email", "value": f"hello@{apex}"}],
        **hq,
    }


def test_us_storefront_does_not_make_a_foreign_headquarters_eligible(caplog):
    import logging

    caplog.set_level(logging.INFO)
    leaked = [
        _foreign_storefront(
            "usa.rivieras.com", "rivieras.com",
            city="Montreuil", location="Montreuil, France",
            administrative_area_level_1="Ile-de-France",
            contact_address="Montreuil, France",
        ),
        _foreign_storefront(
            "usa.glo910.com", "glo910.com",
            city="Vitoria-Gasteiz", location="Vitoria-Gasteiz, Spain",
            contact_info=[{"type": "address", "value": "Vitoria-Gasteiz, Spain"}],
        ),
        _foreign_storefront(
            "us.bbqing.com", "bbqing.com",
            city="Ottawa", location="Ottawa, Ontario, Canada",
            administrative_area_level_1="Ontario",
        ),
    ]
    for store in leaked:
        assert not pipeline.passes_company_fit(store)
        assert not pipeline.passes_company_fit(store, require_niche=False)
        assert not pipeline.store_matches_icp(store)
    text = caplog.text
    assert "headquarters country is FR" in text
    assert "headquarters country is ES" in text
    assert "headquarters country is CA" in text

    flag_only = _foreign_storefront("usa.rivieras.com", "rivieras.com")
    code, reason = pipeline.headquarters_verdict(flag_only)
    assert code == ""
    assert reason == "headquarters country is unknown"
    assert pipeline.passes_company_fit(flag_only)
    assert "headquarters country is unknown; kept" in caplog.text

    rows = {store["name"]: store for store in leaked}
    verdicts = pipeline.eligibility_country_by_domain(rows)
    assert verdicts["rivieras.com"][0] == "FR"
    assert verdicts["glo910.com"][0] == "ES"
    assert verdicts["bbqing.com"][0] == "CA"

    domestic = {
        "name": "lovevery.com",
        "merchant_name": "Lovevery",
        "platform": "shopify",
        "country_code": "US",
        "cluster_best_ranked": "lovevery.com",
        "cluster_domains": ["lovevery.com", "usa.lovevery.com"],
        "ships_to_countries": ["US", "CA", "GB"],
        "city": "San Francisco",
        "location": "San Francisco, California",
        "administrative_area_level_1": "California",
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "Toys",
        "tags": "",
        "contact_info": [{"type": "email", "value": "hi@lovevery.com"}],
    }
    assert pipeline.headquarters_verdict(domestic) == ("US", "")
    assert pipeline.passes_company_fit(domestic)
    assert pipeline.passes_company_fit(domestic, require_niche=False)
    assert pipeline.store_matches_icp(domestic)


def _unknown_hq(**extra: object) -> dict:
    store = {
        "name": "brand.com",
        "merchant_name": "Brand",
        "platform": "shopify",
        "country_code": "",
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "Beauty",
        "tags": "",
        "contact_info": [{"type": "email", "value": "hi@brand.com"}],
    }
    store.update(extra)
    return store


def test_missing_headquarters_is_kept_unless_the_fallback_is_clearly_foreign(caplog):
    import logging

    caplog.set_level(logging.INFO)
    usd = _unknown_hq(currency_code="USD")
    assert pipeline.headquarters_verdict(usd)[0] == ""
    assert pipeline.passes_company_fit(usd)
    assert pipeline.passes_company_fit(usd, require_niche=False)
    assert "headquarters country is unknown; kept" in caplog.text

    caplog.clear()
    bare = _unknown_hq()
    assert pipeline.passes_company_fit(bare)
    assert pipeline.store_matches_icp(bare)
    assert "headquarters country is unknown; kept" in caplog.text

    caplog.clear()
    euro = _unknown_hq(name="brand.de", currency_code="EUR", price_currency="EUR")
    sterling = _unknown_hq(
        name="brand.co.uk", currency_code="GBP",
        contact_info=[{"type": "phone", "value": "+44 20 7946 0958"}],
    )
    assert not pipeline.passes_company_fit(euro)
    assert not pipeline.passes_company_fit(euro, require_niche=False)
    assert not pipeline.store_matches_icp(euro)
    assert not pipeline.passes_company_fit(sterling)
    assert "primary currency is EUR" in caplog.text
    assert "primary currency is GBP" in caplog.text
    phone_only = _unknown_hq(
        name="shop.co.uk", country_code="", currency_code="",
        contact_info=[{"type": "phone", "value": "+44 20 7946 0958"}],
    )
    assert not pipeline.passes_company_fit(phone_only)
    assert "phone country GB" in caplog.text
    store = {
        "name": "coziya-outdoor.com", "merchant_name": "Coziya",
        "country_code": "EE", "categories": "Home",
        "estimated_sales_yearly": 5_000_000_00, "tags": "",
    }
    assert not pipeline.passes_company_fit(store)
    profile = pipeline.store_profile(store)
    assert profile["country_code"] == "EE"
    assert "most_recently_published_product" in profile


def _hq_store(name: str, **extra: object) -> dict:
    store = {
        "name": name,
        "merchant_name": str(extra.pop("merchant_name", name.split(".")[0])),
        "platform": "shopify",
        "country_code": "US",
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "Food",
        "tags": "",
        "currency_code": "USD",
    }
    store.update(extra)
    return store


def test_new_mexico_and_new_england_are_us_headquarters():
    kalm = _hq_store(
        "mykalm.co", merchant_name="MyKalm", country_code="MX",
        city="Albuquerque", administrative_area_level_1="New Mexico",
        location="Albuquerque, New Mexico",
    )
    assert pipeline.headquarters_verdict(kalm) == ("US", "")
    assert pipeline.passes_company_fit(kalm)

    made = _hq_store(
        "madeinnewmexico.com", merchant_name="Made in New Mexico",
        location="New Mexico",
    )
    assert pipeline.headquarters_verdict(made)[0] == "US"
    assert pipeline.passes_company_fit(made)

    england = _hq_store(
        "harbor.com", merchant_name="Harbor Soap",
        location="Portland, New England",
    )
    assert pipeline.headquarters_verdict(england) == ("US", "")
    assert pipeline.passes_company_fit(england)


def test_delaware_registration_is_not_us_hq_and_foreign_storefronts_are_excluded(caplog):
    import logging

    caplog.set_level(logging.INFO)
    registered = _hq_store(
        "brand.com", merchant_name="Brand",
        city="Wilmington", administrative_area_level_1="Delaware",
        location="1209 North Orange Street, Wilmington, Delaware",
        contact_info=[{"type": "address", "value": "1209 North Orange Street, Wilmington, DE"}],
    )
    assert pipeline.headquarters_verdict(registered)[0] == ""
    assert pipeline.passes_company_fit(registered)
    assert "headquarters country is unknown; kept" in caplog.text

    foreign = (
        ("tweezerman.com.au", "Tweezerman Australia", "AU"),
        ("goodevas.se", "Goodevas", "SE"),
        ("decotvframes.ca", "Deco TV Frames", "CA"),
        ("sparkpaws.fr", "Sparkpaws", "FR"),
    )
    for domain, merchant, code in foreign:
        store = _hq_store(
            domain, merchant_name=merchant,
            city="Wilmington", administrative_area_level_1="Delaware",
            location="Wilmington, Delaware",
        )
        verdict, reason = pipeline.headquarters_verdict(store)
        assert verdict == code, (domain, verdict, reason)
        assert not pipeline.passes_company_fit(store), domain

    parent = _hq_store(
        "tweezerman.com.au", merchant_name="Tweezerman",
        city="Port Washington", administrative_area_level_1="New York",
        location="Port Washington, New York",
    )
    assert pipeline.headquarters_verdict(parent) == ("US", "")
    assert pipeline.passes_company_fit(parent)

    street = _hq_store(
        "london-soap.co.uk", merchant_name="London Soap", country_code="GB",
        location="123 Washington Street, London, United Kingdom",
        contact_info=[{"type": "address", "value": "123 Washington Street, London, United Kingdom"}],
    )
    verdict, reason = pipeline.headquarters_verdict(street)
    assert verdict == "GB", (verdict, reason)
    assert not pipeline.passes_company_fit(street)
    seattle = _hq_store(
        "seattle.com", merchant_name="Seattle Soap",
        location="Seattle, Washington",
    )
    assert pipeline.headquarters_verdict(seattle) == ("US", "")
