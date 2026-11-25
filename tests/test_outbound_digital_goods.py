"""Digital-goods sellers stay out of future outbound loads.

Designer's Oasis (shop.designersoasis.com) passed company fit because the
category said Home & Interior Decor. None of its products ship. These tests
cover that catalog, a mixed store, a physical store that merely has an
e-book app, and a med spa.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

import outbound_contacts as contacts
import outbound_enrich as enrich
import outbound_instantly_push as push
import outbound_openers as openers
import outbound_personalization as personalization
import outbound_pipeline as pipeline
import outbound_reengage as reengage


def _product(title: str, ships: bool, *, product_type: str = "") -> dict:
    return {
        "title": title,
        "product_type": product_type,
        "variants": [
            {"title": "Default Title", "requires_shipping": ships},
            {"title": "Spare", "requires_shipping": False},
        ],
    }


def designers_oasis() -> list[dict]:
    """0 of 13 products ship. Titles are the printable templates on that shop."""

    titles = [
        "Boho Gallery Wall Printable Template",
        "Abstract Line Art Download",
        "SVG Floral Cut File",
        "Wedding Invitation Template",
        "Font Pairing Guide PDF",
        "Lightroom Preset Pack",
        "Editable Pattern Sheet",
        "E-book of Interior Styling",
        "Canva Template Bundle",
        "Printable Quote Set",
        "Digital Download Lookbook",
        "Masterclass Companion PDF",
        "Membership Welcome Template",
    ]
    return [_product(title, False, product_type="Digital") for title in titles]


def mixed_store() -> list[dict]:
    return [
        _product("Ceramic Table Lamp", True, product_type="Lighting"),
        _product("Linen Throw Pillow", True, product_type="Textiles"),
        _product("Oak Picture Frame", True, product_type="Frames"),
        _product("SVG Monogram Template", False, product_type="Digital"),
    ]


def physical_with_ebook_app() -> tuple[dict, list[dict]]:
    store = {
        "name": "press.com",
        "merchant_name": "Press Goods",
        "categories": "Beauty",
        "description": "Bar soap and hand cream",
        "apps": [{"name": "E-book Download", "token": "ebook-download", "categories": ["Digital products"]}],
        "estimated_sales_yearly": 5_000_000_00,
        "country_code": "US",
        "platform": "shopify",
        "tags": "",
    }
    catalog = [
        _product("Cedar Bar Soap", True),
        _product("Hand Cream", True),
        _product("Gift Set", True),
    ]
    return store, catalog


def med_spa() -> list[dict]:
    return [
        _product("Hydrafacial Service", False, product_type="Service"),
        _product("Massage Booking", False, product_type="Booking"),
        _product("Intro Facial Class", False, product_type="Class"),
        _product("Gift Card", False, product_type="Gift Card"),
    ]


def _fit(**over):
    store = {
        "name": "goodbrand.com",
        "merchant_name": "Good Brand",
        "platform": "shopify",
        "country_code": "US",
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "Beauty",
        "tags": "",
        "description": "",
        "apps": [],
    }
    store.update(over)
    return store


def test_storeleads_asks_for_product_weights():
    fields = set(pipeline._STORELEADS_FIELDS.split(","))
    assert {"avg_weight", "max_weight", "min_weight"} <= fields


def test_decor_gift_and_stationery_do_not_qualify_a_template_shop():
    decor = _fit(categories="Decor", description="svg template downloads", merchant_name="Oasis")
    gift = _fit(categories="Gifts", description="printable download", merchant_name="Paper")
    stationery = _fit(
        categories="Stationery", description="editable template", merchant_name="Desk",
    )
    for store in (decor, gift, stationery):
        assert pipeline.store_niche(store) == ""
        assert not pipeline.passes_company_fit(store)

    home = _fit(
        categories="Home & Interior Decor",
        description="Printable wall templates",
        merchant_name="Designer's Oasis",
        name="shop.designersoasis.com",
    )
    assert pipeline.store_niche(home) == "home_lifestyle"
    assert pipeline.company_blocked_reason(home) == "digital"
    assert not pipeline.passes_company_fit(home)

    physical = _fit(
        categories="Home & Interior Decor",
        description="Ceramic vases thrown in Ohio",
        avg_weight=420,
    )
    assert pipeline.passes_company_fit(physical)
    assert pipeline.digital_product_reason(physical) == ""
    assert pipeline.store_profile(physical)["avg_weight"] == 420


def test_designers_oasis_catalog_is_digital_and_a_mix_is_kept():
    oasis = _fit(
        categories="Home & Interior Decor",
        merchant_name="Designer's Oasis",
        name="shop.designersoasis.com",
        description="",
    )
    assert pipeline.digital_product_reason(oasis, designers_oasis()) == "digital"
    oasis["catalog"] = designers_oasis()
    assert pipeline.company_blocked_reason(oasis) == "digital"
    assert not pipeline.passes_company_fit(oasis)

    mixed = _fit(categories="Home", description="Lamps, pillows, and one svg file")
    assert pipeline.digital_product_reason(mixed, mixed_store()) == ""
    mixed["catalog"] = mixed_store()
    assert pipeline.company_blocked_reason(mixed) == ""
    assert pipeline.passes_company_fit(mixed)

    mostly_files = [
        _product("Oak Frame", True),
        _product("Template Pack", False),
        _product("Ebook", False),
    ]
    assert pipeline.digital_product_reason(mixed, mostly_files) == "digital"


def test_ebook_app_keeps_physical_goods_and_does_not_reject_on_the_app_alone():
    store, catalog = physical_with_ebook_app()
    assert pipeline.digital_product_reason(store, catalog) == ""
    assert pipeline.passes_company_fit({**store, "catalog": catalog})

    weighted = {**store, "avg_weight": 180}
    assert pipeline.digital_product_reason(weighted, None) == ""
    assert pipeline.passes_company_fit(weighted)

    assert pipeline.digital_product_reason(store, None) == "digital"
    assert not pipeline.passes_company_fit(store)


def test_med_spa_is_no_shippable_goods_and_gift_cards_are_not_counted():
    spa = _fit(categories="Beauty", merchant_name="Glow Med Spa", description="Facials by appointment")
    assert pipeline.digital_product_reason(spa, med_spa()) == "no_shippable_goods"

    only_cards = [_product("Gift Card", False), _product("Shipping Protection", False)]
    assert pipeline.digital_product_reason(_fit(avg_weight=200), only_cards) == ""
    assert pipeline.digital_product_reason(
        _fit(apps=[{"name": "Klaviyo"}]), only_cards,
    ) == ""


def test_zero_physical_without_a_digital_word_is_still_digital():
    unnamed = [_product("Item A", False), _product("Item B", False)]
    assert pipeline.digital_product_reason(_fit(), unnamed) == "digital"


@pytest.mark.real_catalog
def test_catalog_fetch_treats_timeouts_and_html_as_unreadable(monkeypatch):
    import sales_support_agent.services.public_url_guard as guard

    monkeypatch.setattr(guard, "public_http_url", lambda value: value)

    class HTML:
        status_code = 200
        headers = {"Content-Type": "text/html"}

        def json(self):
            raise ValueError("not json")

    assert pipeline.fetch_public_catalog("example.com", get=lambda _url, timeout: HTML()) is None

    def explode(_url, timeout):
        raise TimeoutError("timed out")

    assert pipeline.fetch_public_catalog("example.com", get=explode) is None

    class OK:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def json(self):
            return {"products": designers_oasis()[:2] + ["skip"]}

    found = pipeline.fetch_public_catalog("example.com", get=lambda _url, timeout: OK())
    assert found is not None and len(found) == 2
    assert found[0]["title"].startswith("Boho")


@pytest.fixture
def engine():
    return create_engine("sqlite://")


def _find_guard(calls):
    def find(domain, *, api_key, max_contacts=2, **kwargs):
        calls.append(("prospeo", domain))
        return contacts.FindResult(
            domain=domain, credits_used=4,
            contacts=[contacts.Contact(
                domain=domain, first_name="A", last_name="B",
                email=f"a@{domain}", title="Founder",
            )],
        )

    def apollo(domain, **kwargs):
        calls.append(("apollo", domain))
        raise AssertionError("apollo was called")

    return find, apollo


def test_enrichment_skips_a_digital_catalog_with_no_credits_and_no_retry(engine, monkeypatch):
    calls = []
    find, apollo = _find_guard(calls)
    catalogs = {
        "shop.designersoasis.com": designers_oasis(),
        "soap.com": physical_with_ebook_app()[1],
    }
    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda domain: catalogs.get(domain))
    oasis = {
        "domain": "shop.designersoasis.com",
        "brand": "Designer's Oasis",
        "recipe": "amz_off",
        "tier": "A",
        "categories": "Home & Interior Decor",
        "country_code": "US",
        "store_profile": {"categories": "Home & Interior Decor", "merchant_name": "Designer's Oasis"},
    }
    soap = {
        "domain": "soap.com",
        "brand": "Press Goods",
        "recipe": "amz_off",
        "tier": "A",
        "categories": "Beauty",
        "country_code": "US",
        "store_profile": physical_with_ebook_app()[0],
    }
    report = enrich.run_enrichment(
        engine, [oasis, soap], api_key="k", find=find, write=lambda _lead, _contact: ("Press sells soap.", "ai"),
        apollo_find=apollo, apollo_api_key="present",
    )
    assert calls == [("prospeo", "soap.com")]
    assert report.credits_used == 4
    assert report.companies_tried == 1
    assert report.skip_reasons["digital"] == 1
    with engine.connect() as conn:
        status, found, credits, reason = conn.execute(text(
            "SELECT status, contacts_found, credits_used, reason "
            "FROM outbound_enriched WHERE domain = 'designersoasis.com'"
        )).one()
    assert status == "skipped" and found == 0 and credits == 0 and reason == "digital"
    again = enrich.run_enrichment(
        engine, [oasis], api_key="k", find=find, apollo_find=apollo, apollo_api_key="present",
    )
    assert again.skipped_already_enriched == 1
    assert calls == [("prospeo", "soap.com")]


def test_unreadable_catalog_rejects_only_with_no_weight_and_a_digital_signal(engine):
    calls = []
    find, apollo = _find_guard(calls)

    def timeout(_domain):
        raise TimeoutError("timed out")

    app_only = {
        "domain": "files.com",
        "brand": "Files",
        "recipe": "amz_off",
        "categories": "Beauty",
        "country_code": "US",
        "store_profile": {"apps": [{"name": "Digital Downloads"}], "categories": "Beauty"},
    }
    weighted = {
        "domain": "soap.com",
        "brand": "Soap",
        "recipe": "amz_off",
        "categories": "Beauty",
        "country_code": "US",
        "store_profile": {
            "apps": [{"name": "E-book Download"}],
            "avg_weight": 90,
            "categories": "Beauty",
            "description": "Bar soap",
            "most_recently_published_product": {"title": "Cedar Bar Soap"},
        },
    }
    report = enrich.run_enrichment(
        engine, [app_only, weighted], api_key="k", find=find, catalog_for=timeout,
        write=lambda _lead, _contact: ("Soap sells cedar bar soap.", "ai"),
        apollo_find=apollo,
    )
    assert calls == [("prospeo", "soap.com")]
    assert report.credits_used == 4
    assert report.skip_reasons["digital"] == 1
    with engine.connect() as conn:
        reason = conn.execute(text(
            "SELECT reason FROM outbound_enriched WHERE domain = 'files.com'"
        )).scalar()
    assert reason == "digital"


def test_med_spa_enrichment_records_no_shippable_goods(engine, monkeypatch):
    calls = []
    find, _apollo = _find_guard(calls)
    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda _domain: med_spa())
    lead = {
        "domain": "glowspa.com", "brand": "Glow Med Spa", "recipe": "amz_off",
        "categories": "Beauty", "country_code": "US",
    }
    enrich.run_enrichment(engine, [lead], api_key="k", find=find, write=lambda *_a: ("", "none"))
    assert calls == []
    with engine.connect() as conn:
        status, reason, credits = conn.execute(text(
            "SELECT status, reason, credits_used FROM outbound_enriched WHERE domain = 'glowspa.com'"
        )).one()
    assert status == "skipped" and reason == "no_shippable_goods" and credits == 0


def test_push_pending_rechecks_and_does_not_write_a_suppression_row(engine, monkeypatch):
    from sales_support_agent.services.outbound_memory import save_store_profiles

    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda domain: {
        "shop.designersoasis.com": designers_oasis(),
        "soap.com": physical_with_ebook_app()[1],
    }.get(domain))
    enrich.ensure_tables(engine)
    save_store_profiles(engine, [{
        "domain": "soap.com",
        "store_profile": {
            "merchant_name": "Press Goods",
            "description": "Bar soap made in small batches",
            "categories": "Beauty",
            "avg_weight": 80,
            "most_recently_published_product": {
                "title": "Cedar Bar Soap",
                "url": "https://soap.com/products/cedar-bar-soap",
            },
        },
    }])
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_enriched_contacts "
            "(email, domain, first_name, last_name, title, opener, brand, category, recipe, tier) "
            "VALUES "
            "('ada@shop.designersoasis.com', 'shop.designersoasis.com', 'Ada', 'Lovelace', "
            "'Founder', 'Designers Oasis sells a template.', 'Designers Oasis', "
            "'Home & Interior Decor', 'amz_off', 'A'),"
            "('bea@soap.com', 'soap.com', 'Bea', 'Smith', 'Founder', "
            "'Press Goods sells cedar bar soap.', 'Press Goods', 'Beauty', 'amz_off', 'A')"
        ))
    sent = []

    def record(payload, cid, dry_run=False, **kwargs):
        sent.extend(item["email"] for item in payload)
        return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)

    out = enrich.push_pending(engine, "camp", dry_run=False, push=record)
    assert sent == ["bea@soap.com"]
    assert out["pushed"] == 1
    assert "do not ship" in out["note"]
    with engine.connect() as conn:
        reason = conn.execute(text(
            "SELECT skip_reason FROM outbound_enriched_contacts "
            "WHERE email = 'ada@shop.designersoasis.com'"
        )).scalar()
        tables = {
            row[0] for row in conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ))
        }
    assert reason == "digital"
    assert "outbound_suppression" not in tables
    assert not reason.startswith("suppressed")


def test_partner_load_skips_a_future_digital_domain(engine, monkeypatch):
    calls = []
    find, _apollo = _find_guard(calls)
    monkeypatch.setattr(
        pipeline, "fetch_public_catalog",
        lambda _domain: designers_oasis(),
    )
    out = enrich.enrich_domain_list(
        engine, ["shop.designersoasis.com"], api_key="k", dry_run=False, find=find,
        write=lambda *_a: ("should not be written", "ai"),
    )
    assert calls == []
    assert out["contacts_found"] == 0
    with engine.connect() as conn:
        status, reason, credits = conn.execute(text(
            "SELECT status, reason, credits_used FROM outbound_enriched"
        )).one()
    assert status == "skipped" and reason == "digital" and credits == 0


def test_reengage_load_skips_a_future_digital_domain_and_keeps_a_physical_one(monkeypatch):
    monkeypatch.setenv("OUTBOUND_REENGAGE_TOPUP_ENABLED", "true")
    monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
    catalogs = {
        "templates.com": designers_oasis(),
        "soap.com": physical_with_ebook_app()[1],
    }
    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda domain: catalogs.get(domain))
    calls = []

    class Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    def post(url, **kwargs):
        body = kwargs.get("json") or {}
        calls.append((url, body))
        if url.endswith("/leads/list"):
            campaign = body.get("campaign")
            if campaign == reengage.TARGET_CAMPAIGN_ID:
                return Response({"items": []})
            return Response({"items": [
                {"id": "dig", "email": "ada@templates.com", "status": 3, "first_name": "Ada",
                 "company_name": "Designer's Oasis", "campaign": campaign},
                {"id": "phy", "email": "bea@soap.com", "status": 3, "first_name": "Bea",
                 "company_name": "Press Goods", "campaign": campaign},
            ]})
        return Response({"id": "job"})

    from datetime import datetime
    from zoneinfo import ZoneInfo

    monday = datetime(2026, 11, 16, 7, tzinfo=ZoneInfo("America/Denver"))
    result = reengage.run_weekly_topup(
        post=post, now=monday,
        lookup_countries=lambda domains: {domain: "US" for domain in domains},
    )
    assert result["moved"] == 1
    assert result["rejected"].get("digital", 0) >= 1
    moves = [body for url, body in calls if url == reengage.MOVE_URL]
    assert len(moves) == 1 and moves[0]["ids"] == ["phy"]
    assert all("delete" not in url and "pause" not in url for url, _body in calls)


def test_facts_and_shopper_lines_skip_non_shipping_products():
    lead = {
        "brand": "Press Goods",
        "domain": "press.com",
        "email": "bea@press.com",
        "categories": "Beauty",
        "description": "Bar soap made in small batches",
        "site_product": "Membership welcome kit",
        "store_profile": {
            "merchant_name": "Press Goods",
            "description": "Bar soap made in small batches",
            "most_recently_published_product": {
                "title": "Gift Card",
                "url": "https://press.com/products/gift-card",
            },
            "avg_weight": 80,
        },
    }
    facts = openers.store_verified_facts(lead)
    assert "Gift Card" not in facts
    assert not any("membership" in fact.lower() for fact in facts)
    assert any("bar soap" in fact.lower() for fact in facts)
    opener, _angle = openers.shopper_lines(lead)
    assert "bar soap" in opener.lower()
    assert "gift card" not in opener.lower()
    assert "membership" not in opener.lower()
    assert "template" not in opener.lower()

    digital = {
        "brand": "Designer's Oasis",
        "domain": "shop.designersoasis.com",
        "categories": "Home & Interior Decor",
        "description": "Printable wall templates",
        "store_profile": {
            "description": "Printable wall templates",
            "most_recently_published_product": {"title": "Boho Gallery Wall Printable Template"},
        },
    }
    assert openers.store_verified_facts(digital) == []
    assert openers.shopper_lines(digital) == ("", "")
    assert personalization.fact_sheet(digital).facts == []


def test_shipping_evidence_keeps_a_stationery_niche_that_mentions_patterns():
    notebooks = _fit(
        categories="Stationery",
        description="Notebooks with floral patterns and lined pages",
        merchant_name="Field Notes Co",
        avg_weight=180,
    )
    assert pipeline.store_niche(notebooks) == "family_gifts"
    assert pipeline.passes_company_fit(notebooks)
    catalog = [_product("Floral Pattern Notebook", True), _product("Grid Notebook", True)]
    no_weight = {k: v for k, v in notebooks.items() if k != "avg_weight"}
    no_weight["catalog"] = catalog
    assert pipeline.store_niche(no_weight) == "family_gifts"
    assert pipeline.passes_company_fit(no_weight)
    templates = _fit(categories="Stationery", description="printable template downloads")
    assert pipeline.store_niche(templates) == ""
    assert not pipeline.passes_company_fit(templates)


def test_sourced_leads_keep_the_storefront_host():
    store = _fit(
        name="shop.designersoasis.com",
        merchant_name="Designer's Oasis",
        categories="Home & Interior Decor",
        description="Printable wall templates",
    )
    profile = pipeline.store_profile(store)
    assert profile["catalog_host"] == "shop.designersoasis.com"
    lead = pipeline.to_clay_lead(store)
    assert lead["domain"] == "designersoasis.com"
    assert lead["catalog_host"] == "shop.designersoasis.com"
    assert lead["store_profile"]["catalog_host"] == "shop.designersoasis.com"
    assert "catalog_host" not in pipeline.store_profile(_fit(name="soap.com"))


def test_enrichment_reads_the_storefront_and_keeps_a_physical_catalog_for_copy(engine, monkeypatch):
    """Apex has no feed. The shop host does. A physical catalog is not re-classified as digital."""

    fetched = []

    def fetch(host):
        fetched.append(host)
        if host == "shop.press.com":
            return physical_with_ebook_app()[1]
        if host == "shop.designersoasis.com":
            return designers_oasis()
        return None

    monkeypatch.setattr(pipeline, "fetch_public_catalog", fetch)
    calls = []
    find, apollo = _find_guard(calls)
    press = {
        "domain": "press.com",
        "brand": "Press Goods",
        "recipe": "amz_off",
        "tier": "A",
        "categories": "Beauty",
        "country_code": "US",
        "catalog_host": "shop.press.com",
        "store_profile": {
            "catalog_host": "shop.press.com",
            "merchant_name": "Press Goods",
            "categories": "Beauty",
            "description": "Cedar bar soap",
            "apps": [{"name": "E-book Download", "categories": ["Digital products"]}],
            "most_recently_published_product": {
                "title": "Cedar Bar Soap",
                "url": "https://shop.press.com/products/cedar-bar-soap",
            },
        },
    }
    oasis = {
        "domain": "designersoasis.com",
        "brand": "Designer's Oasis",
        "recipe": "amz_off",
        "categories": "Home & Interior Decor",
        "country_code": "US",
        "store_profile": {"catalog_host": "shop.designersoasis.com", "categories": "Decor"},
    }
    report = enrich.run_enrichment(
        engine, [press, oasis], api_key="k", find=find, apollo_find=apollo, apollo_api_key="present",
        write=lambda _lead, _contact: ("Press Goods sells cedar bar soap.", "ai"),
    )
    assert fetched[0] == "shop.press.com"
    assert "shop.designersoasis.com" in fetched
    assert calls == [("prospeo", "press.com")]
    assert report.credits_used == 4
    assert report.skip_reasons["digital"] == 1
    with engine.connect() as conn:
        skip = conn.execute(text(
            "SELECT COALESCE(skip_reason, '') FROM outbound_enriched_contacts WHERE email = 'a@press.com'"
        )).scalar()
    assert skip == ""


def test_push_keeps_a_physical_catalog_when_the_profile_has_a_digital_app(engine, monkeypatch):
    from sales_support_agent.services.outbound_memory import save_store_profiles

    monkeypatch.setattr(
        pipeline, "fetch_public_catalog",
        lambda host: physical_with_ebook_app()[1] if "soap.com" in host else None,
    )
    enrich.ensure_tables(engine)
    save_store_profiles(engine, [{
        "domain": "soap.com",
        "store_profile": {
            "catalog_host": "shop.soap.com",
            "merchant_name": "Press Goods",
            "description": "Cedar bar soap",
            "categories": "Beauty",
            "apps": ["E-book Download"],
            "most_recently_published_product": {
                "title": "Cedar Bar Soap",
                "url": "https://shop.soap.com/products/cedar-bar-soap",
            },
        },
    }])
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_enriched_contacts "
            "(email, domain, first_name, last_name, title, opener, brand, category, recipe, tier) "
            "VALUES ('bea@soap.com', 'soap.com', 'Bea', 'Smith', 'Founder', "
            "'Press Goods sells cedar bar soap.', 'Press Goods', 'Beauty', 'amz_off', 'A')"
        ))
    sent = []

    def record(payload, cid, dry_run=False, **kwargs):
        sent.extend(item["email"] for item in payload)
        return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)

    out = enrich.push_pending(engine, "camp", dry_run=False, push=record)
    assert sent == ["bea@soap.com"]
    assert out["pushed"] == 1
    with engine.connect() as conn:
        reason = conn.execute(text(
            "SELECT COALESCE(skip_reason, '') FROM outbound_enriched_contacts WHERE email = 'bea@soap.com'"
        )).scalar()
    assert reason == ""


def test_abby_deck_holds_instead_of_building(monkeypatch):
    import abby_deck

    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda *_args, **_kwargs: designers_oasis())

    def explode(self, **_kwargs):
        raise AssertionError("deck was built")

    import sales_support_agent.services.deck_generator as generator

    monkeypatch.setattr(generator.DeckGenerationService, "generate_deck", explode)
    held = abby_deck.build_prospect_deck(
        domain="shop.designersoasis.com", company="Designer's Oasis",
    )
    assert held.view_url == ""
    assert held.content_gate_passed is False
    assert "digital" in held.content_gate_findings

    monkeypatch.setattr(pipeline, "fetch_public_catalog", lambda *_args, **_kwargs: med_spa())
    spa = abby_deck.build_prospect_deck(domain="glowspa.com", company="Glow Med Spa")
    assert spa.content_gate_findings == ("no_shippable_goods",)
    assert spa.view_url == ""
