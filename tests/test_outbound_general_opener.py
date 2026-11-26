"""A missing specific opener uses a general fallback. Disqualifiers stay out."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

import outbound_contacts as oc
import outbound_enrich as oe
import outbound_instantly_push as push
import outbound_openers as op
import outbound_personalization as personalization

DENVER = ZoneInfo("America/Denver")
BANNED = ("amazon", "shopify", "portfolio", "redo", "zantrex")


def _masked(line: str, brand: str) -> str:
    if not brand:
        return line
    return line.replace(brand, "Acme")


def test_each_general_variant_is_one_short_sentence():
    samples = {
        "v1": op._render_general("v1", "Acme", "food and beverage"),
        "v2": op._render_general("v2", "Acme", "food and beverage"),
        "v3": op._render_general("v3", "Acme", ""),
        "v4": op._render_general("v4", "Acme", ""),
        "v5": op._render_general("v5", "", "food and beverage"),
        "v5-bare": op._render_general("v5", "", ""),
    }
    lead = {"brand": "Acme", "domain": "acme.com", "categories": "food_beverage"}
    for key, line in samples.items():
        assert len(line.split()) < 25, key
        assert op.sentence_count(line) == 1, key
        assert op.validate(line, allow_amazon=False)[0], key
        source = "general:v5" if key.startswith("v5") else f"general:{key}"
        assert op.opener_hold_reason(line, lead, source=source) == "", key
        low = line.lower()
        assert "http" not in low and "www." not in low
        assert "—" not in line and "–" not in line and " - " not in line
        assert "?" not in line and not any(ch.isdigit() for ch in line)
        for word in BANNED:
            assert word not in low


def test_tier_selection_and_ugly_brands():
    both = {"brand": "Acme", "domain": "acme.com", "categories": "food_beverage"}
    assert op.general_opener(both)[1] in {"general:v1", "general:v2"}

    store = {"brand": "Acme", "domain": "acme.com"}
    assert op.general_opener(store)[1] == "general:v3"

    name_only = {"brand": "Acme"}
    assert op.general_opener(name_only)[1] == "general:v4"

    for brand in (
        "Shop.bigthirst",
        "Www.innovafoodsco",
        "National Tree Company Business Website · Extra",
        "Order Online @ IMCS",
    ):
        line, source = op.general_opener({"brand": brand, "domain": "example.com"})
        assert line and source.startswith("general:")
        assert source == "general:v5" or brand.split(".")[-1].lower()[:4] in line.lower() or (
            "Bigthirst" in line or "Innovafoodsco" in line
        )

    for brand, domain in (
        ("710 Labs", "710labs.com"),
        ("Y.Chroma Apparel", "ychroma.com"),
        ("A.w.a.k.e. Mode", "awakemode.com"),
    ):
        lead = {"brand": brand, "domain": domain, "categories": "apparel"}
        line, source = op.general_opener(lead)
        assert line and brand in line
        assert source.startswith("general:v")
        masked = _masked(line, brand)
        assert op.validate(masked, allow_amazon=False)[0], (brand, op.validate(masked, allow_amazon=False))
        assert op.opener_hold_reason(line, lead, source=source) == ""


def test_category_cleaning_and_alcoholic_exclusion():
    assert op.clean_category_for_copy({"categories": "food_beverage"}) == "food and beverage"
    assert op.clean_category_for_copy({"categories": "/Business & Industrial/Agriculture"}) == ""
    assert op.clean_category_for_copy({"categories": "religion and belief"}) == ""
    alcohol = {
        "brand": "Harbor",
        "domain": "harbor.com",
        "email": "ada@harbor.com",
        "categories": "alcoholic beverages",
    }
    assert op.clean_category_for_copy(alcohol) == ""
    assert op.lead_exclusion_reason(alcohol) == "restricted_category"
    assert personalization._RESTRICTED.search("alcoholic beverages")
    assert personalization._RESTRICTED.search("craft beers")
    assert personalization._RESTRICTED.search("old distillery")
    assert personalization._RESTRICTED.search("two distilleries")


def test_variant_is_stable_per_domain_and_both_tier_one_lines_appear():
    first = op.general_opener({
        "brand": "Acme", "domain": "www.acme.com", "categories": "food_beverage",
        "email": "a@acme.com",
    })
    second = op.general_opener({
        "brand": "Acme", "domain": "shop.acme.com", "categories": ["food_beverage"],
        "email": "b@acme.com",
    })
    assert first == second
    seen = set()
    for index in range(100):
        _line, source = op.general_opener({
            "brand": "Acme",
            "domain": f"brand{index}.example",
            "categories": "food_beverage",
        })
        seen.add(source)
    assert seen == {"general:v1", "general:v2"}


def _engine():
    return create_engine("sqlite://")


def _push_capture(sent):
    def capture(payload, _cid, dry_run=False, **_kwargs):
        sent.extend(payload)
        return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)

    return capture


def test_discovery_pushes_a_general_opener_when_the_store_has_no_facts(monkeypatch):
    monkeypatch.setattr(
        "sales_support_agent.services.llm_client.llm_available",
        lambda **_kwargs: False,
    )
    engine = _engine()
    sent = []

    def find(domain, *, api_key, max_contacts=2, **_kwargs):
        return oc.FindResult(domain=domain, contacts=[oc.Contact(
            domain=domain, first_name="Ada", last_name="Lee",
            email="ada@plainbrand.com", title="Founder",
        )], credits_used=1)

    lead = {
        "domain": "plainbrand.com",
        "brand": "Plain Brand",
        "categories": ["food_beverage"],
        "recipe": "amz_on",
        "tier": "A",
    }
    result = oe.run_enrichment(
        engine, [lead], api_key="k", find=find, dry_run=False, push=_push_capture(sent),
    )
    assert result.pushed == 1 and sent
    assert sent[0]["custom_variables"]["opener_type"] == "general"
    assert str(sent[0]["custom_variables"]["opener_variant"]).startswith("v")
    with engine.connect() as conn:
        source, reason = conn.execute(text(
            "SELECT opener_source, skip_reason FROM outbound_enriched_contacts "
            "WHERE email = 'ada@plainbrand.com'"
        )).one()
    assert str(source).startswith("general:v")
    assert reason in ("", None)
    assert "held:opener" not in str(reason or "")


def test_push_releases_a_held_opener_and_blanks_a_long_angle():
    engine = _engine()
    oe.ensure_tables(engine)
    from sales_support_agent.services.outbound_memory import save_store_profiles

    save_store_profiles(engine, [{
        "domain": "salsa.com",
        "store_profile": {
            "merchant_name": "Kalm",
            "most_recently_published_product": {"title": "Green Chile Salsa"},
        },
    }])
    long_angle = (
        "Anata helps this brand reach shoppers who already love the product "
        "by making the next order easier than the last one for them."
    )
    assert len(long_angle.split()) > 20
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO outbound_enriched_contacts (
                email, domain, first_name, last_name, title, opener, opener_source,
                brand_angle, brand, category, recipe, tier, email_status, skip_reason
            ) VALUES
            (
                'ada@held.com', 'held.com', 'Ada', 'Lee', 'Founder', '', 'none',
                '', 'Held Brand', 'food_beverage', 'amz_on', 'A', 'VERIFIED',
                'held:opener: no shopper-visible opener'
            ),
            (
                'ada@salsa.com', 'salsa.com', 'Ada', 'Lee', 'Founder',
                'Kalm sells green chile salsa.', 'fallback',
                :angle, 'Kalm', 'food', 'amz_on', 'A', 'VERIFIED', ''
            )
            """
        ), {"angle": long_angle})
    sent = []
    out = oe.push_pending(engine, "camp", dry_run=False, push=_push_capture(sent))
    emails = {row["email"]: row for row in sent}
    assert out["pushed"] == 2
    assert emails["ada@held.com"]["custom_variables"]["opener_type"] == "general"
    assert "held:opener" not in str(emails["ada@held.com"]["personalization"])
    salsa = emails["ada@salsa.com"]
    assert salsa["custom_variables"]["opener_type"] == "specific"
    assert "green chile salsa" in salsa["personalization"].lower()
    assert salsa["custom_variables"]["brandAngle"] == ""
    with engine.connect() as conn:
        held_reason = conn.execute(text(
            "SELECT skip_reason FROM outbound_enriched_contacts WHERE email = 'ada@held.com'"
        )).scalar()
    assert held_reason in ("", None)


def test_dry_run_does_not_release_held_openers():
    engine = _engine()
    oe.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO outbound_enriched_contacts (
                email, domain, first_name, title, opener, opener_source, brand,
                category, recipe, tier, email_status, skip_reason
            ) VALUES (
                'ada@held.com', 'held.com', 'Ada', 'Founder', '', 'none',
                'Held Brand', 'food_beverage', 'amz_on', 'A', 'VERIFIED',
                'held:opener: no shopper-visible opener'
            )
            """
        ))
    sent = []
    oe.push_pending(engine, "camp", dry_run=True, push=_push_capture(sent))
    assert sent == []
    with engine.connect() as conn:
        reason = conn.execute(text(
            "SELECT skip_reason FROM outbound_enriched_contacts WHERE email = 'ada@held.com'"
        )).scalar()
    assert str(reason).startswith("held:opener:")


def _insert(engine, email, domain, brand, **extra):
    row = {
        "email": email,
        "domain": domain,
        "brand": brand,
        "category": extra.get("category", "apparel"),
        "opener": extra.get("opener", f"{brand} sells a cotton tee."),
        "source": extra.get("source", "fallback"),
        "status": extra.get("status", "VERIFIED"),
        "angle": extra.get("angle", ""),
        "recipe": "amz_on",
    }
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO outbound_enriched_contacts (
                email, domain, first_name, last_name, title, opener, opener_source,
                brand_angle, brand, category, recipe, tier, email_status
            ) VALUES (
                :email, :domain, 'Ada', 'Lee', 'Founder', :opener, :source,
                :angle, :brand, :category, :recipe, 'A', :status
            )
            """
        ), row)


def test_disqualifiers_are_excluded_and_never_pushed():
    engine = _engine()
    oe.ensure_tables(engine)
    from sales_support_agent.services.outbound_memory import save_store_profiles
    import outbound_suppression as suppression

    save_store_profiles(engine, [
        {"domain": "soft.com", "store_profile": {"categories": "software", "merchant_name": "Soft"}},
        {"domain": "canada.com", "store_profile": {"country_code": "CA", "merchant_name": "North"}},
        {"domain": "multi.com", "store_profile": {"vendor_count": 30, "merchant_name": "Multi"}},
        {"domain": "innovafoodsco.com", "store_profile": {"merchant_name": "Innova Foods"}},
    ])
    _insert(engine, "ada@soft.com", "soft.com", "Soft", category="software")
    _insert(engine, "ada@canada.com", "canada.com", "North")
    _insert(engine, "ada@brand.co.uk", "brand.com", "Brand")
    _insert(engine, "ada@unverified.com", "unverified.com", "Unverified", status="UNVERIFIED")
    _insert(engine, "info@role.com", "role.com", "Role")
    _insert(engine, "ada@blocked.com", "blocked.com", "Blocked")
    _insert(engine, "ada@pacificproducts.co", "innovafoodsco.com", "Innova Foods")
    _insert(engine, "ada@wine.com", "wine.com", "Harbor", category="alcoholic beverages")
    _insert(engine, "ada@multi.com", "multi.com", "Multi")
    _insert(engine, "ada@ok.com", "ok.com", "Okay", opener="Okay sells a cotton tee.")
    suppression.add(engine, kind="email", value="ada@blocked.com", reason="opt_out", source="manual")
    sent = []
    oe.push_pending(engine, "camp", dry_run=False, push=_push_capture(sent))
    assert [row["email"] for row in sent] == ["ada@ok.com"]
    expected = {
        "ada@soft.com": "excluded:digital_product_seller",
        "ada@canada.com": "excluded:non_us_store",
        "ada@brand.co.uk": "excluded:non_us_email",
        "ada@unverified.com": "excluded:unverified_email",
        "info@role.com": "excluded:role_mailbox",
        "ada@blocked.com": "excluded:suppressed:",
        "ada@pacificproducts.co": "excluded:contact_domain_mismatch",
        "ada@wine.com": "excluded:restricted_category",
        "ada@multi.com": "excluded:multi_brand_retailer",
    }
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT email, skip_reason FROM outbound_enriched_contacts"
        )).fetchall()
    found = {email: str(reason or "") for email, reason in rows}
    for email, prefix in expected.items():
        assert found[email].startswith(prefix), (email, found[email])
        assert not found[email].startswith("held:opener")


def test_a_grounded_product_fact_keeps_the_specific_opener(monkeypatch):
    monkeypatch.setattr(
        "sales_support_agent.services.llm_client.llm_available",
        lambda **_kwargs: False,
    )
    engine = _engine()
    sent = []

    def find(domain, *, api_key, max_contacts=2, **_kwargs):
        return oc.FindResult(domain=domain, contacts=[oc.Contact(
            domain=domain, first_name="Ada", last_name="Lee",
            email="ada@cedar.com", title="Founder",
        )], credits_used=1)

    lead = {
        "domain": "cedar.com",
        "brand": "Cedar",
        "categories": ["apparel"],
        "recipe": "amz_on",
        "tier": "A",
        "site_product": "cedar soap",
        "store_profile": {"description": "cedar soap", "merchant_name": "Cedar"},
    }
    oe.run_enrichment(
        engine, [lead], api_key="k", find=find, dry_run=False, push=_push_capture(sent),
    )
    assert sent
    assert "cedar soap" in sent[0]["personalization"].lower()
    assert sent[0]["custom_variables"]["opener_type"] == "specific"
    assert not str(sent[0]["custom_variables"]["opener_variant"]).startswith("general")
    with engine.connect() as conn:
        source = conn.execute(text(
            "SELECT opener_source FROM outbound_enriched_contacts WHERE email = 'ada@cedar.com'"
        )).scalar()
    assert source in {"ai", "fallback", "shopper"}


def test_morning_status_counts_today_and_does_not_say_held_opener():
    from sales_support_agent.api import outbound_jobs as jobs
    from sales_support_agent.services import outbound_batches as batches

    engine = _engine()
    oe.ensure_tables(engine)
    now = datetime(2026, 11, 26, 12, tzinfo=DENVER)
    stamp = now.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO outbound_enriched_contacts (
                email, domain, first_name, title, opener, opener_source, brand,
                category, recipe, tier, email_status, skip_reason, created_at, pushed_at
            ) VALUES
            (
                'a@specific.com', 'specific.com', 'Ada', 'Founder',
                'Specific sells soap.', 'fallback', 'Specific', 'soap',
                'amz_on', 'A', 'VERIFIED', '', :stamp, :stamp
            ),
            (
                'a@general.com', 'general.com', 'Ada', 'Founder',
                'A general line.', 'general:v1', 'General', 'soap',
                'amz_on', 'A', 'VERIFIED', '', :stamp, NULL
            ),
            (
                'a@gone.com', 'gone.com', 'Ada', 'Founder', '', 'none', 'Gone',
                'soap', 'amz_off', 'A', 'VERIFIED', 'excluded:non_us_store', :stamp, NULL
            )
            """
        ), {"stamp": stamp})
    batch_id, _created = batches.create_batch(
        engine, business_date="2026-11-26", trigger="scheduled",
        recipe_count=1, correlation_id="openers",
    )
    batches.finalize_batch(
        engine, batch_id=batch_id,
        recipe_runs=[{"recipe_key": "amz_on", "status": "complete", "fresh": 1, "scanned": 1, "matched": 1}],
        leads=[{"domain": "specific.com", "brand": "Specific", "recipe": "amz_on"}],
        filename="anata-daily-leads-2026-11-26.csv",
    )
    jobs._morning_state["running"] = False
    jobs._morning_state["last_status"] = ""
    status = jobs.morning_pull_status(engine, now)
    assert "Openers today: 1 specific, 1 general (v1 1)" in status["reason"]
    assert "Pushed today: 1 specific, 0 general" in status["reason"]
    assert "Excluded today: non_us_store 1" in status["reason"]
    assert "amz_on" in status["reason"]
    assert "Held contacts waiting" not in status["reason"]
    assert "held" not in status["reason"].lower() or "opener" not in status["reason"].lower()
    posted = []

    class Slack:
        def post_message(self, *, text, channel):
            posted.append(text)
            return {"ok": True}

    empty = type("Batch", (), {"id": 3, "business_date": "2026-11-26", "unique_companies": 0})()
    jobs.post_sales_alert(empty, [], slack=Slack(), opener_line=status["opener_line"])
    assert posted
    assert "Openers today" in posted[0]
    assert "Held contacts waiting" not in posted[0]
