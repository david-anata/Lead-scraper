"""Openers (rules enforced in code) and the Instantly push (safety defaults)."""

import outbound_instantly_push as push
import outbound_openers as op


def setup_function():
    op._OPENERS_BY_DOMAIN.clear()

LEAD = {"brand": "Huppy", "domain": "huppy.com", "niche": "oral care",
        "reason": "launched a new product line", "recipe": "new_growth_app", "tier": "A",
        "categories": ["Health & Beauty"]}
CONTACT = {"email": "Ann@Huppy.com", "first_name": "Ann", "last_name": "Lee", "title": "Founder"}


def test_clean_replaces_dashes_with_hyphens():
    assert "—" not in op.clean("Nice work — really")
    assert op.clean("Nice work — really") == "Nice work - really"


def test_validate_rejects_numbers_links_cliches_and_length():
    assert not op.validate("You grew 40% last year.")[0]
    assert not op.validate("See huppy.com/x https://x.co")[0]
    assert not op.validate("Hope this finds you well, Ann.")[0]
    assert not op.validate(" ".join(["word"] * 40))[0]
    assert not op.validate("")[0]
    assert op.validate("Huppy sells toothpaste tabs.")[0]
    assert not op.validate("Is Amazon on your radar?")[0]
    assert not op.validate("They have social momentum.")[0]
    assert not op.validate("Look at apparel_accessories next.")[0]
    assert not op.validate("Nice work — really")[0]


def test_ai_line_used_when_valid():
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: "Huppy sells oral care.")
    assert src == "ai" and "Huppy" in line


def test_bad_ai_line_falls_back():
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: "You grew 300% - wow!")
    assert src == "fallback"
    ok, _ = op.validate(line)
    assert ok


def test_ai_exception_falls_back_without_raising():
    def boom(p):
        raise RuntimeError("down")
    line, src = op.write_opener(LEAD, CONTACT, llm=boom)
    assert src == "fallback" and line


def test_no_key_no_llm_uses_fallback(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    _, src = op.write_opener(LEAD, CONTACT)
    assert src == "fallback"


def test_fallback_never_contains_digits_even_if_reason_does():
    line = op.fallback_line("Huppy", "oral care", "grew 40% in a month")
    assert "40" not in line and op.validate(line)[0]


def test_build_lead_maps_instantly_fields():
    lead = push.build_lead(CONTACT, LEAD, "Hello line")
    assert lead["email"] == "ann@huppy.com"
    assert lead["company_name"] == "Huppy" and lead["personalization"] == "Hello line"
    assert lead["custom_variables"]["category"] == "health & beauty"


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
    line = op.fallback_line("Basin", "bath", "They fit our ideal customer profile")
    assert "ideal customer" not in line.lower() and "Basin" in line and op.validate(line)[0]


def test_template_tics_rejected():
    assert not op.validate("I noticed Basin is building something special. Curious?")[0]


def test_retry_once_then_accept():
    lines = iter(["I noticed Huppy is building something special.", "Huppy sells toothpaste tabs."])
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: next(lines))
    assert src == "ai" and line == "Huppy sells toothpaste tabs."


def test_category_slugs_become_plain_words_and_product_wins():
    slug_line = op.fallback_line("Huppy", "apparel_accessories")
    assert "apparel_accessories" not in slug_line
    assert "apparel and accessories" in slug_line
    assert op.plain_category("food_beverage") == "food and beverage"
    assert op.plain_category("beauty_wellness") == "beauty and wellness"
    assert "?" not in slug_line
    product_line = op.fallback_line("Huppy", "apparel_accessories", product="toothpaste tabs")
    assert "toothpaste tabs" in product_line
    assert "apparel_accessories" not in product_line
    signal = op.fallback_line("Huppy", "", recipe="plan_upgrade")
    assert "upgraded their store plan" in signal
    assert "?" not in signal


def test_one_opener_per_company_is_cached_by_domain():
    calls = []

    def llm(prompt):
        calls.append(prompt)
        return "Huppy sells toothpaste tabs."

    lead = {**LEAD, "amz_product": "toothpaste tabs"}
    first = op.write_opener(lead, CONTACT, llm=llm)
    second = op.write_opener(lead, {"title": "CMO", "email": "other@huppy.com"}, llm=llm)
    assert first == second
    assert first[1] == "ai"
    assert len(calls) == 1
    assert "toothpaste tabs" in calls[0]


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
