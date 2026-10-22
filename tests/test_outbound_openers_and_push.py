"""Openers (rules enforced in code) and the Instantly push (safety defaults)."""

import outbound_instantly_push as push
import outbound_openers as op

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
    assert op.validate("Huppy looks like it is growing fast. Is Amazon on your radar?")[0]


def test_ai_line_used_when_valid():
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: "Huppy keeps showing up in my feed. Is Amazon part of the plan?")
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
    lines = iter(["I noticed Huppy is building something special.", "Huppy toothpaste tabs look great. Selling them on Amazon yet?"])
    line, src = op.write_opener(LEAD, CONTACT, llm=lambda p: next(lines))
    assert src == "ai" and line.startswith("Huppy toothpaste")


def test_friendly_category():
    assert push.friendly_category("/Business & Industrial/Agriculture & Forestry, /Home & Garden/Gardening & Landscaping") == "gardening & landscaping"
    assert push.friendly_category("/Apparel") == "apparel"
    assert push.friendly_category(["/Apparel/Clothing Accessories"]) == "clothing accessories"
    assert push.friendly_category("") == ""


def test_live_campaign_not_protected_by_default(monkeypatch):
    monkeypatch.delenv("OUTBOUND_PROTECTED_CAMPAIGN_IDS", raising=False)
    assert push.LIVE_CAMPAIGN_ID not in push.protected_campaigns()
