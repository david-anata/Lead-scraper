"""Sales-org titles, routing, and the draft sequence."""

from types import SimpleNamespace

import outbound_contacts as contacts
import outbound_instantly_push as push
import outbound_pipeline as pipeline
import outbound_sales_org as sales


def test_sales_titles_keep_the_sales_org_and_drop_everyone_else():
    kept = (
        "CRO",
        "Chief Revenue Officer",
        "VP of Sales",
        "Vice President of Sales",
        "Head of Sales",
        "Director of Sales",
        "Sales Director",
        "National Account Manager",
        "Key Account Director",
        "Channel Sales",
        "Ecommerce Sales Manager",
        "Amazon Account Manager",
        "Marketplace Account Manager",
        "Retail Account Manager",
        "Sales Manager",
        "Sales Representative",
        "Account Executive",
    )
    for title in kept:
        assert contacts.sales_title_rank(title) is not None, title
    assert contacts.sales_title_rank("CRO") < contacts.sales_title_rank("Sales Representative")
    dropped = (
        "Founder",
        "CEO",
        "Marketing Manager",
        "Sales Associate",
        "Sales Intern",
        "Sales Assistant",
        "Sales Coordinator",
        "Sales Engineer",
        "Head of Design",
        "VP of Engineering",
        "Account Manager",
        "",
    )
    for title in dropped:
        assert contacts.sales_title_rank(title) is None, title


def test_sales_search_keeps_a_vp_of_sales_and_drops_a_founder():
    results = [
        {"person": {"person_id": "founder", "current_job_title": "Founder"}},
        {"person": {"person_id": "sales", "current_job_title": "VP of Sales"}},
        {"person": {"person_id": "assoc", "current_job_title": "Sales Associate"}},
    ]
    ranked = contacts.rank_candidates(results, contacts.sales_title_rank)
    assert [item["person"]["person_id"] for item in ranked] == ["sales"]


def test_sales_org_stays_queued_until_the_flag_and_campaign_id_are_set(monkeypatch):
    monkeypatch.delenv(push.SALES_ORG_FLAG, raising=False)
    monkeypatch.delenv(push.SALES_ORG_CAMPAIGN_ENV, raising=False)
    campaign, status, route = push.resolve_recipe_campaign(
        "sales_org", ["route:amz_on"],
    )
    assert (campaign, status, route) == ("", "queued", "")

    monkeypatch.setenv(push.SALES_ORG_FLAG, "true")
    campaign, status, route = push.resolve_recipe_campaign(
        "sales_org", ["route:amz_on"],
    )
    assert (campaign, status, route) == ("", "queued", "")

    monkeypatch.setenv(push.SALES_ORG_CAMPAIGN_ENV, "sales-campaign")
    campaign, status, route = push.resolve_recipe_campaign(
        "sales_org", ["route:amz_on"],
    )
    assert (campaign, status, route) == ("sales-campaign", "routed", "sales_org")
    assert campaign not in push.RECIPE_CAMPAIGN_MAP.values()


def test_sales_org_push_does_not_copy_someone_already_in_the_workspace(monkeypatch):
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    monkeypatch.setenv("INSTANTLY_API_KEY", "test-key")
    seen = {}

    def post(url, **kwargs):
        seen["url"] = url
        seen["body"] = kwargs.get("json")
        return SimpleNamespace(status_code=200, json=lambda: {"created_leads": [], "skipped_count": 1})

    outcome = push.push_leads(
        [{"email": "ada@brand.com", "first_name": "Ada"}],
        "sales-campaign",
        dry_run=False,
        post=post,
    )
    assert outcome.errors == []
    assert seen["body"]["skip_if_in_workspace"] is True
    assert seen["body"]["campaign_id"] == "sales-campaign"


def test_sales_org_brand_does_not_need_a_category_and_still_must_be_us():
    store = {
        "country_code": "US",
        "estimated_sales_yearly": 5_000_000_00,
        "categories": "Industrial Equipment",
        "merchant_name": "Bolt",
        "tags": "",
    }
    assert pipeline.store_niche(store) == ""
    assert not pipeline.passes_company_fit(store)
    assert pipeline.passes_company_fit(store, require_niche=False)
    assert not pipeline.passes_company_fit({**store, "country_code": "CA"}, require_niche=False)
    assert not pipeline.passes_company_fit(
        {**store, "brands_page": "https://bolt.example/pages/brands"},
        require_niche=False,
    )


def test_sequence_matches_davids_approved_copy_and_has_no_urls():
    from outbound_cold_sequence import check_cold_steps

    assert sales.EMAIL_1_SUBJECT == "{{companyName}} growth idea"
    assert sales.EMAIL_1.startswith("Hi {{firstName}},")
    assert "{{personalization}}" in sales.EMAIL_1
    assert "{{brandAngle}}" in sales.EMAIL_1
    assert "You can try it free for 14 days, and plans start at $99 a month after that." in sales.EMAIL_1
    assert 'reply "intro" or "deck"' in sales.EMAIL_3
    assert [step["delay"] for step in sales.STEPS[:2]] == [3, 4]
    assert sales.STEPS[1]["subject"] == ""
    assert sales.STEPS[2]["subject"] == ""
    assert check_cold_steps(list(sales.STEPS)) == []
    for step in sales.STEPS:
        assert "http://" not in step["body"] and "https://" not in step["body"]
        assert "www." not in step["body"] and "www." not in step["subject"]
    assert sales.sequence_text().rstrip().endswith("Abby\nAccount Specialist, Anata")
    assert "—" not in sales.sequence_text() and "–" not in sales.sequence_text()
    draft = open("docs/outbound/18-sales-org-sequence.md", encoding="utf-8").read()
    assert sales.EMAIL_1 in draft
    assert sales.EMAIL_2 in draft
    assert sales.EMAIL_3 in draft
    assert "https://" not in draft and "http://" not in draft and "www." not in draft
    assert "—" not in draft and "–" not in draft
