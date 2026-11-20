"""Apollo fallback for the hourly enrich job. HTTP is mocked. No live APIs."""

from sqlalchemy import create_engine, text

import outbound_apollo as apollo
import outbound_contacts as oc
import outbound_enrich as oe
from sales_support_agent.api import outbound_enrich_job as job


def no_sleep(_seconds):
    return None


def engine():
    return create_engine("sqlite://")


def lead(domain):
    brand = domain.split(".")[0].title()
    return {
        "domain": domain,
        "brand": brand,
        "niche": "oral care",
        "recipe": "amz_on",
        "tier": "A",
        "categories": ["Health"],
        "site_product": "toothpaste tabs",
        "store_profile": {
            "merchant_name": brand,
            "description": "Toothpaste tabs",
            "most_recently_published_product": {"title": "Toothpaste Tabs"},
        },
    }


def _apollo_person(domain, *, status="verified", email=None):
    return {
        "id": "p1",
        "first_name": "Ann",
        "last_name": "Lee",
        "title": "Founder",
        "linkedin_url": "https://linkedin.com/in/ann",
        "email": email or f"ann@{domain}",
        "email_status": status,
    }


def router(*, prospeo, apollo_search, apollo_match, calls):
    """One fake HTTP client. ``prospeo`` is 'credits', 'none', or 'unverified'."""

    state = {"domain": ""}

    def post(url, *, key, body, timeout=25):
        calls.append((url, body))
        if url == oc.PROSPEO_SEARCH_URL:
            if prospeo == "credits":
                return 402, {"error": True, "error_code": "INSUFFICIENT_CREDITS", "message": "quota exceeded"}
            if prospeo == "unverified":
                return 200, {"error": False, "results": [
                    {"person": {"person_id": "pro-1", "current_job_title": "Founder"}},
                ]}
            return 400, {"error": True, "error_code": "NO_RESULTS"}
        if url == oc.PROSPEO_ENRICH_URL:
            return 200, {
                "error": False,
                "person": {
                    "person_id": "pro-1",
                    "first_name": "Ann",
                    "current_job_title": "Founder",
                    "email": {"email": "ann@brand.com", "status": "UNVERIFIED"},
                },
            }
        if url == apollo.APOLLO_SEARCH_URL:
            state["domain"] = (body.get("q_organization_domains_list") or [""])[0]
            return 200, apollo_search(state["domain"])
        if url == apollo.APOLLO_MATCH_URL:
            assert body["reveal_personal_emails"] is False
            assert body["reveal_phone_number"] is False
            return 200, apollo_match(state["domain"])
        raise AssertionError(url)

    return post


def _verified_search(domain):
    return {"people": [{"id": "p1", "title": "Founder", "has_email": True}]}


def _verified_match(domain):
    return {"person": _apollo_person(domain), "credits_consumed": 1}


def _run(db, post, *, apollo_key="a" * 20, budget=10, domains=("a.com", "b.com")):
    return oe.run_enrichment(
        db,
        [lead(domain) for domain in domains],
        api_key="k" * 20,
        post=post,
        sleep=no_sleep,
        apollo_api_key=apollo_key,
        apollo_credit_budget=budget,
        write=lambda item, _contact=None: (f"{item['brand']} sells toothpaste tabs.", "ai"),
        write_angle=lambda item, _contact=None: ("", "none"),
    )


def test_verified_prospeo_contact_does_not_call_apollo():
    calls = []

    def post(url, *, key, body, timeout=25):
        calls.append(url)
        if url == oc.PROSPEO_SEARCH_URL:
            return 200, {"error": False, "results": [
                {"person": {"person_id": "pro-1", "current_job_title": "Founder"}},
            ]}
        if url == oc.PROSPEO_ENRICH_URL:
            return 200, {
                "error": False,
                "person": {
                    "person_id": "pro-1",
                    "first_name": "Ann",
                    "last_name": "Lee",
                    "current_job_title": "Founder",
                    "linkedin_url": "",
                    "email": {"email": "ann@brand.com", "status": "VERIFIED"},
                },
            }
        raise AssertionError(url)

    db = engine()
    report = _run(db, post, domains=("brand.com",))
    assert report.contacts_prospeo == 1
    assert report.contacts_apollo == 0
    assert report.apollo_domains == 0
    assert not any(url.startswith("https://api.apollo.io") for url in calls)
    with db.connect() as conn:
        provider = conn.execute(text(
            "SELECT provider FROM outbound_enriched_contacts"
        )).scalar()
    assert provider == "prospeo"


def test_prospeo_out_of_credits_uses_apollo_for_the_rest_of_the_run():
    calls = []
    post = router(
        prospeo="credits",
        apollo_search=_verified_search,
        apollo_match=_verified_match,
        calls=calls,
    )
    db = engine()
    report = _run(db, post)
    prospeo_calls = [url for url, _body in calls if url == oc.PROSPEO_SEARCH_URL]
    apollo_calls = [url for url, _body in calls if url == apollo.APOLLO_SEARCH_URL]
    assert prospeo_calls == [oc.PROSPEO_SEARCH_URL]
    assert apollo_calls == [apollo.APOLLO_SEARCH_URL, apollo.APOLLO_SEARCH_URL]
    assert report.contacts_apollo == 2
    assert report.contacts_prospeo == 0
    assert report.apollo_credits_used == 2
    assert report.credits_used == 0
    assert report.prospeo_exhausted
    assert report.stopped_reason == ""
    assert report.skip_reasons["prospeo_out_of_credits"] == 2
    with db.connect() as conn:
        providers = conn.execute(text(
            "SELECT provider FROM outbound_enriched_contacts ORDER BY email"
        )).scalars().all()
    assert providers == ["apollo", "apollo"]
    from datetime import datetime
    from zoneinfo import ZoneInfo
    spent = oe.enriched_since(
        db, datetime(2020, 1, 1, tzinfo=ZoneInfo("America/Denver")),
    )
    assert spent["apollo_credits"] == 2
    assert spent["credits"] == 0


def test_prospeo_rate_limit_uses_apollo_without_skipping_prospeo_later():
    calls = []
    prospeo_calls = {"n": 0}

    def post(url, *, key, body, timeout=25):
        calls.append(url)
        if url == oc.PROSPEO_SEARCH_URL:
            prospeo_calls["n"] += 1
            # The first search is retried through every 429 wait, then gives up.
            if prospeo_calls["n"] <= len(oc.RETRY_WAITS) + 1:
                return 429, {}
            return 400, {"error": True, "error_code": "NO_RESULTS"}
        if url == apollo.APOLLO_SEARCH_URL:
            return 200, _verified_search("")
        if url == apollo.APOLLO_MATCH_URL:
            domain = "a.com" if calls.count(apollo.APOLLO_SEARCH_URL) == 1 else "b.com"
            return 200, _verified_match(domain)
        raise AssertionError(url)

    report = _run(engine(), post)
    assert prospeo_calls["n"] > 1
    assert report.contacts_apollo == 2
    assert not report.prospeo_exhausted
    assert report.skip_reasons["prospeo_rate_limit"] == 1
    assert report.skip_reasons["prospeo_no_person"] == 1


def test_prospeo_no_result_uses_apollo():
    calls = []
    post = router(
        prospeo="none",
        apollo_search=_verified_search,
        apollo_match=_verified_match,
        calls=calls,
    )
    report = _run(engine(), post, domains=("brand.com",))
    assert report.contacts_apollo == 1
    assert report.samples[0]["provider"] == "apollo"
    assert report.samples[0]["email"] == "ann@brand.com"
    assert any(url == apollo.APOLLO_MATCH_URL for url, _body in calls)
    assert report.skip_reasons["prospeo_no_person"] == 1


def test_prospeo_unverified_email_uses_apollo():
    calls = []
    post = router(
        prospeo="unverified",
        apollo_search=_verified_search,
        apollo_match=_verified_match,
        calls=calls,
    )
    report = _run(engine(), post, domains=("brand.com",))
    assert report.contacts_apollo == 1
    assert report.contacts_prospeo == 0
    assert report.skip_reasons["prospeo_no_verified_email"] == 1
    assert [url for url, _body in calls if url == oc.PROSPEO_ENRICH_URL]


def test_apollo_unverified_extrapolated_and_placeholder_emails_are_skipped():
    cases = (
        ("unverified", "ann@brand.com"),
        ("extrapolated", "guessed@brand.com"),
        ("verified", "email_not_unlocked@domain.com"),
    )
    for status, email in cases:
        calls = []

        def match(_domain, status=status, email=email):
            return {"person": _apollo_person("brand.com", status=status, email=email), "credits_consumed": 1}

        post = router(prospeo="none", apollo_search=_verified_search, apollo_match=match, calls=calls)
        report = _run(engine(), post, domains=("brand.com",))
        assert report.contacts_found == 0, status
        assert report.contacts_apollo == 0
        assert report.skipped_apollo_unverified == 1
        assert report.skip_reasons["apollo_unverified"] == 1
        assert report.as_dict()["contacts_found"] == 0


def test_missing_apollo_key_disables_fallback(caplog):
    calls = []
    post = router(
        prospeo="none",
        apollo_search=_verified_search,
        apollo_match=_verified_match,
        calls=calls,
    )
    with caplog.at_level("INFO"):
        report = _run(engine(), post, apollo_key="", domains=("brand.com",))
    assert report.contacts_found == 0
    assert report.apollo_domains == 0
    assert not any(url == apollo.APOLLO_SEARCH_URL for url, _body in calls)
    assert "APOLLO_API_KEY" in caplog.text
    assert "disabled" in caplog.text.lower()
    assert report.skip_reasons["apollo_disabled"] == 1


def test_missing_key_still_stops_the_run_when_prospeo_is_out_of_credits():
    calls = []
    post = router(
        prospeo="credits",
        apollo_search=_verified_search,
        apollo_match=_verified_match,
        calls=calls,
    )
    report = _run(engine(), post, apollo_key="")
    assert report.companies_tried == 1
    assert "credits" in report.stopped_reason.lower()
    assert not any(url.startswith("https://api.apollo.io") for url, _body in calls)


def test_apollo_cap_stops_further_matches():
    calls = []
    post = router(
        prospeo="none",
        apollo_search=lambda domain: {
            "people": [
                {"id": "p1", "title": "Founder", "has_email": True},
                {"id": "p2", "title": "CEO", "has_email": True},
            ],
        },
        apollo_match=lambda domain: {
            "person": _apollo_person(domain, email=f"ann@{domain}"),
            "credits_consumed": 1,
        },
        calls=calls,
    )
    db = engine()
    report = _run(db, post, budget=1)
    match_calls = [url for url, _body in calls if url == apollo.APOLLO_MATCH_URL]
    assert match_calls == [apollo.APOLLO_MATCH_URL]
    assert report.contacts_apollo == 1
    assert report.apollo_credits_used == 1
    assert report.skipped_apollo_cap == 1
    assert report.stopped_reason == ""
    with db.connect() as conn:
        statuses = dict(conn.execute(text(
            "SELECT domain, status FROM outbound_enriched"
        )).fetchall())
    assert statuses["a.com"] == "found"
    assert statuses["b.com"] == "none"


def test_apollo_client_obeys_credit_budget_before_the_second_match():
    calls = []

    def post(url, *, key, body, timeout=25):
        calls.append(url)
        if url == apollo.APOLLO_SEARCH_URL:
            return 200, {"people": [
                {"id": "p1", "title": "Founder", "has_email": True},
                {"id": "p2", "title": "CEO", "has_email": True},
            ]}
        return 200, {"person": _apollo_person("brand.com"), "credits_consumed": 1}

    found = apollo.find_contacts(
        "brand.com", api_key="a" * 12, credit_budget=1, post=post, sleep=no_sleep, max_contacts=2,
    )
    assert [url for url in calls if url == apollo.APOLLO_MATCH_URL] == [apollo.APOLLO_MATCH_URL]
    assert [item.email for item in found.contacts] == ["ann@brand.com"]
    assert found.credits_used == 1
    assert found.contacts[0].source == "apollo"


def test_search_request_filters_verified_buyers_and_match_does_not_reveal_phone(monkeypatch):
    captured = []

    class Response:
        def __init__(self, payload):
            self.status_code = 200
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, **kwargs):
        captured.append((url, kwargs))
        if url == apollo.APOLLO_SEARCH_URL:
            return Response({"people": [{"id": "p1", "title": "Founder", "has_email": True}]})
        return Response({"credits_consumed": 1, "person": _apollo_person("brand.com")})

    monkeypatch.setattr(apollo.requests, "post", fake_post)
    found = apollo.find_contacts("https://www.brand.com", api_key="a" * 12, sleep=no_sleep)
    search_params = captured[0][1]["params"]
    titles = [value for key, value in search_params if key == "person_titles[]"]
    domains = [value for key, value in search_params if key == "q_organization_domains_list[]"]
    statuses = [value for key, value in search_params if key == "contact_email_status[]"]
    assert "founder" in titles
    assert domains == ["brand.com"]
    assert statuses == ["verified"]
    assert captured[0][1]["headers"]["x-api-key"] == "a" * 12
    match = captured[1][1]
    assert match["params"]["reveal_personal_emails"] == "false"
    assert match["params"]["reveal_phone_number"] == "false"
    assert match["json"]["id"] == "p1"
    assert found.contacts[0].email == "ann@brand.com"
    assert "a" * 12 not in found.reason


def test_daily_cap_reads_spent_apollo_credits(monkeypatch):
    from datetime import datetime

    monkeypatch.setenv("APOLLO_DAILY_CREDITS", "1")
    monkeypatch.setenv("APOLLO_API_KEY", "a" * 20)
    monkeypatch.setenv("PROSPEO_API_KEY", "k" * 20)
    monkeypatch.setenv("OUTBOUND_AUTO_ENRICH", "true")
    monkeypatch.setenv("OUTBOUND_PUSH_ENABLED", "true")
    now = datetime(2026, 10, 22, 9, 0, tzinfo=job.ZoneInfo("America/Denver"))
    db = engine()
    oe.ensure_tables(db)
    with db.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_enriched "
            "(domain, status, contacts_found, credits_used, apollo_credits, reason, enriched_at) "
            "VALUES ('old.com', 'found', 1, 0, 1, '', :t)"
        ), {"t": job._start_of_day_utc(now)})

    def find(domain, *, api_key, max_contacts=2, **_kwargs):
        return oc.FindResult(domain=domain, reason="No decision-maker found at this domain.")

    called = []

    def apollo_find(domain, **_kwargs):
        called.append(domain)
        raise AssertionError("daily cap should block Apollo")

    out, _posts = _job_run(db, now, find, apollo_find)
    assert out["ran"]
    assert called == []
    assert out["report"]["skipped_apollo_cap"] >= 1
    assert out["apollo_credits_today"] == 1


def _job_run(db, now, find, apollo_find):
    from tests.test_outbound_enrich_job import write, ok_push
    posts = []
    out = job.run_enrich_job(
        now=now,
        engine=db,
        leads=[lead("brand.com")],
        find=find,
        apollo_find=apollo_find,
        write=write,
        push=ok_push,
        post_slack=posts.append,
        in_instantly=lambda _domain: False,
    )
    return out, posts


def test_summary_names_both_providers():
    text_out = job.summary_text({
        "credits_today": 4,
        "apollo_credits_today": 2,
        "pushed": 1,
        "push_enabled": True,
        "report": {
            "companies_tried": 3,
            "companies_with_contacts": 2,
            "contacts_found": 2,
            "credits_used": 3,
            "apollo_credits_used": 1,
            "contacts_prospeo": 1,
            "contacts_apollo": 1,
            "skip_reasons": {"prospeo_no_person": 1, "apollo_unverified": 2},
        },
    })
    assert "Prospeo credits: 3" in text_out
    assert "Apollo credits: 1" in text_out
    assert "prospeo 1, apollo 1" in text_out
    assert "prospeo_no_person 1" in text_out
    assert "apollo_unverified 2" in text_out


def test_apollo_credits_survive_when_the_column_is_not_migrated(monkeypatch):
    db = engine()
    oe.ensure_tables(db)
    real = oe._column_names

    def hidden(engine, table):
        names = set(real(engine, table))
        names.discard("apollo_credits")
        names.discard("provider")
        return names

    monkeypatch.setattr(oe, "_column_names", hidden)
    oe._record_domain(db, "brand.com", "found", 1, 2, "Apollo found a buyer.", apollo_credits=3)
    since = oe.enriched_since(db, job._start_of_day_utc(
        __import__("datetime").datetime(2020, 1, 1, tzinfo=job.ZoneInfo("America/Denver")),
    ))
    assert since["credits"] == 2
    assert since["apollo_credits"] == 3
    with db.connect() as conn:
        reason = conn.execute(text("SELECT reason FROM outbound_enriched WHERE domain = 'brand.com'")).scalar()
    assert "apollo_credits=3" in reason
