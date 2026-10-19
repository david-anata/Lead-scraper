"""Contact finder (Clay replacement): ranking, credits, soft failure, secrecy."""

import outbound_contacts as oc


def _person(pid, title):
    return {"person": {"person_id": pid, "current_job_title": title}, "company": {}}


def _enriched(pid, first, email, status="VERIFIED", title="Founder"):
    return 200, {
        "error": False,
        "free_enrichment": False,
        "person": {
            "person_id": pid, "first_name": first, "last_name": "Test",
            "current_job_title": title, "linkedin_url": "https://linkedin.com/in/x",
            "email": {"status": status, "email": email, "revealed": True},
        },
    }


def make_post(search_results, enrich_by_id, free=False, calls=None):
    def post(url, *, key, body, timeout=25):
        if calls is not None:
            calls.append((url, body))
        if url == oc.PROSPEO_SEARCH_URL:
            return 200, {"error": False, "free": free, "results": search_results}
        pid = body["data"]["person_id"]
        return enrich_by_id.get(pid, (400, {"error": True, "error_code": "NO_MATCH"}))
    return post


def no_sleep(_):
    return None


def test_title_rank_orders_owners_before_marketing():
    assert oc.title_rank("Co-Founder & CEO") < oc.title_rank("Head of Ecommerce")
    assert oc.title_rank("Head of Ecommerce") < oc.title_rank("VP Marketing")
    assert oc.title_rank("Marketing Intern") is None
    assert oc.title_rank("Accountant") is None
    assert oc.title_rank("") is None


def test_normalize_domain():
    assert oc.normalize_domain("https://www.Brand.com/shop") == "brand.com"
    assert oc.normalize_domain("brand.com") == "brand.com"


def test_returns_two_best_verified_contacts_and_counts_credits():
    results = [
        _person("p3", "Marketing Manager"),
        _person("p1", "Founder"),
        _person("p2", "Head of Ecommerce"),
    ]
    enrich = {
        "p1": _enriched("p1", "Ann", "ann@brand.com"),
        "p2": _enriched("p2", "Bo", "bo@brand.com", title="Head of Ecommerce"),
        "p3": _enriched("p3", "Cy", "cy@brand.com"),
    }
    r = oc.find_contacts("brand.com", api_key="k" * 20, post=make_post(results, enrich),
                         sleep=no_sleep)
    assert [c.email for c in r.contacts] == ["ann@brand.com", "bo@brand.com"]
    assert r.credits_used == 3  # 1 search + 2 emails found
    assert r.ok


def test_skips_unverified_and_no_match_without_charging():
    results = [_person("p1", "Founder"), _person("p2", "CEO"), _person("p3", "President")]
    enrich = {
        "p1": _enriched("p1", "Ann", "ann@brand.com", status="UNVERIFIED"),
        # p2 missing -> NO_MATCH
        "p3": _enriched("p3", "Cy", "cy@brand.com"),
    }
    r = oc.find_contacts("brand.com", api_key="k" * 20, max_contacts=2,
                         post=make_post(results, enrich), sleep=no_sleep)
    assert [c.email for c in r.contacts] == ["cy@brand.com"]
    assert r.credits_used == 2  # search + the one found email


def test_free_search_is_not_charged():
    results = [_person("p1", "Founder")]
    enrich = {"p1": _enriched("p1", "Ann", "ann@brand.com")}
    r = oc.find_contacts("brand.com", api_key="k" * 20, post=make_post(results, enrich, free=True),
                         sleep=no_sleep)
    assert r.credits_used == 1


def test_no_key_is_a_soft_failure():
    r = oc.find_contacts("brand.com", api_key="")
    assert not r.ok and "PROSPEO_API_KEY" in r.reason


def test_no_decision_maker_gives_reason():
    r = oc.find_contacts("brand.com", api_key="k" * 20,
                         post=make_post([_person("p1", "Accountant")], {}), sleep=no_sleep)
    assert not r.ok and "No decision-maker" in r.reason


def test_search_error_does_not_raise_and_hides_key():
    secret = "sk_live_supersecretkey"

    def boom(url, *, key, body, timeout=25):
        raise RuntimeError(f"connection failed for key {key}")

    r = oc.find_contacts("brand.com", api_key=secret, post=boom, sleep=no_sleep)
    assert not r.ok
    assert secret not in r.reason


def test_stops_on_insufficient_credits():
    results = [_person("p1", "Founder"), _person("p2", "CEO")]

    def post(url, *, key, body, timeout=25):
        if url == oc.PROSPEO_SEARCH_URL:
            return 200, {"error": False, "results": results}
        return 400, {"error": True, "error_code": "INSUFFICIENT_CREDITS"}

    r = oc.find_contacts("brand.com", api_key="k" * 20, post=post, sleep=no_sleep)
    assert not r.ok and "credits" in r.reason.lower()


def test_request_shape_uses_domain_filter_and_verified_only():
    calls = []
    results = [_person("p1", "Founder")]
    enrich = {"p1": _enriched("p1", "Ann", "ann@brand.com")}
    oc.find_contacts("https://www.brand.com", api_key="k" * 20,
                     post=make_post(results, enrich, calls=calls), sleep=no_sleep)
    search_body = calls[0][1]
    assert search_body["filters"]["company"]["websites"]["include"] == ["brand.com"]
    enrich_body = calls[1][1]
    assert enrich_body["only_verified_email"] is True
    assert enrich_body["enrich_mobile"] is False
