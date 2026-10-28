"""Enrichment orchestrator: once per domain, once per email, budget, dry run."""

import pytest
from sqlalchemy import create_engine

import outbound_contacts as oc
import outbound_enrich as oe
import outbound_instantly_push as push


@pytest.fixture
def engine():
    e = create_engine("sqlite://")
    return e


def lead(domain, brand=None):
    return {"domain": domain, "brand": brand or domain.split(".")[0].title(), "niche": "oral care",
            "recipe": "new_growth_app", "tier": "A", "categories": ["Health"]}


def finder(emails_by_domain, credits=3):
    calls = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        calls.append(domain)
        cs = [oc.Contact(domain=domain, first_name="A", last_name="B", email=e, title="Founder")
              for e in emails_by_domain.get(domain, [])][:max_contacts]
        return oc.FindResult(domain=domain, contacts=cs, credits_used=credits if cs else 1,
                             reason="" if cs else "No decision-maker found at this domain.")
    find.calls = calls
    return find


def opener(l, c=None):
    return f"{l['brand']} caught my eye. Is Amazon on your radar?", "ai"


def test_no_key_does_nothing(engine):
    r = oe.run_enrichment(engine, [lead("a.com")], api_key="")
    assert r.stopped_reason and r.companies_tried == 0


def test_enriches_stores_and_reports(engine):
    f = finder({"a.com": ["x@a.com", "y@a.com"], "b.com": []})
    r = oe.run_enrichment(engine, [lead("a.com"), lead("b.com")], api_key="k", find=f, write=opener)
    assert r.companies_tried == 2 and r.companies_with_contacts == 1
    assert r.contacts_found == 2 and r.credits_used == 4
    assert r.find_rate == 0.5 and r.credits_per_contact == 2.0
    assert r.dry_run and r.pushed == 0
    assert len(r.samples) == 2 and "Amazon" in r.samples[0]["opener"]


def test_second_run_never_re_spends_on_same_domain(engine):
    f = finder({"a.com": ["x@a.com"]})
    oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener)
    r = oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener)
    assert f.calls == ["a.com"]
    assert r.skipped_already_enriched == 1 and r.companies_tried == 0


def test_same_email_at_two_domains_is_queued_once(engine):
    f = finder({"a.com": ["shared@x.com"], "b.com": ["shared@x.com"]})
    r = oe.run_enrichment(engine, [lead("a.com"), lead("b.com")], api_key="k", find=f, write=opener)
    assert r.contacts_found == 1


def test_credit_budget_stops_run(engine):
    f = finder({f"d{i}.com": [f"x@d{i}.com"] for i in range(10)}, credits=3)
    r = oe.run_enrichment(engine, [lead(f"d{i}.com") for i in range(10)], api_key="k",
                          find=f, write=opener, credit_budget=6, max_companies=10)
    assert r.credits_used == 6 and "budget" in r.stopped_reason.lower()


def test_max_companies_limits_run(engine):
    f = finder({f"d{i}.com": [f"x@d{i}.com"] for i in range(10)})
    r = oe.run_enrichment(engine, [lead(f"d{i}.com") for i in range(10)], api_key="k",
                          find=f, write=opener, max_companies=3)
    assert r.companies_tried == 3


def test_dry_run_never_pushes_even_with_campaign(engine):
    f = finder({"a.com": ["x@a.com"]})
    called = []
    r = oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener,
                          campaign_id="new-campaign", dry_run=True,
                          push=lambda *a, **k: called.append(1))
    assert not called and r.pushed == 0
    assert len(oe.unpushed(engine)) == 1


def test_live_push_marks_pushed_only_on_success(engine):
    f = finder({"a.com": ["x@a.com"]})
    ok = lambda payload, cid, dry_run=False, **k: push.PushOutcome(  # noqa: E731
        attempted=len(payload), created=len(payload), dry_run=False)
    r = oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener,
                          campaign_id="new-campaign", dry_run=False, push=ok)
    assert r.pushed == 1 and oe.unpushed(engine) == []

    f2 = finder({"b.com": ["y@b.com"]})
    bad = lambda payload, cid, dry_run=False, **k: push.PushOutcome(  # noqa: E731
        attempted=len(payload), dry_run=False, errors=["Instantly returned 500"])
    r2 = oe.run_enrichment(engine, [lead("b.com")], api_key="k", find=f2, write=opener,
                           campaign_id="new-campaign", dry_run=False, push=bad)
    assert r2.pushed == 0 and len(oe.unpushed(engine)) == 1


def test_pushed_lead_carries_brand_and_opener(engine):
    f = finder({"a.com": ["x@a.com"]})
    seen = {}

    def ok(payload, cid, dry_run=False, **k):
        seen["lead"] = payload[0]
        return push.PushOutcome(attempted=1, created=1, dry_run=False)

    oe.run_enrichment(engine, [lead("a.com", "Acme")], api_key="k", find=f, write=opener,
                      campaign_id="new-campaign", dry_run=False, push=ok)
    assert seen["lead"]["company_name"] == "Acme"
    assert "Acme caught my eye" in seen["lead"]["personalization"]
    assert seen["lead"]["email"] == "x@a.com"


def test_out_of_credits_stops_run(engine):
    def find(domain, *, api_key, max_contacts=2, **kw):
        return oc.FindResult(domain=domain, reason="Out of contact-finder credits.")
    r = oe.run_enrichment(engine, [lead("a.com"), lead("b.com")], api_key="k", find=find, write=opener)
    assert r.companies_tried == 1 and "credits" in r.stopped_reason.lower()


def test_init_database_creates_enrich_tables():
    from sqlalchemy import inspect
    from sales_support_agent.models.database import init_database
    from sqlalchemy.orm import sessionmaker
    e = create_engine("sqlite://")
    init_database(sessionmaker(bind=e))
    names = set(inspect(e).get_table_names())
    assert {"outbound_enriched", "outbound_enriched_contacts"} <= names


def test_vercel_postgres_skips_ddl(monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    calls = []

    class Eng:
        class dialect:
            name = "postgresql"

        def begin(self):
            calls.append(1)
            raise AssertionError("no DDL on Vercel")

    oe.ensure_tables(Eng())
    assert calls == []


def test_rewrite_openers_updates_unpushed_only(engine):
    f = finder({"a.com": ["x@a.com"]})
    oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener)
    out = oe.rewrite_openers(engine, [lead("a.com", "Acme")], write=lambda l, c=None: ("New line for Acme.", "ai"))
    assert out["rewritten"] == 1 and out["ai"] == 1
    assert oe.unpushed(engine)[0]["opener"] == "New line for Acme."


def test_push_pending_dry_run_marks_nothing(engine):
    f = finder({"a.com": ["x@a.com"]})
    oe.run_enrichment(engine, [lead("a.com", "Acme")], api_key="k", find=f, write=opener)
    out = oe.push_pending(engine, "camp", dry_run=True)
    assert out["pending"] == 1 and out["pushed"] == 0 and out["dry_run"]
    assert out["preview"][0]["company_name"] == "Acme" and "email" not in out["preview"][0]
    assert len(oe.unpushed(engine)) == 1


def test_email_status_is_stored_on_the_contact(engine):
    def find(domain, *, api_key, max_contacts=2, **kw):
        return oc.FindResult(domain=domain, credits_used=2, contacts=[
            oc.Contact(domain=domain, first_name="A", last_name="B", email=f"a@{domain}",
                       title="Founder", email_status="VERIFIED")])
    oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=find, write=opener)
    from sqlalchemy import text
    with engine.connect() as conn:
        status = conn.execute(text("SELECT email_status FROM outbound_enriched_contacts")).scalar()
    assert status == "VERIFIED"


def test_invalid_filters_failure_is_retried_and_a_normal_miss_is_not(engine):
    oe.ensure_tables(engine)
    oe._record_domain(engine, "petlibro.com", "none", 0, 0, "search returned INVALID_FILTERS: bad host")
    oe._record_domain(engine, "russh.com", "none", 0, 0, "No decision-maker found at this domain.")
    calls = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        calls.append(domain)
        return oc.FindResult(domain=domain, credits_used=1, contacts=[
            oc.Contact(domain=domain, first_name="A", last_name="B", email=f"a@{domain}", title="Founder")])

    r = oe.run_enrichment(engine, [lead("petlibro.com"), lead("russh.com")], api_key="k",
                          find=find, write=opener, already_in_instantly=lambda d: False)
    assert calls == ["petlibro.com"]
    assert r.companies_tried == 1 and r.skipped_already_enriched == 1


def test_old_subdomain_failure_does_not_block_the_brand(engine):
    oe.ensure_tables(engine)
    oe._record_domain(engine, "ca.petlibro.com", "none", 0, 0,
                      "search returned INVALID_FILTERS: websites include host was rejected")
    calls = []

    def find(domain, *, api_key, max_contacts=2, **kw):
        calls.append(domain)
        return oc.FindResult(domain=domain, credits_used=2, contacts=[
            oc.Contact(domain=domain, first_name="A", last_name="B", email="a@petlibro.com",
                       title="Founder", email_status="VERIFIED")])

    first = oe.run_enrichment(engine, [lead("ca.petlibro.com"), lead("shop.petlibro.com")],
                              api_key="k", find=find, write=opener, already_in_instantly=lambda d: False)
    assert calls == ["petlibro.com"] and first.companies_tried == 1
    second = oe.run_enrichment(engine, [lead("uk.petlibro.com")], api_key="k", find=find,
                               write=opener, already_in_instantly=lambda d: False)
    assert second.companies_tried == 0 and calls == ["petlibro.com"]
    from sqlalchemy import text
    with engine.connect() as conn:
        old = conn.execute(text("SELECT reason FROM outbound_enriched WHERE domain = 'ca.petlibro.com'")).scalar()
    assert "INVALID_FILTERS" in old


def test_suppressed_contacts_are_not_pushed(engine):
    import outbound_suppression as suppression
    f = finder({"a.com": ["x@a.com"], "b.com": ["y@b.com"], "c.com": ["z@c.com"]})
    oe.run_enrichment(engine, [lead("a.com"), lead("b.com"), lead("c.com")], api_key="k", find=f, write=opener)
    suppression.add(engine, kind="email", value="x@a.com", reason="opt_out", source="manual")
    suppression.add(engine, kind="domain", value="b.com", reason="hard_bounce", source="manual")
    sent = []

    def ok(payload, cid, dry_run=False, **k):
        sent.extend(lead["email"] for lead in payload)
        return push.PushOutcome(attempted=len(payload), created=len(payload), dry_run=False)

    out = oe.push_pending(engine, "camp", dry_run=False, push=ok)
    assert sent == ["z@c.com"] and out["pushed"] == 1 and out["suppressed"] == 2
    assert [row["email"] for row in oe.unpushed(engine)] == []


def test_push_pending_live_marks_pushed(engine):
    f = finder({"a.com": ["x@a.com"]})
    oe.run_enrichment(engine, [lead("a.com")], api_key="k", find=f, write=opener)
    ok = lambda payload, cid, dry_run=False, **k: push.PushOutcome(  # noqa: E731
        attempted=len(payload), created=len(payload), dry_run=False)
    out = oe.push_pending(engine, "camp", dry_run=False, push=ok)
    assert out["pushed"] == 1 and oe.unpushed(engine) == []
