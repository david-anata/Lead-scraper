"""Abby: rules in code, one reply per email, opt-outs never answered."""

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

import abby
import abby_runner as runner

LINK = abby.BOOKING_LINK
NOW = datetime(2026, 10, 27, 16, 0, tzinfo=timezone.utc)


# ---------- decide(): the rules ----------

def d(cat, reply="", conf=0.9, **kw):
    return abby.decide({"category": cat, "confidence": conf, "reply": reply, **kw},
                       lead_email="ann@huppy.com", thread_categories=kw.pop("prior", []) if "prior" in kw else [])


def test_interested_sends_the_deck_and_ccs_the_founder():
    r = d("interested", f"Thanks Ann. Grab a time with David here: {LINK}\n\n{abby.SIGNATURE}")
    assert r.action == "reply" and r.send_deck and r.cc == [abby.founder_email()]
    assert r.interest is None and LINK not in r.reply and "{deck_url}" in r.reply


def test_interested_without_a_model_link_still_sends_the_deck():
    r = d("interested", "Thanks Ann, David will reach out.")
    assert r.action == "reply" and r.send_deck and "deck" in r.note


def test_price_sends_the_deck_and_does_not_keep_a_made_up_stat():
    ok = d("price", f"Plans start at $99 a month after the free 14-day trial. {LINK}")
    bad = d("price", f"Brands like yours grow 40% with us. {LINK}")
    assert ok.action == "reply" and ok.send_deck
    assert bad.action == "reply" and bad.send_deck and "40" not in bad.reply


def test_opt_out_blocks_domain_and_never_replies():
    r = d("opt_out", "Sorry to bother you")
    assert r.action == "block" and r.block_domain == "huppy.com" and r.reply == "" and r.interest == -1


def test_hostile_blocks_the_lead_and_low_confidence_is_held():
    hostile = d("hostile", "x")
    assert hostile.action == "block" and hostile.reply == "" and hostile.interest == -1
    assert hostile.block_domain == ""
    assert d("interested", f"hi {LINK}", conf=0.4).action == "hold"
    assert d("nonsense-category", "x").action == "hold"


def test_has_solution_answered_once_only():
    first = abby.decide({"category": "has_solution", "confidence": 0.9,
                         "reply": "What does it cost you to run that today? Abby"},
                        lead_email="a@b.com", thread_categories=[])
    second = abby.decide({"category": "has_solution", "confidence": 0.9, "reply": "again"},
                         lead_email="a@b.com", thread_categories=["has_solution"])
    assert first.action == "reply" and first.schedule_kind == "has_solution"
    assert second.action == "ignore" and second.schedule_kind == ""


def test_not_now_schedules_30_and_75_day_followups_no_cc():
    r = d("not_now", "Thanks for letting me know. I'll check back in about a month. Abby")
    assert r.action == "reply" and r.follow_up_days is None and r.cc == []
    assert r.schedule_kind == "not_now"


def test_referral_adds_colleague_and_ccs_david():
    r = d("referral", f"Hi Sam, Ann pointed me your way. {LINK} Abby", referral_email="sam@huppy.com")
    assert r.to_extra == ["sam@huppy.com"] and r.cc == []


def test_auto_reply_without_evidence_is_held():
    held = d("auto_reply")
    assert held.action == "hold" and held.interest is None and held.slack
    assert held.reason != "automatic reply"


# Real mailbox rules from Oct 26-27, 2026. Each one was labeled a referral
# and must stay an auto-reply, not a send.

RIOMAR = """Greetings,


The person associated with this email address is no longer with Riomar Shoes. Please reach out customerservice@riomarshoes.com.

-----

Mac Smith //
Riomar Shoes // riomarshoes.com
mac@riomarshoes.com
"""

GARBARINI = """Thank you for your message. As of the end of January, I have concluded
my role with Garbarini and will no longer be monitoring this inbox.

For assistance moving forward, please contact the appropriate team member below:

Elisa – Operations Manager
elisa@garbarinishop.com

Sierra Romick – E-commerce & Social Media
sierra@garbarinishop.com

I'm grateful for the opportunity to have worked with Garbarini.
If you have a question about an online order, call 303-333-8686.

Don't worry—eligible returns will be processed within 3 business days.

Just a bean living the dream, Carley
"""

BELLOCQ = """Hello,


Thank you for your email. I am no longer with Bellocq. Please email
sales@bellocq.com with any inquiries.

Best,


Danielle
"""

OOO = """Hello,

I am out of the office until November 4 with no access to email.
Please direct inquiries to info@riomarshoes.com.

Mac
"""

SARAH = ("Hi Abby, I do not cover Amazon. My colleague Sarah Chen does. "
         "Her email is sarah.chen@huppy.com.\n\nAnn")

MIKE = ("Thanks for writing. Mike Alvarez runs sales for us, and he is the right person. "
        "You can reach him at mike.alvarez@northwind.com.\n\nJames")


def _referral_json(address: str, name: str, reply: str) -> str:
    return json.dumps({
        "category": "referral", "confidence": 0.95,
        "referral_email": address, "referral_name": name, "reply": reply,
    })


def _bad_referral(_prompt: str) -> str:
    """What the model did on Garbarini: a named person in a mailbox rule, treated as a colleague."""
    return _referral_json(
        "sierra@garbarinishop.com", "Sierra Romick",
        f"Hi Sierra, Carley pointed me your way. Grab a time with David here: {LINK}\n\nAbby",
    )


def _assert_ignored(r, *bits: str):
    assert r.action == "ignore" and r.category == "auto_reply"
    assert r.reply == "" and r.cc == [] and r.to_extra == [] and r.interest is None
    assert r.slack == ""
    assert "Nothing sent." in r.note
    assert "—" not in r.note and "–" not in r.note
    for bit in bits:
        assert bit in r.note


def test_prompt_treats_departed_and_ooo_as_auto_reply():
    text = abby.SYSTEM
    assert "—" not in text and "–" not in text
    for phrase in ("no longer with", "has left the company", "out of the office",
                   "please direct inquiries", "customerservice@", "info@", "sales@", "support@"):
        assert phrase in text
    assert "a human wrote this reply" in text
    assert "If a human wrote it, or you are unsure, choose other" in text
    assert "unsure a human wrote the message, choose auto_reply" not in text


@pytest.mark.parametrize("lead,body,bits", [
    ("mac@riomarshoes.com", RIOMAR, ("Departed contact mac@riomarshoes.com", "customerservice@riomarshoes.com")),
    ("carley@garbarinishop.com", GARBARINI, ("Departed contact carley@garbarinishop.com", "sierra@garbarinishop.com", "elisa@garbarinishop.com")),
    ("danielle@bellocq.com", BELLOCQ, ("Departed contact danielle@bellocq.com", "sales@bellocq.com")),
    ("mac@riomarshoes.com", OOO, ("Out of office from mac@riomarshoes.com", "info@riomarshoes.com")),
])
def test_real_notices_are_ignored_before_the_model(lead, body, bits):
    calls = []

    def llm(prompt):
        calls.append(prompt)
        return _bad_referral(prompt)

    r = abby.think([{"ue_type": 2, "subject": "Re: Amazon", "body": {"text": body}}],
                   lead_email=lead, company="Brand", first_name="There",
                   thread_categories=[], llm=llm)
    assert calls == []
    _assert_ignored(r, *bits)


def test_decide_overrides_a_referral_label_on_the_riomar_notice():
    r = abby.decide(
        {"category": "referral", "confidence": 0.99,
         "referral_email": "customerservice@riomarshoes.com", "referral_name": "Customer Service",
         "reply": f"Hi, Mac pointed me your way. {LINK}\n\nAbby"},
        lead_email="mac@riomarshoes.com", thread_categories=[], message=RIOMAR)
    _assert_ignored(r, "customerservice@riomarshoes.com")


def test_decide_overrides_interested_on_a_departed_notice():
    r = abby.decide(
        {"category": "interested", "confidence": 0.99,
         "reply": f"Thanks Mac. Grab a time with David here: {LINK}\n\nLooping David, our owner, in.\n\nAbby"},
        lead_email="mac@riomarshoes.com", thread_categories=[], message=RIOMAR)
    _assert_ignored(r, "Departed contact")


def test_shared_inbox_referral_is_held_for_review():
    r = d("referral", f"Hi. {LINK} Abby", referral_email="sales@bellocq.com")
    assert r.action == "hold" and r.category == "send_info"
    assert r.reply == "" and r.to_extra == [] and r.interest is None
    assert "sales@bellocq.com" in r.slack and "Nothing was sent" in r.slack


def test_please_email_support_is_held_not_sent():
    body = "Hello, please email support@acme.com and someone on the team will help."
    calls = []
    r = abby.think([{"ue_type": 2, "body": {"text": body}, "is_auto_reply": 0}],
                   lead_email="person@acme.com", company="Acme", first_name="Pat",
                   thread_categories=[], llm=lambda p: calls.append(p) or _bad_referral(p))
    assert calls == []
    assert r.action == "hold" and r.category == "send_info"
    assert r.to_extra == [] and r.interest is None and "support@acme.com" in r.slack


def test_auto_reply_header_blocks_a_named_forward():
    body = "I am away this week. Please contact Jordan Hale at jordan.hale@bellocq.com."
    msg = {"ue_type": 2, "subject": "Re: Amazon", "headers": {"Auto-Submitted": "auto-replied"},
           "body": {"text": body}}
    calls = []
    r = abby.think([msg], lead_email="danielle@bellocq.com", company="Bellocq", first_name="Danielle",
                   thread_categories=[], llm=lambda p: calls.append(p) or _bad_referral(p))
    assert calls == []
    _assert_ignored(r, "jordan.hale@bellocq.com")
    via_header = abby.decide(
        {"category": "referral", "confidence": 0.93, "referral_email": "jordan.hale@bellocq.com",
         "reply": f"Hi Jordan, Danielle pointed me your way. {LINK}\n\nAbby"},
        lead_email="danielle@bellocq.com", thread_categories=[], message=body,
        headers={"X-Autoreply": "yes"})
    _assert_ignored(via_header)


def _no_model(prompt):
    raise AssertionError(prompt)


@pytest.mark.parametrize("subject,body", [
    ("Automatic reply: away next week", "Thank you for your email. I will reply when I am back."),
    ("Out of Office", "Thanks for writing."),
    ("Autoreply: parental leave", "Thank you for your message."),
    ("OOO until Monday", "Back Monday."),
])
def test_headerless_subject_markers_are_auto_replies(subject, body):
    message = {
        "ue_type": 2, "subject": subject,
        "headers": {"Auto-Submitted": "no"}, "body": {"text": body},
    }
    result = abby.think(
        [message], lead_email="pat@brand.com", company="Brand", first_name="Pat",
        thread_categories=[], llm=_no_model,
    )
    _assert_ignored(result)
    assert result.reply == "" and result.interest is None


@pytest.mark.parametrize("body,kind", [
    ("Hi, I'm on annual leave, returning to work on 2 November. Kind regards, Tom", "out_of_office"),
    ("Thanks for your email. I\u2019m currently on leave and will have limited access to emails while I\u2019m away.", "out_of_office"),
    ("I will be out next week with limited access to email. I am returning on 3 March.", "out_of_office"),
    ("Thank you for your email. I am on parental leave until March and will read mail when I return.", "out_of_office"),
    ("Bonjour, je dois m'absenter sans acc\u00e8s \u00e0 mes courriels. Les messages seront lus \u00e0 mon retour.", "out_of_office"),
    ("Please note that I am no longer working at Northwind as of May. Contact the team through the main site.", "departed"),
    ("Jamie is no longer here. This inbox is not monitored.", "departed"),
    ("This mailbox is not monitored. Morgan has left the company.", "departed"),
])
def test_headerless_ooo_and_departed_bodies_skip_the_model(body, kind):
    message = {
        "ue_type": 2, "subject": "Re: Amazon plan",
        "headers": {"Auto-Submitted": "no"}, "body": {"text": body},
    }
    result = abby.think(
        [message], lead_email="pat@brand.com", company="Brand", first_name="Pat",
        thread_categories=[], llm=_no_model,
    )
    _assert_ignored(result)
    assert kind.replace("_", " ") in result.reason or (kind == "out_of_office" and "out of office" in result.reason)
    assert (result.schedule_kind == "out_of_office") is (kind == "out_of_office")


def test_a_thank_you_without_absence_evidence_is_not_an_auto_reply():
    message = {
        "ue_type": 2, "subject": "Re: Amazon",
        "headers": {"Auto-Submitted": "no"},
        "body": {"text": "Thank you for your email. I will reply when I am back."},
    }
    assert abby.automated_notice(message, lead_email="pat@brand.com") is None


@pytest.mark.parametrize("body", [
    "I'll be out next week and back in the office Monday, but yes, please send the plan. What does the trial cost?",
    "I was on vacation and I'm back. I'm interested. Can you send the pricing?",
    "I'm on annual leave, returning to work Monday, but I'd like the plan.",
    "I will be out Thursday. Can you send the pricing before then?",
    "Thanks for the note. I just got back from a trip and I'm happy to review. Send me the plan.",
])
def test_time_off_with_a_real_answer_is_not_an_auto_reply(body):
    message = {
        "ue_type": 2, "is_auto_reply": 0, "subject": "Re: Amazon plan",
        "headers": {}, "body": {"text": body},
    }
    assert abby.automated_notice(message, lead_email="ann@huppy.com") is None
    result = abby.decide(
        {"category": "auto_reply", "confidence": 0.95, "reply": None},
        lead_email="ann@huppy.com", thread_categories=[], message=message,
    )
    assert result.action == "hold" and result.interest is None
    assert result.reason == "model said auto-reply without auto-reply evidence"


def test_instantly_auto_reply_flag_and_subject_are_ignored():
    flagged = {"ue_type": 2, "is_auto_reply": 1,
               "body": {"text": "Looping in my colleague Dana at dana@garbarinishop.com."}}
    r = abby.think([flagged], lead_email="carley@garbarinishop.com", company="Garbarini",
                   first_name="Carley", thread_categories=[], llm=_bad_referral)
    _assert_ignored(r, "dana@garbarinishop.com")
    subject = abby.automated_notice(
        "I will return on Monday. Jordan can be reached at jordan@bellocq.com.",
        lead_email="danielle@bellocq.com", subject="Automatic reply: Out of Office")
    assert subject is not None and subject.kind == "out_of_office"
    assert abby.header_says_auto({"Precedence": "auto_reply"})
    assert not abby.header_says_auto({"Auto-Submitted": "no"})
    assert not abby.header_says_auto({"Precedence": "bulk"})


def test_quoted_departed_notice_does_not_hide_a_new_human_reply():
    body = ("Yes, send the breakdown.\n\n-----\n"
            "The person is no longer with Riomar. Please reach out customerservice@riomarshoes.com.")
    raw = json.dumps({
        "category": "interested", "confidence": 0.92,
        "reply": f"Thanks. Grab a time with David here: {LINK}\n\nLooping David, our owner, in.\n\nAbby",
    })
    r = abby.think([{"ue_type": 2, "body": {"text": body}, "is_auto_reply": 0}],
                   lead_email="newperson@riomarshoes.com", company="Riomar", first_name="Sam",
                   thread_categories=[], llm=lambda _p: raw)
    assert r.action == "reply" and r.send_deck and r.interest is None and r.note == "deck"


def test_signature_info_address_does_not_block_a_human():
    body = "Yes, send the breakdown. Happy to look.\n\nAnn Cole\nEmail: info@huppy.com\n"
    raw = json.dumps({
        "category": "interested", "confidence": 0.93,
        "reply": f"Thanks Ann. Grab a time with David here: {LINK}\n\nLooping David, our owner, in.\n\nAbby",
    })
    r = abby.think([{"ue_type": 2, "body": {"text": body}, "headers": {"Auto-Submitted": "no"}}],
                   lead_email="ann@huppy.com", company="Huppy", first_name="Ann",
                   thread_categories=[], llm=lambda _p: raw)
    assert r.action == "reply" and r.send_deck and r.interest is None and r.cc == [abby.founder_email()]


@pytest.mark.parametrize("lead,body,address,name,first,company", [
    ("ann@huppy.com", SARAH, "sarah.chen@huppy.com", "Sarah Chen", "Ann", "Huppy"),
    ("james@northwind.com", MIKE, "mike.alvarez@northwind.com", "Mike Alvarez", "James", "Northwind"),
])
def test_human_referral_still_writes_the_colleague(lead, body, address, name, first, company):
    reply = (f"Hi {name.split()[0]}, {first} pointed me your way. The free 14-day trial is where we build "
             f"your Amazon strategy and run the ad automation. Grab a time with David here: {LINK}\n\nAbby")
    r = abby.think(
        [{"ue_type": 2, "is_auto_reply": 0, "body": {"text": body}}],
        lead_email=lead, company=company, first_name=first, thread_categories=[],
        llm=lambda _p: _referral_json(address, name, reply))
    assert r.action == "reply" and r.category == "referral"
    assert r.to_extra == [address] and r.cc == [] and r.interest is None
    assert r.note == "" and name.split()[0] in r.reply
    assert "free 14-day trial" in r.reply
    assert r.reply.rstrip().endswith("Account Specialist, Anata")
    assert "\u2014" not in r.reply


def test_human_referral_survives_decide_when_the_message_is_passed():
    reply = (f"Hi Sarah, Ann pointed me your way. The free 14-day trial is where we build "
             f"your Amazon strategy and run the ad automation. Grab a time with David here: {LINK}\n\nAbby")
    r = abby.decide(
        {"category": "referral", "confidence": 0.95, "referral_email": "sarah.chen@huppy.com",
         "referral_name": "Sarah Chen", "reply": reply},
        lead_email="ann@huppy.com", thread_categories=[], message=SARAH)
    assert r.action == "reply" and r.to_extra == ["sarah.chen@huppy.com"] and r.interest is None


def test_em_dash_cleaned_and_unknown_links_held():
    r = d("question", "Great — the trial is 14 days.\n\nAbby")
    assert "—" not in r.reply and r.action == "reply"
    held = d("question", "See https://evil.example.com for the 14 day trial.")
    assert "evil.example.com" not in held.reply
    assert held.action == "reply" and "Can I send it to you?" in held.reply


def test_model_failure_becomes_hold():
    def boom(_):
        raise RuntimeError("down")
    r = abby.think([{"ue_type": 2, "body": {"text": "What happens on day 10?"}}], lead_email="a@b.com",
                   company="B", first_name="A", thread_categories=[], llm=boom)
    assert r.action == "hold"


def test_parse_model_json_tolerates_noise():
    assert abby.parse_model_json('sure: {"category": "price", "confidence": 0.8}')["category"] == "price"
    assert abby.parse_model_json("no json")["category"] == "other"


# ---------- runner ----------

class FakeClient:
    def __init__(self, received, threads=None, interested=None, leads=None):
        self.received, self.threads = received, threads or {}
        self.interested, self.leads = interested or [], leads or {}
        self.replies, self.blocks, self.interest = [], [], []

    def interested_latest(self, campaign_id):
        return list(self.interested)

    def lead(self, email):
        return self.leads.get(email, {})

    def received_since(self, since, campaign_id):
        return list(self.received)

    def thread(self, tid):
        return self.threads.get(tid, [])

    def reply(self, **kw):
        self.replies.append(kw)
        return 200, {"id": f"r{len(self.replies)}"}

    def set_interest(self, lead, value, campaign_id):
        self.interest.append((lead, value))
        self._marked = getattr(self, "_marked", [])
        self._marked.append({
            "email": lead, "campaign": campaign_id, "lt_interest_status": value,
        })
        return 200

    def leads_for_email(self, email):
        return [row for row in getattr(self, "_marked", []) if row.get("email") == email]

    def block(self, value):
        self.blocks.append(value)
        return 200


@pytest.fixture
def engine(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    return create_engine("sqlite://")


def email(i, lead="ann@huppy.com", thread="t1", ts=None):
    return {"id": f"e{i}", "thread_id": thread, "lead": lead, "eaccount": "abby@x.com",
            "subject": "Idea for Huppy", "ue_type": 2, "body": {"text": "yes please"},
            "timestamp_created": ts or f"2026-10-2{i}T10:00:00Z"}


def fixed(decision):
    return lambda *a, **k: decision


def test_handled_row_stores_the_email_campaign(engine):
    message = email(1)
    message["campaign_id"] = "from-the-email"
    runner.run(engine, client=FakeClient([message]), campaign_id="camp", now=NOW,
               think=fixed(abby.Decision("other", "ignore")))
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT campaign_id FROM abby_handled")).scalar()
    assert stored == "from-the-email"


def test_handled_row_uses_the_run_campaign_when_the_email_has_none(engine):
    runner.run(engine, client=FakeClient([email(1)]), campaign_id="camp", now=NOW,
               think=fixed(abby.Decision("other", "ignore")))
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT campaign_id FROM abby_handled")).scalar()
    assert stored == "camp"


def test_live_reply_sent_once_and_recorded(engine):
    c = FakeClient([email(1)])
    dec = abby.Decision("interested", "reply", reply=f"Hi {LINK}", cc=[abby.DAVID_EMAIL], interest=1)
    posts = []
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    again = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    assert out["replied"] == 1 and again["seen"] == 0 and len(c.replies) == 1
    assert c.replies[0]["cc"] == [abby.DAVID_EMAIL] and c.replies[0]["subject"] == "Re: Idea for Huppy"
    assert c.interest == []


def test_shadow_mode_sends_nothing(engine, monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "shadow")
    c = FakeClient([email(1)])
    posts = []
    dec = abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1)
    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    assert c.replies == [] and c.interest == [] and "not sent" in posts[0]


def test_off_mode_does_nothing(engine, monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "off")
    out = runner.run(engine, client=FakeClient([email(1)]), campaign_id="camp", now=NOW)
    assert out["reason"] == "ABBY_MODE is off"


def test_reply_already_answered_is_skipped(engine):
    th = [email(1), {"id": "m1", "ue_type": 3, "timestamp_created": "2026-10-21T12:00:00Z"}]
    c = FakeClient([email(1)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert out["skipped"] == 1 and c.replies == []


def test_takeover_answers_unanswered_reply_in_gabes_thread(engine):
    th = [email(1), {"id": "m1", "ue_type": 3, "timestamp_created": "2026-10-21T12:00:00Z"}, email(3)]
    c = FakeClient([email(3)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert out["replied"] == 1 and c.replies[0]["reply_to"] == "e3"


def test_older_reply_superseded_by_newer_prospect_message(engine):
    th = [email(1), email(3)]
    c = FakeClient([email(1), email(3)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert out["replied"] == 1 and out["skipped"] == 1


def test_name_falls_back_to_instantly_lead(engine):
    seen = {}
    def think(thread, *, lead_email, company, first_name, thread_categories):
        seen.update(company=company, first_name=first_name)
        return abby.Decision("auto_reply", "ignore")
    c = FakeClient([email(1)], leads={"ann@huppy.com": {"first_name": "Ann", "company_name": "Huppy"}})
    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    assert seen == {"company": "Huppy", "first_name": "Ann"}


# ---------- nudges ----------

def ours(i, thread="t9", days_ago=4):
    return {"id": f"o{i}", "thread_id": thread, "lead": "ann@huppy.com", "ue_type": 3,
            "eaccount": "abby@x.com",
            "timestamp_created": (NOW - timedelta(days=days_ago)).isoformat().replace("+00:00", "Z")}


def nudge_client(days_ago):
    latest = ours(1, days_ago=days_ago)
    prospect = email(1, thread="t9", ts="2026-10-10T10:00:00Z")
    return FakeClient([], {"t9": [prospect, latest]}, interested=[latest],
                      leads={"ann@huppy.com": {"first_name": "Ann", "company_name": "Huppy"}})


def test_nudge_1_after_3_days_replies_to_prospects_email(engine):
    c = nudge_client(4)
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW)
    assert out["nudges"] == 1 and c.replies[0]["reply_to"] == "e1"
    body = c.replies[0]["body"]
    assert "Hi Ann" in body and c.replies[0]["cc"] == []
    assert abby.BOOKING_LINK not in body
    assert "Can I send it to you?" in body or "Mind if I send it?" in body


def test_no_nudge_before_3_days_or_when_prospect_wrote_last(engine):
    assert runner.run(engine, client=nudge_client(2), campaign_id="camp", now=NOW)["nudges"] == 0
    c = nudge_client(10)
    c.interested = [email(5, thread="t9")]
    assert runner.run(engine, client=c, campaign_id="camp", now=NOW)["nudges"] == 0


def test_three_nudges_max_and_the_last_does_not_cc_david(engine):
    posts, sent = [], []
    prospect = email(1, thread="t9", ts="2026-10-10T10:00:00Z")
    abby_msgs = []
    for _ in range(5):
        latest = abby_msgs[-1] if abby_msgs else dict(prospect, ue_type=2)
        last = {"id": f"x{len(abby_msgs)}", "thread_id": "t9", "lead": "ann@huppy.com", "ue_type": 3,
                "timestamp_created": (NOW - timedelta(days=20)).isoformat().replace("+00:00", "Z")}
        thread = [prospect] + abby_msgs
        c = FakeClient([], {"t9": thread}, interested=[abby_msgs[-1] if abby_msgs else dict(last, id="seed")])
        runner.run(engine, client=c, campaign_id="camp", now=NOW, post_slack=posts.append)
        for r in c.replies:
            sent.append(r)
            abby_msgs.append({"id": f"r{len(c.replies)}", "thread_id": "t9", "ue_type": 3,
                              "timestamp_created": (NOW - timedelta(days=20 - len(sent))).isoformat().replace("+00:00", "Z")})
    assert len(sent) <= 3 and sent and sent[-1]["cc"] == []
    assert "Looping David" not in sent[-1]["body"]
    assert sent[-1]["body"].endswith(abby.SIGNATURE)
    assert any("last nudge" in p for p in posts)


def test_nudge_text_passes_reply_checks():
    for n in (1, 2, 3):
        body, _ = runner.nudge_text(n, "Ann", "Huppy")
        assert abby.validate_reply("interested", body)[0], n


def test_shadow_nudges_send_nothing(engine, monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "shadow")
    c = nudge_client(4)
    posts = []
    runner.run(engine, client=c, campaign_id="camp", now=NOW, post_slack=posts.append)
    assert c.replies == [] and any("not sent" in p for p in posts)


def test_opt_out_blocks_company_but_freemail_blocks_address(engine):
    c = FakeClient([email(1), email(2, lead="joe@gmail.com", thread="t2")])
    def think(thread, *, lead_email, **k):
        return abby.Decision("opt_out", "block", block_domain=lead_email.split("@")[1], interest=-1)
    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    assert c.blocks == ["huppy.com", "joe@gmail.com"] and c.replies == []


def test_daily_cap_holds(engine, monkeypatch):
    monkeypatch.setenv("ABBY_DAILY_CAP", "1")
    c = FakeClient([email(1), email(2, lead="b@c.com", thread="t2")])
    posts = []
    dec = abby.Decision("interested", "reply", reply=LINK)
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    assert out["replied"] == 1 and out["held"] == 1 and any("daily cap" in p for p in posts)


def test_failed_send_is_held_and_reported(engine):
    class Failing(FakeClient):
        def reply(self, **kw):
            return 500, None
    c = Failing([email(1)])
    posts = []
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)), post_slack=posts.append)
    assert out["held"] == 1 and "failed to send" in posts[0]


def test_not_now_followup_sent_after_60_days(engine):
    c = FakeClient([email(1)])
    dec = abby.Decision("not_now", "reply", reply="Thanks. Abby", follow_up_days=60)
    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec))
    early = runner.run(engine, client=FakeClient([]), campaign_id="camp", now=NOW + timedelta(days=30))
    c2 = FakeClient([])
    late = runner.run(engine, client=c2, campaign_id="camp", now=NOW + timedelta(days=61))
    assert early["followups"] == 0 and late["followups"] == 1
    assert "Mind if I put something together for you?" in c2.replies[0]["body"]


def test_followup_text_has_no_stray_numbers():
    assert abby.validate_reply("interested", runner.followup_text("Ann"))[0]


def test_before_takeover_no_nudges_and_no_backlog(engine, monkeypatch):
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-12-01T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-27T15:00:00Z")
    c = nudge_client(10)
    c.received = [email(1)]
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert out["nudges"] == 0 and out["seen"] == 1  # list call is time-filtered server side


def test_gabes_manual_followups_count_toward_three(engine):
    prospect = email(1, thread="t9", ts="2026-10-10T10:00:00Z")
    g1 = dict(ours(1, days_ago=12)); g2 = dict(ours(2, days_ago=8)); g3 = dict(ours(3, days_ago=4))
    c = FakeClient([], {"t9": [prospect, g1, g2, g3]}, interested=[g3])
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW)
    assert out["nudges"] == 0 and c.replies == []
    again = FakeClient([], {"t9": [prospect, g1, g2, g3]}, interested=[g3])
    runner.run(engine, client=again, campaign_id="camp", now=NOW)
    assert again.replies == []


def test_two_manual_followups_means_next_is_the_last_nudge_without_cc(engine):
    prospect = email(1, thread="t9", ts="2026-10-10T10:00:00Z")
    g1 = dict(ours(1, days_ago=15)); g2 = dict(ours(2, days_ago=8))
    c = FakeClient([], {"t9": [prospect, g1, g2]}, interested=[g2])
    runner.run(engine, client=c, campaign_id="camp", now=NOW)
    assert len(c.replies) == 1 and c.replies[0]["cc"] == []
    assert c.replies[0]["body"].endswith(abby.SIGNATURE)


def test_detail_keeps_their_message_and_abbys_reply(engine):
    c = FakeClient([dict(email(1), body={"text": "Sounds good, tell me more\n\nOn Mon, Oct 26, 2026 Abby wrote:\n> old"})])
    runner.run(engine, client=c, campaign_id="camp", now=NOW,
               think=fixed(abby.Decision("interested", "reply", reply=f"Great. {LINK} Abby")))
    with engine.connect() as conn:
        d = conn.execute(text("SELECT detail FROM abby_handled")).fetchone()[0]
    assert "THEM: Sounds good, tell me more" in d and "old" not in d and "ABBY: Great." in d


# ---------- David's own replies (2026-10-27: Abby answered David as if he were Brad) ----------

def david_msg(i, thread="t1", ts="2026-10-23T10:00:00Z"):
    return {"id": f"d{i}", "thread_id": thread, "lead": "ann@huppy.com", "eaccount": "abby@x.com",
            "from_address_email": "david@anatainc.com", "subject": "Re: Idea for Huppy",
            "ue_type": 2, "body": {"text": "Happy to jump on a call"}, "timestamp_created": ts}


def test_is_team_addresses():
    assert runner.is_team("david@anatainc.com") and runner.is_team("a.orit@workanatagroup.com")
    assert runner.is_team("abbyorit@tryanata.com") and not runner.is_team("brad@gmail.com")


def test_davids_gmail_reply_is_never_answered(engine):
    th = [email(1), david_msg(1)]
    c = FakeClient([david_msg(1)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert c.replies == [] and out["skipped"] == 1


def test_prospect_reply_after_david_joined_is_left_to_david(engine):
    th = [email(1), david_msg(1), email(4)]
    c = FakeClient([email(4)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert c.replies == [] and out["skipped"] == 1


def test_no_nudges_once_david_is_in_the_thread(engine):
    c = nudge_client(10)
    c.threads["t9"].insert(1, david_msg(1, thread="t9", ts="2026-10-11T10:00:00Z"))
    assert runner.run(engine, client=c, campaign_id="camp", now=NOW)["nudges"] == 0
    assert c.replies == []


def test_live_run_records_departed_notices_and_sends_nothing(engine):
    """Riomar, Garbarini, and Bellocq: no reply, no Interested mark, note kept."""
    cases = [
        ("mac@riomarshoes.com", RIOMAR, "customerservice@riomarshoes.com"),
        ("carley@garbarinishop.com", GARBARINI, "sierra@garbarinishop.com"),
        ("danielle@bellocq.com", BELLOCQ, "sales@bellocq.com"),
    ]
    messages, threads = [], {}
    for i, (lead, body, _needle) in enumerate(cases, start=1):
        msg = email(i, lead=lead, thread=f"tn{i}")
        msg["body"] = {"text": body}
        msg["from_address_email"] = lead
        messages.append(msg)
        threads[msg["thread_id"]] = [msg]

    def think(thread, **kw):
        return abby.think(thread, llm=_bad_referral, **kw)

    c = FakeClient(messages, threads)
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    assert out["ignored"] == 3 and out["replied"] == 0 and out["held"] == 0
    assert c.replies == [] and c.interest == [] and c.blocks == []
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT lead, action, detail FROM abby_handled ORDER BY lead")).fetchall()
    saved = {lead: (action, detail) for lead, action, detail in rows}
    for lead, _body, needle in cases:
        action, detail = saved[lead]
        assert action == "ignore"
        assert "Departed contact" in detail and needle in detail and "Nothing sent." in detail
    import outbound_suppression as suppression
    emails, domains = suppression.lists(engine)
    for lead, _body, _needle in cases:
        assert lead in emails
    assert domains == set()


def test_out_of_office_is_not_suppressed(engine):
    import outbound_suppression as suppression
    msg = email(1, lead="ann@huppy.com")
    msg["body"] = {"text": "I am out of the office until Monday."}
    msg["from_address_email"] = "ann@huppy.com"
    runner.run(engine, client=FakeClient([msg], {msg["thread_id"]: [msg]}), campaign_id="camp",
               now=NOW, think=abby.think)
    emails, domains = suppression.lists(engine)
    assert "ann@huppy.com" not in emails and domains == set()
    with engine.connect() as conn:
        action, detail = conn.execute(text("SELECT action, detail FROM abby_handled")).one()
    assert action == "ignore" and "out of office" in detail.lower()


def test_prompt_asks_for_a_yes_and_does_not_offer_the_calendar():
    assert abby.BOOKING_LINK == "https://calendar.app.google/5TwjbbpmKaGAQuBk8"
    assert abby.BOOKING_LINK not in abby.SYSTEM
    assert "AI account specialist on David's team" in abby.SYSTEM
    assert "Never say \"virtual assistant\"" in abby.SYSTEM
    assert "Looping David" not in abby.SYSTEM
    assert "cc_david" in abby.SYSTEM
    assert "Do not send a booking link" in abby.SYSTEM
    assert abby.SIGNATURE in abby.SYSTEM
    reply = (f"Thanks Ann. Grab a time with David here: {LINK}\n\n"
             f"David, any thoughts on timing for the 14-day trial?\n\n{abby.SIGNATURE}")
    assert abby.validate_reply("interested", reply) == (False, "booking link")
    chosen = d("interested", reply, cc_david=True)
    assert chosen.send_deck and chosen.cc == [abby.founder_email()] and chosen.interest is None


def test_templates_sign_as_account_specialist_and_do_not_cc():
    assert runner.followup_text("Ann").endswith(abby.SIGNATURE)
    for n in (1, 2, 3):
        body, cc = runner.nudge_text(n, "Ann", "Huppy")
        assert body.endswith(abby.SIGNATURE) and cc == []
        assert "Looping David" not in body
        assert abby.validate_reply("interested", body)[0]


V2_CAMPAIGN_IDS = (
    "8e0df427-1f18-43ac-b0c6-e8ea787123d7",
    "138a162d-6654-4916-9959-20c11e31214b",
    "4dc10f79-f7c9-43c5-9c3e-5e28e9cf4c72",
    "d1c197e3-bf1f-4f41-bcb9-e30e73975a2c",
    "907d32fd-d6db-4ce6-8edc-485b78cce120",
)


def test_default_watch_list_includes_the_signal_campaigns(monkeypatch):
    monkeypatch.delenv("ABBY_CAMPAIGN_IDS", raising=False)
    ids = runner.watched_campaign_ids(["reengage-id"])
    assert ids[0] == "4f118329-c07e-4279-9c08-12059f2a49f4"
    assert "reengage-id" in ids and runner.TRIAL_CAMPAIGN_ID in ids
    for campaign_id in (*runner.SIGNAL_CAMPAIGN_IDS, *V2_CAMPAIGN_IDS):
        assert campaign_id in ids
    assert ids == runner.watched_campaign_ids(["reengage-id"], create_engine("sqlite://"))
    assert len(ids) == len(set(ids))
    monkeypatch.setenv("ABBY_CAMPAIGN_IDS", "custom-1")
    replaced = runner.watched_campaign_ids(["reengage-id"])
    assert runner.TRIAL_CAMPAIGN_ID not in replaced
    for campaign_id in (*runner.SIGNAL_CAMPAIGN_IDS, *V2_CAMPAIGN_IDS):
        assert campaign_id not in replaced
    assert "custom-1" in replaced and "reengage-id" in replaced


def test_watch_list_includes_new_outbound_campaigns_and_skips_proposals(monkeypatch):
    monkeypatch.delenv("ABBY_CAMPAIGN_IDS", raising=False)
    import outbound_campaigns as campaigns

    engine = create_engine("sqlite://")
    campaigns.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO outbound_campaigns "
            "(campaign_id, name, recipe_key, status, daily_cap) "
            "VALUES ('new-live', 'future|standard|x', 'future', 'active', 10)"
        ))
        conn.execute(text(
            "INSERT INTO outbound_campaigns "
            "(campaign_id, name, recipe_key, status, daily_cap) "
            "VALUES ('kept-cut', 'old|standard|x', 'old_cut', 'cut', 0)"
        ))
        conn.execute(text(
            "INSERT INTO outbound_campaigns "
            "(campaign_id, name, recipe_key, status, daily_cap) "
            "VALUES ('only-an-idea', 'future|standard|idea', 'future_idea', 'proposed', 0)"
        ))
    ids = runner.watched_campaign_ids(["reengage-id"], engine)
    assert "new-live" in ids and "kept-cut" in ids
    assert "only-an-idea" not in ids
    for campaign_id in (*runner.SIGNAL_CAMPAIGN_IDS, *V2_CAMPAIGN_IDS, runner.TRIAL_CAMPAIGN_ID):
        assert campaign_id in ids
    monkeypatch.setenv("ABBY_CAMPAIGN_IDS", "custom-1")
    replaced = runner.watched_campaign_ids(["reengage-id"], engine)
    assert "new-live" not in replaced and "custom-1" in replaced


def test_watched_campaigns_keep_sdr_and_reengage_and_default_to_the_trial(monkeypatch):
    monkeypatch.delenv("ABBY_CAMPAIGN_IDS", raising=False)
    ids = runner.watched_campaign_ids(["reengage-id"])
    assert ids[0] == "4f118329-c07e-4279-9c08-12059f2a49f4"
    assert "reengage-id" in ids and runner.TRIAL_CAMPAIGN_ID in ids
    monkeypatch.setenv("ABBY_CAMPAIGN_IDS", "custom-1, custom-1")
    replaced = runner.watched_campaign_ids(["reengage-id"])
    assert runner.TRIAL_CAMPAIGN_ID not in replaced
    assert replaced.count("custom-1") == 1
    assert "reengage-id" in replaced and ids[0] in replaced


def test_live_opt_out_is_remembered_on_our_suppression_list(engine):
    import outbound_suppression as suppression
    c = FakeClient([email(1), email(2, lead="joe@gmail.com", thread="t2")])

    def think(thread, *, lead_email, **k):
        return abby.Decision("opt_out", "block", block_domain=lead_email.split("@")[1], interest=-1)

    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    emails, domains = suppression.lists(engine)
    assert "huppy.com" in domains and "joe@gmail.com" in emails


def test_domain_opt_out_stores_the_registrable_domain(engine):
    import outbound_suppression as suppression
    c = FakeClient([email(1, lead="ann@shop.petlibro.com")])

    def think(thread, *, lead_email, **k):
        return abby.Decision("opt_out", "block", block_domain="shop.petlibro.com", interest=-1)

    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    _emails, domains = suppression.lists(engine)
    assert "petlibro.com" in domains
    assert "shop.petlibro.com" not in domains


def test_shadow_opt_out_is_recorded_but_not_blocked_in_instantly(engine, monkeypatch):
    import outbound_suppression as suppression
    monkeypatch.setenv("ABBY_MODE", "shadow")
    c = FakeClient([email(1, lead="ann@huppy.com")])

    def think(thread, *, lead_email, **k):
        return abby.Decision("opt_out", "block", block_domain="huppy.com", interest=-1)

    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think)
    assert c.blocks == []
    _emails, domains = suppression.lists(engine)
    assert "huppy.com" in domains


def test_opt_out_save_failure_posts_slack(engine, monkeypatch):
    import outbound_suppression as suppression

    def boom(*_args, **_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(suppression, "add", boom)
    posts = []
    c = FakeClient([email(1)])

    def think(thread, *, lead_email, **k):
        return abby.Decision("opt_out", "block", block_domain="huppy.com", interest=-1)

    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=think, post_slack=posts.append)
    assert "[abby] opt-out not saved: huppy.com" in posts


def test_out_of_office_schedules_the_day_after_return_and_sends_nothing(engine):
    body = "I am out of the office until November 4 with no access to email."
    message = dict(email(1), body={"text": body})
    client = FakeClient([message])
    out = runner.run(engine, client=client, campaign_id="camp", now=NOW)
    assert out["ignored"] == 1 and out["replied"] == 0 and client.replies == []
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT kind, status, body, due_at FROM abby_followups_scheduled"
        )).fetchone()
    assert row[0] == "out_of_office" and row[1] == "pending"
    assert "Hope your time away went well." in row[2]
    assert row[2].rstrip().endswith("Account Specialist, Anata")
    assert "\u2014" not in row[2]
    assert "2026-11-05" in str(row[3])


def test_not_now_schedules_30_and_75_not_the_old_60(engine):
    decision = abby.decide(
        {"category": "not_now", "confidence": 0.9, "reply": "Thanks. I'll check back in about a month.\n\nAbby"},
        lead_email="ann@huppy.com", thread_categories=[], first_name="Ann",
    )
    client = FakeClient([email(1)])
    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=fixed(decision))
    with engine.connect() as conn:
        kinds = [row[0] for row in conn.execute(text(
            "SELECT kind FROM abby_followups_scheduled ORDER BY due_at"
        )).fetchall()]
        old = conn.execute(text("SELECT COUNT(*) FROM abby_followups")).fetchone()[0]
    assert kinds == ["not_now_30", "not_now_75"]
    assert old == 0
    assert len(client.replies) == 1


def test_has_solution_schedules_one_second_note(engine):
    decision = abby.decide(
        {"category": "has_solution", "confidence": 0.9,
         "reply": "What does that setup cost you to run today?\n\nAbby"},
        lead_email="ann@huppy.com", thread_categories=[], first_name="Ann",
    )
    runner.run(engine, client=FakeClient([email(1)]), campaign_id="camp", now=NOW, think=fixed(decision))
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT kind, body FROM abby_followups_scheduled"
        )).fetchall()
    assert len(rows) == 1 and rows[0][0] == "has_solution_second"
    assert "alongside what you already use" in rows[0][1]
    assert rows[0][1].rstrip().endswith("Account Specialist, Anata")
    assert "\u2014" not in rows[0][1]


def test_hostile_blocks_the_lead_and_a_later_email_is_not_sent(engine):
    import outbound_suppression as suppression

    def think(thread, *, lead_email, **_k):
        return abby.decide({"category": "hostile", "confidence": 0.95},
                           lead_email=lead_email, thread_categories=[])

    client = FakeClient([email(1)])
    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=think)
    assert client.blocks == ["ann@huppy.com"] and client.replies == []
    emails, _domains = suppression.lists(engine)
    assert "ann@huppy.com" in emails
    later = FakeClient([email(2)])
    out = runner.run(engine, client=later, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=f"Hi {LINK}")))
    assert later.replies == [] and out["skipped"] == 1


def test_handle_email_records_once(engine):
    client = FakeClient([email(1)])
    decision = abby.Decision("interested", "reply", reply=f"Hi {LINK}")
    message = email(1)
    first = runner.handle_email(engine, message, client=client, campaign_id="camp", now=NOW, think=fixed(decision))
    second = runner.handle_email(engine, message, client=client, campaign_id="camp", now=NOW, think=fixed(decision))
    assert first["replied"] == 1 and second["skipped"] == 1 and len(client.replies) == 1


def test_scheduled_followup_sends_once_and_stops_at_the_cap(engine):
    runner.ensure_tables(engine)
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        for kind in ("not_now_30", "not_now_75"):
            conn.execute(text(
                "INSERT INTO abby_followups_scheduled "
                "(lead, reply_to_id, eaccount, subject, first_name, kind, due_at, status, body, source_email_id) "
                "VALUES ('ann@huppy.com', 'e1', 'abby@x.com', 'Re: Idea', 'Ann', :k, :d, 'pending', :b, :src)"
            ), {"k": kind, "d": now.replace(tzinfo=None), "b": f"Hi Ann,\n\n{abby.SIGNATURE}", "src": f"e1-{kind}"})
    client = FakeClient([])
    posts = []
    sent = runner.send_due_scheduled(engine, client, now, "live", posts.append, 1)
    assert sent == 1 and len(client.replies) == 1
    assert runner.send_due_scheduled(engine, client, now, "live", posts.append, 1) == 0
    assert any("daily cap" in post for post in posts)
    assert runner.send_due_scheduled(engine, FakeClient([]), now, "shadow", posts.append, 5) == 0


GLEEFULL = (
    "Hi Anna,\n\nThanks for reaching out and sharing the idea. "
    "Please send your information and collaboration details to our Creator "
    "Partnerships Team at partnerships@gleefullsupplements.com.\n\nBest,\nJenny\n"
)

JACK = (
    "Thank you for your email, I have now left Arena.Online\n"
    "Please message a member of the consumer team using the emails below\n"
    "olivia.smith@hellopatch.com (Head of consumer trading)\n"
    "Thanks, Jack\n"
)


def test_send_info_to_a_role_inbox_is_held_and_not_sent():
    calls = []
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": "support@gleefullsupps.com",
        "subject": "Re: Trial", "body": {"text": GLEEFULL},
    }
    result = abby.think(
        [message], lead_email="marissa@gleefullsupps.com", company="GleeFull",
        first_name="Marissa", thread_categories=[],
        llm=lambda prompt: calls.append(prompt) or _bad_referral(prompt),
    )
    assert calls == []
    assert result.action == "hold" and result.category == "send_info"
    assert result.reply == "" and result.to_extra == [] and result.interest is None
    assert "partnerships@gleefullsupplements.com" in result.slack
    assert "Nothing was sent" in result.slack
    assert "Marissa" not in result.reply


def test_second_human_send_info_is_not_dropped_as_an_auto_reply():
    """The model used to label the repeat 'auto_reply' and ignore it."""
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": "support@gleefullsupps.com",
        "body": {"text": "I don't have a specific contact name to share, but " + GLEEFULL},
    }
    result = abby.decide(
        {"category": "auto_reply", "confidence": 0.99, "reply": None},
        lead_email="marissa@gleefullsupps.com", thread_categories=[],
        message=message, first_name="Marissa", company="GleeFull",
    )
    assert result.action == "hold" and result.category == "send_info"
    assert result.reason != "automatic reply"
    assert result.interest is None and result.to_extra == []


def test_model_auto_reply_on_a_human_question_is_held():
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "body": {"text": "Can you give me a brief on what kind of service you provide?"},
    }
    result = abby.decide(
        {"category": "auto_reply", "confidence": 0.8},
        lead_email="fischer@xviveaudio.com", thread_categories=[], message=message,
    )
    assert result.action == "hold" and result.interest is None and result.slack
    assert result.reason != "automatic reply"


def test_satisfaction_survey_stays_an_auto_reply():
    body = (
        "Hi Abby, How would you rate the help Jenny gave you? "
        "https://example.gorgias.com/satisfaction-survey?token=secret-token"
    )
    result = abby.think(
        [{"ue_type": 2, "is_auto_reply": 0, "body": {"text": body}}],
        lead_email="marissa@gleefullsupps.com", company="GleeFull", first_name="Marissa",
        thread_categories=[], llm=_bad_referral,
    )
    assert result.action == "ignore" and "survey" in result.reason
    assert "secret-token" not in result.slack
    assert result.slack


def test_wrong_person_addresses_the_writer_when_from_differs():
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": "support@gleefullsupps.com",
        "body": {"text": "This is not my area.\n\nBest,\nJenny"},
    }
    result = abby.decide(
        {"category": "wrong_person", "confidence": 0.95, "reply": "Thanks Marissa, who handles this?"},
        lead_email="marissa@gleefullsupps.com", thread_categories=[],
        message=message, first_name="Marissa", company="GleeFull",
    )
    assert result.action == "reply" and result.interest is None
    assert "Thanks Jenny," in result.reply
    assert "Marissa" not in result.reply
    assert "wrong-person" in result.slack


def test_wrong_person_drops_the_lead_name_when_the_writer_is_unknown():
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": "support@gleefullsupps.com",
        "body": {"text": "This is not my area."},
    }
    result = abby.decide(
        {"category": "wrong_person", "confidence": 0.95, "reply": "Thanks Marissa"},
        lead_email="marissa@gleefullsupps.com", thread_categories=[],
        message=message, first_name="Marissa", company="GleeFull",
    )
    assert result.reply.startswith("Thanks, who handles")
    assert "Marissa" not in result.reply


def test_i_have_now_left_is_departed_and_not_interested():
    calls = []
    message = {
        "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": "jack@arena.online",
        "subject": "OOO - Thank you for your email, I have left the business",
        "body": {"text": JACK},
    }
    result = abby.think(
        [message], lead_email="jack@arena.online", company="Arena", first_name="Jack",
        thread_categories=[], llm=lambda prompt: calls.append(prompt) or _bad_referral(prompt),
    )
    assert calls == []
    assert result.action == "ignore" and result.interest is None and result.to_extra == []
    assert "departed" in result.reason
    assert "olivia.smith@hellopatch.com" in result.note
    assert "Nothing sent." in result.note
    assert result.slack and "Instantly marked as human" in result.slack


@pytest.mark.parametrize("body", [
    "I no longer work at Phizz. Please reach out to thom@phizz.co.",
    "Shenoa is no longer part of KaramMD Skin.",
    "This email is no longer active. Please contact account@earthbath.com.",
    "Mo has left REFY. This inbox is no longer monitored.",
])
def test_more_departed_wording_is_ignored_before_the_model(body):
    calls = []
    result = abby.think(
        [{"ue_type": 2, "is_auto_reply": 0, "body": {"text": body}}],
        lead_email="person@brand.com", company="Brand", first_name="Pat",
        thread_categories=[], llm=lambda prompt: calls.append(prompt) or _bad_referral(prompt),
    )
    assert calls == []
    assert result.action == "ignore" and "departed" in result.reason
    assert result.interest is None and result.to_extra == []


def test_question_and_referral_do_not_mark_interest():
    question = d("question", "Happy to explain. David can cover the rest on a call.\n\nAbby")
    assert question.action == "reply" and question.interest is None
    referral = d("referral", f"Hi Sam. {LINK}\n\nAbby", referral_email="sam@huppy.com")
    assert referral.action == "reply" and referral.interest is None and referral.to_extra == ["sam@huppy.com"]


def test_flagged_auto_reply_still_ignores_a_role_forward():
    message = {"ue_type": 2, "is_auto_reply": 1,
               "body": {"text": "Hello, please email support@acme.com and someone will help."}}
    result = abby.think(
        [message], lead_email="person@acme.com", company="Acme", first_name="Pat",
        thread_categories=[], llm=_bad_referral,
    )
    _assert_ignored(result, "support@acme.com")


def test_campaign_send_and_nudge_do_not_skip_a_human_question(engine):
    question = email(1)
    campaign = {"id": "step1", "thread_id": "t1", "ue_type": 1,
                "timestamp_created": "2026-10-22T12:00:00Z", "subsequence_id": "0c2f113f"}
    nudge = {"id": "nudge1", "thread_id": "t1", "ue_type": 3,
             "timestamp_created": "2026-10-23T12:00:00Z"}
    runner.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled "
            "(email_id, thread_id, lead, category, action, mode, reply_id, detail) "
            "VALUES ('nudge-row', 't1', 'ann@huppy.com', 'nudge', 'nudge', 'live', 'nudge1', '')"
        ))
    client = FakeClient([question], {"t1": [question, campaign, nudge]})
    posts = []
    decision = abby.Decision("question", "reply", reply="Here is the short version.\n\nAbby", interest=1)
    out = runner.run(engine, client=client, campaign_id="camp", now=NOW,
                     think=fixed(decision), post_slack=posts.append)
    assert out["replied"] == 1 and client.replies[0]["reply_to"] == "e1"
    assert client.interest == []


def test_runner_posts_a_wrong_person_send(engine):
    message = email(1)
    message["from_address_email"] = "support@gleefullsupps.com"
    message["lead"] = "marissa@gleefullsupps.com"
    message["body"] = {"text": "This is not my area.\n\nBest,\nJenny"}

    def think(thread, *, lead_email, company, first_name, thread_categories):
        return abby.decide(
            {"category": "wrong_person", "confidence": 0.95, "reply": "Thanks Marissa"},
            lead_email=lead_email, thread_categories=thread_categories,
            message=thread[-1], first_name=first_name, company=company,
        )

    client = FakeClient([message], {message["thread_id"]: [message]},
                        leads={"marissa@gleefullsupps.com": {"first_name": "Marissa", "company_name": "GleeFull"}})
    posts = []
    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=think, post_slack=posts.append)
    assert client.replies and "Thanks Jenny," in client.replies[0]["body"]
    assert "Marissa" not in client.replies[0]["body"]
    assert client.interest == []
    assert any("wrong-person" in post for post in posts)


def test_unassigned_mail_is_read_outside_the_campaign_poll(engine):
    loose = email(1, lead="cheyanne@rookiewellness.com", thread="tu")
    owned = dict(email(2, lead="ann@huppy.com", thread="t2"), campaign_id="camp")

    class Client(FakeClient):
        def received_since(self, since, campaign_id, pages=3):
            self.asked = campaign_id
            if campaign_id:
                return [item for item in self.received if item.get("campaign_id") == campaign_id]
            return [item for item in self.received if not item.get("campaign_id")]

        def leads_for_email(self, address):
            marked = [row for row in getattr(self, "_marked", []) if row.get("email") == address]
            if marked:
                return marked
            if address == "cheyanne@rookiewellness.com":
                return [{"email": address, "campaign": "camp"}]
            return []

    client = Client([loose, owned])
    out = runner.run_unassigned(
        engine, client=client, watched=["camp"], now=NOW,
        think=fixed(abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1)),
    )
    assert client.asked == ""
    assert out["replied"] == 1 and client.replies[0]["reply_to"] == "e1"
    assert client.interest == []
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT campaign_id FROM abby_handled")).scalar()
    assert stored == "camp"


def test_unassigned_lead_on_an_unwatched_campaign_is_skipped(engine):
    loose = email(1, lead="other@brand.com", thread="tu")

    class Client(FakeClient):
        def received_since(self, since, campaign_id, pages=3):
            return list(self.received)

        def leads_for_email(self, address):
            return [{"email": address, "campaign": "not-watched"}]

    client = Client([loose])
    out = runner.run_unassigned(
        engine, client=client, watched=["camp"], now=NOW,
        think=fixed(abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1)),
    )
    assert out["skipped"] == 1 and client.replies == [] and client.interest == []


def test_left_the_office_is_out_of_office_not_a_departure(engine):
    import outbound_suppression as suppression

    message = email(1)
    message["body"] = {"text": "I have now left the office and will be back Monday."}
    message["from_address_email"] = "ann@huppy.com"
    runner.run(engine, client=FakeClient([message], {message["thread_id"]: [message]}),
               campaign_id="camp", now=NOW, think=abby.think)
    emails, _domains = suppression.lists(engine)
    assert "ann@huppy.com" not in emails
    with engine.connect() as conn:
        detail = conn.execute(text("SELECT detail FROM abby_handled")).scalar()
    assert "out of office" in detail.lower()
    assert "departed" not in detail.lower()
    with engine.connect() as conn:
        kind = conn.execute(text("SELECT kind FROM abby_followups_scheduled")).scalar()
    assert kind == "out_of_office"


def test_left_a_message_is_not_a_departure():
    assert abby.automated_notice(
        "I have now left a message for the team.",
        lead_email="ann@huppy.com",
    ) is None
    still_gone = abby.automated_notice(
        "I have now left Arena.Online.",
        lead_email="jack@arena.online",
    )
    assert still_gone is not None and still_gone.kind == "departed"


def test_failed_lead_lookup_does_not_settle_unassigned_mail(engine):
    loose = email(1, lead="cheyanne@rookiewellness.com", thread="tu")

    class Client(FakeClient):
        def received_since(self, since, campaign_id, pages=3):
            return list(self.received)

        def lead_lookup(self, _address):
            return [], False

    client = Client([loose])
    decision = fixed(abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1))
    first = runner.run_unassigned(engine, client=client, watched=["camp"], now=NOW, think=decision)
    second = runner.run_unassigned(engine, client=client, watched=["camp"], now=NOW, think=decision)
    assert first["deferred"] == 1 and first["replied"] == 0 and client.replies == []
    assert second["seen"] == 1 and second["deferred"] == 1
    with engine.connect() as conn:
        stored = conn.execute(text("SELECT COUNT(*) FROM abby_handled")).scalar()
    assert stored == 0


def test_unassigned_scan_reads_past_assigned_mail(engine):
    loose = email(1, lead="cheyanne@rookiewellness.com", thread="tu")
    loose["timestamp_created"] = "2026-11-01T00:00:00Z"
    assigned = []
    for index in range(600):
        assigned.append({
            "id": f"a{index}", "campaign_id": "camp", "lead": "owned@brand.com",
            "ue_type": 2, "timestamp_created": f"2026-10-{(index % 27) + 1:02d}T00:00:00Z",
        })

    class Client(FakeClient):
        def scan_received(self, since, campaign_id="", max_pages=20):
            self.max_pages = max_pages
            self.since = since
            return assigned + [loose], True

        def leads_for_email(self, address):
            marked = [row for row in getattr(self, "_marked", []) if row.get("email") == address]
            if marked:
                return marked
            if address == "cheyanne@rookiewellness.com":
                return [{"email": address, "campaign": "camp"}]
            return []

    client = Client([loose])
    out = runner.run_unassigned(
        engine, client=client, watched=["camp"], now=NOW,
        think=fixed(abby.Decision("interested", "reply", reply=f"Hi {LINK}", interest=1)),
    )
    assert client.max_pages == 20
    assert out["replied"] == 1 and client.replies[0]["reply_to"] == "e1"
    import outbound_suppression as suppression
    assert suppression.get_cursor(engine, runner._UNASSIGNED_CURSOR) == "2026-11-01T00:00:00Z"
