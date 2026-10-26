"""Abby: rules in code, one reply per email, opt-outs never answered."""

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


def test_interested_replies_with_link_and_ccs_david():
    r = d("interested", f"Thanks Ann. Grab a time with David here: {LINK}\n\nLooping David, our owner, in.\n\nAbby")
    assert r.action == "reply" and abby.DAVID_EMAIL in r.cc and r.interest == 1 and r.slack


def test_interested_without_link_is_held():
    r = d("interested", "Thanks Ann, David will reach out.")
    assert r.action == "hold" and "missing booking link" in r.reason


def test_price_mentions_99_ok_but_made_up_numbers_held():
    ok = d("price", f"Plans start at $99 a month after the free 14-day trial. {LINK}")
    bad = d("price", f"Brands like yours grow 40% with us. {LINK}")
    assert ok.action == "reply" and bad.action == "hold"


def test_opt_out_blocks_domain_and_never_replies():
    r = d("opt_out", "Sorry to bother you")
    assert r.action == "block" and r.block_domain == "huppy.com" and r.reply == "" and r.interest == -1


def test_hostile_and_low_confidence_are_held():
    assert d("hostile", "x").action == "hold"
    assert d("interested", f"hi {LINK}", conf=0.4).action == "hold"
    assert d("nonsense-category", "x").action == "hold"


def test_has_solution_answered_once_only():
    first = abby.decide({"category": "has_solution", "confidence": 0.9,
                         "reply": "What does it cost you to run that today? Abby"},
                        lead_email="a@b.com", thread_categories=[])
    second = abby.decide({"category": "has_solution", "confidence": 0.9, "reply": "again"},
                         lead_email="a@b.com", thread_categories=["has_solution"])
    assert first.action == "reply" and second.action == "ignore"


def test_not_now_schedules_60_day_followup_no_cc():
    r = d("not_now", "Thanks for letting me know. I'll check back in a couple of months. Abby")
    assert r.action == "reply" and r.follow_up_days == 60 and r.cc == []


def test_referral_adds_colleague_and_ccs_david():
    r = d("referral", f"Hi Sam, Ann pointed me your way. {LINK} Abby", referral_email="sam@huppy.com")
    assert r.to_extra == ["sam@huppy.com"] and abby.DAVID_EMAIL in r.cc


def test_auto_reply_ignored():
    assert d("auto_reply").action == "ignore"


def test_em_dash_cleaned_and_unknown_links_held():
    r = d("interested", f"Great — here: {LINK} Abby")
    assert "—" not in r.reply and r.action == "reply"
    assert d("interested", f"See https://evil.example.com and {LINK}").action == "hold"


def test_model_failure_becomes_hold():
    def boom(_):
        raise RuntimeError("down")
    r = abby.think([{"ue_type": 2, "body": {"text": "yes"}}], lead_email="a@b.com", company="B",
                   first_name="A", thread_categories=[], llm=boom)
    assert r.action == "hold"


def test_parse_model_json_tolerates_noise():
    assert abby.parse_model_json('sure: {"category": "price", "confidence": 0.8}')["category"] == "price"
    assert abby.parse_model_json("no json")["category"] == "other"


# ---------- runner ----------

class FakeClient:
    def __init__(self, received, threads=None):
        self.received, self.threads = received, threads or {}
        self.replies, self.blocks, self.interest = [], [], []

    def received_since(self, since, campaign_id):
        return list(self.received)

    def thread(self, tid):
        return self.threads.get(tid, [])

    def reply(self, **kw):
        self.replies.append(kw)
        return 200, {"id": f"r{len(self.replies)}"}

    def set_interest(self, lead, value, campaign_id):
        self.interest.append((lead, value))
        return 200

    def block(self, value):
        self.blocks.append(value)
        return 200


@pytest.fixture
def engine(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    return create_engine("sqlite://")


def email(i, lead="ann@huppy.com", thread="t1"):
    return {"id": f"e{i}", "thread_id": thread, "lead": lead, "eaccount": "abby@x.com",
            "subject": "Idea for Huppy", "ue_type": 2, "body": {"text": "yes please"}}


def fixed(decision):
    return lambda *a, **k: decision


def test_live_reply_sent_once_and_recorded(engine):
    c = FakeClient([email(1)])
    dec = abby.Decision("interested", "reply", reply=f"Hi {LINK}", cc=[abby.DAVID_EMAIL], interest=1)
    posts = []
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    again = runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec), post_slack=posts.append)
    assert out["replied"] == 1 and again["seen"] == 0 and len(c.replies) == 1
    assert c.replies[0]["cc"] == [abby.DAVID_EMAIL] and c.replies[0]["subject"] == "Re: Idea for Huppy"
    assert c.interest == [("ann@huppy.com", 1)]


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


def test_thread_with_human_reply_is_left_alone(engine):
    th = [email(1), {"id": "m1", "ue_type": 3, "body": {"text": "Gabe here"}}, email(2)]
    c = FakeClient([email(2)], {"t1": th})
    out = runner.run(engine, client=c, campaign_id="camp", now=NOW,
                     think=fixed(abby.Decision("interested", "reply", reply=LINK)))
    assert out["skipped"] == 1 and c.replies == []


def test_abbys_own_reply_does_not_count_as_human(engine):
    c = FakeClient([email(1)])
    dec = abby.Decision("question", "reply", reply=f"Good question. {LINK}")
    runner.run(engine, client=c, campaign_id="camp", now=NOW, think=fixed(dec))
    th = [email(1), {"id": "r1", "ue_type": 3}, email(2)]
    c2 = FakeClient([email(2)], {"t1": th})
    out = runner.run(engine, client=c2, campaign_id="camp", now=NOW, think=fixed(dec))
    assert out["replied"] == 1


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
    assert LINK in c2.replies[0]["body"]


def test_followup_text_has_no_stray_numbers():
    assert abby.validate_reply("interested", runner.followup_text("Ann"))[0]
