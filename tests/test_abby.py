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
        return 200

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
    assert LINK in c.replies[0]["body"] and "Hi Ann" in c.replies[0]["body"] and c.replies[0]["cc"] == []


def test_no_nudge_before_3_days_or_when_prospect_wrote_last(engine):
    assert runner.run(engine, client=nudge_client(2), campaign_id="camp", now=NOW)["nudges"] == 0
    c = nudge_client(10)
    c.interested = [email(5, thread="t9")]
    assert runner.run(engine, client=c, campaign_id="camp", now=NOW)["nudges"] == 0


def test_three_nudges_max_last_one_ccs_david(engine):
    posts = []
    for _ in range(3):
        c = nudge_client(8)
        runner.run(engine, client=c, campaign_id="camp", now=NOW, post_slack=posts.append)
    assert c.replies[0]["cc"] == [abby.DAVID_EMAIL]
    c4 = nudge_client(30)
    assert runner.run(engine, client=c4, campaign_id="camp", now=NOW)["nudges"] == 0
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
    assert LINK in c2.replies[0]["body"]


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
