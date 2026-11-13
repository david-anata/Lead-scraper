"""Yes detection, the automatic deck send, and the hourly fixer."""

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from types import SimpleNamespace

import abby
import abby_runner as runner
from outbound_yes_cta import FIRST_ASK, FOLLOW_ASK, apply_yes_cta, rewrite_body
from sales_support_agent.api.abby_job import router as abby_router

NOW = datetime(2026, 11, 13, 16, 0, tzinfo=timezone.utc)
DECK = "https://agent.anatainc.com/decks/huppy/9/token"


def d(cat, reply="", conf=0.9, **kw):
    return abby.decide(
        {"category": cat, "confidence": conf, "reply": reply, **kw},
        lead_email="ann@huppy.com", thread_categories=[], message=kw.get("message", ""),
    )


def test_explicit_yes_is_a_deck_and_a_role_inbox_is_not():
    assert abby.explicit_yes("Yes")
    assert abby.explicit_yes("Sure, send it")
    assert not abby.explicit_yes("No thanks")
    assert not abby.explicit_yes("Sure, talk to Sam")
    assert not abby.explicit_yes("Please send your information to partnerships@huppy.com")
    yes = abby.think(
        [{"ue_type": 2, "body": {"text": "Yes please"}}],
        lead_email="ann@huppy.com", company="Huppy", first_name="Ann", thread_categories=[],
        llm=lambda _p: (_ for _ in ()).throw(AssertionError("the model should not run")),
    )
    assert yes.send_deck and yes.cc == [abby.founder_email()] and yes.interest is None
    assert "book" not in yes.reply.lower()
    route = abby.think(
        [{"ue_type": 2, "body": {"text": "Please send your information to partnerships@huppy.com"}}],
        lead_email="ann@huppy.com", company="Huppy", first_name="Ann", thread_categories=[],
        llm=lambda _p: (_ for _ in ()).throw(AssertionError("send-info is decided in code")),
    )
    assert route.action == "hold" and route.category == "send_info" and route.reply == ""


def test_a_question_asks_for_a_yes_and_does_not_offer_a_call():
    reply = d("question", f"The trial is 14 days. Grab a time here: {abby.BOOKING_LINK}\n\nAbby")
    assert reply.action == "reply" and not reply.send_deck
    assert abby.BOOKING_LINK not in reply.reply
    assert "Can I send it to you?" in reply.reply


class FakeClient:
    def __init__(self, received, threads=None):
        self.received = received
        self.threads = threads or {}
        self.replies = []
        self.interest = []

    def received_since(self, since, campaign_id):
        return list(self.received)

    def thread(self, tid):
        return self.threads.get(tid, [])

    def reply(self, **kw):
        self.replies.append(kw)
        return 200, {"id": f"r{len(self.replies)}"}

    def set_interest(self, lead, value, campaign_id):
        self.interest.append((lead, value, campaign_id))
        return 200

    def leads_for_email(self, email):
        return []

    def block(self, value):
        return 200


def email(body="yes please", ts="2026-11-13T15:00:00Z"):
    return {
        "id": "e1", "thread_id": "t1", "lead": "ann@huppy.com", "eaccount": "abby@x.com",
        "subject": "Idea for Huppy", "ue_type": 2, "body": {"text": body},
        "timestamp_created": ts, "campaign_id": "camp",
    }


def deck(**_kw):
    return DECK


def test_yes_sends_the_deck_ccs_the_founder_and_logs_it(engine=None):
    engine = engine or create_engine("sqlite://")
    message = email()
    client = FakeClient([message], {"t1": [message]})
    posts = []
    out = runner.handle_email(
        engine, message, client=client, campaign_id="camp", now=NOW, think=abby.think,
        post_slack=posts.append, lead_info=lambda _e: {"first_name": "Ann", "company": "Huppy", "domain": "huppy.com"},
        run_mode="live", build_deck=deck,
    )
    assert out["replied"] == 1 and posts == []
    assert client.interest == []
    sent = client.replies[0]
    assert sent["cc"] == [abby.founder_email()]
    assert DECK in sent["body"]
    assert sent["body"].rstrip().endswith("Account Specialist, Anata")
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT lead, campaign_id, thread_id, source_email_id, reply_id, cc_address, deck_url, status "
            "FROM abby_deck_sends"
        )).one()
    assert row[0] == "ann@huppy.com" and row[1] == "camp" and row[2] == "t1"
    assert row[3] == "e1" and row[4] == "r1" and row[5] == abby.founder_email()
    assert row[6] == DECK and row[7] == "sent"


def test_subsequence_calendar_email_blocks_a_second_reply():
    engine = create_engine("sqlite://")
    message = email()
    subsequence = {
        "id": "sub-1", "thread_id": "t1", "ue_type": 3,
        "subsequence_id": "0c2f113f-ebed-4890-92df-c241d925dcdf",
        "timestamp_created": "2026-11-13T15:05:00Z",
        "from_address_email": "abby@x.com",
    }
    client = FakeClient([message], {"t1": [message, subsequence]})
    out = runner.handle_email(
        engine, message, client=client, campaign_id="camp", now=NOW, think=abby.think,
        post_slack=lambda _t: None, run_mode="live", build_deck=deck,
        lead_info=lambda _e: {"first_name": "Ann", "company": "Huppy"},
    )
    assert client.replies == [] and client.interest == []
    assert out["replied"] == 0
    with engine.connect() as conn:
        action, detail = conn.execute(text("SELECT action, detail FROM abby_handled")).one()
    assert action == "skip" and "subsequence already replied" in detail


def test_fixer_sends_once_and_leaves_a_send_info_hold(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    missed = email("Can you tell me what the 14 day trial includes?")
    held = email("Please send your information to partnerships@huppy.com")
    held["id"] = "e2"
    held["thread_id"] = "t2"
    client = FakeClient([missed, held], {"t1": [missed], "t2": [held]})
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
            "VALUES ('e2', 't2', 'ann@huppy.com', 'send_info', 'hold', 'live', '', "
            "'human asked to send info to an address', 'camp')"
        ))

    def think(thread, **_kw):
        return abby.Decision("question", "reply", reply=abby.yes_ask_reply("Ann", "Huppy"))

    first = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=think, build_deck=deck,
    )
    second = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=think, build_deck=deck,
    )
    assert first["sent"] == 1 and second["sent"] == 0
    assert len(client.replies) == 1
    with engine.connect() as conn:
        outcomes = conn.execute(text(
            "SELECT email_id, outcome FROM abby_missed_reply_fixes ORDER BY email_id"
        )).fetchall()
        held_action = conn.execute(text("SELECT action FROM abby_handled WHERE email_id = 'e2'")).scalar()
    assert outcomes == [("e1", "sent")]
    assert held_action == "hold"


def test_list_hides_a_thread_the_founder_already_answered():
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    sent = NOW.replace(tzinfo=None)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_deck_sends (lead, campaign_id, thread_id, source_email_id, reply_id, "
            "cc_address, deck_url, status, sent_at) VALUES "
            "('ann@huppy.com', 'camp', 't1', 'e1', 'r1', :cc, :url, 'sent', :sent), "
            "('brad@huppy.com', 'camp', 't2', 'e2', 'r2', :cc, :url, 'sent', :sent)"
        ), {"cc": abby.founder_email(), "url": DECK, "sent": sent})
    founder_note = {
        "id": "david-1", "ue_type": 2, "from_address_email": abby.founder_email(),
        "timestamp_created": (NOW + timedelta(minutes=25)).isoformat().replace("+00:00", "Z"),
    }
    pending = runner.list_deck_sends_since(
        engine, NOW - timedelta(hours=1),
        thread_lookup=lambda tid: [founder_note] if tid == "t1" else [],
    )
    assert [row["lead"] for row in pending] == ["brad@huppy.com"]


def test_deck_list_requires_the_internal_key(monkeypatch):
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-key")
    app.include_router(abby_router)
    client = TestClient(app)
    missing = client.get("/api/jobs/abby/deck-sends?since=2026-11-13T00:00:00Z")
    assert missing.status_code == 401
    monkeypatch.setattr(runner, "list_deck_sends_since", lambda *_a, **_k: [{"lead": "ann@huppy.com"}])
    monkeypatch.setattr(
        "sales_support_agent.models.database.get_engine", lambda: object(),
    )
    ok = client.get(
        "/api/jobs/abby/deck-sends?since=2026-11-13T00:00:00Z",
        headers={"X-Internal-Api-Key": "internal-key"},
    )
    assert ok.status_code == 200 and ok.json()["sends"] == [{"lead": "ann@huppy.com"}]


def test_cold_rewrite_keeps_the_opener_and_drops_the_calendar():
    before = (
        "Hi {{firstName}},\n\n"
        "{{personalization}}\n\n"
        "We run Amazon for brands your size.\n\n"
        "Grab a time on my calendar: https://calendar.app.google/example\n"
    )
    after = rewrite_body(before, 0)
    assert "{{personalization}}" in after
    assert "https://" not in after
    assert FIRST_ASK in after
    follow = rewrite_body("Hi {{firstName}},\n\nWorth a look?\n", 1)
    assert FOLLOW_ASK in follow
    assert "Worth a look" not in follow


class _Instantly:
    def __init__(self):
        self.patched = []
        self.paused = []

    def get_campaign(self, campaign_id):
        return 200, {"id": campaign_id, "sequences": [{"steps": [
            {"variants": [{"subject": "hi", "body": "Hi {{firstName}},\n\n{{personalization}}\n\nOpen to a quick intro with David?"}]},
        ]}]}

    def patch_campaign(self, campaign_id, body):
        self.patched.append((campaign_id, body))
        return 200, {}

    def get_subsequence(self, subsequence_id):
        return 200, {"id": subsequence_id, "status": 1, "name": "Interested - book with David"}

    def pause_subsequence(self, subsequence_id):
        self.paused.append(subsequence_id)
        return 200, {"status": "paused"}


def test_yes_cta_apply_runs_once_and_retries_after_a_failure():
    from sqlalchemy import create_engine

    from outbound_yes_cta import maybe_apply_yes_cta

    engine = create_engine("sqlite://")
    calls = {"n": 0}

    def fail_once():
        calls["n"] += 1
        if calls["n"] == 1:
            return {"campaigns": [], "subsequence": {}, "errors": [{"error": "read 500"}]}
        return {"campaigns": [{"name": "Trial Offer", "patched": True}], "subsequence": {"paused": True}, "errors": []}

    first = maybe_apply_yes_cta(engine, apply=fail_once)
    second = maybe_apply_yes_cta(engine, apply=fail_once)
    third = maybe_apply_yes_cta(engine, apply=fail_once)
    assert first["ok"] is False and second["ok"] is True and calls["n"] == 2
    assert third["skipped"] is True and calls["n"] == 2


def test_apply_rewrites_the_ask_and_pauses_the_subsequence_once():
    client = _Instantly()
    report = apply_yes_cta(client, campaigns=(("Trial Offer", "camp-1"),), subsequence_id="0c2f113f-ebed-4890-92df-c241d925dcdf")
    assert report["campaigns"][0]["patched"] is True
    body = client.patched[0][1]["sequences"][0]["steps"][0]["variants"][0]["body"]
    assert "{{personalization}}" in body and FIRST_ASK in body
    assert "intro with David" not in body
    assert client.paused == ["0c2f113f-ebed-4890-92df-c241d925dcdf"]
    again = _Instantly()
    again.get_subsequence = lambda _id: (200, {"id": _id, "status": "paused"})
    second = apply_yes_cta(again, campaigns=(("Trial Offer", "camp-1"),))
    # The rewritten ask is not a calendar line, so a second apply does not patch.
    fresh = rewrite_body(body, 0)
    assert fresh == body
    assert second["subsequence"]["action"] == "already paused"
    assert again.paused == []
