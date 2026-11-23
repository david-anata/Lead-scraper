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
    def __init__(self, received, threads=None, leads=None):
        self.received = received
        self.threads = threads or {}
        self.leads = leads or {}
        self.replies = []
        self.interest = []
        self._marked = []
        self.forwards = []
        self.removed = []

    def received_since(self, since, campaign_id):
        return list(self.received)

    def thread(self, tid):
        return self.threads.get(tid, [])

    def reply(self, **kw):
        self.replies.append(kw)
        return 200, {"id": f"r{len(self.replies)}"}

    def set_interest(self, lead, value, campaign_id):
        self.interest.append((lead, value, campaign_id))
        self._marked.append({
            "email": lead, "campaign": campaign_id, "lt_interest_status": value,
        })
        return 200

    def leads_for_email(self, email):
        return [row for row in self._marked if row.get("email") == email]

    def list_lead_labels(self):
        return [{"label": "Left company", "interest_status": 51}]

    def remove_from_subsequence(self, lead_id):
        self.removed.append(lead_id)
        return 200, {"id": lead_id}

    def forward(self, **kw):
        self.forwards.append(kw)
        return 200, {"id": "fwd"}

    def block(self, value):
        return 200

    def lead(self, address):
        return self.leads.get(address, {})


def email(body="yes please", ts="2026-11-13T15:00:00Z"):
    return {
        "id": "e1", "thread_id": "t1", "lead": "ann@huppy.com", "eaccount": "abby@x.com",
        "subject": "Idea for Huppy", "ue_type": 2, "body": {"text": body},
        "timestamp_created": ts, "campaign_id": "camp",
    }


def deck(**_kw):
    return DECK


def test_instantly_fallback_includes_website_or_company_domain():
    class Client:
        def __init__(self, rec):
            self.rec = rec

        def lead(self, _email):
            return self.rec

    website = runner._instantly_info(Client({
        "first_name": "Ann",
        "company_name": "North Trampoline",
        "website": "northtrampoline.com",
        "company_domain": "other.example",
    }), "pat@northtrampoline.com")
    assert website == {
        "first_name": "Ann",
        "company": "North Trampoline",
        "domain": "https://northtrampoline.com",
    }
    company_domain = runner._instantly_info(Client({
        "first_name": "Ann",
        "company_name": "North Trampoline",
        "website": "",
        "company_domain": "northtrampoline.com",
    }), "pat@northtrampoline.com")
    assert company_domain["domain"] == "https://northtrampoline.com"
    spaced = runner._instantly_info(Client({
        "website": "North Trampoline",
        "company_domain": "https://northtrampoline.com",
    }), "pat@northtrampoline.com")
    assert spaced["domain"] == "https://northtrampoline.com"
    neither = runner._instantly_info(Client({
        "first_name": "Ann", "company_name": "North Trampoline",
    }), "pat@northtrampoline.com")
    assert neither["domain"] == "" and neither["company"] == "North Trampoline"


def _deck_builder(calls):
    def build(**kwargs):
        calls.append(kwargs)
        return DECK if kwargs.get("domain") else ""
    return build


def test_yes_with_website_and_no_db_row_sends_the_deck_and_logs_it():
    engine = create_engine("sqlite://")
    message = email("yes please")
    message["lead"] = "pat@northtrampoline.com"
    client = FakeClient([message], {"t1": [message]}, leads={
        "pat@northtrampoline.com": {
            "first_name": "Ann",
            "company_name": "North Trampoline",
            "website": "northtrampoline.com",
        },
    })
    calls = []
    out = runner.handle_email(
        engine, message, client=client, campaign_id="camp", now=NOW, think=abby.think,
        post_slack=lambda _t: None, lead_info=lambda _e: {},
        run_mode="live", build_deck=_deck_builder(calls),
    )
    assert out["replied"] == 1 and client.interest == []
    assert calls and calls[0]["domain"] == "https://northtrampoline.com"
    assert calls[0]["company"] == "North Trampoline" and calls[0]["first_name"] == "Ann"
    sent = client.replies[0]
    assert sent["cc"] == [abby.founder_email()] and DECK in sent["body"]
    with engine.connect() as conn:
        row = conn.execute(text("SELECT status, deck_url, cc_address FROM abby_deck_sends")).one()
    assert row[0] == "sent" and row[1] == DECK and row[2] == abby.founder_email()


def test_yes_with_neither_domain_holds_and_records_failed():
    engine = create_engine("sqlite://")
    message = email("yes please")
    message["lead"] = "pat@northtrampoline.com"
    client = FakeClient([message], {"t1": [message]}, leads={
        "pat@northtrampoline.com": {"first_name": "Ann", "company_name": "North Trampoline"},
    })
    calls = []
    out = runner.handle_email(
        engine, message, client=client, campaign_id="camp", now=NOW, think=abby.think,
        post_slack=lambda _t: None, lead_info=lambda _e: {},
        run_mode="live", build_deck=_deck_builder(calls),
    )
    assert out["held"] == 1 and out["replied"] == 0 and client.replies == []
    assert calls and calls[0]["domain"] == ""
    with engine.connect() as conn:
        status, detail = conn.execute(text("SELECT status, detail FROM abby_deck_sends")).one()
        action = conn.execute(text("SELECT action FROM abby_handled")).scalar()
    assert status == "failed" and detail == "deck generation failed" and action == "hold"


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


def test_fixer_reclassifies_a_held_headerless_auto_reply_and_does_not_send(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    ooo = email(
        "Hi, I'm on annual leave, returning to work on 2 November. Kind regards, Tom"
    )
    ooo["lead"] = "tom@brand.com"
    ooo["from_address_email"] = "tom@brand.com"
    ooo["is_auto_reply"] = 0
    ooo["subject"] = "Re: Amazon plan"
    ooo["headers"] = {}
    gone = email("Please note that I am no longer working at Northwind. This inbox is not monitored.")
    gone["id"] = "e2"
    gone["thread_id"] = "t2"
    gone["lead"] = "larah@brand.com"
    gone["from_address_email"] = "larah@brand.com"
    gone["is_auto_reply"] = 0
    gone["subject"] = "Re: Amazon plan"
    gone["headers"] = {}
    client = FakeClient([ooo, gone], {"t1": [ooo], "t2": [gone]})
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
            "VALUES ('e1', 't1', 'tom@brand.com', 'auto_reply', 'hold', 'live', '', "
            "'model said auto-reply without auto-reply evidence', 'camp'), "
            "('e2', 't2', 'larah@brand.com', 'auto_reply', 'hold', 'live', '', "
            "'model said auto-reply without auto-reply evidence', 'camp')"
        ))
        conn.execute(text(
            "INSERT INTO abby_missed_reply_fixes (email_id, lead, campaign_id, previous_action, outcome, attempts) "
            "VALUES ('e1', 'tom@brand.com', 'camp', 'hold', 'still_held', 1), "
            "('e2', 'larah@brand.com', 'camp', 'hold', 'still_held', 1)"
        ))

    def think(thread, **kwargs):
        return abby.think(thread, llm=_no_model, **kwargs)

    first = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=think,
    )
    second = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=think,
    )
    assert client.replies == []
    assert client.interest == [("larah@brand.com", 51, "camp")]
    assert first["sent"] == 0 and second["sent"] == 0
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT email_id, action, detail FROM abby_handled ORDER BY email_id"
        )).fetchall()
        outcomes = conn.execute(text(
            "SELECT email_id, outcome FROM abby_missed_reply_fixes ORDER BY email_id"
        )).fetchall()
        kinds = conn.execute(text(
            "SELECT lead, kind FROM abby_followups_scheduled ORDER BY lead"
        )).fetchall()
    assert [row[1] for row in rows] == ["ignore", "ignore"]
    assert "out of office" in rows[0][2].lower()
    assert "departed" in rows[1][2].lower()
    assert outcomes == [("e1", "skipped"), ("e2", "skipped")]
    assert kinds == [("tom@brand.com", "out_of_office")]


def _no_model(_prompt):
    raise AssertionError("the fixer must not ask the model")


def test_fixer_does_not_reopen_a_still_held_human_question(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    question = email("I will be out Thursday. Can you send the pricing before then?")
    question["is_auto_reply"] = 0
    question["subject"] = "Re: Amazon plan"
    client = FakeClient([question], {"t1": [question]})
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
            "VALUES ('e1', 't1', 'ann@huppy.com', 'auto_reply', 'hold', 'live', '', "
            "'model said auto-reply without auto-reply evidence', 'camp')"
        ))
        conn.execute(text(
            "INSERT INTO abby_missed_reply_fixes (email_id, lead, campaign_id, previous_action, outcome, attempts) "
            "VALUES ('e1', 'ann@huppy.com', 'camp', 'hold', 'still_held', 1)"
        ))
    out = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW,
        think=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("still held")),
    )
    assert out["sent"] == 0 and client.replies == []
    with engine.connect() as conn:
        action = conn.execute(text("SELECT action FROM abby_handled")).scalar()
        outcome = conn.execute(text("SELECT outcome FROM abby_missed_reply_fixes")).scalar()
    assert action == "hold" and outcome == "still_held"


def _held_deck_failure(email_id, lead, thread_id, sent_at, *, detail="deck generation failed"):
    return (
        "INSERT INTO abby_handled (email_id, thread_id, lead, category, action, mode, reply_id, detail, campaign_id) "
        "VALUES (:email, :thread, :lead, 'interested', 'hold', 'live', '', :detail, 'camp')",
        "INSERT INTO abby_missed_reply_fixes "
        "(email_id, lead, campaign_id, previous_action, outcome, attempts, detail, sent_at) "
        "VALUES (:email, :lead, 'camp', 'hold', 'failed', 3, :detail, :sent)",
        {"email": email_id, "thread": thread_id, "lead": lead, "detail": detail, "sent": sent_at},
    )


def test_fixer_retries_a_recent_exhausted_deck_failure_once(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    message = email("yes please")
    message["lead"] = "pat@northtrampoline.com"
    client = FakeClient([message], {"t1": [message]}, leads={
        "pat@northtrampoline.com": {
            "first_name": "Ann",
            "company_name": "North Trampoline",
            "website": "northtrampoline.com",
        },
    })
    handled, fix, params = _held_deck_failure(
        "e1", "pat@northtrampoline.com", "t1",
        (NOW - timedelta(days=2)).replace(tzinfo=None),
    )
    with engine.begin() as conn:
        conn.execute(text(handled), params)
        conn.execute(text(fix), params)
    calls = []
    first = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=abby.think,
        lead_info=lambda _e: {}, build_deck=_deck_builder(calls),
    )
    second = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=abby.think,
        lead_info=lambda _e: {}, build_deck=_deck_builder(calls),
    )
    assert first["sent"] == 1 and second["sent"] == 0
    assert len(client.replies) == 1 and DECK in client.replies[0]["body"]
    assert calls[0]["domain"] == "https://northtrampoline.com"
    with engine.connect() as conn:
        outcome, attempts = conn.execute(text(
            "SELECT outcome, attempts FROM abby_missed_reply_fixes WHERE email_id = 'e1'"
        )).one()
        status = conn.execute(text("SELECT status FROM abby_deck_sends")).scalar()
    assert outcome == "sent" and attempts == 4 and status == "sent"


def test_fixer_leaves_an_old_or_non_deck_exhausted_failure(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    engine = create_engine("sqlite://")
    runner.ensure_tables(engine)
    old = email("yes please")
    old["lead"] = "old@northtrampoline.com"
    other = email("Can you tell me the price?")
    other["id"] = "e2"
    other["thread_id"] = "t2"
    other["lead"] = "ann@huppy.com"
    client = FakeClient([old, other], {"t1": [old], "t2": [other]})
    old_sql = _held_deck_failure(
        "e1", "old@northtrampoline.com", "t1",
        (NOW - timedelta(days=15)).replace(tzinfo=None),
    )
    other_sql = _held_deck_failure(
        "e2", "ann@huppy.com", "t2",
        (NOW - timedelta(days=1)).replace(tzinfo=None),
        detail="send failed (500)",
    )
    with engine.begin() as conn:
        for handled, fix, params in (old_sql, other_sql):
            conn.execute(text(handled), params)
            conn.execute(text(fix), params)

    def think(*_args, **_kwargs):
        raise AssertionError("an exhausted hold outside the deck window must stay closed")

    out = runner.run_missed_replies(
        engine, client=client, campaign_ids=["camp"], now=NOW, think=think,
        lead_info=lambda _e: {},
    )
    assert out["sent"] == 0 and client.replies == []
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT email_id, outcome, attempts FROM abby_missed_reply_fixes ORDER BY email_id"
        )).fetchall()
    assert rows == [("e1", "failed", 3), ("e2", "failed", 3)]


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


def test_live_step_one_ask_moves_above_the_signature():
    before = (
        "Hi {{firstName}},\n\n{{personalization}}\n\n"
        "Worth a quick intro with our founder, David, or would a short strategy deck for {{companyName}} help first?\n\n"
        "Abby\nAccount Specialist, Anata\n"
        "I found something that could help your Amazon growth. Can I send it to you?"
    )
    after = rewrite_body(before, 0)
    assert "intro with our founder" not in after
    assert "Worth a chat" not in after
    assert "{{companyName}}'s Amazon growth" in after
    assert after.index("Can I send it to you?") < after.index("Account Specialist")
    assert after.count("Can I send it to you?") == 1
    again = rewrite_body(after, 0)
    assert again.count("Can I send it to you?") == 1

    listed = (
        "Since then we've added a free 14-day trial.\n\n"
        "Is Amazon still on your list?\n\nAbby\nAccount Specialist, Anata\n"
        "I found something that could help your Amazon growth. Can I send it to you?"
    )
    cleaned = rewrite_body(listed, 0)
    assert "still on your list?" in cleaned
    assert cleaned.rstrip().endswith("Account Specialist, Anata")
    assert "Can I send it to you?" not in cleaned
    html = (
        "<div>Hi {{firstName}},</div><div>{{personalization}}</div>"
        "<div>I found something that could help your Amazon growth. Can I send it to you?</div>"
        "<div>Anna</div>"
        "<div>I found something that could help your Amazon growth. Can I send it to you?</div>"
    )
    fixed = rewrite_body(html, 0)
    assert fixed.count("Can I send it to you?") == 1
    assert fixed.index("Can I send it to you?") < fixed.index("Anna")


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


def test_yes_holds_when_the_deck_content_gate_fails():
    engine = create_engine("sqlite://")
    message = email()
    client = FakeClient([message], {"t1": [message]})
    posts = []

    class Held:
        view_url = DECK
        content_gate_passed = False
        content_gate_findings = ("$0", "Zantrex")

    out = runner.handle_email(
        engine, message, client=client, campaign_id="camp", now=NOW, think=abby.think,
        post_slack=posts.append,
        lead_info=lambda _e: {"first_name": "Ann", "company": "Huppy", "domain": "huppy.com"},
        run_mode="live", build_deck=lambda **_kw: Held(),
    )
    assert client.replies == []
    assert out["held"] == 1 and out["replied"] == 0
    assert posts and "content gate failed" in posts[0]
    with engine.connect() as conn:
        status, detail, url = conn.execute(text(
            "SELECT status, detail, deck_url FROM abby_deck_sends"
        )).one()
    assert status == "held" and url == DECK and "$0" in detail
