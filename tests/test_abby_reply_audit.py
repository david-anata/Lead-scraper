"""Regressions from the Nov 20 reply audit.

Jack at Arena, Danielle at CMY Cubes, Dante at Edika, and the dead inboxes
at Mata Traders, earthbath, and Dandylion. Addresses are the ones in the audit.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

import abby
import abby_runner as runner

NOW = datetime(2026, 11, 20, 19, 25, tzinfo=timezone.utc)
LEFT = 51
WENT_WELL = "Hope your time away went well."
STILL_OUT = "Hope all is well."


def _engine(monkeypatch):
    monkeypatch.setenv("ABBY_MODE", "live")
    monkeypatch.setenv("ABBY_TAKEOVER_AT", "2026-10-26T00:00:00Z")
    monkeypatch.setenv("ABBY_START_AT", "2026-10-26T00:00:00Z")
    return create_engine("sqlite://")


def _cold(body: str) -> None:
    assert "\u2014" not in body and "\u2013" not in body
    assert " - " not in body
    assert "http://" not in body and "https://" not in body
    assert body.rstrip().endswith("Account Specialist, Anata")


class _Client:
    def __init__(self, received, threads=None, leads=None):
        self.received = received
        self.threads = threads or {}
        self.lead_rows = leads or {}
        self.replies, self.forwards, self.interest, self.removed = [], [], [], []
        self._marked = []

    def received_since(self, _since, _campaign_id):
        return list(self.received)

    def thread(self, thread_id):
        return self.threads.get(thread_id, [])

    def reply(self, **kwargs):
        self.replies.append(kwargs)
        return 200, {"id": f"r{len(self.replies)}"}

    def forward(self, **kwargs):
        self.forwards.append(kwargs)
        return 200, {"id": f"f{len(self.forwards)}"}

    def set_interest(self, lead, value, campaign_id):
        self.interest.append((lead, value, campaign_id))
        self._marked.append({
            "email": lead, "campaign": campaign_id, "lt_interest_status": value,
        })
        return 200

    def leads_for_email(self, email):
        marked = [row for row in self._marked if row.get("email") == email]
        base = self.lead_rows.get(email, [])
        if not marked:
            return list(base)
        merged = []
        for row in marked:
            prior = next((item for item in base if item.get("email") == email), {})
            merged.append({**prior, **row})
        return merged

    def list_lead_labels(self):
        return [{"label": "Left company", "interest_status": LEFT}]

    def remove_from_subsequence(self, lead_id):
        self.removed.append(lead_id)
        return 200, {"id": lead_id}

    def block(self, _value):
        return 200

    def lead(self, _email):
        return {}

    def interested_latest(self, _campaign_id):
        return []


def _mail(email_id, lead, body, *, thread="t1", subject="Re: Amazon", when="2026-11-10T15:43:00Z"):
    return {
        "id": email_id, "thread_id": thread, "lead": lead, "eaccount": "abby@anatainc.com",
        "subject": subject, "ue_type": 2, "is_auto_reply": 0,
        "from_address_email": lead, "body": {"text": body},
        "timestamp_created": when, "campaign_id": "camp",
        "headers": {"Auto-Submitted": "no"},
    }


def test_jack_left_company_stops_sequences_and_writes_only_olivia(monkeypatch):
    engine = _engine(monkeypatch)
    body = (
        "Thank you for your email, I have now left Arena.Online\n"
        "Please message a member of the consumer team using the emails below\n"
        "olivia.smith@hellopatch.com (Head of consumer trading)\n"
        "Thanks, Jack\n"
    )
    first = _mail(
        "jack-1", "jack@arena.online", body,
        subject="OOO - Thank you for your email, I have left the business",
        when="2026-11-10T15:43:00Z",
    )
    repeat = _mail(
        "jack-2", "jack@arena.online", body, thread="t1",
        subject="Automatic reply: I have left the business",
        when="2026-11-15T15:55:00Z",
    )
    lead_row = {
        "id": "lead-jack", "email": "jack@arena.online", "campaign": "camp",
        "subsequence_id": "0c2f113f-ebed-4890-92df-c241d925dcdf",
        "lt_interest_status": 1,
    }
    client = _Client([first], {"t1": [first]}, {"jack@arena.online": [lead_row]})
    info = lambda _e: {"first_name": "Jack", "company": "Arena"}
    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=abby.think, lead_info=info)
    client.received = [first, repeat]
    client.threads = {"t1": [first, repeat]}
    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=abby.think, lead_info=info)
    assert client.replies == []
    assert len(client.forwards) == 1
    sent = client.forwards[0]
    assert sent["to"] == ["olivia.smith@hellopatch.com"]
    assert "jack@arena.online" not in ",".join(sent["to"])
    assert sent.get("cc", []) == []
    assert "Hi Olivia, Jack mentioned you at Arena." in sent["body"]
    _cold(sent["body"])
    assert "lead-jack" in client.removed
    assert ("jack@arena.online", LEFT, "camp") in client.interest
    assert all(value != 1 for _lead, value, _camp in client.interest)
    with engine.connect() as conn:
        actions = [row[0] for row in conn.execute(text(
            "SELECT action FROM abby_handled ORDER BY email_id"
        )).fetchall()]
    assert actions == ["redirect", "skip"]


def test_a_repeat_ooo_after_the_followup_waits_again(monkeypatch):
    engine = _engine(monkeypatch)
    first_body = "I am out of the office until November 19 with no access to email."
    first = _mail(
        "ooo-1", "danielle@cmycubes.com", first_body,
        subject="Automatic reply: Out of office", when="2026-11-05T16:00:00Z",
    )
    opened = datetime(2026, 11, 5, 16, 0, tzinfo=timezone.utc)
    client = _Client([first], {"t1": [first]})
    runner.run(
        engine, client=client, campaign_id="camp", now=opened, think=abby.think,
        lead_info=lambda _e: {"first_name": "Danielle", "company": "CMY Cubes"},
    )
    assert client.replies == [] and client.forwards == []
    with engine.connect() as conn:
        kind, body, due, status = conn.execute(text(
            "SELECT kind, body, due_at, status FROM abby_followups_scheduled"
        )).one()
    assert kind == "out_of_office" and status == "pending"
    assert "2026-11-20" in str(due)
    assert WENT_WELL in body
    _cold(body)
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE abby_followups_scheduled SET status = 'sent' WHERE kind = 'out_of_office'"
        ))
    repeat = _mail(
        "ooo-2", "danielle@cmycubes.com",
        "I am out of the office until November 19. I will reply when I return.",
        when="2026-11-20T19:26:00Z",
    )
    client.received = [repeat]
    client.threads = {"t1": [first, repeat]}
    runner.run(
        engine, client=client, campaign_id="camp", now=NOW, think=abby.think,
        lead_info=lambda _e: {"first_name": "Danielle", "company": "CMY Cubes"},
    )
    assert client.replies == []
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT status, body, due_at FROM abby_followups_scheduled ORDER BY due_at"
        )).fetchall()
    assert rows[0][0] == "sent"
    assert rows[1][0] == "pending"
    assert WENT_WELL not in rows[1][1]
    assert STILL_OUT in rows[1][1]
    assert "2026-11-27" in str(rows[1][2])
    _cold(rows[1][1])


def test_no_return_date_does_not_say_the_trip_went_well(monkeypatch):
    engine = _engine(monkeypatch)
    message = _mail(
        "ooo-3", "danielle@cmycubes.com",
        "I am out of the office and will reply when I can.",
        when="2026-11-05T16:00:00Z",
    )
    client = _Client([message], {"t1": [message]})
    runner.run(
        engine, client=client, campaign_id="camp",
        now=datetime(2026, 11, 5, 16, 0, tzinfo=timezone.utc), think=abby.think,
    )
    with engine.connect() as conn:
        body, due = conn.execute(text(
            "SELECT body, due_at FROM abby_followups_scheduled"
        )).one()
    assert WENT_WELL not in body and STILL_OUT in body
    assert "2026-11-12" in str(due)


def test_send_waits_when_the_return_date_has_not_passed(monkeypatch):
    engine = _engine(monkeypatch)
    runner.ensure_tables(engine)
    early = NOW.replace(tzinfo=None)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_followups_scheduled "
            "(lead, reply_to_id, eaccount, subject, body, kind, due_at, status, detail) "
            "VALUES ('danielle@cmycubes.com', 'ooo-1', 'abby@anatainc.com', 'Re: Hi', :body, "
            "'out_of_office', :due, 'pending', 'out_of_office return=2026-11-25')"
        ), {"body": f"Hi Danielle, {WENT_WELL}\n\nAbby\nAccount Specialist, Anata", "due": early})
    client = _Client([])
    sent = runner.send_due_scheduled(engine, client, NOW, "live", lambda _t: None, 5)
    assert sent == 0 and client.replies == []
    with engine.connect() as conn:
        status, due, body = conn.execute(text(
            "SELECT status, due_at, body FROM abby_followups_scheduled"
        )).one()
    assert status == "pending"
    assert "2026-11-26" in str(due)
    assert WENT_WELL in body


def test_a_due_note_without_a_return_date_drops_the_returned_line(monkeypatch):
    engine = _engine(monkeypatch)
    runner.ensure_tables(engine)
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO abby_followups_scheduled "
            "(lead, reply_to_id, eaccount, subject, body, kind, due_at, status, detail) "
            "VALUES ('danielle@cmycubes.com', 'ooo-1', 'abby@anatainc.com', 'Re: Hi', :body, "
            "'out_of_office', :due, 'pending', 'out_of_office return=')"
        ), {
            "body": f"Hi Danielle, {WENT_WELL} Circling back.\n\nAbby\nAccount Specialist, Anata",
            "due": (NOW - timedelta(minutes=5)).replace(tzinfo=None),
        })
    client = _Client([])
    sent = runner.send_due_scheduled(engine, client, NOW, "live", lambda _t: None, 5)
    assert sent == 1
    assert WENT_WELL not in client.replies[0]["body"]
    assert STILL_OUT in client.replies[0]["body"]


def test_traveling_until_a_weekday_is_an_out_of_office(monkeypatch):
    engine = _engine(monkeypatch)
    body = "Thank you for your email. I am traveling until Mon Nov 23 and will reply when I return."
    message = _mail(
        "edika-1", "dante@edika.com", body,
        subject="Re: Amazon growth", when="2026-11-16T16:06:00Z",
    )
    assert abby.automated_notice(message, lead_email="dante@edika.com").kind == "out_of_office"
    asked = abby.automated_notice(
        "I am traveling until Mon Nov 23. Can you send the pricing?",
        lead_email="dante@edika.com",
    )
    assert asked is None
    client = _Client([message], {"t1": [message]})
    when = datetime(2026, 11, 16, 16, 6, tzinfo=timezone.utc)
    runner.run(
        engine, client=client, campaign_id="camp", now=when, think=abby.think,
        lead_info=lambda _e: {"first_name": "Dante", "company": "Edika"},
    )
    assert client.replies == [] and client.forwards == []
    with engine.connect() as conn:
        kind, body, due = conn.execute(text(
            "SELECT kind, body, due_at FROM abby_followups_scheduled"
        )).one()
    assert kind == "out_of_office"
    assert "2026-11-24" in str(due)
    assert "Hi Dante," in body
    _cold(body)


def test_dead_inboxes_are_asked_once_and_then_stopped(monkeypatch):
    engine = _engine(monkeypatch)
    cases = [
        (
            "mata-1", "maureen@matatraders.com", "Mata Traders",
            "This email is no longer active. Please contact sales@matatraders.com, "
            "wholesale@matatraders.com, or support@matatraders.com.",
            ["sales@matatraders.com", "wholesale@matatraders.com", "support@matatraders.com"],
        ),
        (
            "earth-1", "arianna@earthbath.com", "earthbath",
            "This email is no longer active. Please contact account@earthbath.com.",
            ["account@earthbath.com"],
        ),
        (
            "dandy-1", "vebs@dandylionworld.com", "Dandylion",
            "This inbox is no longer monitored. Please use hello@dandylionworld.com.",
            ["hello@dandylionworld.com"],
        ),
    ]
    messages, threads, info = [], {}, {}
    for email_id, lead, brand, body, _targets in cases:
        message = _mail(email_id, lead, body, thread=email_id, when="2026-11-09T16:00:00Z")
        messages.append(message)
        threads[email_id] = [message]
        info[lead] = {"first_name": "", "company": brand}
    again = _mail(
        "earth-2", "arianna@earthbath.com",
        "This email is no longer active. Please contact account@earthbath.com.",
        thread="earth-2", when="2026-11-12T16:00:00Z",
    )
    messages.append(again)
    threads["earth-2"] = [again]
    client = _Client(messages, threads)

    def lead_info(email):
        return info.get(email, {})

    runner.run(engine, client=client, campaign_id="camp", now=NOW, think=abby.think, lead_info=lead_info)
    assert client.replies == []
    assert len(client.forwards) == 3
    by_source = {item["reply_to"]: item for item in client.forwards}
    for email_id, lead, brand, _body, targets in cases:
        sent = by_source[email_id]
        assert sent["to"] == targets
        assert lead not in sent["to"]
        assert f"who handles ecommerce or growth for {brand}?" in sent["body"]
        _cold(sent["body"])
    assert {item[0] for item in client.interest} == {lead for _i, lead, _b, _body, _t in cases}
    assert {item[1] for item in client.interest} == {LEFT}
    with engine.connect() as conn:
        second = conn.execute(text(
            "SELECT action, detail FROM abby_handled WHERE email_id = 'earth-2'"
        )).one()
    assert second[0] == "skip" and "suppressed" in second[1].lower()


def test_nudges_do_not_go_back_to_a_departed_address(monkeypatch):
    engine = _engine(monkeypatch)
    import outbound_suppression as suppression

    suppression.add(engine, kind="email", value="jack@arena.online", reason="departed", source="abby")
    latest = {
        "id": "sent-1", "thread_id": "t9", "lead": "jack@arena.online", "ue_type": 3,
        "eaccount": "abby@anatainc.com",
        "timestamp_created": (NOW - timedelta(days=4)).isoformat().replace("+00:00", "Z"),
    }
    prospect = _mail("jack-ooo", "jack@arena.online", "I have left.", thread="t9", when="2026-11-10T15:43:00Z")
    client = _Client([], {"t9": [prospect, latest]})
    client.interested_latest = lambda _campaign: [latest]
    out = runner.run(engine, client=client, campaign_id="camp", now=NOW)
    assert out["nudges"] == 0 and client.replies == []
