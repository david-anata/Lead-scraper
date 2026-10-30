"""Booked-call prep: calendar parse, tracker dedupe, notes, Slack, and draft."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_call_prep_test.db",
)

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import SalesCallTracker  # noqa: E402
from sales_support_agent.services.job_lease import JobLease  # noqa: E402
from sales_support_agent.services.sales.call_script import service  # noqa: E402
from sales_support_agent.services.sales.call_script.bookings import (  # noqa: E402
    booking_from_event,
    booking_from_gmail,
    load_bookings,
)
from sales_support_agent.services.sales.call_script.prep import (  # noqa: E402
    AMAZON_UNVERIFIED,
    SALES_SLACK_CHANNEL,
    PITCH_POINTS,
    as_list,
    gather_context,
    names_match,
    run_prep,
    verify_links,
)
from sales_support_agent.api import sales_call_prep_job as job  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402


NICK_EVENT = {
    "id": "mds227i2dqb61or1imf1cslvlg",
    "status": "confirmed",
    "summary": "Schedule a Meeting with anata (Nicolas Manderfield)",
    "description": (
        "<b>Booked by</b>\nNicolas Manderfield\nnick@reincorporated.org\n"
        "<br><b>Purpose</b>\nLearn more about 14 day trial\n"
        "<br><p>Have questions about our services or want to explore how Anata can support your business?</p>"
    ),
    "organizer": {"email": "abby@anatainc.com"},
    "start": {"dateTime": "2026-10-30T10:30:00-06:00"},
    "end": {"dateTime": "2026-10-30T11:00:00-06:00"},
    "attendees": [
        {"email": "david@anatainc.com"},
        {"email": "abby@anatainc.com", "organizer": True},
        {"email": "nick@reincorporated.org", "displayName": "Nicolas Manderfield"},
    ],
    "conferenceData": {
        "entryPoints": [{"entryPointType": "video", "uri": "https://meet.google.com/duy-vknd-ywj"}]
    },
}

NOW = datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)  # 9:00am MT, 90 minutes before the call


class FakeSlack:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def is_configured(self, channel: str = "") -> bool:
        return True

    def post_message(self, *, text: str, blocks=None, channel: str = "") -> dict:
        self.messages.append({"text": text, "channel": channel})
        return {"ok": True}


class FakeGmail:
    def __init__(self) -> None:
        self.drafts: list[dict] = []
        self.sent: list[dict] = []

    def is_configured(self) -> bool:
        return True

    def create_draft(self, *, to, subject, text, cc=()) -> dict:
        self.drafts.append({"to": to, "subject": subject, "text": text})
        return {"id": "draft-1"}

    def send_message(self, **kwargs):
        self.sent.append(kwargs)
        raise AssertionError("prep must not send email")


def _notes(booking, context):
    from sales_support_agent.services.sales.call_script.prep import fallback_notes

    notes = fallback_notes(booking, context)
    notes["who"] = "Nicolas Manderfield — nick@reincorporated.org. Company: Unknown."
    return notes, "facts"


class BookingParseTests(unittest.TestCase):
    def test_calendar_event_keeps_the_external_prospect_and_form_answer(self) -> None:
        booking = booking_from_event(NICK_EVENT)
        self.assertIsNotNone(booking)
        assert booking is not None
        self.assertEqual(booking.event_id, "mds227i2dqb61or1imf1cslvlg")
        self.assertEqual(booking.prospect_name, "Nicolas Manderfield")
        self.assertEqual(booking.prospect_email, "nick@reincorporated.org")
        self.assertEqual(booking.purpose, "Learn more about 14 day trial")
        self.assertEqual(booking.meet_url, "https://meet.google.com/duy-vknd-ywj")
        self.assertEqual(booking.starts_at.isoformat(), "2026-10-30T10:30:00-06:00")

    def test_internal_meeting_is_not_a_booking(self) -> None:
        event = dict(NICK_EVENT, description="Internal pipeline review", id="internal")
        self.assertIsNone(booking_from_event(event))

    def test_other_organizer_is_ignored(self) -> None:
        event = dict(NICK_EVENT, organizer={"email": "someone@example.com"})
        self.assertIsNone(booking_from_event(event))

    def test_appointment_booked_mail_parses(self) -> None:
        import base64

        body = (
            "Booked by\nNicolas Manderfield\nnick@reincorporated.org\n\n"
            "Purpose\nLearn more about 14 day trial\n\n"
            "Have questions about our services or want to explore how Anata can support your business?\n"
        )
        encoded = base64.urlsafe_b64encode(body.encode()).decode()
        message = {
            "id": "1a1898c06afba587",
            "payload": {
                "mimeType": "text/plain",
                "headers": [{
                    "name": "Subject",
                    "value": (
                        "Appointment booked: Schedule a Meeting with anata (Nicolas Manderfield) "
                        "@ Fri Oct 30, 2026 10:30am - 11am (MDT) (David Narayan)"
                    ),
                }],
                "body": {"data": encoded},
            },
        }
        booking = booking_from_gmail(message)
        self.assertIsNotNone(booking)
        assert booking is not None
        self.assertEqual(booking.event_id, "gmail:1a1898c06afba587")
        self.assertEqual(booking.prospect_email, "nick@reincorporated.org")
        self.assertEqual(booking.purpose, "Learn more about 14 day trial")
        self.assertEqual(booking.starts_at.hour, 10)
        self.assertEqual(booking.starts_at.minute, 30)
        self.assertEqual(str(booking.starts_at.tzinfo), "America/Denver")

    def test_missing_calendar_access_reports_the_gap(self) -> None:
        cleared = {
            "GOOGLE_SERVICE_ACCOUNT_JSON": "",
            "BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON": "",
            "HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON": "",
        }
        with patch.dict(os.environ, cleared):
            loaded = load_bookings(None, NOW, gmail_clients=[])
        self.assertEqual(loaded.source, "none")
        self.assertEqual(loaded.bookings, [])
        self.assertIn("Google Calendar", loaded.gap)
        self.assertIn("abby@anatainc.com", loaded.gap)
        self.assertIn("Meeting Set", loaded.gap)
        self.assertIn("Zapier", loaded.gap)

    def test_gmail_is_the_fallback_when_calendar_credentials_are_absent(self) -> None:
        class ListingGmail(FakeGmail):
            def list_messages(self, *, query, max_results):
                self.query = query
                return [{"id": "m1"}]

            def get_message(self, message_id):
                return {
                    "id": message_id,
                    "payload": {
                        "mimeType": "text/plain",
                        "headers": [{
                            "name": "Subject",
                            "value": (
                                "Appointment booked: Schedule a Meeting with anata (Nicolas Manderfield) "
                                "@ Fri Oct 30, 2026 10:30am - 11am (MDT) (David Narayan)"
                            ),
                        }],
                        "body": {"data": ""},
                        "snippet": "Booked by Nicolas Manderfield nick@reincorporated.org Purpose Learn more about 14 day trial",
                    },
                }

        cleared = {
            "GOOGLE_SERVICE_ACCOUNT_JSON": "",
            "BUILDING_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON": "",
            "HR_OOO_GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON": "",
        }
        with patch.dict(os.environ, cleared):
            loaded = load_bookings(None, NOW, gmail_clients=[ListingGmail()])
        self.assertEqual(loaded.source, "gmail")
        self.assertEqual(len(loaded.bookings), 1)
        self.assertIn("Google Calendar", loaded.gap)


class PrepJobTests(unittest.TestCase):
    def setUp(self) -> None:
        with session_scope(app.state.session_factory) as session:
            for row in session.query(SalesCallTracker).all():
                session.delete(row)

    def _booking(self):
        booking = booking_from_event(NICK_EVENT)
        assert booking is not None
        return booking

    def _run(self, slack, gmail, booking=None):
        booking = booking or self._booking()

        def context_for(item, tracker=None):
            return {
                "company": "",
                "booking": {"purpose": item.purpose},
                "outbound_contact": {"available": True, "row": None},
                "outbound_enriched": {"available": True, "row": None},
                "outbound_domain": {"available": True, "row": None},
                "abby_handled": {"available": True, "rows": []},
                "clickup": {"available": True, "matched": False},
                "website": {"domain": "reincorporated.org", "text": ""},
            }

        with session_scope(app.state.session_factory) as session:
            return run_prep(
                session,
                now=NOW,
                bookings=[booking],
                source="google_calendar",
                gap="",
                context_for=context_for,
                notes_for=_notes,
                slack=slack,
                gmail=gmail,
            )

    def test_nick_and_nicolas_are_the_same_person(self) -> None:
        self.assertTrue(names_match("Nick Manderfield", "Nicolas Manderfield"))
        self.assertFalse(names_match("Nick Smith", "Nicolas Manderfield"))

    def test_existing_tracker_is_reused_and_notified_once(self) -> None:
        with session_scope(app.state.session_factory) as session:
            service.create_tracker(
                session,
                {"email": "david@anatainc.com", "is_superadmin": True},
                prospect_name="Nick Manderfield",
                company="",
                lead_link="",
            )
        slack, gmail = FakeSlack(), FakeGmail()
        first = self._run(slack, gmail)
        self.assertEqual(first["created"], 0)
        self.assertEqual(first["prepped"], 1)
        self.assertEqual(first["slack"], 1)
        self.assertEqual(first["drafts"], 1)
        self.assertEqual(slack.messages[0]["channel"], SALES_SLACK_CHANNEL)
        self.assertIn("/admin/sales/call/", slack.messages[0]["text"])
        self.assertNotIn("—", slack.messages[0]["text"])
        self.assertIn("Free 14-day trial.", slack.messages[0]["text"])
        self.assertIn("Plans from $99/mo.", slack.messages[0]["text"])
        self.assertIn("2x to 3.5x", slack.messages[0]["text"])
        self.assertEqual(gmail.drafts[0]["to"], ("david@anatainc.com",))
        self.assertTrue(gmail.drafts[0]["subject"].startswith("Call prep:"))
        self.assertEqual(gmail.sent, [])

        second = self._run(slack, gmail)
        self.assertEqual(second["reused"], 1)
        self.assertEqual(second["created"], 0)
        self.assertEqual(len(slack.messages), 1)
        self.assertEqual(len(gmail.drafts), 1)
        with session_scope(app.state.session_factory) as session:
            rows = list(session.query(SalesCallTracker).all())
            self.assertEqual(len(rows), 1)
            prep = rows[0].state_json["ss-prep"]
            self.assertEqual(prep["event_id"], "mds227i2dqb61or1imf1cslvlg")
            self.assertEqual([step["id"] for step in prep["sections"]["discovery"]], [f"s{i}" for i in range(1, 14)])
            self.assertIn("Unknown", prep["sections"]["how_they_came_in"])
            self.assertIn("Amazon presence: not verified", prep["links"]["amazon"]["line"])

    def test_page_save_keeps_prep_and_the_slack_flag(self) -> None:
        slack, gmail = FakeSlack(), FakeGmail()
        self._run(slack, gmail)
        with session_scope(app.state.session_factory) as session:
            tracker = session.query(SalesCallTracker).one()
            service.save_state(session, tracker, {"ss-beliefs": {"Pain": True}, "ss-tab": "flow"})
            session.refresh(tracker)
            self.assertEqual(tracker.state_json["ss-beliefs"], {"Pain": True})
            self.assertNotIn("ss-tab", tracker.state_json)
            self.assertTrue(tracker.state_json["ss-prep"]["slack"]["posted_at"])
            self.assertIn("ss-prep", service.PER_CALL_KEYS)

    def test_call_inside_the_hour_still_posts(self) -> None:
        booking = self._booking()
        soon = booking.starts_at - timedelta(minutes=20)
        slack, gmail = FakeSlack(), FakeGmail()

        def context_for(item, tracker=None):
            return {"company": "", "abby_handled": {"rows": []}, "clickup": {}, "outbound_contact": {"row": None},
                    "outbound_enriched": {"row": None}}

        with session_scope(app.state.session_factory) as session:
            result = run_prep(
                session, now=soon, bookings=[booking], source="google_calendar", gap="",
                context_for=context_for, notes_for=_notes, slack=slack, gmail=gmail,
            )
        self.assertEqual(result["slack"], 1)
        self.assertIn("under an hour", slack.messages[0]["text"])

    def test_started_call_stores_prep_without_slack_or_a_draft(self) -> None:
        booking = self._booking()
        slack, gmail = FakeSlack(), FakeGmail()
        with session_scope(app.state.session_factory) as session:
            result = run_prep(
                session, now=booking.starts_at + timedelta(minutes=5), bookings=[booking],
                source="google_calendar", gap="",
                context_for=lambda item, tracker=None: {"company": "", "abby_handled": {"rows": []}, "clickup": {}},
                notes_for=_notes, slack=slack, gmail=gmail,
            )
        self.assertEqual(result["errors"], [])
        self.assertEqual(result["created"] + result["prepped"], 1)
        self.assertEqual(result["slack"], 0)
        self.assertEqual(result["drafts"], 0)
        self.assertEqual(slack.messages, [])
        self.assertEqual(gmail.drafts, [])
        self.assertEqual(gmail.sent, [])

    def test_context_marks_missing_sources_and_reads_a_clickup_task(self) -> None:
        booking = self._booking()
        clickup = SimpleNamespace(get_tasks=lambda *args, **kwargs: [{
            "id": "abc",
            "name": "Reincorporated",
            "url": "https://app.clickup.com/t/abc",
            "status": {"status": "working"},
            "custom_fields": [
                {"id": "12e960ef-df48-4ce9-b0bd-c77f453590d4", "value": "nick@reincorporated.org"},
                {"id": "0bb668f4-a49a-4831-947f-b1f5d80a437c", "value": "Reincorporated"},
            ],
        }])
        with session_scope(app.state.session_factory) as session:
            context = gather_context(
                session, clickup, booking,
                fetch=lambda url: {"ok": False, "url": "", "status": 0, "html": "", "text": ""},
            )
        self.assertEqual(context["company"], "Reincorporated")
        self.assertTrue(context["clickup"]["matched"])
        self.assertEqual(context["website"]["domain"], "reincorporated.org")
        self.assertEqual(context["abby_handled"]["rows"], [])

    def test_model_notes_drop_em_dashes_and_keep_the_pitch(self) -> None:
        from sales_support_agent.services.sales.call_script.prep import generate_notes

        booking = self._booking()
        payload = {
            "who": "Nicolas — the founder",
            "how_they_came_in": "Unknown",
            "likely_needs": "Learn more about 14 day trial",
            "discovery_questions": ["One?", "Two?", "Three?", "Four?", "Five?"],
            "trial_pitch": ["Something else"],
            "objections": [{"objection": "Price — too high", "answer": "Plans from $99/mo."}],
            "next_step": "Ask for the trial.",
            "unknowns": ["Company"],
        }
        message = SimpleNamespace(content=[SimpleNamespace(type="text", text=json.dumps(payload))])
        seen = {}

        class FakeClient:
            def __init__(self, caller, api_key=None):
                seen["caller"] = caller
                self.messages = self

            def create(self, **kwargs):
                seen["system"] = kwargs["system"]
                return message

        with patch(
            "sales_support_agent.services.llm_client.llm_available", return_value=True
        ), patch(
            "sales_support_agent.services.llm_client.llm_client", FakeClient
        ):
            notes, source = generate_notes(booking, {"company": ""})
        self.assertEqual(source, "llm")
        self.assertEqual(seen["caller"], "call_prep")
        self.assertIn("Do not guess", seen["system"])
        self.assertNotIn("—", json.dumps(notes))
        self.assertTrue(any("2x to 3.5x" in point for point in notes["trial_pitch"]))
        for point in PITCH_POINTS:
            self.assertIn(point, notes["trial_pitch"])
        self.assertEqual([step["id"] for step in notes["discovery"]], [f"s{i}" for i in range(1, 14)])
        self.assertNotIn("discovery_questions", notes)


class PrepShapeTests(unittest.TestCase):
    def setUp(self) -> None:
        with session_scope(app.state.session_factory) as session:
            for row in session.query(SalesCallTracker).all():
                session.delete(row)

    def test_a_paragraph_is_not_split_into_characters(self) -> None:
        paragraph = (
            "Walk the free 14-day trial, then plans from $99/mo, "
            "then the Zantrex proof."
        )
        items = as_list(paragraph)
        self.assertEqual(items, [paragraph])
        self.assertNotIn("W", items)
        self.assertGreater(min(len(item) for item in items), 1)

    def test_a_character_list_is_joined_before_it_is_shown(self) -> None:
        items = as_list(list("Walk the trial. Then the price."))
        self.assertTrue(all(len(item) > 1 for item in items))
        self.assertIn("Walk the trial.", items)

    def test_sentences_in_one_string_become_separate_pitch_points(self) -> None:
        from sales_support_agent.services.sales.call_script.bookings import booking_from_event
        from sales_support_agent.services.sales.call_script.prep import _ensure_shape

        booking = booking_from_event(NICK_EVENT)
        notes = _ensure_shape(
            {"trial_pitch": "Free 14-day trial. Plans from $99/mo. Zantrex proof: ROAS went from 2x to 3.5x."},
            booking,
            {},
        )
        self.assertGreater(min(len(point) for point in notes["trial_pitch"]), 1)
        self.assertNotIn("W", notes["trial_pitch"])
        self.assertTrue(any("2x to 3.5x" in point for point in notes["trial_pitch"]))

    def test_amazon_url_is_included_only_after_it_resolves(self) -> None:
        html = '<a href="https://www.amazon.com/stores/Reincorporated/page/ABC">shop</a> cdn.shopify.com'

        def fetch(url):
            if "amazon.com" in url:
                return {"ok": True, "url": url, "status": 200, "html": "store", "text": "store"}
            return {"ok": True, "url": "https://reincorporated.org/", "status": 200, "html": html, "text": "beads"}

        links = verify_links("reincorporated.org", fetch=fetch)
        self.assertEqual(links["website"]["url"], "https://reincorporated.org/")
        self.assertTrue(links["amazon"]["verified"])
        self.assertIn("amazon.com/stores/Reincorporated", links["amazon"]["urls"][0])
        self.assertTrue(links["shopify"])

    def test_an_unresolved_amazon_url_is_not_claimed(self) -> None:
        html = 'See https://www.amazon.com/dp/B00EXAMPLE on our site'

        def fetch(url):
            if "amazon.com" in url:
                return {"ok": False, "url": url, "status": 404, "html": "", "text": ""}
            return {"ok": True, "url": "https://reincorporated.org/", "status": 200, "html": html, "text": "home"}

        links = verify_links("reincorporated.org", fetch=fetch)
        self.assertFalse(links["amazon"]["verified"])
        self.assertEqual(links["amazon"]["urls"], [])
        self.assertEqual(links["amazon"]["line"], AMAZON_UNVERIFIED)

    def test_discovery_follows_the_script_and_quotes_prefill_the_tracker(self) -> None:
        booking = booking_from_event(NICK_EVENT)
        assert booking is not None
        asked = {"notes": 0}

        def context_for(item, tracker=None):
            return {
                "company": "Reincorporated NFP",
                "links": {
                    "website": {"verified": True, "url": "https://reincorporated.org/"},
                    "amazon": {"verified": False, "urls": [], "line": AMAZON_UNVERIFIED},
                    "shopify": True,
                    "text": "bead project",
                },
                "abby_handled": {"rows": [{"category": "interested", "action": "reply", "detail": "positive reply", "created_at": "2026-09-01"}]},
                "clickup": {
                    "matched": True,
                    "url": "https://app.clickup.com/t/868kzf9rd",
                    "comments": [{"by": "Gabe Smedley", "text": "Nick said: I am interested but I need two weeks."}],
                },
                "gmail": [{"from": "David Narayan <david@anatainc.com>", "date": "Oct 28", "subject": "Checking back in", "snippet": "David wrote, not Nick."}],
                "instantly": {"available": True, "emails": []},
                "mailbox": [],
            }

        def notes_for(item, context):
            asked["notes"] += 1
            from sales_support_agent.services.sales.call_script.prep import fallback_notes
            return fallback_notes(item, context), "facts"

        slack, gmail = FakeSlack(), FakeGmail()
        with session_scope(app.state.session_factory) as session:
            for row in session.query(SalesCallTracker).all():
                session.delete(row)
        with session_scope(app.state.session_factory) as session:
            result = run_prep(
                session, now=NOW, bookings=[booking], source="google_calendar", gap="",
                context_for=context_for, notes_for=notes_for, slack=slack, gmail=gmail,
            )
            tracker = session.get(SalesCallTracker, result["bookings"][0]["tracker_id"])
            prep = tracker.state_json["ss-prep"]
            self.assertEqual(result["errors"], [])
            self.assertEqual(asked["notes"], 1)
            self.assertIn("Reincorporated", prep["sections"]["who"])
            self.assertEqual([step["id"] for step in prep["sections"]["discovery"]], [f"s{i}" for i in range(1, 14)])
            self.assertIn("https://reincorporated.org/", slack.messages[0]["text"])
            self.assertIn(AMAZON_UNVERIFIED, slack.messages[0]["text"])
            self.assertNotIn("https://www.amazon.com", slack.messages[0]["text"])
            self.assertIn("I am interested but I need two weeks.", tracker.state_json["ss-gap"]["quotes"])
            self.assertIn("positive reply", prep["sections"]["how_they_came_in"])
        with session_scope(app.state.session_factory) as session:
            again = run_prep(
                session, now=NOW, bookings=[booking], source="google_calendar", gap="",
                context_for=context_for, notes_for=notes_for, slack=slack, gmail=gmail,
            )
        self.assertEqual(again["reused"], 1)
        self.assertEqual(asked["notes"], 1)
        self.assertEqual(len(gmail.drafts), 1)


class PrepRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = app.state.agent_settings.admin_cookie_name, create_user_session_token(
            app.state.agent_settings, email="david@anatainc.com", name="David", role="member"
        )
        cls.client.cookies.set(cookie_name, token)

    def test_cron_rejects_a_missing_secret(self) -> None:
        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}):
            response = self.client.get("/api/jobs/sales-call-prep/run")
        self.assertEqual(response.status_code, 401)

    def test_cron_runs_when_the_bearer_matches(self) -> None:
        from sales_support_agent.services.sales.call_script.bookings import SourceLoad

        lease = JobLease("sales_call_prep", "slot", "token")
        with patch.dict(os.environ, {"CRON_SECRET": "cron-test-key"}), patch.object(
            job, "load_bookings", return_value=SourceLoad([], "none", "no calendar access")
        ), patch.object(job, "claim_scheduled_job", return_value=lease), patch.object(
            job, "finish_scheduled_job"
        ):
            response = self.client.get(
                "/api/jobs/sales-call-prep/run",
                headers={"Authorization": "Bearer cron-test-key"},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "gap")
        self.assertEqual(response.json()["source"], "none")

    def test_call_page_saves_ss_prep_and_ships_the_panel(self) -> None:
        with session_scope(app.state.session_factory) as session:
            tracker = service.create_tracker(
                session,
                {"email": "david@anatainc.com", "is_superadmin": True},
                prospect_name="Nick Manderfield",
            )
            service.save_state(session, tracker, {
                "ss-prep": {
                    "generated_at": "2026-10-30T15:00:00+00:00",
                    "when": "Fri Oct 30, 10:30 AM MT",
                    "purpose": "Learn more about 14 day trial",
                    "unknowns": ["Company"],
                    "sections": {
                        "who": "Nick Manderfield. Company: Unknown.",
                        "how_they_came_in": "Unknown.",
                        "likely_needs": "Learn more about 14 day trial",
                        "discovery_questions": ["One?", "Two?", "Three?", "Four?", "Five?"],
                        "trial_pitch": list(PITCH_POINTS),
                        "objections": [{"objection": "Prepared", "answer": "Trial is free."}],
                        "next_step": "Ask them to start the trial.",
                    },
                }
            })
            tracker_id = tracker.id
        page = self.client.get(f"/admin/sales/call/{tracker_id}").text
        adapter = page.split("window.__ANATA_CALL__", 1)[1]
        self.assertIn("ss-prep", adapter)
        self.assertIn("call-prep", adapter)
        self.assertIn("Learn more about 14 day trial", adapter)
        self.assertIn("textContent = 'Prep'", adapter)
        script = (Path(service.ASSETS_DIR) / "runtime_adapter.js").read_text(encoding="utf-8")
        self.assertIn("'ss-prep'", script)
