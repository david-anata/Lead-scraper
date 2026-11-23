"""Public front-desk review page, storage, alerts, and admin list."""

from __future__ import annotations

import dataclasses
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/building_visitor_reviews_boot.db",
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "building-visitor-reviews-test-secret",
)

try:
    from fastapi.testclient import TestClient
    from sqlalchemy import delete, select

    from sales_support_agent.main import app
    from sales_support_agent.models.database import (
        create_session_factory,
        init_database,
        session_scope,
    )
    from sales_support_agent.models.entities import (
        AutomationRun,
        BuildingAuditEvent,
        BuildingVisitorReview,
    )
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.services.building_sender import building_from_address
    from sales_support_agent.services.building_visitor_reviews import (
        GOOGLE_REVIEW_URL,
    )

    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


MIGRATION = Path("supabase/migrations/20261123193000_building_visitor_reviews.sql")
FORBIDDEN_PUBLIC_WORDS = ("soda", "pricing", "price")


def _review_id(html: str) -> str:
    match = re.search(r'name="review_id" value="([^"]+)"', html)
    if match is None:
        raise AssertionError(html)
    return match.group(1)


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class BuildingVisitorReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = os.path.join(tempfile.gettempdir(), "building_visitor_reviews_isolated.db")
        if os.path.exists(path):
            os.remove(path)
        factory = create_session_factory("sqlite:///" + path)
        init_database(factory)
        app.state.session_factory = factory
        app.state.settings = dataclasses.replace(
            app.state.settings,
            resend_api_key="",
            resend_from="",
        )
        cls.factory = factory
        cls.client = TestClient(app)
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings,
            email="david@anatainc.com",
            name="David",
            role="admin",
        )
        cls.client.cookies.set(settings.admin_cookie_name, token)

    def setUp(self) -> None:
        with session_scope(self.factory) as session:
            session.execute(delete(BuildingVisitorReview))
            session.execute(delete(BuildingAuditEvent))
            session.execute(delete(AutomationRun))
        self.slack = patch(
            "sales_support_agent.services.building_visitor_reviews.SlackClient.post_message",
            return_value={"ok": True, "ts": "123.456"},
        ).start()
        self.email = patch(
            "sales_support_agent.services.building_visitor_reviews.ResendClient.send_message",
            return_value="email-1",
        ).start()
        self.addCleanup(patch.stopall)

    def _rows(self) -> list[dict]:
        with session_scope(self.factory) as session:
            found = session.execute(
                select(BuildingVisitorReview).order_by(BuildingVisitorReview.created_at)
            ).scalars().all()
            return [
                {
                    "id": row.id,
                    "stars": row.stars,
                    "comment": row.comment,
                    "visitor_name": row.visitor_name,
                    "contact": row.contact,
                    "reasons": list(row.visit_reasons_json or []),
                    "feedback_submitted": bool(row.feedback_submitted),
                    "notification_status": row.notification_status,
                }
                for row in found
            ]

    def _assert_public_copy(self, html: str) -> None:
        lowered = html.lower()
        for word in FORBIDDEN_PUBLIC_WORDS:
            self.assertNotIn(word, lowered)
        self.assertIn("1657 N State Street, Lehi, UT 84043", html)
        self.assertIn("The Anata Building", html)

    def test_migration_is_additive_and_keeps_the_runtime_role(self) -> None:
        sql = MIGRATION.read_text()
        self.assertIn("CREATE TABLE IF NOT EXISTS building_visitor_reviews", sql)
        self.assertIn("ENABLE ROW LEVEL SECURITY", sql)
        self.assertIn("REVOKE ALL ON TABLE building_visitor_reviews FROM anon, authenticated", sql)
        self.assertIn(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE building_visitor_reviews TO agent_app",
            sql,
        )
        self.assertNotIn("DROP TABLE", sql.upper())

    def test_review_page_is_public_on_both_paths(self) -> None:
        for path in ("/review", "/building/review"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn("How was your visit?", response.text)
            self.assertIn('action="/review"', response.text)
            self.assertIn("/static/building/anata-logo.png", response.text)
            self.assertNotIn("/admin/login", response.headers.get("location", ""))
            self._assert_public_copy(response.text)
            self.assertEqual(response.headers["x-robots-tag"], "noindex")
        logo = self.client.get("/static/building/anata-logo.png")
        self.assertEqual(logo.status_code, 200)
        self.assertIn("image", logo.headers["content-type"])

    def test_high_stars_are_stored_and_sent_toward_google(self) -> None:
        for stars in ("4", "5"):
            with self.subTest(stars=stars):
                response = self.client.post("/review", data={"stars": stars})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIn("Thank you. That means a lot.", response.text)
                self.assertIn(GOOGLE_REVIEW_URL, response.text)
                self.assertIn(
                    f'content="2;url={GOOGLE_REVIEW_URL}"',
                    response.text,
                )
                self.assertIn("Leave a Google review", response.text)
                self.assertNotIn("What could we do better?", response.text)
                self._assert_public_copy(response.text)
        rows = self._rows()
        self.assertEqual(sorted(row["stars"] for row in rows), [4, 5])
        self.assertTrue(all(not row["feedback_submitted"] for row in rows))
        self.slack.assert_not_called()
        self.email.assert_not_called()

    def test_low_stars_keep_the_score_and_store_a_private_note(self) -> None:
        opened = self.client.post("/review", data={"stars": "2"})
        self.assertEqual(opened.status_code, 200, opened.text)
        self.assertIn("What could we do better?", opened.text)
        self.assertIn('value="2"', opened.text)
        self.assertIn(" checked", opened.text)
        self.assertIn("Office or tenant", opened.text)
        self.assertNotIn(GOOGLE_REVIEW_URL, opened.text)
        self._assert_public_copy(opened.text)
        review_id = _review_id(opened.text)

        saved = self.client.post(
            "/review",
            data={
                "step": "feedback",
                "review_id": review_id,
                "stars": "2",
                "comment": "The lobby felt <cold> & loud.",
                "visitor_name": "Ada Lovelace",
                "contact": "ada@example.com",
                "visit_reason": ["event", "coworking", "pricing"],
            },
        )
        self.assertEqual(saved.status_code, 200, saved.text)
        self.assertIn("The building team will read this.", saved.text)
        self.assertNotIn("<cold>", saved.text)
        self.assertNotIn(GOOGLE_REVIEW_URL, saved.text)
        self._assert_public_copy(saved.text)

        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], review_id)
        self.assertEqual(rows[0]["stars"], 2)
        self.assertEqual(rows[0]["comment"], "The lobby felt <cold> & loud.")
        self.assertEqual(rows[0]["visitor_name"], "Ada Lovelace")
        self.assertEqual(rows[0]["contact"], "ada@example.com")
        self.assertEqual(rows[0]["reasons"], ["event", "coworking"])
        self.assertTrue(rows[0]["feedback_submitted"])
        self.assertEqual(rows[0]["notification_status"], "delivered")
        self.slack.assert_called_once()
        self.assertIn("The lobby felt <cold> & loud.", self.slack.call_args.kwargs["text"])
        self.assertEqual(self.slack.call_args.kwargs.get("channel", ""), "")
        self.email.assert_not_called()

        again = self.client.post(
            "/review",
            data={
                "step": "feedback",
                "review_id": review_id,
                "stars": "2",
                "comment": "The lobby felt <cold> & loud.",
                "visitor_name": "Ada Lovelace",
                "contact": "ada@example.com",
                "visit_reason": ["event", "coworking"],
            },
        )
        self.assertEqual(again.status_code, 200, again.text)
        self.slack.assert_called_once()

        page = self.client.get("/admin/building/operations")
        self.assertEqual(page.status_code, 200, page.text)
        self.assertIn("Front desk reviews", page.text)
        self.assertIn("Ada Lovelace", page.text)
        self.assertIn("ada@example.com", page.text)
        self.assertIn("Event, Coworking", page.text)
        self.assertIn("&lt;cold&gt;", page.text)
        self.assertNotIn("<cold>", page.text)
        self.assertIn("1 private note", page.text)

    def test_raising_the_score_to_google_updates_the_same_tap(self) -> None:
        opened = self.client.post("/building/review", data={"stars": "3"})
        review_id = _review_id(opened.text)
        saved = self.client.post(
            "/building/review",
            data={
                "step": "feedback",
                "review_id": review_id,
                "stars": "5",
                "comment": "",
                "contact": "801-555-0100",
            },
        )
        self.assertEqual(saved.status_code, 200, saved.text)
        self.assertIn(GOOGLE_REVIEW_URL, saved.text)
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stars"], 5)
        self.assertEqual(rows[0]["contact"], "801-555-0100")
        self.assertFalse(rows[0]["feedback_submitted"])
        self.slack.assert_not_called()

    def test_validation_honeypot_and_rate_limit(self) -> None:
        invalid = self.client.post("/review", data={"stars": "9"})
        self.assertEqual(invalid.status_code, 400, invalid.text)
        self.assertIn("Choose a star from 1 to 5.", invalid.text)
        self.assertEqual(self._rows(), [])

        bait = self.client.post(
            "/review",
            data={"stars": "1", "hp_company": "https://spam.example"},
        )
        self.assertEqual(bait.status_code, 200, bait.text)
        self.assertIn("The building team will read this.", bait.text)
        self.assertEqual(self._rows(), [])
        self.slack.assert_not_called()

        opened = self.client.post("/review", data={"stars": "1"})
        review_id = _review_id(opened.text)
        empty = self.client.post(
            "/review",
            data={"step": "feedback", "review_id": review_id, "stars": "1", "comment": "  "},
        )
        self.assertEqual(empty.status_code, 422, empty.text)
        self.assertIn("What could we do better?", empty.text)
        self.assertFalse(self._rows()[0]["feedback_submitted"])

        bad_contact = self.client.post(
            "/review",
            data={
                "step": "feedback",
                "review_id": review_id,
                "stars": "1",
                "comment": "The hallway was confusing.",
                "contact": "not-a-contact",
            },
        )
        self.assertEqual(bad_contact.status_code, 422, bad_contact.text)
        self.assertIn("email address or a phone number", bad_contact.text)
        self.assertIn("The hallway was confusing.", bad_contact.text)
        self.slack.assert_not_called()

    def test_star_taps_are_rate_limited_without_dropping_the_first(self) -> None:
        with patch(
            "sales_support_agent.services.building_visitor_reviews.STAR_RATE_LIMIT",
            1,
        ):
            first = self.client.post("/review", data={"stars": "5"})
            second = self.client.post("/review", data={"stars": "5"})
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 429, second.text)
        self.assertEqual(second.headers["retry-after"], "600")
        self.assertIn("Please wait a moment.", second.text)
        self.assertEqual([row["stars"] for row in self._rows()], [5])

    def test_rotating_the_marketing_client_key_does_not_reset_the_limit(self) -> None:
        with patch(
            "sales_support_agent.services.building_visitor_reviews.STAR_RATE_LIMIT",
            1,
        ):
            first = self.client.post(
                "/review",
                data={"stars": "4"},
                headers={"X-Marketing-Client-Key": "a" * 64},
            )
            second = self.client.post(
                "/review",
                data={"stars": "4"},
                headers={"X-Marketing-Client-Key": "b" * 64},
            )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 429, second.text)
        self.assertEqual([row["stars"] for row in self._rows()], [4])

    def test_private_note_also_emails_the_building_inbox_when_resend_is_configured(self) -> None:
        original = app.state.settings
        app.state.settings = dataclasses.replace(
            original,
            resend_api_key="re_test",
            resend_from="Anata <noreply@anatainc.com>",
        )
        try:
            opened = self.client.post("/review", data={"stars": "1"})
            saved = self.client.post(
                "/review",
                data={
                    "step": "feedback",
                    "review_id": _review_id(opened.text),
                    "stars": "1",
                    "comment": "Please add a clearer sign by the door.",
                    "contact": "ada@example.com",
                },
            )
            self.assertEqual(saved.status_code, 200, saved.text)
        finally:
            app.state.settings = original
        self.email.assert_called_once()
        self.assertEqual(self.email.call_args.kwargs["to"], building_from_address())
        self.assertEqual(self.email.call_args.kwargs["reply_to"], "ada@example.com")
        self.assertIn("clearer sign", self.email.call_args.kwargs["text"])
        self.assertEqual(self._rows()[0]["notification_status"], "delivered")
