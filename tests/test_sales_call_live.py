"""Tests for live calls: tokens, sessions, Tape endpoints, coach rules, after-call."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_call_live_test.db",
)
os.environ.setdefault("ADMIN_DASHBOARD_SESSION_SECRET", "test-live-secret")

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import (  # noqa: E402
    SalesCallCoachEvent,
    SalesCallLiveSession,
    SalesCallReview,
    SalesCallTracker,
    SalesCallTranscriptLine,
)
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.call_script import after_call, coach  # noqa: E402
from sales_support_agent.services.sales.call_script import live as live_svc  # noqa: E402


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


def _wipe() -> None:
    with session_scope(app.state.session_factory) as session:
        for model in (SalesCallCoachEvent, SalesCallTranscriptLine, SalesCallLiveSession, SalesCallReview, SalesCallTracker):
            for row in session.query(model).all():
                session.delete(row)


GOOD_PATCH = {
    "sections": {"s1": {"done": True, "quote": "our ACoS is killing us"}, "s99": {"done": True}},
    "beliefs": {
        "Pain": {"status": "said", "quote": "our ACoS is killing us"},
        "Cost": {"status": "said", "quote": "words the prospect never said"},
        "Doubt": {"status": "said", "quote": "we tried fixing it ourselves"},
    },
    "numbers": {"total": "120,000", "spend": "$20000", "bogus": 4, "goal": "lots"},
    "pains": ["acos", "nonsense", "acos"],
    "objs": ["price"],
    "res": ["ai", "zzz"],
    "three": {"problem": True, "gap": False},
    "quotes": ["our ACoS is killing us", "invented quote"],
    "tieDowns": [{"pain": "acos", "line": "So ACoS is eating margin — right?", "uses": 2}],
    "nextMove": {"kind": "ask", "say": "How long has it been like that?", "why": "Cost", "section": "s6"},
    "pitch": {"ready": False, "offer": "growth", "bridge": "", "slides": [9, 40, "10"], "case": "greengobbler"},
    "temperature": {"asked": False, "score": None, "quality": "none"},
    "warnings": ["Pitching before minute 20"],
}


class LiveTokenTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        token, exp = live_svc.sign_token("sid-1", "tid-1", "a@b.com")
        payload = live_svc.verify_token(token, "sid-1")
        self.assertEqual((payload["tid"], payload["em"]), ("tid-1", "a@b.com"))
        self.assertGreater(exp, time.time())

    def test_wrong_session_tamper_and_expiry(self) -> None:
        token, _ = live_svc.sign_token("sid-1", "tid-1", "a@b.com")
        with self.assertRaises(live_svc.LiveError):
            live_svc.verify_token(token, "sid-2")
        v, p, s = token.split(".")
        with self.assertRaises(live_svc.LiveError):
            live_svc.verify_token(f"{v}.{p}.{s[:-2]}xx", "sid-1")
        expired, _ = live_svc.sign_token("sid-1", "tid-1", "a@b.com", ttl=-10)
        with self.assertRaises(live_svc.LiveError) as ctx:
            live_svc.verify_token(expired, "sid-1")
        self.assertEqual(ctx.exception.status, 401)
        with self.assertRaises(live_svc.LiveError):
            live_svc.verify_token("garbage", "sid-1")


class CoachRuleTests(unittest.TestCase):
    def _apply(self, locks=None):
        return coach.apply_rules(
            GOOD_PATCH,
            prospect_lines=["Honestly our ACoS is killing us.", "We tried fixing it ourselves, twice."],
            locks=locks or {},
        )

    def test_quotes_must_come_from_prospect(self) -> None:
        p = self._apply()
        self.assertEqual(p["beliefs"]["Pain"]["status"], "said")
        self.assertEqual(p["beliefs"]["Cost"], {"status": "partial", "quote": ""})
        self.assertEqual(p["beliefs"]["Doubt"]["status"], "said")
        self.assertEqual(p["quotes"], ["our ACoS is killing us"])

    def test_rep_words_never_tick_a_belief(self) -> None:
        p = coach.apply_rules(
            {"beliefs": {"Pain": {"status": "said", "quote": "your ACoS is high"}}},
            prospect_lines=["yeah maybe"],
            locks={},
        )
        self.assertEqual(p["beliefs"]["Pain"]["status"], "partial")

    def test_locks_win(self) -> None:
        p = self._apply(locks={"Pain": False, "Money": True})
        self.assertEqual(p["beliefs"]["Pain"], {"status": "none", "quote": "", "locked": True})
        self.assertEqual(p["beliefs"]["Money"]["status"], "said")

    def test_whitelists_numbers_dashes_and_cases(self) -> None:
        p = self._apply()
        self.assertEqual(p["sections"], {"s1": {"done": True, "quote": "our ACoS is killing us"}})
        self.assertEqual(p["numbers"], {"total": 120000.0, "spend": 20000.0})
        self.assertEqual(p["pains"], ["acos"])
        self.assertEqual(p["res"], ["ai"])
        self.assertNotIn("—", p["tieDowns"][0]["line"])
        self.assertEqual(p["pitch"]["case"], "")
        self.assertEqual(p["pitch"]["slides"], [9, 10])
        self.assertEqual(p["pitch"]["offer"], "growth")
        self.assertEqual(p["nextMove"]["section"], "s6")

    def test_non_object_rejected(self) -> None:
        with self.assertRaises(coach.CoachError):
            coach.apply_rules(["x"], prospect_lines=[], locks={})


class LiveRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)
        cls.public = TestClient(app)

    def setUp(self) -> None:
        _wipe()

    def _tracker(self) -> str:
        resp = self.client.post(
            "/admin/sales/call",
            data={"prospect_name": "Jordan Lee", "company": "Acme Socks"},
            follow_redirects=False,
        )
        return resp.headers["location"].rsplit("/", 1)[1]

    def _go_live(self, tid: str) -> dict:
        resp = self.client.post(
            f"/admin/sales/call/{tid}/live", json={"coachContext": "PLAYBOOK", "label": "Jordan Lee, Acme Socks"}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def _post_lines(self, sid: str, token: str, lines: list) -> dict:
        resp = self.public.post(
            f"/api/sales-call/live/{sid}/lines",
            json={"lines": lines},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def test_start_reuses_open_session_and_returns_token(self) -> None:
        tid = self._tracker()
        a = self._go_live(tid)
        b = self._go_live(tid)
        self.assertEqual(a["sessionId"], b["sessionId"])
        self.assertEqual(a["relayUrl"], "https://tape.anatainc.com")
        self.assertEqual(live_svc.verify_token(a["token"], a["sessionId"])["tid"], tid)

    def test_public_endpoints_require_token(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        sid = live["sessionId"]
        self.assertEqual(self.public.post(f"/api/sales-call/live/{sid}/verify").status_code, 401)
        other, _ = live_svc.sign_token("other-session", tid, "x@y.com")
        bad = self.public.post(f"/api/sales-call/live/{sid}/verify", headers={"Authorization": f"Bearer {other}"})
        self.assertEqual(bad.status_code, 401)
        ok = self.public.post(f"/api/sales-call/live/{sid}/verify", headers={"Authorization": f"Bearer {live['token']}"})
        self.assertEqual(ok.status_code, 200)
        self.assertIn("TACoS", ok.json()["keyterms"])

    def test_lines_state_after_and_transcript(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        sid, token = live["sessionId"], live["token"]
        state = self.client.get(f"/admin/sales/call/{tid}/live/state").json()
        self.assertEqual(state["session"]["status"], "pending")
        first = self._post_lines(sid, token, [
            {"speaker": "rep", "text": "Thanks for jumping on.", "startMs": 0, "endMs": 900},
            {"speaker": "prospect", "text": "Happy to. Our ACoS is killing us.", "startMs": 1000, "endMs": 3000},
        ])
        with patch.object(coach, "run_pass") as run:
            state = self.client.get(f"/admin/sales/call/{tid}/live/state?after=0").json()
            run.assert_called_once()
        self.assertEqual(state["session"]["status"], "live")
        self.assertEqual([ln["speaker"] for ln in state["lines"]], ["rep", "prospect"])
        self.assertEqual(state["lastLineId"], first["lastLineId"])
        later = self.client.get(f"/admin/sales/call/{tid}/live/state?after={first['lastLineId']}").json()
        self.assertEqual(later["lines"], [])
        bad = self.public.post(
            f"/api/sales-call/live/{sid}/lines",
            json={"lines": [{"speaker": "ghost", "text": "x"}]},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(bad.status_code, 400)
        self.assertEqual(self.client.post(f"/admin/sales/call/{tid}/live/consent").status_code, 200)
        end = self.client.post(f"/admin/sales/call/{tid}/live/end").json()
        self.assertEqual(end["transcript"], "Rep: Thanks for jumping on.\nProspect: Happy to. Our ACoS is killing us.")
        gone = self.public.post(
            f"/api/sales-call/live/{sid}/lines",
            json={"lines": [{"speaker": "rep", "text": "late"}]},
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(gone.status_code, 410)
        self.assertEqual(self.public.post(f"/api/sales-call/live/{sid}/ended", headers={"Authorization": f"Bearer {token}"}).status_code, 200)

    def test_state_runs_a_real_pass_and_returns_new_rev_once(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        self._post_lines(live["sessionId"], live["token"], [
            {"speaker": "prospect", "text": "Honestly our ACoS is killing us.", "startMs": 0, "endMs": 2000},
        ])
        with patch.object(coach, "_call_claude", return_value=json.dumps(GOOD_PATCH)) as call:
            state = self.client.get(f"/admin/sales/call/{tid}/live/state").json()
            self.assertEqual(call.call_count, 1)
            system = call.call_args[0][0]
            self.assertIn("PLAYBOOK", system[0]["text"])
            self.assertEqual(system[0]["cache_control"], {"type": "ephemeral"})
        self.assertEqual(state["coach"]["rev"], 1)
        self.assertEqual(state["coach"]["patch"]["beliefs"]["Pain"]["status"], "said")
        again = self.client.get(f"/admin/sales/call/{tid}/live/state?rev=1").json()
        self.assertIsNone(again["coach"])

    def test_lease_blocks_a_second_pass(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        self._post_lines(live["sessionId"], live["token"], [{"speaker": "prospect", "text": "hello there", "startMs": 0, "endMs": 1}])
        with session_scope(app.state.session_factory) as session:
            row = session.get(SalesCallLiveSession, live["sessionId"])
            tracker = session.get(SalesCallTracker, tid)
            self.assertTrue(live_svc.try_acquire_lease(session, row))
            caller = MagicMock(return_value=json.dumps(GOOD_PATCH))
            self.assertIsNone(coach.run_pass(session, row, tracker, caller=caller))
            caller.assert_not_called()
            self.assertFalse(live_svc.coach_due(session, row))
            live_svc.release_lease(session, row)
            event = coach.run_pass(session, row, tracker, caller=caller)
            self.assertEqual(event.rev, 1)
            self.assertFalse(live_svc.coach_due(session, row))  # no new lines since

    def test_after_call_skips_when_prospect_never_spoke(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        self._post_lines(live["sessionId"], live["token"], [
            {"speaker": "rep", "text": "What brought you to the call?", "startMs": 0, "endMs": 2000}])
        clickup = MagicMock()
        gmail = MagicMock()
        with patch("sales_support_agent.api.call_script_router._clickup_client", return_value=clickup), \
             patch("sales_support_agent.api.call_script_router._gmail_client", return_value=gmail):
            body = self.client.post(f"/admin/sales/call/{tid}/after-call",
                                    json={"analysis": {"outcome": "closed"}, "offer": "starter"}).json()
        for step in ("clickup", "recap", "day10"):
            self.assertTrue(body["steps"][step]["skipped"])
        clickup.create_task.assert_not_called()
        gmail.create_draft.assert_not_called()

    def test_after_call_is_idempotent(self) -> None:
        tid = self._tracker()
        live = self._go_live(tid)
        self._post_lines(live["sessionId"], live["token"], [
            {"speaker": "prospect", "text": "Our ACoS is way too high.", "startMs": 0, "endMs": 2000}])
        clickup = MagicMock()
        clickup.get_tasks.return_value = []
        clickup.create_task.side_effect = [
            {"id": "task1", "url": "https://app.clickup.com/t/task1", "custom_fields": []},
            {"id": "task2"},
        ]
        gmail = MagicMock()
        analysis = {
            "outcome": "closed", "pitch": "after", "prospect": "Acme Socks",
            "beliefs": [{"b": "Pain", "evidence": "x"}], "pains": ["acos"], "objs": [{"k": "price"}],
            "notes": {"summary": "Good call.", "nextSteps": [{"step": "Connect Seller Central", "owner": "prospect", "when": "today"}]},
            "email": {"subject": "Great talking today", "body": "Thanks Jordan."},
        }
        with patch("sales_support_agent.api.call_script_router._clickup_client", return_value=clickup), \
             patch("sales_support_agent.api.call_script_router._gmail_client", return_value=gmail):
            first = self.client.post(f"/admin/sales/call/{tid}/after-call", json={"analysis": analysis, "offer": "starter"}).json()
            second = self.client.post(f"/admin/sales/call/{tid}/after-call", json={"analysis": analysis, "offer": "starter"}).json()
        steps = first["steps"]
        self.assertTrue(steps["clickup"]["ok"])
        self.assertEqual(steps["clickup"]["status"], "won - onboarding")
        self.assertTrue(steps["day10"]["ok"])
        self.assertTrue(steps["recap"]["skipped"])  # no email on the CRM task
        self.assertEqual(clickup.create_task.call_count, 2)
        self.assertEqual(second["steps"], steps)
        values = {c.args[1]: c.args[2] for c in clickup.set_custom_field_value.call_args_list}
        self.assertEqual(values[after_call.FIELD_VALUE], 99)
        self.assertEqual(values[after_call.FIELD_MEETING_SET], True)
        gmail.create_draft.assert_not_called()


class AfterCallUnitTests(unittest.TestCase):
    def test_status_mapping(self) -> None:
        self.assertEqual(after_call.status_for("closed", True), "won - onboarding")
        self.assertEqual(after_call.status_for("followup", True), "follow up")
        self.assertEqual(after_call.status_for("lost", True), "lost")
        self.assertEqual(after_call.status_for("nodecision", True), "working offered")
        self.assertEqual(after_call.status_for("nodecision", False), "working qualified")

    def test_task_id_from_link(self) -> None:
        self.assertEqual(after_call.task_id_from_link("https://app.clickup.com/t/86b1abcd"), "86b1abcd")
        self.assertEqual(after_call.task_id_from_link("https://app.clickup.com/t/2295762/ABC-12"), "ABC-12")
        self.assertEqual(after_call.task_id_from_link(""), "")

    def test_recap_draft_adds_trial_link(self) -> None:
        gmail = MagicMock()
        gmail.create_draft.return_value = {"id": "d1"}
        res = after_call._recap_step(gmail, {"email": {"subject": "Hi", "body": "Thanks."}}, "starter", "j@acme.com")
        self.assertEqual(res, {"ok": True, "mode": "draft", "draftId": "d1", "to": "j@acme.com"})
        body = gmail.create_draft.call_args.kwargs["text"]
        self.assertIn(after_call.TRIAL_LINE, body)

    def test_failed_step_retries_without_duplicates(self) -> None:
        live = SalesCallLiveSession(id="s", tracker_id="t", owner_email="a", after_call_json={})
        tracker = SalesCallTracker(id="t", owner_email="a", prospect_name="P", company="C", lead_link="")
        clickup = MagicMock()
        clickup.get_tasks.side_effect = RuntimeError("ClickUp down")
        session = MagicMock()
        steps = after_call.run_after_call(session, live, tracker, {"outcome": "followup", "email": {}}, "none", clickup_client=clickup, gmail_client=MagicMock())
        self.assertFalse(steps["clickup"]["ok"])
        self.assertFalse(steps["day10"]["ok"])
        clickup.get_tasks.side_effect = None
        clickup.get_tasks.return_value = [{"id": "x1", "name": "C", "custom_fields": []}]
        steps = after_call.run_after_call(session, live, tracker, {"outcome": "followup", "email": {}}, "none", clickup_client=clickup, gmail_client=MagicMock())
        self.assertTrue(steps["clickup"]["ok"])
        self.assertEqual(steps["clickup"]["taskId"], "x1")
        self.assertTrue(steps["day10"]["skipped"])
        clickup.create_task.assert_not_called()


if __name__ == "__main__":
    unittest.main()


def test_transcript_text_merges_same_speaker_fragments():
    from types import SimpleNamespace as L
    from sales_support_agent.services.sales.call_script import live as lv

    lines = [
        L(id=1, speaker="rep", start_ms=0, end_ms=1000, text="Well, it's"),
        L(id=2, speaker="rep", start_ms=1200, end_ms=3000, text="two more."),
        L(id=3, speaker="prospect", start_ms=3500, end_ms=5000, text="Sure."),
        L(id=4, speaker="rep", start_ms=20000, end_ms=21000, text="Next."),
    ]
    assert lv.transcript_text(lines) == "Rep: Well, it's two more.\nProspect: Sure.\nRep: Next."
