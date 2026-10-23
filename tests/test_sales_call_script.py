"""Route and service tests for the live call script (/admin/sales/call)."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + tempfile.gettempdir() + "/sales_call_script_test.db",
)

from fastapi.testclient import TestClient  # noqa: E402

from sales_support_agent.main import app  # noqa: E402
from sales_support_agent.models.database import session_scope  # noqa: E402
from sales_support_agent.models.entities import SalesCallReview, SalesCallTracker  # noqa: E402
from sales_support_agent.services.access.catalog import ALL_TOOL_KEYS  # noqa: E402
from sales_support_agent.services.admin_auth import create_user_session_token  # noqa: E402
from sales_support_agent.services.sales.call_script import ai as call_ai  # noqa: E402
from sales_support_agent.services.sales.call_script import service  # noqa: E402


def _cookie_for(email: str, name: str = "User", role: str = "member"):
    s = app.state.agent_settings
    return s.admin_cookie_name, create_user_session_token(s, email=email, name=name, role=role)


class CallScriptRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cookie_name, token = _cookie_for("david@anatainc.com", "David")
        cls.client.cookies.set(cookie_name, token)

    def setUp(self) -> None:
        with session_scope(app.state.session_factory) as session:
            for model in (SalesCallReview, SalesCallTracker):
                for row in session.query(model).all():
                    session.delete(row)

    def _start(self, name="Jordan Lee", company="Acme Socks") -> str:
        resp = self.client.post(
            "/admin/sales/call",
            data={"prospect_name": name, "company": company, "lead_link": ""},
            follow_redirects=False,
        )
        self.assertEqual(resp.status_code, 303)
        location = resp.headers["location"]
        self.assertTrue(location.startswith("/admin/sales/call/"))
        return location.rsplit("/", 1)[1]

    def test_tool_is_registered(self) -> None:
        self.assertIn("sales.call", ALL_TOOL_KEYS)

    def test_list_page_renders_empty_state(self) -> None:
        resp = self.client.get("/admin/sales/call")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("Call Script", resp.text)
        self.assertIn("No calls yet", resp.text)

    def test_start_requires_name_or_company(self) -> None:
        resp = self.client.post(
            "/admin/sales/call", data={"prospect_name": " ", "company": ""}, follow_redirects=False
        )
        self.assertEqual(resp.status_code, 303)
        self.assertIn("error=", resp.headers["location"])

    def test_call_page_injects_adapter_and_state(self) -> None:
        tracker_id = self._start()
        resp = self.client.get(f"/admin/sales/call/{tracker_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("window.__ANATA_CALL__", resp.text)
        self.assertIn("window.claude = {", resp.text)
        # Adapter must run before the script reads storage.
        self.assertLess(resp.text.index("window.__ANATA_CALL__"), resp.text.index("window.SS = {"))
        self.assertIn("Call: Jordan Lee, Acme Socks", resp.text)

    def test_state_round_trip_keeps_only_per_call_keys(self) -> None:
        tracker_id = self._start()
        state = {
            "ss-beliefs": {"Pain": True, "Cost": True},
            "ss-gap": {"goal": "$100K/month by June"},
            "ss-tab": "flow",  # UI preference: never stored per lead
        }
        resp = self.client.put(f"/admin/sales/call/{tracker_id}/state", json={"state": state})
        self.assertEqual(resp.status_code, 200, resp.text)
        page = self.client.get(f"/admin/sales/call/{tracker_id}").text
        self.assertIn('"ss-beliefs": {"Pain": true, "Cost": true}', page)
        self.assertNotIn('"ss-tab"', page.split("window.__ANATA_CALL__", 1)[1].split("</script>", 1)[0])
        listing = self.client.get("/admin/sales/call").text
        self.assertIn("2 of 7", listing)

    def test_beacon_post_saves_state(self) -> None:
        tracker_id = self._start()
        resp = self.client.post(
            f"/admin/sales/call/{tracker_id}/state",
            content=json.dumps({"state": {"ss-nums": {"total": "120000"}}}),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(resp.status_code, 200)
        with session_scope(app.state.session_factory) as session:
            row = session.get(SalesCallTracker, tracker_id)
            self.assertEqual(row.state_json, {"ss-nums": {"total": "120000"}})

    def test_browser_write_without_token_is_rejected(self) -> None:
        tracker_id = self._start()
        resp = self.client.put(
            f"/admin/sales/call/{tracker_id}/state",
            json={"state": {}},
            headers={"Sec-Fetch-Mode": "cors"},
        )
        self.assertEqual(resp.status_code, 403)

    def test_cross_site_write_is_rejected(self) -> None:
        tracker_id = self._start()
        resp = self.client.put(
            f"/admin/sales/call/{tracker_id}/state",
            json={"state": {}},
            headers={"Sec-Fetch-Site": "cross-site"},
        )
        self.assertEqual(resp.status_code, 403)

    def test_unknown_tracker_is_404(self) -> None:
        self.assertEqual(self.client.get("/admin/sales/call/not-a-real-id").status_code, 404)

    def test_archive_and_restore(self) -> None:
        tracker_id = self._start()
        resp = self.client.post(
            f"/admin/sales/call/{tracker_id}/status", data={"status": "archived"}, follow_redirects=False
        )
        self.assertEqual(resp.status_code, 303)
        self.assertIn("No calls yet", self.client.get("/admin/sales/call").text)
        self.assertIn("Jordan Lee", self.client.get("/admin/sales/call?archived=1").text)
        self.client.post(f"/admin/sales/call/{tracker_id}/status", data={"status": "open"})
        self.assertIn("Jordan Lee", self.client.get("/admin/sales/call").text)

    def test_reviews_crud(self) -> None:
        review = {"id": "r1", "type": "call", "ts": 1, "score": 82}
        resp = self.client.put("/admin/sales/call/api/reviews/r1", json={"review": review, "trackerId": ""})
        self.assertEqual(resp.status_code, 200)
        listed = self.client.get("/admin/sales/call/api/reviews").json()["reviews"]
        self.assertEqual([r["id"] for r in listed], ["r1"])
        self.assertEqual(listed[0]["score"], 82)
        self.assertEqual(self.client.get("/admin/sales/call/api/reviews/r1").json()["review"]["score"], 82)
        self.assertTrue(self.client.delete("/admin/sales/call/api/reviews/r1").json()["deleted"])
        self.assertEqual(self.client.get("/admin/sales/call/api/reviews").json()["reviews"], [])

    def test_slides_are_served_and_paths_are_contained(self) -> None:
        resp = self.client.get("/admin/sales/call/slides/s01.jpg")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers["content-type"], "image/jpeg")
        self.assertEqual(self.client.get("/admin/sales/call/slides/..%2Fservice.py").status_code, 404)
        self.assertEqual(self.client.get("/admin/sales/call/slides/s99.jpg").status_code, 404)

    def test_sample_without_key_returns_503(self) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}):
            resp = self.client.post("/admin/sales/call/api/sample", json={"input": "hi"})
        self.assertEqual(resp.status_code, 503)

    def test_sample_returns_text(self) -> None:
        fake = call_ai.SampleResult(text="Ask about their TACoS.", truncated=False, model="m")
        with patch.object(call_ai, "run_sample", return_value=fake) as run:
            resp = self.client.post(
                "/admin/sales/call/api/sample", json={"input": "next move?", "tier": "quick"}
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"text": "Ask about their TACoS.", "truncated": False})
        run.assert_called_once_with("next move?", tier="quick", want_json=False)


class CallScriptServiceTests(unittest.TestCase):
    def test_sanitize_state_rejects_non_objects_and_oversize(self) -> None:
        with self.assertRaises(service.CallScriptError):
            service.sanitize_state(["not", "a", "dict"])
        with self.assertRaises(service.CallScriptError):
            service.sanitize_state({"ss-gap": {"notes": "x" * (service.MAX_STATE_BYTES + 1)}})

    def test_can_access_is_owner_or_superadmin(self) -> None:
        tracker = SalesCallTracker(id="t", owner_email="rep@anatainc.com")
        self.assertTrue(service.can_access({"email": "rep@anatainc.com"}, tracker))
        self.assertTrue(service.can_access({"email": "boss@anatainc.com", "is_superadmin": True}, tracker))
        self.assertFalse(service.can_access({"email": "other@anatainc.com"}, tracker))
        self.assertFalse(service.can_access(None, tracker))

    def test_summary_stage_follows_call_meta(self) -> None:
        tracker = SalesCallTracker(
            id="t", owner_email="a", prospect_name="P", company="C",
            state_json={"ss-callmeta": {"start": 1, "pitchAt": 2}, "ss-beliefs": {"Pain": True}},
            status="open",
        )
        s = service.summarize(tracker)
        self.assertEqual((s.stage, s.beliefs_said), ("Pitched", 1))

    def test_normalize_messages(self) -> None:
        self.assertEqual(call_ai.normalize_messages("hi"), [{"role": "user", "content": "hi"}])
        with self.assertRaises(call_ai.SampleError):
            call_ai.normalize_messages([{"role": "assistant", "content": "x"}])
        with self.assertRaises(call_ai.SampleError):
            call_ai.normalize_messages("")


if __name__ == "__main__":
    unittest.main()
