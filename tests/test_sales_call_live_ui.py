"""The call page ships the Live block, the coach context builder and the ss-live key."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sales_support_agent.models.entities import SalesCallTracker  # noqa: E402
from sales_support_agent.services.sales.call_script import service  # noqa: E402


class LiveUiTests(unittest.TestCase):
    def setUp(self) -> None:
        tracker = SalesCallTracker(
            id="t1", owner_email="rep@anatainc.com", prospect_name="Jordan Lee",
            company="Acme Socks", state_json={}, status="open",
        )
        self.page = service.render_call_page(
            tracker, {"email": "rep@anatainc.com"}, csrf="x", ai_enabled=True
        )

    def test_live_block_and_hooks_render(self) -> None:
        for needle in ("data-live", "SS.coachContext", "SS.liveReview", "ss:aianalyzed", "LIVE_ARM"):
            self.assertIn(needle, self.page)

    def test_ss_live_is_persisted_per_call(self) -> None:
        self.assertIn("ss-live", service.PER_CALL_KEYS)
        state = {"ss-live": {"locks": {"beliefs": {"Pain": True}}}, "ss-tab": "flow"}
        self.assertEqual(service.sanitize_state(state), {"ss-live": state["ss-live"]})

    def test_live_copy_has_no_em_dashes(self) -> None:
        adapter = (service.ASSETS_DIR / "runtime_adapter.js").read_text(encoding="utf-8")
        self.assertNotIn("—", adapter)
        page = (service.ASSETS_DIR / "call_script.html").read_text(encoding="utf-8")
        self.assertNotIn("—", page)

    def test_start_live_call_is_visible_before_the_prep_brief(self) -> None:
        adapter = (service.ASSETS_DIR / "runtime_adapter.js").read_text(encoding="utf-8")
        idle = adapter.split("function idleHTML()", 1)[1].split("function render()", 1)[0]
        self.assertLess(idle.find("Start live call"), idle.find("prepBrief"))
        armed = adapter.split("if (!live) {", 1)[1].split("if (!s.consentAt)", 1)[0]
        self.assertIn("Start live call", armed)
        self.assertIn(">Start live call</button>", self.page)

    def test_share_tab_uses_a_browser_tab_and_broadcast_channel(self) -> None:
        page = (service.ASSETS_DIR / "call_script.html").read_text(encoding="utf-8")
        self.assertNotIn("popup=yes", page)
        self.assertNotIn("Audience window", page)
        self.assertIn("BroadcastChannel", page)
        self.assertIn(">Share tab</button>", page)
        self.assertIn("anata-call-present:", page)

    def test_sidebar_uses_the_agent_wordmark(self) -> None:
        self.assertIn('class="brandmark">agent<span class="dot">.</span>', self.page)
        self.assertNotIn('class="mark"', self.page)
        self.assertIn("lvfill", self.page)
        adapter = (service.ASSETS_DIR / "runtime_adapter.js").read_text(encoding="utf-8")
        self.assertIn("MOVE_HOLD_MS = 5000", adapter)


if __name__ == "__main__":
    unittest.main()
