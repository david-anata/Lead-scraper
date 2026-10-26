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


if __name__ == "__main__":
    unittest.main()
