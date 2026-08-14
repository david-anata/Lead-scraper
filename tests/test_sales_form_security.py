from __future__ import annotations

import os
import tempfile

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + os.path.join(tempfile.gettempdir(), "sales_form_security.db"),
)
os.environ.setdefault(
    "ADMIN_DASHBOARD_SESSION_SECRET",
    "sales-form-security-session-secret",
)

import re
import unittest
from unittest import mock

try:
    from fastapi.testclient import TestClient

    from sales_support_agent.main import app
    from sales_support_agent.services.admin_auth import create_user_session_token
    from sales_support_agent.services.sales.security import csrf_token

    DEPS = True
except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class SalesFormSecurityTests(unittest.TestCase):
    """Sales writes reach ClickUp, HubSpot, and Slack.

    Building, Finance, and HR already required a session-bound form token on
    browser writes; sales accepted them with no token at all.
    """

    #: Every mutating sales route, with the smallest body that reaches the guard.
    WRITE_ROUTES = (
        ("/admin/sales/writeback", {"mode": "preview", "limit": "1"}),
        ("/admin/sales/deals/sync", {}),
        ("/admin/sales/deals/cleanup", {}),
        ("/admin/sales/deals/alerts/send", {}),
        ("/admin/sales/website-notes/1/retry", {}),
        ("/admin/sales/website-intakes/1/retry", {}),
        ("/admin/sales/deals/create", {"deal_name": "x"}),
        ("/admin/sales/deals/abc/actions/approve", {}),
        ("/admin/sales/deals/abc/send-followup", {}),
    )

    @classmethod
    def setUpClass(cls) -> None:
        settings = app.state.agent_settings
        token = create_user_session_token(
            settings, email="david@anatainc.com", name="David", role="admin"
        )
        cls.client = TestClient(app)
        cls.client.cookies.set(settings.admin_cookie_name, token)
        cls.browser_headers = {
            "Origin": "http://testserver",
            "Sec-Fetch-Mode": "navigate",
        }

    def test_browser_write_without_a_token_is_rejected(self) -> None:
        for path, body in self.WRITE_ROUTES:
            with self.subTest(path=path):
                response = self.client.post(
                    path,
                    headers=self.browser_headers,
                    follow_redirects=False,
                    data=body,
                )
                self.assertEqual(response.status_code, 403, path)

    def test_cross_site_write_is_rejected_even_with_a_token(self) -> None:
        user = {
            "email": "david@anatainc.com",
            "session_issued_at": "",
            "permissions": set(),
            "is_superadmin": True,
        }
        response = self.client.post(
            "/admin/sales/deals/sync",
            headers={**self.browser_headers, "Sec-Fetch-Site": "cross-site"},
            follow_redirects=False,
            data={"_csrf_token": csrf_token(user)},
        )
        self.assertEqual(response.status_code, 403)

    def test_vercel_sync_button_completes_inside_the_request(self) -> None:
        page = self.client.get("/admin/sales/deals")
        token_match = re.search(
            r'name="_csrf_token" value="([0-9a-f]{16,})"', page.text
        )
        self.assertIsNotNone(token_match)
        with (
            mock.patch.dict(os.environ, {"VERCEL": "1"}),
            mock.patch(
                "sales_support_agent.api.sales_router.run_hubspot_sync_now"
            ) as run_now,
            mock.patch(
                "sales_support_agent.api.sales_router.start_hubspot_sync"
            ) as start_background,
        ):
            response = self.client.post(
                "/admin/sales/deals/sync",
                headers=self.browser_headers,
                follow_redirects=False,
                data={"_csrf_token": token_match.group(1)},
            )

        self.assertEqual(response.status_code, 303)
        run_now.assert_called_once_with(app)
        start_background.assert_not_called()

    def test_mismatched_origin_is_rejected(self) -> None:
        response = self.client.post(
            "/admin/sales/deals/sync",
            headers={"Origin": "https://attacker.example", "Sec-Fetch-Mode": "navigate"},
            follow_redirects=False,
            data={},
        )
        self.assertEqual(response.status_code, 403)

    def test_every_rendered_sales_form_carries_a_token(self) -> None:
        # A guard nothing can satisfy is just an outage. Wherever a page renders
        # a sales write form, it must also render a usable token. Pages that
        # render no such form are correctly exempt.
        checked_a_form = False
        for path in (
            "/admin/sales/deals",
            "/admin/sales/deals/create",
            "/admin/sales/deals/cleanup",
        ):
            with self.subTest(path=path):
                page = self.client.get(path)
                self.assertEqual(page.status_code, 200, path)
                forms = re.findall(
                    r'<form[^>]*action="/admin/sales[^"]*"[^>]*>', page.text
                )
                if not forms:
                    continue
                checked_a_form = True
                tokens = re.findall(
                    r'name="_csrf_token" value="([0-9a-f]{16,})"', page.text
                )
                self.assertGreaterEqual(
                    len(tokens),
                    len(forms),
                    f"{path} rendered {len(forms)} sales form(s) but "
                    f"{len(tokens)} token(s)",
                )
        self.assertTrue(checked_a_form, "no sales form was exercised")


if __name__ == "__main__":
    unittest.main()
