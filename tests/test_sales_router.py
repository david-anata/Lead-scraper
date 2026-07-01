import unittest
from unittest import mock

try:
    from fastapi.testclient import TestClient
    from sales_support_agent.main import app
    from sales_support_agent.services.admin_auth import create_user_session_token
    DEPS = True
except ModuleNotFoundError as exc:
    if exc.name not in {"sqlalchemy", "fastapi"}:
        raise
    DEPS = False


def _cookie_for(email: str, name: str = "David Narayan", role: str = "admin"):
    settings = app.state.agent_settings
    token = create_user_session_token(settings, email=email, name=name, role=role)
    return settings.admin_cookie_name, token


@unittest.skipUnless(DEPS, "fastapi + sqlalchemy required")
class SalesRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        self.cookie_name, self.cookie_token = _cookie_for("david@anatainc.com")

    def _get(self, path: str):
        self.client.cookies.set(self.cookie_name, self.cookie_token)
        try:
            return self.client.get(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_sales_operator_shows_unavailable_page_when_snapshot_fails(self) -> None:
        with mock.patch(
            "sales_support_agent.api.sales_router.get_operator_snapshot",
            side_effect=RuntimeError("HubSpot token is not configured for this environment."),
        ):
            resp = self._get("/admin/sales")

        self.assertEqual(resp.status_code, 503)
        self.assertIn("Sales Control Room unavailable", resp.text)
        self.assertIn("HubSpot token is not configured for this environment.", resp.text)

    def test_sales_operator_snapshot_returns_json_error_when_snapshot_fails(self) -> None:
        with mock.patch(
            "sales_support_agent.api.sales_router.get_operator_snapshot",
            side_effect=RuntimeError("HubSpot token is not configured for this environment."),
        ):
            resp = self._get("/admin/sales/snapshot")

        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.json()["ok"], False)
        self.assertIn("HubSpot token is not configured", resp.json()["error"])

    def test_run_sales_operator_job_route_returns_summary(self) -> None:
        internal_api_key = app.state.settings.internal_api_key
        headers = {"X-Internal-Api-Key": internal_api_key} if internal_api_key else {}
        with mock.patch(
            "sales_support_agent.api.sales_jobs_router.SalesOperatorReviewJob"
        ) as job_cls:
            job_cls.return_value.run.return_value = {"status": "completed", "candidate_deals": 2}
            resp = self.client.post(
                "/api/jobs/sales-operator/run",
                json={"dry_run": True, "limit": 5},
                headers=headers,
            )

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "ok")
        self.assertEqual(resp.json()["details"]["candidate_deals"], 2)


if __name__ == "__main__":
    unittest.main()
