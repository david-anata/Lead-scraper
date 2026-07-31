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

    def _post(self, path: str):
        self.client.cookies.set(self.cookie_name, self.cookie_token)
        try:
            return self.client.post(path, follow_redirects=False)
        finally:
            self.client.cookies.clear()

    def test_failed_website_intake_can_be_queued_for_operator_retry(self) -> None:
        from sales_support_agent.models.database import session_scope
        from sales_support_agent.models.entities import AutomationRun
        from sales_support_agent.services.audit import AuditService

        with session_scope(app.state.session_factory) as session:
            run = AuditService(session).start_run(
                "marketing_intake",
                trigger="test",
                metadata={
                    "email": "retry@example.com",
                    "source": "anatainc.com",
                    "qualification": {"company": "Retry Brand"},
                },
            )
            AuditService(session).finish_run(
                run,
                status="failed",
                summary={
                    "kind": "asin",
                    "asin": "B012345678",
                    "brand_name": "Retry Brand",
                    "needs": ["advertising"],
                    "error": "provider timed out",
                },
            )
            run_id = int(run.id)

        try:
            with mock.patch(
                "sales_support_agent.api.marketing_router._run_analysis_and_deliver"
            ) as retry:
                response = self._post(f"/admin/sales/website-intakes/{run_id}/retry")

            self.assertEqual(response.status_code, 303)
            retry.assert_called_once()
            self.assertEqual(retry.call_args.kwargs["intake_run_id"], run_id)
            self.assertEqual(retry.call_args.kwargs["trigger"], "sales_operator_retry")
            with session_scope(app.state.session_factory) as session:
                persisted = session.get(AutomationRun, run_id)
                self.assertEqual(persisted.status, "running")
                self.assertEqual((persisted.summary_json or {}).get("error"), "")
                self.assertTrue((persisted.summary_json or {}).get("operator_retry_queued_at"))
        finally:
            with session_scope(app.state.session_factory) as session:
                persisted = session.get(AutomationRun, run_id)
                if persisted is not None:
                    session.delete(persisted)

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
