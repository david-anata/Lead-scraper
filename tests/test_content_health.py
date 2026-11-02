from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from sales_support_agent.main import create_app


def test_health_exposes_safe_content_runtime_truth(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SALES_AGENT_DB_URL", f"sqlite:///{tmp_path / 'agent.db'}")
    app = create_app()
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    runtime = response.json()["details"]["content_runtime"]
    assert runtime["source_assets"] == 0
    assert runtime["artifacts"] == 0
    assert runtime["verified_publications"] == 0
    assert "dependency_states" in runtime
    assert "riverside" in runtime["dependency_states"]
    assert "linkedin_personal" in runtime["dependency_states"]
    assert runtime["latest_run"] is None


def test_anonymous_health_does_not_call_plaid(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SALES_AGENT_DB_URL", f"sqlite:///{tmp_path / 'agent.db'}")
    app = create_app()
    with patch("sales_support_agent.api.router.requests.post") as post:
        with TestClient(app) as client:
            response = client.get("/health?plaid_probe=1")
    assert response.status_code == 200
    assert "plaid_probe" not in response.json()["details"]
    post.assert_not_called()


def test_finance_operator_can_still_probe_plaid(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SALES_AGENT_DB_URL", f"sqlite:///{tmp_path / 'agent.db'}")
    app = create_app()
    plaid_response = MagicMock()
    plaid_response.json.return_value = {"error_code": "INVALID_API_KEYS"}
    plaid_response.status_code = 400
    with (
        patch(
            "sales_support_agent.api.router.require_tool_inline",
            return_value=({"email": "finance@anatainc.com"}, None),
        ),
        patch("sales_support_agent.api.router.requests.post", return_value=plaid_response) as post,
    ):
        with TestClient(app) as client:
            response = client.get("/health?plaid_probe=1")
    assert response.status_code == 200
    probe = response.json()["details"]["plaid_probe"]
    assert probe["sandbox"] == "INVALID_API_KEYS"
    assert probe["production"] == "INVALID_API_KEYS"
    assert post.call_count == 3
    for call in post.call_args_list:
        assert "plaid.com" in call.args[0]
