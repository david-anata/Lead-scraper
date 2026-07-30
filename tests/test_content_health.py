from __future__ import annotations

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
