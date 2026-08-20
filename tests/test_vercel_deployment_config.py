"""Regression coverage for the production Vercel function boundary."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_vercel_routes_all_requests_to_the_fastapi_python_function() -> None:
    config = json.loads((ROOT / "vercel.json").read_text(encoding="utf-8"))

    service = config["services"]["sales-support-agent"]
    assert service["entrypoint"] == "api/index.py"
    assert service["framework"] == "fastapi"
    assert service["functions"] == {
        "api/index.py": {
            "maxDuration": 800,
            "memory": 2048,
            "regions": ["pdx1"],
        }
    }
    assert all(
        rewrite["destination"]["service"] == "sales-support-agent"
        for rewrite in config["rewrites"]
    )


def test_vercel_entrypoint_exports_the_complete_fastapi_application() -> None:
    entrypoint = (ROOT / "api" / "index.py").read_text(encoding="utf-8")

    assert "from sales_support_agent.main import app" in entrypoint
    assert '__all__ = ["app"]' in entrypoint
