"""Generate the user-visible Agent route inventory.

The application uses FastAPI's lazy included-router objects, so inspecting only
``app.routes`` misses routes that are present at runtime. This generator walks
those routers recursively and records a stable migration ledger.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path

os.environ.setdefault(
    "SALES_AGENT_DB_URL",
    "sqlite:///" + (Path(tempfile.gettempdir()) / "agent_route_inventory.db").as_posix(),
)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.routing import APIRoute  # noqa: E402

from sales_support_agent.main import app  # noqa: E402


def _walk(routes: Iterable[object]) -> Iterable[APIRoute]:
    for route in routes:
        if isinstance(route, APIRoute):
            yield route
            continue
        original = getattr(route, "original_router", None)
        if original is not None:
            yield from _walk(getattr(original, "routes", ()))


def _family(path: str) -> tuple[str, str]:
    rules = (
        (r"^/admin/hr", "HR", "8"),
        (r"^/admin/finances", "Finance", "9"),
        (r"^/admin/sales|^/admin/api/sales", "Sales", "4–5"),
        (r"^/admin/fulfillment", "Fulfillment", "4–5"),
        (r"^/admin/website-ops", "Website Ops", "7"),
        (r"^/admin/building", "Building", "7"),
        (r"^/admin/advertising", "Advertising", "6"),
        (r"^/admin/executive|^/admin/brand", "Executive / Brand", "6"),
        (r"^/admin/access|^/admin/settings|^/admin/auth|^/admin/login", "Access / transition", "3"),
        (r"^/admin", "Admin / shared", "3–6"),
        (r"^/decks|^/public|^/api/public|^/amazon-profit", "Public deliverable", "10"),
    )
    for pattern, family, phase in rules:
        if re.search(pattern, path):
            return family, phase
    return "Service / API", "Exempt"


def _access(path: str) -> str:
    if path.startswith(("/admin/login", "/admin/auth", "/public", "/decks", "/api/public", "/amazon-profit")):
        return "Public or token-gated"
    if path.startswith("/admin"):
        return "Authenticated + route permission"
    return "Service contract"


def _job(path: str, methods: str) -> str:
    if "GET" in methods:
        return "Read, navigate, or download"
    if any(method in methods for method in ("POST", "PUT", "PATCH", "DELETE")):
        return "Mutation; preserve confirmation/audit contract"
    return "Service contract"


def render() -> str:
    rows = []
    seen: set[tuple[str, str]] = set()
    for route in _walk(app.routes):
        methods = ", ".join(sorted((route.methods or set()) - {"HEAD", "OPTIONS"}))
        key = (route.path, methods)
        if key in seen:
            continue
        seen.add(key)
        family, phase = _family(route.path)
        rows.append((family, route.path, methods, _access(route.path), route.name or "—", _job(route.path, methods), phase))
    rows.sort(key=lambda row: (row[0], row[1], row[2]))

    lines = [
        "# Agent route and migration inventory",
        "",
        "Generated from the mounted FastAPI application. Re-run",
        "`python scripts/generate_agent_route_inventory.py --output docs/agent-route-state-inventory.md`",
        "after adding, removing, or moving a route.",
        "",
        f"Routes inventoried: **{len(rows)}**",
        "",
        "| Family | Route | Method | Access | Renderer / handler | Primary job | Phase |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        escaped = [str(value).replace("|", "\\|") for value in row]
        lines.append("| " + " | ".join(f"`{value}`" if index in (1, 2, 4) else value for index, value in enumerate(escaped)) + " |")
    lines.extend(
        [
            "",
            "## State coverage contract",
            "",
            "Every user-visible HTML family must verify the states that apply: default, loading, empty, filtered-empty, partial, stale, error, permission denied, success, long-running, and destructive confirmation.",
            "",
            "Binary, JSON, CSV, webhook, and internal service routes are intentionally exempt from visual migration. Their contracts, security, and error behavior remain in scope for regression testing.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    content = render()
    if args.output:
        args.output.write_text(content, encoding="utf-8")
    else:
        print(content, end="")


if __name__ == "__main__":
    main()
