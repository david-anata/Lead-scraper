"""Post-deploy smoke gate for Agent production."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _get(url: str) -> tuple[int, dict]:
    request = Request(url, headers={"User-Agent": "agent-release-verifier/1.0"})
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read())
    except HTTPError as exc:
        try:
            payload = json.loads(exc.read())
        except ValueError:
            payload = {}
        return exc.code, payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://agent.anatainc.com")
    parser.add_argument("--expected-commit", default="")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    try:
        live_status, live = _get(f"{base}/health/live")
        ready_status, ready = _get(f"{base}/health/ready")
        storage_status, storage = _get(f"{base}/health/storage")
    except (URLError, TimeoutError) as exc:
        print(f"production verification failed: {exc}", file=sys.stderr)
        return 1
    checks = {
        "liveness": live_status == 200 and live.get("status") == "live",
        "readiness": ready_status == 200 and ready.get("status") == "ready",
        "storage": storage_status == 200 and storage.get("status") == "ready",
        "commit": (
            not args.expected_commit
            or ready.get("render_git_commit") == args.expected_commit
        ),
    }
    print(json.dumps({"checks": checks, "ready": ready, "storage": storage}))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
