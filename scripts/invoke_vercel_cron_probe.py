"""Invoke a protected Vercel cron probe without printing its credential."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


def _dotenv_value(path: Path, key: str) -> str:
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.startswith(f"{key}="):
            continue
        value = raw_line.split("=", 1)[1].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        return value.replace("\\n", "\n")
    raise RuntimeError(f"{key} is missing from the supplied environment file.")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    secret = _dotenv_value(args.env_file, "CRON_SECRET")
    request = Request(args.url, headers={"Authorization": f"Bearer {secret}"})
    with urlopen(request, timeout=60) as response:  # noqa: S310 - operator-supplied HTTPS URL
        payload = json.loads(response.read().decode("utf-8"))
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
