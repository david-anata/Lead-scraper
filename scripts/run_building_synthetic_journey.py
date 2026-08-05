"""Run the disposable Building golden path and verify local artifact cleanup."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


PATTERNS = (
    "building_event_billing_journey_*.db",
    "building_transactional_*.db",
)
TESTS = (
    "tests/test_building_event_billing_journey.py",
    "tests/test_building_transactional_lifecycle.py",
)


def _artifacts() -> set[Path]:
    root = Path(tempfile.gettempdir())
    return {
        path.resolve()
        for pattern in PATTERNS
        for path in root.glob(pattern)
        if path.is_file()
    }


def main() -> int:
    before = _artifacts()
    result = subprocess.run(
        [sys.executable, "-m", "pytest", *TESTS, "-q", "--maxfail=1"],
        check=False,
    )
    created = _artifacts() - before
    cleanup_errors: list[str] = []
    for path in created:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            cleanup_errors.append(f"{path.name}: {exc}")
    leftovers = sorted(path.name for path in created if path.exists())
    report = {
        "journey": "building-intake-to-confirmation",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "test_exit_code": result.returncode,
        "provider_mode": "isolated-dry-run",
        "live_provider_objects_created": False,
        "created_test_databases": len(created),
        "cleanup_verified": not leftovers and not cleanup_errors,
        "leftovers": leftovers,
        "cleanup_errors": cleanup_errors,
    }
    print(json.dumps(report, sort_keys=True))
    return 0 if result.returncode == 0 and report["cleanup_verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
