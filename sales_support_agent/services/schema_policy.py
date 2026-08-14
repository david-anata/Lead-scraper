"""Policy boundary between controlled migrations and restricted app requests."""

from __future__ import annotations

import os
from typing import Any


def schema_maintenance_allowed(engine: Any, *, force: bool = False) -> bool:
    """Return whether this process may execute schema DDL on the target engine."""

    if force or engine.dialect.name != "postgresql":
        return True
    return os.getenv("AGENT_RUNTIME_SCHEMA_MAINTENANCE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
