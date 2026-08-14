"""Fail closed when a migration target cannot safely hold the source snapshot.

Required environment variables:
    MIGRATION_TARGET_DATABASE_URL
    MIGRATION_SOURCE_SNAPSHOT_BYTES

Optional environment variables:
    MIGRATION_CAPACITY_HEADROOM_MULTIPLIER (default: 2.0)

The script is read-only. It prints a credential-free JSON receipt and exits
non-zero when the target's advertised project limit cannot accommodate the
current target database plus the source snapshot and configured headroom.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

from sqlalchemy import create_engine, text


def _positive_int(name: str) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive number")
    return value


def assess_capacity(
    *,
    source_snapshot_bytes: int,
    target_database_bytes: int,
    project_limit_bytes: int | None,
    headroom_multiplier: float = 2.0,
) -> dict[str, Any]:
    """Return a deterministic capacity decision without touching a database."""

    required_bytes = target_database_bytes + int(source_snapshot_bytes * headroom_multiplier)
    capacity_known = project_limit_bytes is not None and project_limit_bytes > 0
    fits = capacity_known and required_bytes <= project_limit_bytes
    return {
        "ok": fits,
        "capacity_known": capacity_known,
        "source_snapshot_bytes": source_snapshot_bytes,
        "target_database_bytes": target_database_bytes,
        "headroom_multiplier": headroom_multiplier,
        "required_bytes": required_bytes,
        "project_limit_bytes": project_limit_bytes,
        "available_bytes": (
            max(project_limit_bytes - target_database_bytes, 0)
            if capacity_known and project_limit_bytes is not None
            else None
        ),
    }


def _target_capacity(url: str) -> tuple[int, int | None]:
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            connection.execute(text("SET TRANSACTION READ ONLY"))
            database_bytes = int(
                connection.execute(text("SELECT pg_database_size(current_database())")).scalar_one()
            )
            raw_limit = connection.execute(
                text(
                    "SELECT pg_size_bytes("
                    "current_setting('neon.max_cluster_size', true)"
                    ")"
                )
            ).scalar_one_or_none()
            transaction.rollback()
    finally:
        engine.dispose()

    try:
        project_limit = int(raw_limit) if raw_limit else None
    except (TypeError, ValueError):
        project_limit = None
    return database_bytes, project_limit


def main() -> int:
    target_url = os.getenv("MIGRATION_TARGET_DATABASE_URL", "").strip()
    if not target_url:
        raise RuntimeError("Missing required environment variable: MIGRATION_TARGET_DATABASE_URL")
    source_bytes = _positive_int("MIGRATION_SOURCE_SNAPSHOT_BYTES")
    multiplier = _positive_float("MIGRATION_CAPACITY_HEADROOM_MULTIPLIER", 2.0)
    target_bytes, project_limit = _target_capacity(target_url)
    receipt = assess_capacity(
        source_snapshot_bytes=source_bytes,
        target_database_bytes=target_bytes,
        project_limit_bytes=project_limit,
        headroom_multiplier=multiplier,
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0 if receipt["ok"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        raise SystemExit(2) from exc
