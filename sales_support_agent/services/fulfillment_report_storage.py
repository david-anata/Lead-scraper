"""Durable PostgreSQL mirror for Fulfillment CS report artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware


_MAX_REPORT_BYTES = 25 * 1024 * 1024


def ensure_fulfillment_report_storage_schema(engine: Any) -> None:
    binary_type = "BYTEA" if engine.dialect.name == "postgresql" else "BLOB"
    with engine.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS fulfillment_report_files (
                relative_path VARCHAR(1024) PRIMARY KEY,
                content {binary_type} NOT NULL,
                content_sha256 VARCHAR(64) NOT NULL,
                size_bytes INTEGER NOT NULL,
                updated_at VARCHAR(64) NOT NULL
            )
        """))


def snapshot_fulfillment_reports(engine: Any, root: Path) -> dict[str, int]:
    root = root.resolve()
    stats = {"files": 0, "bytes": 0, "skipped": 0}
    if not root.exists():
        return stats
    upsert = text("""
        INSERT INTO fulfillment_report_files (
            relative_path, content, content_sha256, size_bytes, updated_at
        ) VALUES (
            :relative_path, :content, :content_sha256, :size_bytes, :updated_at
        ) ON CONFLICT (relative_path) DO UPDATE SET
            content = EXCLUDED.content,
            content_sha256 = EXCLUDED.content_sha256,
            size_bytes = EXCLUDED.size_bytes,
            updated_at = EXCLUDED.updated_at
    """)
    with engine.begin() as connection:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            size = path.stat().st_size
            if size > _MAX_REPORT_BYTES:
                stats["skipped"] += 1
                continue
            content = path.read_bytes()
            connection.execute(upsert, {
                "relative_path": path.relative_to(root).as_posix(),
                "content": content,
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            stats["files"] += 1
            stats["bytes"] += len(content)
    return stats


def restore_fulfillment_reports(engine: Any, root: Path) -> dict[str, int]:
    root.mkdir(parents=True, exist_ok=True)
    stats = {"files": 0, "bytes": 0, "skipped": 0}
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT relative_path, content, content_sha256
            FROM fulfillment_report_files ORDER BY relative_path
        """)).mappings()
        for row in rows:
            relative = Path(str(row["relative_path"]))
            if relative.is_absolute() or ".." in relative.parts:
                stats["skipped"] += 1
                continue
            content = bytes(row["content"])
            if hashlib.sha256(content).hexdigest() != row["content_sha256"]:
                stats["skipped"] += 1
                continue
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            stats["files"] += 1
            stats["bytes"] += len(content)
    return stats


def synchronize_fulfillment_reports(engine: Any, root: Path) -> dict[str, int]:
    """Snapshot Render's durable disk or hydrate Vercel's temporary cache."""

    normalized = str(root).replace("\\", "/")
    if normalized.startswith("/var/data/"):
        return snapshot_fulfillment_reports(engine, root)
    return restore_fulfillment_reports(engine, root)


class FulfillmentReportStorageMiddleware(BaseHTTPMiddleware):
    """Hydrate Vercel's report cache only when a CS report route needs it."""

    _PREFIXES = ("/admin/fulfillment/cs",)

    def __init__(self, app: Any) -> None:
        super().__init__(app)
        self._hydrated = False

    async def dispatch(self, request: Any, call_next: Any) -> Any:
        if not request.url.path.startswith(self._PREFIXES):
            return await call_next(request)
        if not self._hydrated:
            synchronize_fulfillment_reports(
                request.app.state.session_factory.kw["bind"],
                request.app.state.settings.fulfillment_cs_reports_dir,
            )
            self._hydrated = True
        return await call_next(request)
