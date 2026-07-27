"""PostgreSQL-backed mirror for Website Ops filesystem artifacts.

Website Ops historically wrote reports and state to a Render persistent disk.
That disk prevents zero-downtime deploys and multiple web instances. The mirror
keeps the existing file-based service contract while making PostgreSQL the
durable store and each web instance's filesystem a disposable local cache.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

from sqlalchemy import text
from starlette.middleware.base import BaseHTTPMiddleware


_MAX_FILE_BYTES = 25 * 1024 * 1024


def ensure_website_ops_storage_schema(engine: Any) -> None:
    binary_type = "BYTEA" if engine.dialect.name == "postgresql" else "BLOB"
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS website_ops_files (
                    relative_path VARCHAR(1024) PRIMARY KEY,
                    content {binary_type} NOT NULL,
                    content_sha256 VARCHAR(64) NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    updated_at VARCHAR(64) NOT NULL
                )
                """
            )
        )


def snapshot_website_ops_root(engine: Any, root: Path) -> dict[str, int]:
    """Copy the current durable-disk tree into PostgreSQL."""

    root = root.resolve()
    if not root.exists():
        return {"files": 0, "bytes": 0, "skipped": 0}
    stats = {"files": 0, "bytes": 0, "skipped": 0}
    upsert = text(
        """
        INSERT INTO website_ops_files (
            relative_path, content, content_sha256, size_bytes, updated_at
        ) VALUES (
            :relative_path, :content, :content_sha256, :size_bytes, :updated_at
        )
        ON CONFLICT (relative_path) DO UPDATE SET
            content = EXCLUDED.content,
            content_sha256 = EXCLUDED.content_sha256,
            size_bytes = EXCLUDED.size_bytes,
            updated_at = EXCLUDED.updated_at
        """
    )
    with engine.begin() as connection:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            size = path.stat().st_size
            if size > _MAX_FILE_BYTES:
                stats["skipped"] += 1
                continue
            content = path.read_bytes()
            relative_path = path.relative_to(root).as_posix()
            connection.execute(
                upsert,
                {
                    "relative_path": relative_path,
                    "content": content,
                    "content_sha256": hashlib.sha256(content).hexdigest(),
                    "size_bytes": len(content),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            stats["files"] += 1
            stats["bytes"] += len(content)
    return stats


def restore_website_ops_root(engine: Any, root: Path) -> dict[str, int]:
    """Materialize the PostgreSQL mirror into this instance's local cache."""

    root.mkdir(parents=True, exist_ok=True)
    stats = {"files": 0, "bytes": 0, "skipped": 0}
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT relative_path, content, content_sha256
                  FROM website_ops_files
                 ORDER BY relative_path
                """
            )
        ).mappings()
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


def website_ops_storage_status(engine: Any) -> dict[str, int]:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT COUNT(*) AS files, COALESCE(SUM(size_bytes), 0) AS bytes
                  FROM website_ops_files
                """
            )
        ).mappings().one()
    return {"files": int(row["files"]), "bytes": int(row["bytes"])}


def database_mirror_enabled() -> bool:
    import os

    return os.getenv("WEBSITE_OPS_DATABASE_MIRROR", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def synchronize_website_ops_cache(settings: Any, engine: Any) -> dict[str, int]:
    """Snapshot the old Render disk, otherwise hydrate an ephemeral cache."""

    root = Path(settings.website_ops_root)
    if str(root).replace("\\", "/").startswith("/var/data/"):
        return snapshot_website_ops_root(engine, root)
    return restore_website_ops_root(engine, root)


class WebsiteOpsStorageMiddleware(BaseHTTPMiddleware):
    """Keep each web instance's disposable Website Ops cache synchronized."""

    _PREFIXES = (
        "/admin/website-ops",
        "/admin/api/website-ops",
        "/api/jobs/website-ops",
    )

    async def dispatch(self, request: Any, call_next: Any) -> Any:
        if not database_mirror_enabled() or not request.url.path.startswith(
            self._PREFIXES
        ):
            return await call_next(request)
        settings = request.app.state.settings
        engine = request.app.state.session_factory.kw["bind"]
        restore_website_ops_root(engine, Path(settings.website_ops_root))
        response = await call_next(request)
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            snapshot_website_ops_root(engine, Path(settings.website_ops_root))
        return response
