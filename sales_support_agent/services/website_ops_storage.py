"""PostgreSQL-backed mirror for Website Ops filesystem artifacts.

Website Ops historically wrote reports and state to a Render persistent disk.
That disk prevents zero-downtime deploys and multiple web instances. The mirror
keeps the existing file-based service contract while making PostgreSQL the
durable store and each web instance's filesystem a disposable local cache.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from threading import RLock
from typing import Any

from sqlalchemy import text
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


_MAX_FILE_BYTES = 25 * 1024 * 1024
_CACHE_SYNC_LOCK = RLock()
_RESTORED_CACHE_KEYS: set[str] = set()
# (resolved root, relative path) -> (mtime_ns, size, sha256)
_SNAPSHOT_INDEX: dict[tuple[str, str], tuple[int, int, str]] = {}
logger = logging.getLogger(__name__)


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
    """Copy changed Website Ops files into PostgreSQL.

    Unchanged files are left in place. A process-local mtime cache avoids
    re-reading the mirror on every pulse after the first restore.
    """

    root = root.resolve()
    stats = {"files": 0, "bytes": 0, "skipped": 0, "unchanged": 0}
    if not root.exists():
        return stats
    resolved = str(root)
    existing: dict[str, str] = {}
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT relative_path, content_sha256
                  FROM website_ops_files
                """
            )
        ).mappings()
        for row in rows:
            existing[str(row["relative_path"])] = str(row["content_sha256"])
    pending: list[tuple[str, bytes, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        file_stat = path.stat()
        if file_stat.st_size > _MAX_FILE_BYTES:
            stats["skipped"] += 1
            continue
        relative_path = path.relative_to(root).as_posix()
        cache_key = (resolved, relative_path)
        cached = _SNAPSHOT_INDEX.get(cache_key)
        if (
            cached is not None
            and cached[0] == file_stat.st_mtime_ns
            and cached[1] == file_stat.st_size
        ):
            digest = cached[2]
            content: bytes | None = None
        else:
            content = path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            _SNAPSHOT_INDEX[cache_key] = (
                file_stat.st_mtime_ns,
                file_stat.st_size,
                digest,
            )
        if existing.get(relative_path) == digest:
            stats["unchanged"] += 1
            continue
        if content is None:
            content = path.read_bytes()
        pending.append((relative_path, content, digest))
    if not pending:
        return stats
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
    updated_at = datetime.now(timezone.utc).isoformat()
    with engine.begin() as connection:
        for relative_path, content, digest in pending:
            connection.execute(
                upsert,
                {
                    "relative_path": relative_path,
                    "content": content,
                    "content_sha256": digest,
                    "size_bytes": len(content),
                    "updated_at": updated_at,
                },
            )
            stats["files"] += 1
            stats["bytes"] += len(content)
    return stats


def restore_website_ops_root(engine: Any, root: Path) -> dict[str, int]:
    """Materialize the PostgreSQL mirror into this instance's local cache."""

    root.mkdir(parents=True, exist_ok=True)
    resolved = str(root.resolve())
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
            digest = str(row["content_sha256"])
            if hashlib.sha256(content).hexdigest() != digest:
                stats["skipped"] += 1
                continue
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            file_stat = target.stat()
            _SNAPSHOT_INDEX[(resolved, relative.as_posix())] = (
                file_stat.st_mtime_ns,
                file_stat.st_size,
                digest,
            )
            stats["files"] += 1
            stats["bytes"] += len(content)
    return stats


def ensure_website_ops_mirror(engine: Any, root: Path) -> dict[str, int]:
    """Restore the mirror once per process before Website Ops reads it.

    Cold starts of unrelated routes must not download the mirror. The first
    Website Ops request, or the embedded scheduler's first pulse, materializes
    it. Later work reuses the local cache and does not hydrate again.
    """

    root = Path(root)
    key = str(root.resolve())
    if key in _RESTORED_CACHE_KEYS:
        return {"files": 0, "bytes": 0, "skipped": 0}
    raw = str(root).replace("\\", "/")
    if raw.startswith("/var/data/"):
        stats = snapshot_website_ops_root(engine, root)
    else:
        stats = restore_website_ops_root(engine, root)
    _RESTORED_CACHE_KEYS.add(key)
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


@contextmanager
def website_ops_cache_transaction(engine: Any, root: Path):
    """Own a cache write/snapshot cycle without request clobbering.

    The embedded scheduler uses this before any HTTP request. The first cycle
    on this process restores the mirror under the cache lock, before the body
    reads state. Later cycles only snapshot changed files, so they cannot
    replace newer in-process work with an older database snapshot.
    """

    with _CACHE_SYNC_LOCK:
        ensure_website_ops_mirror(engine, root)
        try:
            yield
        finally:
            snapshot_website_ops_root(engine, root)


_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_CRON_RUN_PATH = "/api/jobs/website-ops/run"


def website_ops_request_persists_state(method: str, path: str) -> bool:
    """Return whether this request must mirror Website Ops files to Postgres.

    Vercel Cron calls ``GET /api/jobs/website-ops/run``. That invocation writes
    run state and pipeline progress, so it has to persist even though it is a
    GET. Read-only GETs, including health, stay out of the snapshot path.
    """

    if str(method or "").upper() in _MUTATING_METHODS:
        return True
    normalized = str(path or "").rstrip("/") or "/"
    return normalized == _CRON_RUN_PATH


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
        settings = getattr(
            request.app.state,
            "website_ops_settings",
            request.app.state.settings,
        )
        engine = request.app.state.session_factory.kw["bind"]
        persists = website_ops_request_persists_state(request.method, request.url.path)
        acquired = _CACHE_SYNC_LOCK.acquire(blocking=False)
        if not acquired:
            if persists:
                return JSONResponse(
                    {"detail": "Website Ops is completing a scheduled pulse. Retry shortly."},
                    status_code=409,
                    headers={"Retry-After": "30"},
                )
            return await call_next(request)
        try:
            try:
                ensure_website_ops_mirror(engine, Path(settings.website_ops_root))
            except Exception:
                logger.exception("Website Ops mirror restore failed")
            response = await call_next(request)
            if persists:
                snapshot_website_ops_root(engine, Path(settings.website_ops_root))
            return response
        finally:
            _CACHE_SYNC_LOCK.release()
