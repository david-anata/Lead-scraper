"""Durable guards shared by website-reachable Agent endpoints."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy import select, text

from sales_support_agent.models.database import session_scope
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.audit import AuditService


RATE_LIMIT_RUN_TYPE_PREFIX = "marketing_limit_"
_CLIENT_KEY_RE = re.compile(r"^[a-f0-9]{64}$")


def _client_key(request: Request) -> str:
    provided = str(
        request.headers.get("X-Marketing-Client-Key", "") or ""
    ).strip().lower()
    if _CLIENT_KEY_RE.fullmatch(provided):
        return provided

    settings = request.app.state.settings
    salt = str(
        getattr(settings, "marketing_site_intake_key", "") or ""
    )
    host = str(request.client.host if request.client else "unknown")
    return hashlib.sha256(f"{salt}|{host}".encode("utf-8")).hexdigest()


def durable_rate_limited(
    request: Request,
    *,
    scope: str,
    limit: int,
    window_seconds: int = 600,
) -> bool:
    """Cross-instance, fixed-query limiter with no raw visitor IP storage."""

    now = datetime.now(timezone.utc)
    bucket = int(now.timestamp()) // window_seconds
    # Keep one durable row per scope and anonymized client. The active window
    # belongs in metadata so normal traffic does not create a permanent audit
    # row every ten minutes forever.
    material = f"{scope}|{_client_key(request)}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    run_type = f"{RATE_LIMIT_RUN_TYPE_PREFIX}{digest[:40]}"

    with session_scope(request.app.state.session_factory) as session:
        bind = session.get_bind()
        if bind.dialect.name == "postgresql":
            lock_key = int.from_bytes(
                bytes.fromhex(digest[:16]),
                "big",
                signed=True,
            )
            session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": lock_key},
            )
        row = session.execute(
            select(AutomationRun)
            .where(AutomationRun.run_type == run_type)
            .order_by(AutomationRun.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            AuditService(session).start_run(
                run_type,
                trigger="public_rate_limit",
                metadata={
                    "scope": scope,
                    "bucket": bucket,
                    "count": 1,
                    "window_seconds": window_seconds,
                },
            )
            return False

        metadata = dict(row.metadata_json or {})
        if int(metadata.get("bucket", -1) or -1) != bucket:
            count = 1
        else:
            count = int(metadata.get("count", 0) or 0) + 1
        row.metadata_json = {
            **metadata,
            "bucket": bucket,
            "count": count,
            "window_seconds": window_seconds,
        }
        session.add(row)
        return count > limit


def durable_rate_limit_response(
    request: Request,
    *,
    scope: str,
    limit: int,
    window_seconds: int = 600,
) -> Optional[JSONResponse]:
    if not durable_rate_limited(
        request,
        scope=scope,
        limit=limit,
        window_seconds=window_seconds,
    ):
        return None
    return JSONResponse(
        status_code=429,
        headers={"Retry-After": str(window_seconds)},
        content={"reason": "rate_limited"},
    )
