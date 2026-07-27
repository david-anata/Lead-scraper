"""Low-overhead request performance instrumentation for Agent.

The collector is request-scoped through ``ContextVar`` so synchronous FastAPI
handlers running in the request context can contribute SQL timings without
sharing data between requests. Logs intentionally contain route templates and
counts only; SQL text, query parameters, user data, and tokens are excluded.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass
import logging
from time import perf_counter
from typing import Any

from sqlalchemy import event
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger("agent.performance")


@dataclass
class RequestPerformance:
    """Mutable counters owned by one request context."""

    query_count: int = 0
    query_ms: float = 0.0


_current: ContextVar[RequestPerformance | None] = ContextVar(
    "agent_request_performance",
    default=None,
)


def begin_request_performance() -> tuple[RequestPerformance, Token]:
    """Install and return a fresh request collector."""

    collector = RequestPerformance()
    return collector, _current.set(collector)


def end_request_performance(token: Token) -> None:
    """Restore the context that existed before this request."""

    _current.reset(token)


def _before_cursor_execute(
    conn: Any,
    cursor: Any,
    statement: Any,
    parameters: Any,
    context: Any,
    executemany: Any,
) -> None:
    context._agent_query_started_at = perf_counter()


def _after_cursor_execute(
    conn: Any,
    cursor: Any,
    statement: Any,
    parameters: Any,
    context: Any,
    executemany: Any,
) -> None:
    collector = _current.get()
    started_at = getattr(context, "_agent_query_started_at", None)
    if collector is None or started_at is None:
        return
    collector.query_count += 1
    collector.query_ms += max(0.0, (perf_counter() - started_at) * 1000)


def install_engine_performance_hooks(engine: Any) -> None:
    """Attach idempotent SQL timing listeners to one SQLAlchemy engine."""

    if engine is None or getattr(engine, "_agent_performance_hooks", False):
        return
    event.listen(engine, "before_cursor_execute", _before_cursor_execute)
    event.listen(engine, "after_cursor_execute", _after_cursor_execute)
    engine._agent_performance_hooks = True


def _route_name(request: Any) -> str:
    route = request.scope.get("route")
    template = getattr(route, "path", "") if route is not None else ""
    return str(template or request.url.path)


def _response_bytes(response: Any) -> int:
    raw = response.headers.get("content-length", "")
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return -1


def _static_cache_policy(request: Any) -> str:
    if request.query_params.get("v"):
        return "public, max-age=31536000, immutable"
    return "public, max-age=3600, must-revalidate"


class PerformanceMiddleware(BaseHTTPMiddleware):
    """Measure request/SQL time and apply safe static-asset caching."""

    async def dispatch(self, request: Any, call_next: Any) -> Any:
        collector, token = begin_request_performance()
        started_at = perf_counter()
        response = None
        try:
            response = await call_next(request)
            total_ms = max(0.0, (perf_counter() - started_at) * 1000)
            response.headers["Server-Timing"] = (
                f"app;dur={total_ms:.1f}, db;dur={collector.query_ms:.1f};"
                f'desc="{collector.query_count} queries"'
            )
            if request.url.path.startswith(("/static/", "/brand-static/")):
                response.headers["Cache-Control"] = _static_cache_policy(request)
            logger.info(
                "request_performance route=%s method=%s status=%s total_ms=%.1f "
                "db_ms=%.1f queries=%d response_bytes=%d cf_ray=%s",
                _route_name(request),
                request.method,
                getattr(response, "status_code", 0),
                total_ms,
                collector.query_ms,
                collector.query_count,
                _response_bytes(response),
                str(request.headers.get("cf-ray", "") or "-")[:64],
            )
            return response
        finally:
            end_request_performance(token)


def install_performance_middleware(app: Any, engine: Any = None) -> None:
    """Install request middleware and optional SQL timing hooks."""

    install_engine_performance_hooks(engine)
    app.add_middleware(PerformanceMiddleware)
