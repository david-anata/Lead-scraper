"""CREATE TABLE guards for the outbound tables.

Postgres checks CREATE privilege before it honors IF NOT EXISTS. The runtime
role agent_app has no CREATE on schema public, so a table that already exists
still raises permission denied. Callers check for the relation first and skip
CREATE when it is present. A CREATE that still fails with InsufficientPrivilege
is logged once and ignored.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import inspect, text

logger = logging.getLogger(__name__)

_privilege_logged: set[str] = set()


def _is_postgres(engine: Any) -> bool:
    dialect = str(getattr(getattr(engine, "dialect", None), "name", "") or "")
    if dialect:
        return dialect == "postgresql"
    return "postgres" in str(getattr(engine, "url", "")).lower()


def relation_exists(engine: Any, table: str) -> bool:
    """True when ``table`` is already present.

    Postgres uses ``to_regclass`` so the check is a read, not DDL. Other
    engines, including SQLite, use SQLAlchemy's inspector.
    """

    try:
        if _is_postgres(engine):
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT to_regclass(:name)"),
                    {"name": f"public.{table}"},
                ).fetchone()
            return bool(row and row[0])
        return bool(inspect(engine).has_table(table))
    except Exception:  # noqa: BLE001
        logger.exception("[outbound-ddl] could not check whether %s exists", table)
        return False


def _is_insufficient_privilege(exc: BaseException) -> bool:
    seen: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in seen:
        seen.append(current)
        if type(current).__name__ == "InsufficientPrivilege":
            return True
        if getattr(current, "pgcode", None) == "42501":
            return True
        current = getattr(current, "orig", None) or current.__cause__
    message = str(exc).lower()
    return "permission denied for schema" in message or "insufficientprivilege" in message


def _log_privilege_once(table: str, exc: BaseException) -> None:
    if table in _privilege_logged:
        return
    _privilege_logged.add(table)
    logger.warning(
        "[outbound-ddl] CREATE for %s skipped after InsufficientPrivilege: %s",
        table,
        exc,
    )


def create_table_if_absent(engine: Any, sql: str, table: str) -> None:
    """Run ``sql`` only when ``table`` is missing.

    SQLite keeps its existing CREATE TABLE IF NOT EXISTS path. Postgres skips
    the statement when ``to_regclass`` finds the table. InsufficientPrivilege
    is logged once per table and does not propagate.
    """

    if engine is None or relation_exists(engine, table):
        return
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
    except Exception as exc:  # noqa: BLE001
        if _is_insufficient_privilege(exc):
            _log_privilege_once(table, exc)
            return
        raise
