from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from sqlalchemy.pool import NullPool

from sales_support_agent.models import database


def test_vercel_postgres_factory_does_not_hold_idle_pool_connections() -> None:
    engine = MagicMock()
    with (
        patch.dict(os.environ, {"VERCEL": "1"}, clear=False),
        patch.object(database, "create_engine", return_value=engine) as create,
    ):
        factory = database.create_session_factory(
            "postgresql://user:secret@example.test/app"
        )

    assert factory.kw["bind"] is engine
    assert create.call_args.kwargs["poolclass"] is NullPool


def test_vercel_sqlite_keeps_its_normal_local_pool() -> None:
    engine = MagicMock()
    with (
        patch.dict(os.environ, {"VERCEL": "1"}, clear=False),
        patch.object(database, "create_engine", return_value=engine) as create,
    ):
        database.create_session_factory("sqlite:///:memory:")

    assert "poolclass" not in create.call_args.kwargs
    assert create.call_args.kwargs["connect_args"] == {"check_same_thread": False}


def test_cashflow_engine_uses_the_same_serverless_pool_policy() -> None:
    engine = MagicMock()
    with (
        patch.dict(os.environ, {"VERCEL": "1"}, clear=False),
        patch.object(database, "create_engine", return_value=engine) as create,
    ):
        database.init_cashflow_db("postgres://user:secret@example.test/app")

    assert create.call_args.args[0].startswith("postgresql://")
    assert create.call_args.kwargs["poolclass"] is NullPool
