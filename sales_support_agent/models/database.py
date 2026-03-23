"""Database helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base SQLAlchemy declarative model."""


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, future=True, connect_args=connect_args)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_database(session_factory: sessionmaker[Session]) -> None:
    engine = session_factory.kw.get("bind")
    if engine is None:
        raise RuntimeError("Session factory is missing an engine binding.")
    if engine.dialect.name == "sqlite":
        Base.metadata.create_all(bind=engine)
        _apply_sqlite_compat_migrations(engine)
        return

    # Production deployments use a persistent Postgres database. Running
    # `create_all` on every boot slows startup enough to interfere with
    # Render's promotion health checks, so we do a lightweight connectivity
    # probe instead.
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


@contextmanager
def session_scope(session_factory: sessionmaker[Session]):
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _apply_sqlite_compat_migrations(engine: Any) -> None:
    """Add newly introduced columns for existing SQLite deployments.

    This keeps small single-file deployments moving without a separate
    migration tool while the schema is still evolving.
    """

    if engine.dialect.name != "sqlite":
        return

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    migrations: dict[str, dict[str, str]] = {
        "communication_events": {
            "external_event_key": "ALTER TABLE communication_events ADD COLUMN external_event_key VARCHAR(255) DEFAULT ''",
        },
    }

    with engine.begin() as connection:
        for table_name, column_migrations in migrations.items():
            if table_name not in existing_tables:
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, statement in column_migrations.items():
                if column_name in existing_columns:
                    continue
                connection.execute(text(statement))
