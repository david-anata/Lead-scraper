"""Database helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base SQLAlchemy declarative model."""


def _register_models() -> None:
    """Import ORM models so Base.metadata is fully populated before schema work."""

    import sales_support_agent.models.entities  # noqa: F401


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, future=True, connect_args=connect_args)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_database(session_factory: sessionmaker[Session]) -> None:
    engine = session_factory.kw.get("bind")
    if engine is None:
        raise RuntimeError("Session factory is missing an engine binding.")
    _register_models()
    if engine.dialect.name == "sqlite":
        Base.metadata.create_all(bind=engine)
        _apply_sqlite_compat_migrations(engine)
        return

    # Production deployments use a persistent Postgres database. Running
    # `create_all` on every boot slows startup enough to interfere with
    # Render's promotion health checks. We only bootstrap a fresh Postgres
    # database when no tables exist yet, then rely on additive compat
    # migrations for subsequent boots.
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    inspector = inspect(engine)
    if not inspector.get_table_names():
        Base.metadata.create_all(bind=engine)
    _apply_postgres_compat_migrations(engine)


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
        "lead_mirrors": {
            "status_key": "ALTER TABLE lead_mirrors ADD COLUMN status_key VARCHAR(128) DEFAULT ''",
            "is_closed": "ALTER TABLE lead_mirrors ADD COLUMN is_closed BOOLEAN DEFAULT 0",
            "is_active": "ALTER TABLE lead_mirrors ADD COLUMN is_active BOOLEAN DEFAULT 0",
            "task_updated_at": "ALTER TABLE lead_mirrors ADD COLUMN task_updated_at DATETIME",
        },
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


def _apply_postgres_compat_migrations(engine: Any) -> None:
    """Apply additive compatibility migrations for persistent Postgres deployments."""

    if engine.dialect.name != "postgresql":
        return

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "lead_mirrors" not in existing_tables:
        return

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                ALTER TABLE lead_mirrors
                ADD COLUMN IF NOT EXISTS status_key VARCHAR(128) NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS is_closed BOOLEAN NOT NULL DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS task_updated_at TIMESTAMPTZ NULL
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE lead_mirrors
                SET
                  status_key = trim(regexp_replace(lower(coalesce(status, '')), '\\s+', ' ', 'g')),
                  task_updated_at = COALESCE(task_updated_at, updated_at)
                WHERE
                  status_key = ''
                  OR task_updated_at IS NULL
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE lead_mirrors
                SET
                  is_closed = CASE
                    WHEN status_key = '' THEN FALSE
                    WHEN status_key IN (
                      'won onboarding',
                      'won active',
                      'lost',
                      'lost not qualified',
                      'won canceled'
                    ) THEN TRUE
                    WHEN status_key LIKE '%won%'
                      OR status_key LIKE '%lost%'
                      OR status_key LIKE '%canceled%'
                      OR status_key LIKE '%cancelled%'
                      OR status_key LIKE '%closed%'
                      OR status_key LIKE '%archive%'
                      OR status_key LIKE '%archived%' THEN TRUE
                    ELSE FALSE
                  END,
                  is_active = CASE
                    WHEN status_key = '' THEN FALSE
                    WHEN status_key IN (
                      'new lead',
                      'contacted cold',
                      'contacted warm',
                      'working qualified',
                      'working needs offer',
                      'working offered',
                      'working negotiating'
                    ) THEN TRUE
                    WHEN status_key IN (
                      'won onboarding',
                      'won active',
                      'lost',
                      'lost not qualified',
                      'won canceled'
                    ) THEN FALSE
                    WHEN status_key LIKE '%won%'
                      OR status_key LIKE '%lost%'
                      OR status_key LIKE '%canceled%'
                      OR status_key LIKE '%cancelled%'
                      OR status_key LIKE '%closed%'
                      OR status_key LIKE '%archive%'
                      OR status_key LIKE '%archived%' THEN FALSE
                    ELSE TRUE
                  END
                WHERE TRUE
                """
            )
        )
        connection.execute(text("CREATE INDEX IF NOT EXISTS lead_mirrors_status_key_idx ON lead_mirrors (status_key)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS lead_mirrors_is_closed_idx ON lead_mirrors (is_closed)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS lead_mirrors_is_active_idx ON lead_mirrors (is_active)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS lead_mirrors_task_updated_at_idx ON lead_mirrors (task_updated_at)"))
