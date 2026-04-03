"""Database helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base SQLAlchemy declarative model."""


# Module-level engine reference — set by create_session_factory so cashflow
# services can import it directly without needing the request object.
engine: Any = None


def _register_models() -> None:
    """Import ORM models so Base.metadata is fully populated before schema work."""

    import sales_support_agent.models.entities  # noqa: F401


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    global engine
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

    # Cashflow tables — created by create_all on fresh DBs; for existing
    # Postgres deployments we create them if absent.
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS cash_events (
                    id                      TEXT         PRIMARY KEY,
                    source                  VARCHAR(32)  NOT NULL DEFAULT 'manual',
                    source_id               VARCHAR(255) NOT NULL DEFAULT '',
                    event_type              VARCHAR(16)  NOT NULL DEFAULT 'outflow',
                    category                VARCHAR(64)  NOT NULL DEFAULT 'uncategorized',
                    subcategory             VARCHAR(64)  NOT NULL DEFAULT '',
                    name                    VARCHAR(255) NOT NULL DEFAULT '',
                    description             TEXT         NOT NULL DEFAULT '',
                    vendor_or_customer      VARCHAR(255) NOT NULL DEFAULT '',
                    amount_cents            INTEGER      NOT NULL DEFAULT 0,
                    due_date                TIMESTAMPTZ  NULL,
                    effective_date          TIMESTAMPTZ  NULL,
                    expected_date           TIMESTAMPTZ  NULL,
                    status                  VARCHAR(32)  NOT NULL DEFAULT 'planned',
                    confidence              VARCHAR(16)  NOT NULL DEFAULT 'estimated',
                    recurring_template_id   TEXT         NULL,
                    recurring_rule          VARCHAR(64)  NOT NULL DEFAULT '',
                    matched_to_id           TEXT         NULL,
                    clickup_task_id         VARCHAR(64)  NOT NULL DEFAULT '',
                    account_balance_cents   INTEGER      NULL,
                    bank_transaction_type   VARCHAR(32)  NOT NULL DEFAULT '',
                    bank_reference          VARCHAR(128) NOT NULL DEFAULT '',
                    notes                   TEXT         NOT NULL DEFAULT '',
                    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS recurring_templates (
                    id                    TEXT         PRIMARY KEY,
                    name                  VARCHAR(255) NOT NULL DEFAULT '',
                    vendor_or_customer    VARCHAR(255) NOT NULL DEFAULT '',
                    event_type            VARCHAR(16)  NOT NULL DEFAULT 'outflow',
                    category              VARCHAR(64)  NOT NULL DEFAULT 'uncategorized',
                    amount_cents          INTEGER      NOT NULL DEFAULT 0,
                    confidence            VARCHAR(16)  NOT NULL DEFAULT 'estimated',
                    notes                 TEXT         NOT NULL DEFAULT '',
                    frequency             VARCHAR(32)  NOT NULL DEFAULT 'monthly',
                    next_due_date         TIMESTAMPTZ  NULL,
                    day_of_month          INTEGER      NULL,
                    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
                    clickup_task_id       VARCHAR(64)  NOT NULL DEFAULT '',
                    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        # Migrate existing SERIAL (integer) id columns to TEXT for UUID support.
        # This is a no-op when tables are freshly created with TEXT primary keys.
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'cash_events'
                    AND column_name = 'id'
                    AND data_type = 'integer'
                  ) THEN
                    ALTER TABLE cash_events DROP CONSTRAINT IF EXISTS cash_events_pkey CASCADE;
                    ALTER TABLE cash_events ALTER COLUMN id DROP DEFAULT;
                    ALTER TABLE cash_events ALTER COLUMN id TYPE TEXT USING id::TEXT;
                    ALTER TABLE cash_events ADD PRIMARY KEY (id);
                    ALTER TABLE cash_events ALTER COLUMN recurring_template_id TYPE TEXT USING recurring_template_id::TEXT;
                    ALTER TABLE cash_events ALTER COLUMN matched_to_id TYPE TEXT USING matched_to_id::TEXT;
                  END IF;
                END $$;
                """
            )
        )
        connection.execute(
            text(
                """
                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'recurring_templates'
                    AND column_name = 'id'
                    AND data_type = 'integer'
                  ) THEN
                    ALTER TABLE recurring_templates DROP CONSTRAINT IF EXISTS recurring_templates_pkey CASCADE;
                    ALTER TABLE recurring_templates ALTER COLUMN id DROP DEFAULT;
                    ALTER TABLE recurring_templates ALTER COLUMN id TYPE TEXT USING id::TEXT;
                    ALTER TABLE recurring_templates ADD PRIMARY KEY (id);
                  END IF;
                END $$;
                """
            )
        )
        # Indexes for the most common cashflow queries
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_due_date ON cash_events (due_date)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_status ON cash_events (status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_event_type ON cash_events (event_type)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_source ON cash_events (source)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_due_date_status ON cash_events (due_date, status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_source_source_id ON cash_events (source, source_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_matched_to_id ON cash_events (matched_to_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_clickup_task_id ON cash_events (clickup_task_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recurring_templates_is_active ON recurring_templates (is_active)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recurring_templates_next_due_date ON recurring_templates (next_due_date)"))
