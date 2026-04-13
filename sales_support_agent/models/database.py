"""Database helpers."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Base SQLAlchemy declarative model."""


# Module-level engine reference — set by create_session_factory so cashflow
# services can import it directly without needing the request object.
engine: Any = None


def get_engine():
    """Return the initialized SQLAlchemy engine.

    Raises RuntimeError if called before init_cashflow_db().
    Use this instead of importing `engine` directly.
    """
    if engine is None:
        raise RuntimeError(
            "Database engine not initialized. "
            "Call init_cashflow_db() before accessing the engine."
        )
    return engine


def _register_models() -> None:
    """Import ORM models so Base.metadata is fully populated before schema work."""

    import sales_support_agent.models.entities  # noqa: F401


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    global engine
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, future=True, connect_args=connect_args)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_cashflow_db(db_url: str) -> None:
    """Re-initialize the module-level engine from a specific DB URL.
    Call this at app startup to ensure cashflow services use the same DB as the main app."""
    global engine
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    engine = create_engine(db_url, future=True, connect_args=connect_args)
    logger.info("Cashflow DB initialized: %s", db_url[:40])


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
        "cash_events": {
            "subcategory":           "ALTER TABLE cash_events ADD COLUMN subcategory VARCHAR(64) NOT NULL DEFAULT ''",
            "description":           "ALTER TABLE cash_events ADD COLUMN description TEXT NOT NULL DEFAULT ''",
            "bank_transaction_type": "ALTER TABLE cash_events ADD COLUMN bank_transaction_type VARCHAR(32) NOT NULL DEFAULT ''",
            "bank_reference":        "ALTER TABLE cash_events ADD COLUMN bank_reference VARCHAR(128) NOT NULL DEFAULT ''",
            "notes":                 "ALTER TABLE cash_events ADD COLUMN notes TEXT NOT NULL DEFAULT ''",
            "recurring_rule":        "ALTER TABLE cash_events ADD COLUMN recurring_rule VARCHAR(64) NOT NULL DEFAULT ''",
            "clickup_task_id":       "ALTER TABLE cash_events ADD COLUMN clickup_task_id VARCHAR(64) NOT NULL DEFAULT ''",
            "account_balance_cents": "ALTER TABLE cash_events ADD COLUMN account_balance_cents INTEGER",
            "effective_date":        "ALTER TABLE cash_events ADD COLUMN effective_date DATETIME",
            "expected_date":         "ALTER TABLE cash_events ADD COLUMN expected_date DATETIME",
            "recurring_template_id": "ALTER TABLE cash_events ADD COLUMN recurring_template_id TEXT",
            "matched_to_id":         "ALTER TABLE cash_events ADD COLUMN matched_to_id TEXT",
            "friendly_name":         "ALTER TABLE cash_events ADD COLUMN friendly_name TEXT",
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

        # QuickBooks OAuth tables for SQLite deployments
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS quickbooks_tokens (
                id            TEXT PRIMARY KEY DEFAULT 'singleton',
                access_token  TEXT NOT NULL DEFAULT '',
                refresh_token TEXT NOT NULL DEFAULT '',
                realm_id      TEXT NOT NULL DEFAULT '',
                expires_at    TEXT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS qb_oauth_state (
                state      TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                expires_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
        """))

    # Add indexes for Calendar/Ledger range queries
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_due_date ON cash_events(due_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_source ON cash_events(source)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_status ON cash_events(status)"))


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
        # Additive migrations — ensure columns added after initial deployment exist.
        # ADD COLUMN IF NOT EXISTS is a no-op when the column already exists.
        connection.execute(
            text(
                """
                ALTER TABLE cash_events
                    ADD COLUMN IF NOT EXISTS subcategory           VARCHAR(64)  NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS description           TEXT         NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS bank_transaction_type VARCHAR(32)  NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS bank_reference        VARCHAR(128) NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS notes                 TEXT         NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS recurring_rule        VARCHAR(64)  NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS clickup_task_id       VARCHAR(64)  NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS account_balance_cents INTEGER      NULL,
                    ADD COLUMN IF NOT EXISTS effective_date        TIMESTAMPTZ  NULL,
                    ADD COLUMN IF NOT EXISTS expected_date         TIMESTAMPTZ  NULL,
                    ADD COLUMN IF NOT EXISTS recurring_template_id TEXT         NULL,
                    ADD COLUMN IF NOT EXISTS matched_to_id         TEXT         NULL
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
        # friendly_name column — additive migration for existing Postgres deployments
        connection.execute(text("ALTER TABLE cash_events ADD COLUMN IF NOT EXISTS friendly_name TEXT NULL"))

    # QuickBooks OAuth token + state tables
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS quickbooks_tokens (
                id           TEXT        PRIMARY KEY DEFAULT 'singleton',
                access_token TEXT        NOT NULL DEFAULT '',
                refresh_token TEXT       NOT NULL DEFAULT '',
                realm_id     TEXT        NOT NULL DEFAULT '',
                expires_at   TIMESTAMPTZ NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS qb_oauth_state (
                state      TEXT        PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
        """))


# Canonical column order for cash_events upsert
_CASH_EVENT_UPSERT_COLS = (
    "id", "source", "source_id", "event_type", "category", "subcategory",
    "description", "name", "vendor_or_customer", "amount_cents", "due_date",
    "status", "confidence", "recurring_rule", "clickup_task_id",
    "bank_transaction_type", "bank_reference", "notes", "friendly_name",
)


def upsert_cash_event(conn, event: dict) -> str:
    """Insert or update a cash_event row.

    Args:
        conn: An active SQLAlchemy connection (inside engine.begin()).
        event: Dict with cash_event fields. Must include 'id'.
            Due dates should be str (ISO format) or date objects.

    Returns:
        'created' if a new row was inserted, 'updated' if an existing row was updated.
    """
    from datetime import date, datetime

    now_str = datetime.utcnow().isoformat()

    # Normalize due_date to ISO string
    due_date_val = event.get("due_date")
    if isinstance(due_date_val, (date, datetime)):
        due_str = due_date_val.isoformat()[:10]
    elif due_date_val:
        due_str = str(due_date_val)[:10]
    else:
        due_str = None

    existing = conn.execute(
        text("SELECT id FROM cash_events WHERE id = :id"),
        {"id": event["id"]},
    ).fetchone()

    if existing:
        conn.execute(
            text("""
                UPDATE cash_events SET
                    source=:source, source_id=:source_id,
                    event_type=:event_type, category=:category,
                    subcategory=:subcategory, description=:description,
                    name=:name, vendor_or_customer=:vendor_or_customer,
                    amount_cents=:amount_cents, due_date=:due_date,
                    status=:status, confidence=:confidence,
                    recurring_rule=:recurring_rule,
                    clickup_task_id=:clickup_task_id,
                    bank_transaction_type=:bank_transaction_type,
                    bank_reference=:bank_reference,
                    notes=:notes, friendly_name=:friendly_name,
                    updated_at=:updated_at
                WHERE id=:id
            """),
            {
                "id": event["id"],
                "source": event.get("source", ""),
                "source_id": event.get("source_id", event["id"]),
                "event_type": event.get("event_type", "outflow"),
                "category": event.get("category", "other"),
                "subcategory": event.get("subcategory", ""),
                "description": event.get("description", ""),
                "name": event.get("name", ""),
                "vendor_or_customer": event.get("vendor_or_customer", ""),
                "amount_cents": event.get("amount_cents", 0),
                "due_date": due_str,
                "status": event.get("status", "planned"),
                "confidence": event.get("confidence", "estimated"),
                "recurring_rule": event.get("recurring_rule", ""),
                "clickup_task_id": event.get("clickup_task_id", ""),
                "bank_transaction_type": event.get("bank_transaction_type", ""),
                "bank_reference": event.get("bank_reference", ""),
                "notes": event.get("notes", ""),
                "friendly_name": event.get("friendly_name"),
                "updated_at": now_str,
            },
        )
        return "updated"
    else:
        conn.execute(
            text("""
                INSERT INTO cash_events (
                    id, source, source_id, event_type, category,
                    subcategory, description, name, vendor_or_customer,
                    amount_cents, due_date, status, confidence,
                    recurring_rule, clickup_task_id,
                    bank_transaction_type, bank_reference,
                    notes, friendly_name, created_at, updated_at
                ) VALUES (
                    :id, :source, :source_id, :event_type, :category,
                    :subcategory, :description, :name, :vendor_or_customer,
                    :amount_cents, :due_date, :status, :confidence,
                    :recurring_rule, :clickup_task_id,
                    :bank_transaction_type, :bank_reference,
                    :notes, :friendly_name, :created_at, :updated_at
                )
            """),
            {
                "id": event["id"],
                "source": event.get("source", ""),
                "source_id": event.get("source_id", event["id"]),
                "event_type": event.get("event_type", "outflow"),
                "category": event.get("category", "other"),
                "subcategory": event.get("subcategory", ""),
                "description": event.get("description", ""),
                "name": event.get("name", ""),
                "vendor_or_customer": event.get("vendor_or_customer", ""),
                "amount_cents": event.get("amount_cents", 0),
                "due_date": due_str,
                "status": event.get("status", "planned"),
                "confidence": event.get("confidence", "estimated"),
                "recurring_rule": event.get("recurring_rule", ""),
                "clickup_task_id": event.get("clickup_task_id", ""),
                "bank_transaction_type": event.get("bank_transaction_type", ""),
                "bank_reference": event.get("bank_reference", ""),
                "notes": event.get("notes", ""),
                "friendly_name": event.get("friendly_name"),
                "created_at": now_str,
                "updated_at": now_str,
            },
        )
        return "created"


def insert_cash_event(conn, *, id, source, source_id, event_type, category,
                       subcategory="", description="", name="", vendor_or_customer="",
                       amount_cents=0, due_date=None, status="planned",
                       confidence="estimated", account_balance_cents=None,
                       bank_transaction_type="", bank_reference="", notes="",
                       recurring_rule="", clickup_task_id="", friendly_name=None,
                       created_at, updated_at):
    """Single canonical INSERT for cash_events. Use this everywhere instead of inline SQL."""
    due_str = due_date.isoformat() if hasattr(due_date, "isoformat") else (str(due_date)[:10] if due_date else None)
    conn.execute(text("""
        INSERT INTO cash_events (
            id, source, source_id, event_type, category,
            subcategory, description, name, vendor_or_customer,
            amount_cents, due_date, status, confidence,
            account_balance_cents, bank_transaction_type, bank_reference,
            notes, recurring_rule, clickup_task_id, friendly_name,
            created_at, updated_at
        ) VALUES (
            :id, :source, :source_id, :event_type, :category,
            :subcategory, :description, :name, :vendor_or_customer,
            :amount_cents, :due_date, :status, :confidence,
            :account_balance_cents, :bank_transaction_type, :bank_reference,
            :notes, :recurring_rule, :clickup_task_id, :friendly_name,
            :created_at, :updated_at
        )
    """), {
        "id": id, "source": source, "source_id": source_id, "event_type": event_type,
        "category": category, "subcategory": subcategory, "description": description,
        "name": name, "vendor_or_customer": vendor_or_customer, "amount_cents": amount_cents,
        "due_date": due_str, "status": status, "confidence": confidence,
        "account_balance_cents": account_balance_cents,
        "bank_transaction_type": bank_transaction_type, "bank_reference": bank_reference,
        "notes": notes, "recurring_rule": recurring_rule, "clickup_task_id": clickup_task_id,
        "friendly_name": friendly_name, "created_at": created_at, "updated_at": updated_at,
    })


# ---------------------------------------------------------------------------
# KV store helpers — lightweight key-value persistence (kv_store table)
# ---------------------------------------------------------------------------

def kv_set(key: str, value: str) -> None:
    """Upsert a single key-value pair into kv_store."""
    now = datetime.utcnow().isoformat()
    try:
        with get_engine().begin() as conn:
            conn.execute(text("""
                INSERT INTO kv_store (key, value, updated_at)
                VALUES (:key, :value, :now)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """), {"key": key, "value": value, "now": now})
    except Exception as exc:
        logger.warning("kv_set('%s') failed: %s", key, exc)


def kv_get(key: str, default: str | None = None) -> str | None:
    """Read a single value from kv_store by key, or return default."""
    try:
        with get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT value FROM kv_store WHERE key = :key"),
                {"key": key},
            ).fetchone()
        return row[0] if row else default
    except Exception as exc:
        logger.warning("kv_get('%s') failed: %s", key, exc)
        return default


def kv_set_json(key: str, data: dict) -> None:
    """Serialize *data* to JSON and store under *key*."""
    kv_set(key, json.dumps(data, default=str))


def kv_get_json(key: str, default: dict | None = None) -> dict | None:
    """Read a JSON-encoded dict from kv_store, or return *default*."""
    raw = kv_get(key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default
