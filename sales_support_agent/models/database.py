"""Database helpers."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

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
    import sales_support_agent.models.hr  # noqa: F401  — HR / payroll tables


def _normalize_db_url(url: str) -> str:
    """Accept the 'postgres://' scheme that Render/Supabase/Heroku hand out —
    SQLAlchemy 2.0 only recognizes 'postgresql://'. So a pasted persistent-DB URL
    boots cleanly regardless of which form it's in. Other URLs (sqlite,
    postgresql+driver) pass through untouched."""
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://"):]
    return url


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    global engine
    database_url = _normalize_db_url(database_url)
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    engine = create_engine(database_url, future=True, connect_args=connect_args)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, expire_on_commit=False)


def init_cashflow_db(db_url: str) -> None:
    """Re-initialize the module-level engine from a specific DB URL.
    Call this at app startup to ensure cashflow services use the same DB as the main app."""
    global engine
    db_url = _normalize_db_url(db_url)
    connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
    engine = create_engine(db_url, future=True, connect_args=connect_args)
    logger.info("Cashflow DB initialized: %s", db_url.split("@")[-1][:40] if "@" in db_url else db_url[:40])


def init_database(session_factory: sessionmaker[Session]) -> None:
    engine = session_factory.kw.get("bind")
    if engine is None:
        raise RuntimeError("Session factory is missing an engine binding.")
    _register_models()
    if engine.dialect.name == "sqlite":
        Base.metadata.create_all(bind=engine)
        _ensure_hr_columns(engine)
        _apply_sqlite_compat_migrations(engine)
        ensure_finance_trust_schema(engine)
        _backfill_legacy_settlements(engine)
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
    _ensure_building_tables(engine)
    _ensure_finance_settlement_tables(engine)
    ensure_finance_trust_schema(engine)
    _backfill_legacy_settlements(engine)
    _ensure_hr_tables(engine)
    _ensure_hr_columns(engine)


def _ensure_building_tables(engine: Any) -> None:
    """Create additive Anata Building tables on persistent databases."""

    table_names = {
        "building_spaces",
        "building_offerings",
        "building_availability_blocks",
        "building_inquiries",
        "building_audit_events",
        "building_contacts",
        "building_contact_merges",
        "building_relationships",
        "building_communication_preferences",
        "building_suppressions",
        "building_privacy_requests",
        "building_segments",
        "building_campaigns",
        "building_campaign_recipients",
        "building_email_events",
        "building_reservations",
        "building_agreements",
        "building_proposals",
        "building_tours",
        "building_deposit_evidence",
        "building_billing_accounts",
        "building_billing_schedules",
        "building_invoices",
        "building_payments",
        "building_stripe_events",
    }
    tables = [table for name, table in Base.metadata.tables.items() if name in table_names]
    if tables:
        Base.metadata.create_all(bind=engine, tables=tables, checkfirst=True)


def _ensure_hr_tables(engine: Any) -> None:
    """Create any missing HR/payroll tables (single-org Anata port of Base44 HR).

    On a fresh DB the bulk create_all already made them; on an existing Postgres
    (where create_all is skipped to keep boot fast) this adds just the new HR
    tables. checkfirst=True makes it idempotent and emits dialect-correct DDL
    straight from the ORM models, so there's no hand-written SQL to drift."""
    import sales_support_agent.models.hr  # noqa: F401 — ensure models are registered
    hr_tables = [t for name, t in Base.metadata.tables.items() if name.startswith("hr_")]
    if hr_tables:
        Base.metadata.create_all(bind=engine, tables=hr_tables, checkfirst=True)


def _ensure_hr_columns(engine: Any) -> None:
    """Apply additive HR columns to databases created by earlier releases."""
    inspector = inspect(engine)
    if "hr_tax_elections" not in inspector.get_table_names():
        return
    columns = {column["name"] for column in inspector.get_columns("hr_tax_elections")}
    if "exempt_from_federal_withholding" not in columns:
        default = "FALSE" if engine.dialect.name == "postgresql" else "0"
        with engine.begin() as connection:
            connection.execute(text(
                "ALTER TABLE hr_tax_elections "
                "ADD COLUMN exempt_from_federal_withholding BOOLEAN "
                f"NOT NULL DEFAULT {default}"
            ))
    additions = {
        "hr_time_entries": {
            "elapsed_seconds": "INTEGER NOT NULL DEFAULT 0",
        },
        "hr_tax_liabilities": {
            "confirmed_amount_cents": "INTEGER",
            "filing_confirmation_number": "VARCHAR(128) NOT NULL DEFAULT ''",
        },
        "hr_company_profiles": {
            "utah_withholding_payment_frequency": "VARCHAR(16) NOT NULL DEFAULT 'unknown'",
        },
        "hr_payroll_inputs": {
            "source_reference": "VARCHAR(255) NOT NULL DEFAULT ''",
            "recurring": "BOOLEAN NOT NULL DEFAULT FALSE",
            "recurrence_key": "VARCHAR(64) NOT NULL DEFAULT ''",
        },
    }
    inspector = inspect(engine)
    for table_name, table_columns in additions.items():
        if table_name not in inspector.get_table_names():
            continue
        existing = {column["name"] for column in inspector.get_columns(table_name)}
        with engine.begin() as connection:
            for column_name, ddl in table_columns.items():
                if column_name not in existing:
                    connection.execute(text(
                        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}"
                    ))


def _ensure_finance_settlement_tables(engine: Any) -> None:
    """Create the additive Finance V2 tables on persistent databases."""

    table_names = {
        "payment_installments",
        "settlement_allocations",
        "finance_source_records",
        "finance_import_batches",
        "finance_import_rows",
        "finance_settings",
        "finance_action_audit",
        "finance_reconciliation_reports",
        "finance_savings_reviews",
        "finance_savings_review_events",
        "plaid_items",
        "plaid_accounts",
    }
    tables = [table for name, table in Base.metadata.tables.items() if name in table_names]
    if tables:
        Base.metadata.create_all(bind=engine, tables=tables, checkfirst=True)


def _backfill_legacy_settlements(engine: Any) -> None:
    """Convert legacy matched bank rows into amount-based, idempotent evidence."""
    with engine.begin() as connection:
        matches = connection.execute(text("""
            SELECT
                actual.id AS transaction_id,
                actual.amount_cents AS transaction_amount_cents,
                COALESCE(actual.effective_date, actual.due_date, actual.updated_at) AS allocation_date,
                obligation.id AS obligation_id,
                obligation.amount_cents AS obligation_amount_cents,
                obligation.due_date AS obligation_due_date,
                obligation.status AS obligation_status
            FROM cash_events AS actual
            JOIN cash_events AS obligation ON obligation.id = actual.matched_to_id
            WHERE actual.source IN ('csv', 'qbo_bank')
              AND actual.matched_to_id IS NOT NULL
              AND actual.status = 'matched'
        """)).fetchall()

        for match in matches:
            row = dict(match._mapping)
            key = f"legacy-match:{row['transaction_id']}:{row['obligation_id']}"
            if connection.execute(text("""
                SELECT 1 FROM settlement_allocations WHERE idempotency_key = :key
            """), {"key": key}).fetchone() is not None:
                continue
            obligation_settled = int(connection.execute(text("""
                SELECT COALESCE(SUM(allocation.amount_cents), 0)
                FROM settlement_allocations AS allocation
                WHERE allocation.obligation_event_id = :id
                  AND allocation.reversed_allocation_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM settlement_allocations AS reversal
                      WHERE reversal.reversed_allocation_id = allocation.id
                  )
            """), {"id": row["obligation_id"]}).scalar_one() or 0)
            transaction_used = int(connection.execute(text("""
                SELECT COALESCE(SUM(allocation.amount_cents), 0)
                FROM settlement_allocations AS allocation
                WHERE allocation.transaction_event_id = :id
                  AND allocation.reversed_allocation_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM settlement_allocations AS reversal
                      WHERE reversal.reversed_allocation_id = allocation.id
                  )
            """), {"id": row["transaction_id"]}).scalar_one() or 0)
            amount_cents = min(
                max(int(row["transaction_amount_cents"] or 0) - transaction_used, 0),
                max(int(row["obligation_amount_cents"] or 0) - obligation_settled, 0),
            )
            if amount_cents <= 0:
                continue
            connection.execute(text("""
                INSERT INTO settlement_allocations (
                    id, obligation_event_id, transaction_event_id, installment_id,
                    amount_cents, allocation_date, source, confidence,
                    idempotency_key, reversed_allocation_id, notes, created_at
                ) VALUES (
                    :id, :obligation_id, :transaction_id, NULL,
                    :amount_cents, :allocation_date, 'legacy_match', 'confirmed',
                    :idempotency_key, NULL, 'Backfilled from legacy matched_to_id', :created_at
                )
                ON CONFLICT (idempotency_key) DO NOTHING
            """), {
                "id": str(uuid5(NAMESPACE_URL, key)),
                "obligation_id": row["obligation_id"],
                "transaction_id": row["transaction_id"],
                "amount_cents": amount_cents,
                "allocation_date": row["allocation_date"] or datetime.utcnow(),
                "idempotency_key": key,
                "created_at": datetime.utcnow(),
            })

        obligation_ids = {str(dict(match._mapping)["obligation_id"]) for match in matches}
        for obligation_id in obligation_ids:
            obligation = connection.execute(text("""
                SELECT amount_cents, due_date, status FROM cash_events WHERE id = :id
            """), {"id": obligation_id}).fetchone()
            if obligation is None:
                continue
            settled = int(connection.execute(text("""
                SELECT COALESCE(SUM(allocation.amount_cents), 0)
                FROM settlement_allocations AS allocation
                WHERE allocation.obligation_event_id = :id
                  AND allocation.reversed_allocation_id IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM settlement_allocations AS reversal
                      WHERE reversal.reversed_allocation_id = allocation.id
                  )
            """), {"id": obligation_id}).scalar_one() or 0)
            data = dict(obligation._mapping)
            if settled >= int(data["amount_cents"] or 0) or data["status"] not in {"paid", "matched"}:
                continue
            due_value = data["due_date"]
            if isinstance(due_value, datetime):
                due_day = due_value.date()
            elif isinstance(due_value, date):
                due_day = due_value
            else:
                try:
                    due_day = date.fromisoformat(str(due_value)[:10])
                except (TypeError, ValueError):
                    due_day = None
            today = datetime.utcnow().date()
            status = "overdue" if due_day and due_day < today else (
                "pending" if due_day and due_day <= today + timedelta(days=7) else "planned"
            )
            connection.execute(text("""
                UPDATE cash_events SET status = :status, updated_at = :now WHERE id = :id
            """), {"id": obligation_id, "status": status, "now": datetime.utcnow()})


def ensure_finance_trust_schema(target_engine: Any | None = None) -> None:
    """Install only additive Phase 0 Finance schema changes.

    This is intentionally callable by import services because some tests and
    small deployments initialize the shared engine without ``init_database``.
    """
    db_engine = target_engine or get_engine()
    _register_models()
    if db_engine.dialect.name == "sqlite":
        _apply_sqlite_compat_migrations(db_engine)
    table_names = {
        "finance_source_records",
        "finance_import_batches",
        "finance_import_rows",
        "finance_settings",
        "finance_action_audit",
        "finance_reconciliation_reports",
        "finance_savings_reviews",
        "finance_savings_review_events",
    }
    tables = [table for name, table in Base.metadata.tables.items() if name in table_names]
    if tables:
        Base.metadata.create_all(bind=db_engine, tables=tables, checkfirst=True)

    inspector = inspect(db_engine)
    if "cash_events" not in set(inspector.get_table_names()):
        return
    if db_engine.dialect.name == "postgresql":
        with db_engine.begin() as connection:
            connection.execute(text("""
                ALTER TABLE cash_events
                    ADD COLUMN IF NOT EXISTS source_status VARCHAR(32) NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS source_open_amount_cents INTEGER NULL,
                    ADD COLUMN IF NOT EXISTS source_updated_at TIMESTAMPTZ NULL,
                    ADD COLUMN IF NOT EXISTS match_status VARCHAR(16) NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS match_candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb
            """))
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_cash_events_source_status ON cash_events(source_status)"
            ))
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_cash_events_match_status ON cash_events(match_status)"
            ))
        return
    if db_engine.dialect.name != "sqlite":
        return

    columns = {column["name"] for column in inspect(db_engine).get_columns("cash_events")}
    statements = {
        "source_status": "ALTER TABLE cash_events ADD COLUMN source_status VARCHAR(32) NOT NULL DEFAULT ''",
        "source_open_amount_cents": "ALTER TABLE cash_events ADD COLUMN source_open_amount_cents INTEGER",
        "source_updated_at": "ALTER TABLE cash_events ADD COLUMN source_updated_at DATETIME",
        "match_status": "ALTER TABLE cash_events ADD COLUMN match_status VARCHAR(16) NOT NULL DEFAULT ''",
        "match_candidates_json": "ALTER TABLE cash_events ADD COLUMN match_candidates_json JSON NOT NULL DEFAULT '[]'",
    }
    with db_engine.begin() as connection:
        for column, statement in statements.items():
            if column not in columns:
                connection.execute(text(statement))
        connection.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_cash_events_source_status ON cash_events(source_status)"
        ))
        connection.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_cash_events_match_status ON cash_events(match_status)"
        ))


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
            "hubspot_deal_id": "ALTER TABLE communication_events ADD COLUMN hubspot_deal_id VARCHAR(64) DEFAULT NULL",
        },
        "mailbox_signals": {
            "matched_deal_id": "ALTER TABLE mailbox_signals ADD COLUMN matched_deal_id VARCHAR(64) DEFAULT ''",
        },
        "hubspot_deals": {
            "deal_stage_label": "ALTER TABLE hubspot_deals ADD COLUMN deal_stage_label VARCHAR(255) DEFAULT ''",
            "is_won": "ALTER TABLE hubspot_deals ADD COLUMN is_won BOOLEAN DEFAULT 0",
            "created_at": "ALTER TABLE hubspot_deals ADD COLUMN created_at DATETIME",
            "updated_at": "ALTER TABLE hubspot_deals ADD COLUMN updated_at DATETIME",
            "description": "ALTER TABLE hubspot_deals ADD COLUMN description TEXT DEFAULT ''",
            "last_meaningful_touch_at": "ALTER TABLE hubspot_deals ADD COLUMN last_meaningful_touch_at DATETIME",
            "last_outbound_at": "ALTER TABLE hubspot_deals ADD COLUMN last_outbound_at DATETIME",
            "last_inbound_at": "ALTER TABLE hubspot_deals ADD COLUMN last_inbound_at DATETIME",
            "next_follow_up_at": "ALTER TABLE hubspot_deals ADD COLUMN next_follow_up_at DATETIME",
            "follow_up_state": "ALTER TABLE hubspot_deals ADD COLUMN follow_up_state VARCHAR(64) DEFAULT ''",
            "communication_summary": "ALTER TABLE hubspot_deals ADD COLUMN communication_summary TEXT DEFAULT ''",
            "recommended_next_action": "ALTER TABLE hubspot_deals ADD COLUMN recommended_next_action TEXT DEFAULT ''",
        },
        "brand_analysis_reports": {
            "slug": "ALTER TABLE brand_analysis_reports ADD COLUMN slug VARCHAR(96) NOT NULL DEFAULT ''",
            "share_token": "ALTER TABLE brand_analysis_reports ADD COLUMN share_token VARCHAR(64) NOT NULL DEFAULT ''",
            "report_html": "ALTER TABLE brand_analysis_reports ADD COLUMN report_html TEXT NOT NULL DEFAULT ''",
            "brand_website": "ALTER TABLE brand_analysis_reports ADD COLUMN brand_website VARCHAR(512) NOT NULL DEFAULT ''",
            "context_notes": "ALTER TABLE brand_analysis_reports ADD COLUMN context_notes TEXT NOT NULL DEFAULT ''",
            "stage": "ALTER TABLE brand_analysis_reports ADD COLUMN stage VARCHAR(32) NOT NULL DEFAULT 'new'",
            "notes": "ALTER TABLE brand_analysis_reports ADD COLUMN notes TEXT NOT NULL DEFAULT ''",
            "ask_price_cents": "ALTER TABLE brand_analysis_reports ADD COLUMN ask_price_cents INTEGER",
            "contact_name": "ALTER TABLE brand_analysis_reports ADD COLUMN contact_name VARCHAR(255) NOT NULL DEFAULT ''",
            "contact_email": "ALTER TABLE brand_analysis_reports ADD COLUMN contact_email VARCHAR(255) NOT NULL DEFAULT ''",
        },
        "app_users": {
            "picture_url": "ALTER TABLE app_users ADD COLUMN picture_url TEXT NOT NULL DEFAULT ''",
            "permissions_json": "ALTER TABLE app_users ADD COLUMN permissions_json JSON NOT NULL DEFAULT '[]'",
        },
        "ad_goals": {
            "client_id": "ALTER TABLE ad_goals ADD COLUMN client_id VARCHAR(64)",
        },
        "audit_runs": {
            "client_id": "ALTER TABLE audit_runs ADD COLUMN client_id VARCHAR(64)",
        },
        "cash_events": {
            "record_kind":          "ALTER TABLE cash_events ADD COLUMN record_kind VARCHAR(16) NOT NULL DEFAULT 'obligation'",
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
            "pay_priority":          "ALTER TABLE cash_events ADD COLUMN pay_priority VARCHAR(16) NOT NULL DEFAULT 'review'",
            "minimum_payment_cents": "ALTER TABLE cash_events ADD COLUMN minimum_payment_cents INTEGER",
            "flexibility":           "ALTER TABLE cash_events ADD COLUMN flexibility VARCHAR(16) NOT NULL DEFAULT 'unknown'",
            "commitment_type":       "ALTER TABLE cash_events ADD COLUMN commitment_type VARCHAR(32) NOT NULL DEFAULT 'general'",
            "workflow_status":       "ALTER TABLE cash_events ADD COLUMN workflow_status VARCHAR(32) NOT NULL DEFAULT 'draft'",
            "owner":                 "ALTER TABLE cash_events ADD COLUMN owner VARCHAR(255) NOT NULL DEFAULT ''",
            "approval_status":       "ALTER TABLE cash_events ADD COLUMN approval_status VARCHAR(32) NOT NULL DEFAULT 'not_required'",
            "created_by":            "ALTER TABLE cash_events ADD COLUMN created_by VARCHAR(255) NOT NULL DEFAULT 'system'",
            "archived_at":           "ALTER TABLE cash_events ADD COLUMN archived_at DATETIME",
        },
        "finance_action_audit": {
            "idempotency_key": "ALTER TABLE finance_action_audit ADD COLUMN idempotency_key VARCHAR(128)",
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

        if "cash_events" in existing_tables:
            connection.execute(text("""
                UPDATE cash_events
                SET record_kind = 'transaction'
                WHERE source IN ('csv', 'qbo_bank')
                   OR status = 'posted'
                   OR account_balance_cents IS NOT NULL
            """))
            connection.execute(text("""
                CREATE TRIGGER IF NOT EXISTS set_cash_event_transaction_kind_insert
                AFTER INSERT ON cash_events
                WHEN NEW.source IN ('csv', 'qbo_bank')
                  OR NEW.status = 'posted'
                  OR NEW.account_balance_cents IS NOT NULL
                BEGIN
                    UPDATE cash_events SET record_kind = 'transaction' WHERE id = NEW.id;
                END
            """))
            connection.execute(text("""
                CREATE TRIGGER IF NOT EXISTS set_cash_event_transaction_kind_update
                AFTER UPDATE OF source, status, account_balance_cents ON cash_events
                WHEN NEW.source IN ('csv', 'qbo_bank')
                  OR NEW.status = 'posted'
                  OR NEW.account_balance_cents IS NOT NULL
                BEGIN
                    UPDATE cash_events SET record_kind = 'transaction' WHERE id = NEW.id;
                END
            """))
        if "finance_action_audit" in existing_tables:
            connection.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_finance_action_audit_idempotency_key "
                "ON finance_action_audit(idempotency_key) WHERE idempotency_key IS NOT NULL"
            ))

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
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS inbox_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider VARCHAR(32) NOT NULL DEFAULT 'gmail',
                connection_source VARCHAR(32) NOT NULL DEFAULT 'user_oauth',
                account_key VARCHAR(128) NOT NULL UNIQUE,
                account_label VARCHAR(255) NOT NULL DEFAULT '',
                account_email VARCHAR(255) NOT NULL DEFAULT '',
                owner_user_id VARCHAR(64) NOT NULL DEFAULT '',
                owner_user_email VARCHAR(255) NOT NULL DEFAULT '',
                owner_user_name VARCHAR(255) NOT NULL DEFAULT '',
                gmail_user_id VARCHAR(64) NOT NULL DEFAULT 'me',
                sealed_access_token TEXT NOT NULL DEFAULT '',
                sealed_refresh_token TEXT NOT NULL DEFAULT '',
                poll_query VARCHAR(255) NOT NULL DEFAULT 'newer_than:2d',
                poll_max_messages INTEGER NOT NULL DEFAULT 25,
                source_domains_json JSON NOT NULL DEFAULT '[]',
                status VARCHAR(32) NOT NULL DEFAULT 'connected',
                last_error TEXT NOT NULL DEFAULT '',
                last_validated_at DATETIME NULL,
                last_sync_at DATETIME NULL,
                disconnected_at DATETIME NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_inbox_connections_owner_user_email ON inbox_connections(owner_user_email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_inbox_connections_status ON inbox_connections(status)"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_deal_notes (
                hubspot_note_id VARCHAR(64) PRIMARY KEY,
                hubspot_deal_id VARCHAR(64) NOT NULL,
                owner_id VARCHAR(64) NOT NULL DEFAULT '',
                body_text TEXT NOT NULL DEFAULT '',
                body_preview VARCHAR(512) NOT NULL DEFAULT '',
                override_state VARCHAR(64) NOT NULL DEFAULT '',
                override_reason VARCHAR(255) NOT NULL DEFAULT '',
                note_timestamp DATETIME NULL,
                raw_properties JSON NOT NULL DEFAULT '{}',
                last_sync_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hubspot_deal_notes_deal_id ON hubspot_deal_notes(hubspot_deal_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hubspot_deal_notes_note_timestamp ON hubspot_deal_notes(note_timestamp)"))

    # Add indexes for Calendar/Ledger range queries
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_due_date ON cash_events(due_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_source ON cash_events(source)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cash_events_status ON cash_events(status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_record_kind ON cash_events(record_kind)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_pay_priority ON cash_events(pay_priority)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_flexibility ON cash_events(flexibility)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_commitment_type ON cash_events(commitment_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_workflow_status ON cash_events(workflow_status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_owner ON cash_events(owner)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_approval_status ON cash_events(approval_status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_archived_at ON cash_events(archived_at)"))


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

    # PR54: deck-analytics tables — added after the initial bootstrap, so
    # existing Postgres deployments need an explicit CREATE TABLE IF NOT
    # EXISTS to pick them up. Fresh DBs get them via create_all earlier.
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS deck_visit_sessions (
                    id                  SERIAL       PRIMARY KEY,
                    run_id              INTEGER      NOT NULL,
                    visitor_token       VARCHAR(64)  NOT NULL,
                    is_internal         BOOLEAN      NOT NULL DEFAULT FALSE,
                    started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    last_heartbeat_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    total_seconds       INTEGER      NOT NULL DEFAULT 0,
                    max_scroll_pct      INTEGER      NOT NULL DEFAULT 0,
                    ip_country          VARCHAR(8)   NOT NULL DEFAULT '',
                    ip_region           VARCHAR(64)  NOT NULL DEFAULT '',
                    ip_city             VARCHAR(96)  NOT NULL DEFAULT '',
                    device              VARCHAR(16)  NOT NULL DEFAULT '',
                    os                  VARCHAR(32)  NOT NULL DEFAULT '',
                    browser             VARCHAR(32)  NOT NULL DEFAULT '',
                    user_agent_raw      VARCHAR(512) NOT NULL DEFAULT '',
                    referrer_host       VARCHAR(128) NOT NULL DEFAULT '',
                    referrer_category   VARCHAR(16)  NOT NULL DEFAULT 'direct'
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_sessions_run_id ON deck_visit_sessions (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_sessions_visitor_token ON deck_visit_sessions (visitor_token)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_sessions_is_internal ON deck_visit_sessions (is_internal)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_sessions_started_at ON deck_visit_sessions (started_at)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_sessions_run_visitor ON deck_visit_sessions (run_id, visitor_token)"))
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS deck_section_views (
                    id              SERIAL       PRIMARY KEY,
                    session_id      INTEGER      NOT NULL,
                    section_id      VARCHAR(64)  NOT NULL,
                    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    last_seen_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                    total_seconds   INTEGER      NOT NULL DEFAULT 0
                )
                """
            )
        )
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_section_session_id ON deck_section_views (session_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_deck_section_section_id ON deck_section_views (section_id)"))
        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_deck_section_session_sec ON deck_section_views (session_id, section_id)"))

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
                    record_kind             VARCHAR(16)  NOT NULL DEFAULT 'obligation',
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
                    pay_priority            VARCHAR(16)  NOT NULL DEFAULT 'review',
                    minimum_payment_cents   INTEGER      NULL,
                    flexibility             VARCHAR(16)  NOT NULL DEFAULT 'unknown',
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
                    ADD COLUMN IF NOT EXISTS record_kind          VARCHAR(16)  NOT NULL DEFAULT 'obligation',
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
                    ADD COLUMN IF NOT EXISTS matched_to_id         TEXT         NULL,
                    ADD COLUMN IF NOT EXISTS pay_priority          VARCHAR(16)  NOT NULL DEFAULT 'review',
                    ADD COLUMN IF NOT EXISTS minimum_payment_cents INTEGER      NULL,
                    ADD COLUMN IF NOT EXISTS flexibility           VARCHAR(16)  NOT NULL DEFAULT 'unknown',
                    ADD COLUMN IF NOT EXISTS commitment_type       VARCHAR(32)  NOT NULL DEFAULT 'general',
                    ADD COLUMN IF NOT EXISTS workflow_status       VARCHAR(32)  NOT NULL DEFAULT 'draft',
                    ADD COLUMN IF NOT EXISTS owner                 VARCHAR(255) NOT NULL DEFAULT '',
                    ADD COLUMN IF NOT EXISTS approval_status       VARCHAR(32)  NOT NULL DEFAULT 'not_required',
                    ADD COLUMN IF NOT EXISTS created_by            VARCHAR(255) NOT NULL DEFAULT 'system',
                    ADD COLUMN IF NOT EXISTS archived_at           TIMESTAMPTZ  NULL
                """
            )
        )
        connection.execute(text(
            "ALTER TABLE finance_action_audit "
            "ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128) NULL"
        ))
        connection.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_finance_action_audit_idempotency_key "
            "ON finance_action_audit(idempotency_key) WHERE idempotency_key IS NOT NULL"
        ))
        connection.execute(text("""
            UPDATE cash_events
            SET record_kind = 'transaction'
            WHERE source IN ('csv', 'qbo_bank')
               OR status = 'posted'
               OR account_balance_cents IS NOT NULL
        """))
        connection.execute(text("""
            CREATE OR REPLACE FUNCTION set_cash_event_record_kind()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.source IN ('csv', 'qbo_bank')
                   OR NEW.status = 'posted'
                   OR NEW.account_balance_cents IS NOT NULL THEN
                    NEW.record_kind := 'transaction';
                ELSIF NEW.record_kind IS NULL OR NEW.record_kind = '' THEN
                    NEW.record_kind := 'obligation';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """))
        connection.execute(text("DROP TRIGGER IF EXISTS set_cash_event_record_kind_trigger ON cash_events"))
        connection.execute(text("""
            CREATE TRIGGER set_cash_event_record_kind_trigger
            BEFORE INSERT OR UPDATE
            ON cash_events
            FOR EACH ROW EXECUTE FUNCTION set_cash_event_record_kind()
        """))
        # Indexes for the most common cashflow queries
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_due_date ON cash_events (due_date)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_status ON cash_events (status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_event_type ON cash_events (event_type)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_source ON cash_events (source)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_due_date_status ON cash_events (due_date, status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_source_source_id ON cash_events (source, source_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_matched_to_id ON cash_events (matched_to_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_clickup_task_id ON cash_events (clickup_task_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_record_kind ON cash_events (record_kind)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_pay_priority ON cash_events (pay_priority)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_flexibility ON cash_events (flexibility)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_commitment_type ON cash_events (commitment_type)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_workflow_status ON cash_events (workflow_status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_owner ON cash_events (owner)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_approval_status ON cash_events (approval_status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_cash_events_archived_at ON cash_events (archived_at)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recurring_templates_is_active ON recurring_templates (is_active)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recurring_templates_next_due_date ON recurring_templates (next_due_date)"))
        # friendly_name column — additive migration for existing Postgres deployments
        connection.execute(text("ALTER TABLE cash_events ADD COLUMN IF NOT EXISTS friendly_name TEXT NULL"))

    # Advertising > Audit tables — created by create_all on fresh DBs; for
    # existing Postgres deployments we create them if absent. Money is cents,
    # percentages are basis points (see entities.py).
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS ad_clients (
                id          TEXT         PRIMARY KEY,
                name        VARCHAR(255) NOT NULL DEFAULT '',
                objectives  TEXT         NOT NULL DEFAULT '',
                status      VARCHAR(16)  NOT NULL DEFAULT 'active',
                created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS ad_goals (
                id                   TEXT         PRIMARY KEY,
                label                VARCHAR(255) NOT NULL DEFAULT '',
                period               VARCHAR(32)  NOT NULL DEFAULT 'monthly',
                revenue_target_cents INTEGER      NULL,
                acos_target_bps      INTEGER      NULL,
                tacos_target_bps     INTEGER      NULL,
                units_target         INTEGER      NULL,
                is_active            BOOLEAN      NOT NULL DEFAULT TRUE,
                notes                TEXT         NOT NULL DEFAULT '',
                created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS external_costs (
                id           TEXT         PRIMARY KEY,
                run_id       TEXT         NULL,
                channel      VARCHAR(32)  NOT NULL DEFAULT 'other',
                cost_type    VARCHAR(32)  NOT NULL DEFAULT 'ad_spend',
                label        VARCHAR(255) NOT NULL DEFAULT '',
                amount_cents INTEGER      NOT NULL DEFAULT 0,
                period_start TIMESTAMPTZ  NULL,
                period_end   TIMESTAMPTZ  NULL,
                note         TEXT         NOT NULL DEFAULT '',
                created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS audit_runs (
                id                 TEXT         PRIMARY KEY,
                label              VARCHAR(255) NOT NULL DEFAULT '',
                week_start         TIMESTAMPTZ  NULL,
                week_end           TIMESTAMPTZ  NULL,
                status             VARCHAR(32)  NOT NULL DEFAULT 'draft',
                goal_snapshot_json JSON         NOT NULL DEFAULT '{}',
                summary_json       JSON         NOT NULL DEFAULT '{}',
                narrative          TEXT         NOT NULL DEFAULT '',
                error              TEXT         NOT NULL DEFAULT '',
                created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS ad_snapshots (
                id            TEXT          PRIMARY KEY,
                run_id        TEXT          NOT NULL DEFAULT '',
                ad_type       VARCHAR(16)   NOT NULL DEFAULT 'SP',
                entity_level  VARCHAR(32)   NOT NULL DEFAULT 'campaign',
                campaign_name VARCHAR(512)  NOT NULL DEFAULT '',
                ad_group_name VARCHAR(512)  NOT NULL DEFAULT '',
                entity_text   VARCHAR(1024) NOT NULL DEFAULT '',
                match_type    VARCHAR(32)   NOT NULL DEFAULT '',
                impressions   INTEGER       NOT NULL DEFAULT 0,
                clicks        INTEGER       NOT NULL DEFAULT 0,
                spend_cents   INTEGER       NOT NULL DEFAULT 0,
                sales_cents   INTEGER       NOT NULL DEFAULT 0,
                orders        INTEGER       NOT NULL DEFAULT 0,
                units         INTEGER       NOT NULL DEFAULT 0,
                bid_cents     INTEGER       NULL,
                raw_json      JSON          NOT NULL DEFAULT '{}',
                created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS sales_snapshots (
                id                          TEXT         PRIMARY KEY,
                run_id                      TEXT         NOT NULL DEFAULT '',
                asin                        VARCHAR(32)  NOT NULL DEFAULT '',
                sku                         VARCHAR(64)  NOT NULL DEFAULT '',
                title                       VARCHAR(512) NOT NULL DEFAULT '',
                sessions                    INTEGER      NOT NULL DEFAULT 0,
                page_views                  INTEGER      NOT NULL DEFAULT 0,
                units                       INTEGER      NOT NULL DEFAULT 0,
                ordered_product_sales_cents INTEGER      NOT NULL DEFAULT 0,
                buy_box_pct_bps             INTEGER      NULL,
                conversion_bps              INTEGER      NULL,
                raw_json                    JSON         NOT NULL DEFAULT '{}',
                created_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id                   TEXT         PRIMARY KEY,
                run_id               TEXT         NOT NULL DEFAULT '',
                search_query         VARCHAR(512) NOT NULL DEFAULT '',
                asin                 VARCHAR(32)  NOT NULL DEFAULT '',
                search_query_volume  INTEGER      NOT NULL DEFAULT 0,
                impressions_total    INTEGER      NOT NULL DEFAULT 0,
                impression_share_bps INTEGER      NULL,
                clicks_total         INTEGER      NOT NULL DEFAULT 0,
                click_share_bps      INTEGER      NULL,
                purchases_total      INTEGER      NOT NULL DEFAULT 0,
                purchase_share_bps   INTEGER      NULL,
                raw_json             JSON         NOT NULL DEFAULT '{}',
                created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS recommendations (
                id                    TEXT          PRIMARY KEY,
                run_id                TEXT          NOT NULL DEFAULT '',
                rank                  INTEGER       NOT NULL DEFAULT 0,
                category              VARCHAR(48)   NOT NULL DEFAULT '',
                ad_type               VARCHAR(16)   NOT NULL DEFAULT '',
                severity              VARCHAR(16)   NOT NULL DEFAULT 'medium',
                title                 VARCHAR(512)  NOT NULL DEFAULT '',
                detail                TEXT          NOT NULL DEFAULT '',
                rationale             TEXT          NOT NULL DEFAULT '',
                entity_ref            VARCHAR(1024) NOT NULL DEFAULT '',
                current_value         VARCHAR(128)  NOT NULL DEFAULT '',
                proposed_value        VARCHAR(128)  NOT NULL DEFAULT '',
                projected_impact_json JSON          NOT NULL DEFAULT '{}',
                bulk_row_json         JSON          NOT NULL DEFAULT '{}',
                is_bulk_actionable    BOOLEAN       NOT NULL DEFAULT FALSE,
                status                VARCHAR(16)   NOT NULL DEFAULT 'open',
                created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
        """))
        # Per-client scoping (added after the ad tables shipped) — additive,
        # NULL means the global/ad-hoc set (no client selected).
        connection.execute(text("ALTER TABLE ad_goals ADD COLUMN IF NOT EXISTS client_id TEXT NULL"))
        connection.execute(text("ALTER TABLE audit_runs ADD COLUMN IF NOT EXISTS client_id TEXT NULL"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_clients_name ON ad_clients (name)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_clients_status ON ad_clients (status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_goals_client_id ON ad_goals (client_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_runs_client_id ON audit_runs (client_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_goals_is_active ON ad_goals (is_active)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_external_costs_run_id ON external_costs (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_external_costs_channel ON external_costs (channel)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_runs_status ON audit_runs (status)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_runs_created_at ON audit_runs (created_at)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_runs_week_start ON audit_runs (week_start)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_snapshots_run_id ON ad_snapshots (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_ad_snapshots_run_type_level ON ad_snapshots (run_id, ad_type, entity_level)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sales_snapshots_run_id ON sales_snapshots (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sales_snapshots_asin ON sales_snapshots (asin)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_market_snapshots_run_id ON market_snapshots (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recommendations_run_id ON recommendations (run_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_recommendations_run_rank ON recommendations (run_id, rank)"))

    # Executive > Brand Analysis — saved acquisition reports (History).
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS brand_analysis_reports (
                id             TEXT         PRIMARY KEY,
                label          VARCHAR(255) NOT NULL DEFAULT '',
                brand          VARCHAR(255) NOT NULL DEFAULT '',
                category       VARCHAR(32)  NOT NULL DEFAULT 'dtc',
                status         VARCHAR(32)  NOT NULL DEFAULT 'complete',
                grade          VARCHAR(2)   NOT NULL DEFAULT '',
                score_100      INTEGER      NOT NULL DEFAULT 0,
                confidence     VARCHAR(16)  NOT NULL DEFAULT '',
                period_current VARCHAR(64)  NOT NULL DEFAULT '',
                period_prior   VARCHAR(64)  NOT NULL DEFAULT '',
                report_json    JSON         NOT NULL DEFAULT '{}',
                error          TEXT         NOT NULL DEFAULT '',
                slug           VARCHAR(96)  NOT NULL DEFAULT '',
                share_token    VARCHAR(64)  NOT NULL DEFAULT '',
                report_html    TEXT         NOT NULL DEFAULT '',
                brand_website  VARCHAR(512) NOT NULL DEFAULT '',
                context_notes  TEXT         NOT NULL DEFAULT '',
                created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_analysis_reports_created_at ON brand_analysis_reports (created_at)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_analysis_reports_brand ON brand_analysis_reports (brand)"))
        # Additive columns for the investor-package redesign (existing tables).
        for _stmt in (
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS slug VARCHAR(96) NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS share_token VARCHAR(64) NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS report_html TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS brand_website VARCHAR(512) NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS context_notes TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS stage VARCHAR(32) NOT NULL DEFAULT 'new'",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS ask_price_cents INTEGER",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS contact_name VARCHAR(255) NOT NULL DEFAULT ''",
            "ALTER TABLE brand_analysis_reports ADD COLUMN IF NOT EXISTS contact_email VARCHAR(255) NOT NULL DEFAULT ''",
        ):
            connection.execute(text(_stmt))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_analysis_reports_share_token ON brand_analysis_reports (share_token)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_brand_analysis_reports_stage ON brand_analysis_reports (stage)"))

    # Access control (RBAC) — users, custom roles, invites, access requests.
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS app_roles (
                id               TEXT         PRIMARY KEY,
                name             VARCHAR(128) NOT NULL DEFAULT '',
                description      TEXT         NOT NULL DEFAULT '',
                permissions_json JSON         NOT NULL DEFAULT '[]',
                created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_app_roles_name ON app_roles (name)"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS app_users (
                id            TEXT         PRIMARY KEY,
                email         VARCHAR(255) NOT NULL DEFAULT '',
                name          VARCHAR(255) NOT NULL DEFAULT '',
                picture_url   TEXT         NOT NULL DEFAULT '',
                role_id       TEXT         NULL,
                permissions_json JSON      NOT NULL DEFAULT '[]',
                status        VARCHAR(16)  NOT NULL DEFAULT 'active',
                is_superadmin BOOLEAN      NOT NULL DEFAULT FALSE,
                created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                last_login_at TIMESTAMPTZ  NULL
            )
        """))
        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_app_users_email ON app_users (email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_app_users_role_id ON app_users (role_id)"))
        connection.execute(text("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS picture_url TEXT NOT NULL DEFAULT ''"))
        if engine.dialect.name == "postgresql":
            connection.execute(text("ALTER TABLE app_users ALTER COLUMN picture_url TYPE TEXT"))
        connection.execute(text("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS permissions_json JSON NOT NULL DEFAULT '[]'"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS app_invites (
                id          TEXT         PRIMARY KEY,
                email       VARCHAR(255) NOT NULL DEFAULT '',
                role_id     TEXT         NULL,
                token_hash  VARCHAR(128) NOT NULL DEFAULT '',
                invited_by  VARCHAR(255) NOT NULL DEFAULT '',
                status      VARCHAR(16)  NOT NULL DEFAULT 'pending',
                created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                expires_at  TIMESTAMPTZ  NULL,
                accepted_at TIMESTAMPTZ  NULL
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_app_invites_email ON app_invites (email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_app_invites_token_hash ON app_invites (token_hash)"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS app_access_requests (
                id               TEXT         PRIMARY KEY,
                email            VARCHAR(255) NOT NULL DEFAULT '',
                name             VARCHAR(255) NOT NULL DEFAULT '',
                status           VARCHAR(16)  NOT NULL DEFAULT 'pending',
                assigned_role_id TEXT         NULL,
                decided_by       VARCHAR(255) NOT NULL DEFAULT '',
                requested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
                decided_at       TIMESTAMPTZ  NULL
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_app_access_requests_email ON app_access_requests (email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_app_access_requests_status ON app_access_requests (status)"))

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

    # HubSpot sales mirror tables (read mirror; HubSpot stays canonical) +
    # additive columns generalizing comms off ClickUp. Fresh DBs get the
    # tables via create_all; existing Postgres needs explicit DDL here.
    with engine.begin() as connection:
        connection.execute(text("""
            ALTER TABLE communication_events
            ADD COLUMN IF NOT EXISTS hubspot_deal_id VARCHAR(64) NOT NULL DEFAULT ''
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_comm_events_hs_deal ON communication_events (hubspot_deal_id)"))
        connection.execute(text("""
            ALTER TABLE mailbox_signals
            ADD COLUMN IF NOT EXISTS matched_deal_id VARCHAR(64) NOT NULL DEFAULT ''
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_mailbox_signals_matched_deal ON mailbox_signals (matched_deal_id)"))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_companies (
                hubspot_company_id VARCHAR(64)  PRIMARY KEY,
                name               VARCHAR(512) NOT NULL DEFAULT '',
                domain             VARCHAR(255) NOT NULL DEFAULT '',
                industry           VARCHAR(255) NOT NULL DEFAULT '',
                city               VARCHAR(128) NOT NULL DEFAULT '',
                state              VARCHAR(128) NOT NULL DEFAULT '',
                raw_properties     JSON         NOT NULL DEFAULT '{}',
                last_sync_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_companies_name ON hubspot_companies (name)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_companies_domain ON hubspot_companies (domain)"))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_contacts (
                hubspot_contact_id VARCHAR(64)  PRIMARY KEY,
                hubspot_company_id VARCHAR(64)  NOT NULL DEFAULT '',
                first_name         VARCHAR(255) NOT NULL DEFAULT '',
                last_name          VARCHAR(255) NOT NULL DEFAULT '',
                email              VARCHAR(255) NOT NULL DEFAULT '',
                phone              VARCHAR(128) NOT NULL DEFAULT '',
                job_title          VARCHAR(255) NOT NULL DEFAULT '',
                raw_properties     JSON         NOT NULL DEFAULT '{}',
                last_sync_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_contacts_email ON hubspot_contacts (email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_contacts_company ON hubspot_contacts (hubspot_company_id)"))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_deals (
                hubspot_deal_id          VARCHAR(64)  PRIMARY KEY,
                deal_name                VARCHAR(512) NOT NULL DEFAULT '',
                amount_cents             INTEGER      NOT NULL DEFAULT 0,
                deal_stage               VARCHAR(128) NOT NULL DEFAULT '',
                deal_stage_label         VARCHAR(255) NOT NULL DEFAULT '',
                pipeline                 VARCHAR(128) NOT NULL DEFAULT '',
                close_date               TIMESTAMPTZ  NULL,
                owner_id                 VARCHAR(64)  NOT NULL DEFAULT '',
                owner_email              VARCHAR(255) NOT NULL DEFAULT '',
                hubspot_company_id       VARCHAR(64)  NOT NULL DEFAULT '',
                is_closed                BOOLEAN      NOT NULL DEFAULT FALSE,
                is_won                   BOOLEAN      NOT NULL DEFAULT FALSE,
                created_at               TIMESTAMPTZ  NULL,
                updated_at               TIMESTAMPTZ  NULL,
                description              TEXT         NOT NULL DEFAULT '',
                last_meaningful_touch_at TIMESTAMPTZ  NULL,
                last_outbound_at         TIMESTAMPTZ  NULL,
                last_inbound_at          TIMESTAMPTZ  NULL,
                next_follow_up_at        TIMESTAMPTZ  NULL,
                follow_up_state          VARCHAR(64)  NOT NULL DEFAULT '',
                communication_summary    TEXT         NOT NULL DEFAULT '',
                recommended_next_action  TEXT         NOT NULL DEFAULT '',
                raw_properties           JSON         NOT NULL DEFAULT '{}',
                last_sync_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deals_close_date ON hubspot_deals (close_date)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deals_stage ON hubspot_deals (deal_stage)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deals_owner ON hubspot_deals (owner_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deals_company ON hubspot_deals (hubspot_company_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deals_is_closed ON hubspot_deals (is_closed)"))
        # Phase 6b staleness / accountability fields — added after the initial
        # hubspot_deals table shipped. ADD COLUMN IF NOT EXISTS is a no-op on
        # columns that already exist from a fresh CREATE TABLE.
        connection.execute(text("""
            ALTER TABLE hubspot_deals
                ADD COLUMN IF NOT EXISTS created_at               TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS updated_at               TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS description              TEXT        NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS last_meaningful_touch_at TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS last_outbound_at         TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS last_inbound_at          TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS next_follow_up_at        TIMESTAMPTZ NULL,
                ADD COLUMN IF NOT EXISTS follow_up_state          VARCHAR(64) NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS communication_summary    TEXT        NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS recommended_next_action  TEXT        NOT NULL DEFAULT ''
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_line_items (
                hubspot_line_item_id VARCHAR(64)  PRIMARY KEY,
                hubspot_deal_id      VARCHAR(64)  NOT NULL DEFAULT '',
                name                 VARCHAR(512) NOT NULL DEFAULT '',
                quantity             INTEGER      NOT NULL DEFAULT 0,
                unit_price_cents     INTEGER      NOT NULL DEFAULT 0,
                amount_cents         INTEGER      NOT NULL DEFAULT 0,
                raw_properties       JSON         NOT NULL DEFAULT '{}',
                last_sync_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_line_items_deal ON hubspot_line_items (hubspot_deal_id)"))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_deal_contacts (
                id                 SERIAL      PRIMARY KEY,
                hubspot_deal_id    VARCHAR(64) NOT NULL,
                hubspot_contact_id VARCHAR(64) NOT NULL,
                role               VARCHAR(64) NOT NULL DEFAULT ''
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deal_contacts_deal ON hubspot_deal_contacts (hubspot_deal_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hs_deal_contacts_contact ON hubspot_deal_contacts (hubspot_contact_id)"))
        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_hs_deal_contact_unique ON hubspot_deal_contacts (hubspot_deal_id, hubspot_contact_id)"))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS sales_deal_assets (
                id              SERIAL       PRIMARY KEY,
                hubspot_deal_id VARCHAR(64)  NOT NULL,
                asset_type      VARCHAR(32)  NOT NULL,
                run_id          VARCHAR(64)  NOT NULL DEFAULT '',
                url             VARCHAR(1024) NOT NULL DEFAULT '',
                label           VARCHAR(255) NOT NULL DEFAULT '',
                linked_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sales_deal_assets_deal ON sales_deal_assets (hubspot_deal_id)"))
        connection.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ix_sales_deal_asset_unique ON sales_deal_assets (hubspot_deal_id, asset_type, run_id)"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS inbox_connections (
                id BIGSERIAL PRIMARY KEY,
                provider VARCHAR(32) NOT NULL DEFAULT 'gmail',
                connection_source VARCHAR(32) NOT NULL DEFAULT 'user_oauth',
                account_key VARCHAR(128) NOT NULL UNIQUE,
                account_label VARCHAR(255) NOT NULL DEFAULT '',
                account_email VARCHAR(255) NOT NULL DEFAULT '',
                owner_user_id VARCHAR(64) NOT NULL DEFAULT '',
                owner_user_email VARCHAR(255) NOT NULL DEFAULT '',
                owner_user_name VARCHAR(255) NOT NULL DEFAULT '',
                gmail_user_id VARCHAR(64) NOT NULL DEFAULT 'me',
                sealed_access_token TEXT NOT NULL DEFAULT '',
                sealed_refresh_token TEXT NOT NULL DEFAULT '',
                poll_query VARCHAR(255) NOT NULL DEFAULT 'newer_than:2d',
                poll_max_messages INTEGER NOT NULL DEFAULT 25,
                source_domains_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                status VARCHAR(32) NOT NULL DEFAULT 'connected',
                last_error TEXT NOT NULL DEFAULT '',
                last_validated_at TIMESTAMPTZ NULL,
                last_sync_at TIMESTAMPTZ NULL,
                disconnected_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_inbox_connections_owner_user_email ON inbox_connections(owner_user_email)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_inbox_connections_status ON inbox_connections(status)"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS hubspot_deal_notes (
                hubspot_note_id VARCHAR(64) PRIMARY KEY,
                hubspot_deal_id VARCHAR(64) NOT NULL,
                owner_id VARCHAR(64) NOT NULL DEFAULT '',
                body_text TEXT NOT NULL DEFAULT '',
                body_preview VARCHAR(512) NOT NULL DEFAULT '',
                override_state VARCHAR(64) NOT NULL DEFAULT '',
                override_reason VARCHAR(255) NOT NULL DEFAULT '',
                note_timestamp TIMESTAMPTZ NULL,
                raw_properties JSONB NOT NULL DEFAULT '{}'::jsonb,
                last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hubspot_deal_notes_deal_id ON hubspot_deal_notes(hubspot_deal_id)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS ix_hubspot_deal_notes_note_timestamp ON hubspot_deal_notes(note_timestamp)"))


# Canonical column order for cash_events upsert
_CASH_EVENT_UPSERT_COLS = (
    "id", "source", "source_id", "record_kind", "event_type", "category", "subcategory",
    "description", "name", "vendor_or_customer", "amount_cents", "due_date",
    "status", "confidence", "source_status", "source_open_amount_cents", "source_updated_at",
    "pay_priority", "minimum_payment_cents", "flexibility",
    "recurring_rule", "clickup_task_id",
    "bank_transaction_type", "bank_reference", "match_status", "match_candidates_json",
    "notes", "friendly_name",
)


def refresh_obligation_status_from_evidence(conn, event_id: str) -> str:
    """Derive canonical obligation status without trusting provider terminals."""
    row = conn.execute(text(
        "SELECT amount_cents, due_date, status, record_kind FROM cash_events WHERE id=:id"
    ), {"id": event_id}).fetchone()
    if row is None:
        raise ValueError(f"Unknown cash event: {event_id}")
    data = dict(row._mapping)
    if data.get("record_kind") == "transaction":
        return str(data.get("status") or "posted")

    allocated = int(conn.execute(text("""
        SELECT COALESCE(SUM(allocation.amount_cents), 0)
        FROM settlement_allocations AS allocation
        WHERE allocation.obligation_event_id=:id
          AND allocation.reversed_allocation_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM settlement_allocations AS reversal
              WHERE reversal.reversed_allocation_id=allocation.id
          )
    """), {"id": event_id}).scalar_one() or 0)
    face = max(0, int(data.get("amount_cents") or 0))
    if face > 0 and allocated >= face:
        status = "paid"
    elif str(data.get("status") or "").lower() in {"cancelled", "canceled", "void", "completed"}:
        # Explicit local cancellation remains authoritative.
        status = str(data["status"])
    else:
        raw_due = data.get("due_date")
        try:
            due_day = raw_due.date() if isinstance(raw_due, datetime) else date.fromisoformat(str(raw_due)[:10])
        except (AttributeError, TypeError, ValueError):
            due_day = None
        today = datetime.utcnow().date()
        status = "overdue" if due_day and due_day < today else (
            "pending" if due_day and due_day <= today + timedelta(days=7) else "planned"
        )
    conn.execute(text(
        "UPDATE cash_events SET status=:status, updated_at=:now WHERE id=:id"
    ), {"status": status, "now": datetime.utcnow(), "id": event_id})
    return status


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
    preserve_settlement_truth = bool(event.get("preserve_settlement_truth"))
    apply_source_lifecycle = bool(event.get("apply_source_lifecycle"))

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
                    record_kind=COALESCE(:record_kind, record_kind),
                    event_type=:event_type, category=:category,
                    subcategory=:subcategory, description=:description,
                    name=:name, vendor_or_customer=:vendor_or_customer,
                    amount_cents=:amount_cents, due_date=:due_date,
                    status=CASE
                        WHEN :apply_source_lifecycle AND status NOT IN ('paid', 'matched') THEN :status
                        WHEN :preserve_settlement_truth THEN status
                        ELSE :status
                    END,
                    confidence=:confidence,
                    source_status=COALESCE(:source_status, source_status),
                    source_open_amount_cents=:source_open_amount_cents,
                    source_updated_at=COALESCE(:source_updated_at, source_updated_at),
                    pay_priority=COALESCE(:pay_priority, pay_priority),
                    minimum_payment_cents=COALESCE(:minimum_payment_cents, minimum_payment_cents),
                    flexibility=COALESCE(:flexibility, flexibility),
                    recurring_rule=:recurring_rule,
                    clickup_task_id=:clickup_task_id,
                    bank_transaction_type=:bank_transaction_type,
                    bank_reference=:bank_reference,
                    match_status=COALESCE(:match_status, match_status),
                    match_candidates_json=COALESCE(:match_candidates_json, match_candidates_json),
                    notes=:notes, friendly_name=:friendly_name,
                    updated_at=:updated_at
                WHERE id=:id
            """),
            {
                "id": event["id"],
                "source": event.get("source", ""),
                "source_id": event.get("source_id", event["id"]),
                "record_kind": event.get("record_kind"),
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
                "source_status": event.get("source_status"),
                "source_open_amount_cents": event.get("source_open_amount_cents"),
                "source_updated_at": event.get("source_updated_at"),
                "preserve_settlement_truth": preserve_settlement_truth,
                "apply_source_lifecycle": apply_source_lifecycle,
                "pay_priority": event.get("pay_priority"),
                "minimum_payment_cents": event.get("minimum_payment_cents"),
                "flexibility": event.get("flexibility"),
                "recurring_rule": event.get("recurring_rule", ""),
                "clickup_task_id": event.get("clickup_task_id", ""),
                "bank_transaction_type": event.get("bank_transaction_type", ""),
                "bank_reference": event.get("bank_reference", ""),
                "match_status": event.get("match_status"),
                "match_candidates_json": json.dumps(event.get("match_candidates_json")) if event.get("match_candidates_json") is not None else None,
                "notes": event.get("notes", ""),
                "friendly_name": event.get("friendly_name"),
                "updated_at": now_str,
            },
        )
        result = "updated"
    else:
        conn.execute(
            text("""
                INSERT INTO cash_events (
                    id, source, source_id, record_kind, event_type, category,
                    subcategory, description, name, vendor_or_customer,
                    amount_cents, due_date, status, confidence,
                    source_status, source_open_amount_cents, source_updated_at,
                    pay_priority, minimum_payment_cents, flexibility,
                    recurring_rule, clickup_task_id,
                    bank_transaction_type, bank_reference,
                    match_status, match_candidates_json,
                    notes, friendly_name, created_at, updated_at
                ) VALUES (
                    :id, :source, :source_id, :record_kind, :event_type, :category,
                    :subcategory, :description, :name, :vendor_or_customer,
                    :amount_cents, :due_date, :status, :confidence,
                    :source_status, :source_open_amount_cents, :source_updated_at,
                    :pay_priority, :minimum_payment_cents, :flexibility,
                    :recurring_rule, :clickup_task_id,
                    :bank_transaction_type, :bank_reference,
                    :match_status, :match_candidates_json,
                    :notes, :friendly_name, :created_at, :updated_at
                )
            """),
            {
                "id": event["id"],
                "source": event.get("source", ""),
                "source_id": event.get("source_id", event["id"]),
                "record_kind": event.get("record_kind", "obligation"),
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
                "source_status": event.get("source_status", ""),
                "source_open_amount_cents": event.get("source_open_amount_cents"),
                "source_updated_at": event.get("source_updated_at"),
                "pay_priority": event.get("pay_priority", "review"),
                "minimum_payment_cents": event.get("minimum_payment_cents"),
                "flexibility": event.get("flexibility", "unknown"),
                "recurring_rule": event.get("recurring_rule", ""),
                "clickup_task_id": event.get("clickup_task_id", ""),
                "bank_transaction_type": event.get("bank_transaction_type", ""),
                "bank_reference": event.get("bank_reference", ""),
                "match_status": event.get("match_status", ""),
                "match_candidates_json": json.dumps(event.get("match_candidates_json") or []),
                "notes": event.get("notes", ""),
                "friendly_name": event.get("friendly_name"),
                "created_at": now_str,
                "updated_at": now_str,
            },
        )
        result = "created"

    if preserve_settlement_truth and not apply_source_lifecycle:
        refresh_obligation_status_from_evidence(conn, str(event["id"]))
    return result


def insert_cash_event(conn, *, id, source, source_id, event_type, category,
                       subcategory="", description="", name="", vendor_or_customer="",
                       amount_cents=0, due_date=None, status="planned",
                       confidence="estimated", account_balance_cents=None,
                       bank_transaction_type="", bank_reference="", notes="",
                       recurring_rule="", clickup_task_id="", friendly_name=None,
                       record_kind="obligation", pay_priority="review",
                       minimum_payment_cents=None, flexibility="unknown",
                       source_status="", source_open_amount_cents=None,
                       source_updated_at=None, match_status="",
                       match_candidates_json=None,
                       created_at, updated_at):
    """Single canonical INSERT for cash_events. Use this everywhere instead of inline SQL."""
    due_str = due_date.isoformat() if hasattr(due_date, "isoformat") else (str(due_date)[:10] if due_date else None)
    conn.execute(text("""
        INSERT INTO cash_events (
            id, source, source_id, record_kind, event_type, category,
            subcategory, description, name, vendor_or_customer,
            amount_cents, due_date, status, confidence,
            source_status, source_open_amount_cents, source_updated_at,
            pay_priority, minimum_payment_cents, flexibility,
            account_balance_cents, bank_transaction_type, bank_reference,
            match_status, match_candidates_json,
            notes, recurring_rule, clickup_task_id, friendly_name,
            created_at, updated_at
        ) VALUES (
            :id, :source, :source_id, :record_kind, :event_type, :category,
            :subcategory, :description, :name, :vendor_or_customer,
            :amount_cents, :due_date, :status, :confidence,
            :source_status, :source_open_amount_cents, :source_updated_at,
            :pay_priority, :minimum_payment_cents, :flexibility,
            :account_balance_cents, :bank_transaction_type, :bank_reference,
            :match_status, :match_candidates_json,
            :notes, :recurring_rule, :clickup_task_id, :friendly_name,
            :created_at, :updated_at
        )
    """), {
        "id": id, "source": source, "source_id": source_id, "record_kind": record_kind,
        "event_type": event_type,
        "category": category, "subcategory": subcategory, "description": description,
        "name": name, "vendor_or_customer": vendor_or_customer, "amount_cents": amount_cents,
        "due_date": due_str, "status": status, "confidence": confidence,
        "source_status": source_status,
        "source_open_amount_cents": source_open_amount_cents,
        "source_updated_at": source_updated_at,
        "pay_priority": pay_priority, "minimum_payment_cents": minimum_payment_cents,
        "flexibility": flexibility,
        "account_balance_cents": account_balance_cents,
        "bank_transaction_type": bank_transaction_type, "bank_reference": bank_reference,
        "match_status": match_status,
        "match_candidates_json": json.dumps(match_candidates_json or []),
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
