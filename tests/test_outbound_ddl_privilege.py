"""Postgres CREATE is skipped when the outbound table already exists.

agent_app cannot CREATE in schema public. Postgres checks that privilege
before IF NOT EXISTS, so an existing table still raises unless we look first.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, inspect

from sales_support_agent.services import outbound_batches, outbound_ddl, outbound_memory, outbound_settings
import outbound_enrich


class InsufficientPrivilege(Exception):
    """Stand-in for psycopg2.errors.InsufficientPrivilege."""


class _ProgrammingError(Exception):
    def __init__(self) -> None:
        super().__init__("permission denied for schema public")
        self.orig = InsufficientPrivilege("permission denied for schema public")
        self.pgcode = "42501"


class _Result:
    def __init__(self, value):
        self._value = value

    def fetchone(self):
        return (self._value,)


class _Conn:
    def __init__(self, engine: "Postgres"):
        self.engine = engine

    def execute(self, statement, params=None):
        sql = str(statement)
        self.engine.statements.append(sql)
        if "to_regclass" in sql:
            name = (params or {}).get("name")
            return _Result(name if name in self.engine.present else None)
        if sql.lstrip().upper().startswith("CREATE") and self.engine.create_error is not None:
            raise self.engine.create_error
        return _Result(None)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class Postgres:
    """Minimal engine: dialect name postgresql, to_regclass, and CREATE."""

    def __init__(self) -> None:
        self.dialect = SimpleNamespace(name="postgresql")
        self.url = "postgresql://agent_app@db/app"
        self.present: set[str] = set()
        self.statements: list[str] = []
        self.create_error: Exception | None = None

    def connect(self):
        return _Conn(self)

    def begin(self):
        return _Conn(self)


@pytest.fixture(autouse=True)
def _reset_privilege_log():
    outbound_ddl._privilege_logged.clear()
    yield
    outbound_ddl._privilege_logged.clear()


def test_sqlite_contacts_and_sibling_tables_are_created():
    engine = create_engine("sqlite://", future=True)
    outbound_memory.ensure_contacts_table(engine)
    outbound_memory.ensure_contacts_table(engine)
    outbound_memory.ensure_heyreach_table(engine)
    outbound_memory.ensure_table(engine)
    outbound_memory.ensure_runs_table(engine)
    outbound_settings.ensure_tables(engine)
    outbound_batches.ensure_tables(engine)
    outbound_enrich.ensure_tables(engine)
    names = set(inspect(engine).get_table_names())
    assert {
        "outbound_contacts",
        "outbound_heyreach_sent",
        "outbound_contacted_domains",
        "outbound_pull_runs",
        "outbound_settings",
        "outbound_setting_changes",
        "outbound_daily_batches",
        "outbound_enriched",
        "outbound_enriched_contacts",
    } <= names
    assert outbound_memory.record_contacts(engine, [{"email": "Ann@Example.com", "first_name": "Ann"}]) == 1
    assert outbound_memory.load_contacts(engine)[0]["email"] == "ann@example.com"


def test_postgres_skips_create_when_contacts_table_exists():
    engine = Postgres()
    engine.present.add("public.outbound_contacts")
    outbound_memory.ensure_contacts_table(engine)
    assert not any(sql.lstrip().upper().startswith("CREATE") for sql in engine.statements)
    assert any("to_regclass" in sql for sql in engine.statements)


def test_postgres_creates_contacts_table_when_missing():
    engine = Postgres()
    outbound_memory.ensure_contacts_table(engine)
    assert any("CREATE TABLE IF NOT EXISTS outbound_contacts" in sql for sql in engine.statements)


def test_insufficient_privilege_is_logged_once_and_does_not_raise(caplog):
    engine = Postgres()
    engine.create_error = _ProgrammingError()
    with caplog.at_level("WARNING"):
        outbound_memory.ensure_contacts_table(engine)
        outbound_memory.ensure_heyreach_table(engine)
        outbound_memory.ensure_contacts_table(engine)
    assert "CREATE for outbound_contacts skipped after InsufficientPrivilege" in caplog.text
    assert caplog.text.count("CREATE for outbound_contacts skipped") == 1
    assert "CREATE for outbound_heyreach_sent skipped after InsufficientPrivilege" in caplog.text


def test_other_create_errors_still_raise():
    engine = Postgres()
    engine.create_error = RuntimeError("disk full")
    with pytest.raises(RuntimeError, match="disk full"):
        outbound_memory.ensure_contacts_table(engine)


def test_postgres_siblings_skip_create_when_present_and_create_when_missing():
    present = Postgres()
    for table in (
        "outbound_contacted_domains",
        "outbound_pull_runs",
        "outbound_pull_run_leads",
        "outbound_delivery_settings",
        "outbound_export_history",
        "outbound_delivery_history",
        "outbound_heyreach_sent",
        "outbound_settings",
        "outbound_setting_changes",
        "outbound_daily_batches",
        "outbound_daily_batch_runs",
        "outbound_daily_batch_companies",
        "outbound_recipe_definitions",
        "outbound_enriched",
        "outbound_enriched_contacts",
    ):
        present.present.add(f"public.{table}")
    outbound_memory.ensure_table(present, force=True)
    outbound_memory.ensure_runs_table(present, force=True)
    outbound_memory.ensure_heyreach_table(present)
    outbound_settings.ensure_tables(present, force=True)
    outbound_batches.ensure_tables(present, force=True)
    outbound_enrich.ensure_tables(present)
    assert not any(sql.lstrip().upper().startswith("CREATE") for sql in present.statements)

    missing = Postgres()
    outbound_memory.ensure_heyreach_table(missing)
    outbound_settings.ensure_tables(missing, force=True)
    outbound_enrich.ensure_tables(missing)
    created = "\n".join(missing.statements)
    assert "CREATE TABLE IF NOT EXISTS outbound_heyreach_sent" in created
    assert "CREATE TABLE IF NOT EXISTS outbound_settings" in created
    assert "CREATE TABLE IF NOT EXISTS outbound_enriched" in created
