"""Prepare Agent's database before Render promotes a new web instance."""

from __future__ import annotations

from contextlib import contextmanager
import logging
from pathlib import Path
import sys
import time
from typing import Iterator

from sqlalchemy import text

# Render invokes this file directly, which otherwise places ``scripts/`` rather
# than the repository root on Python's import path.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from sales_support_agent.config import load_settings
from sales_support_agent.models.database import (
    backfill_building_inquiry_assignments,
    create_session_factory,
    init_database,
)
from sales_support_agent.services.job_lease import ensure_job_lease_schema
from sales_support_agent.services.website_ops_storage import (
    ensure_website_ops_storage_schema,
)


logger = logging.getLogger("agent.predeploy")
_LOCK_KEY = 1_834_624_661


@contextmanager
def _migration_lock(engine) -> Iterator[None]:
    if engine.dialect.name != "postgresql":
        yield
        return
    connection = engine.connect()
    try:
        connection.execute(
            text("SELECT pg_advisory_lock(:key)"),
            {"key": _LOCK_KEY},
        )
        yield
    finally:
        try:
            connection.execute(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": _LOCK_KEY},
            )
        finally:
            connection.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    started = time.perf_counter()
    settings = load_settings()
    factory = create_session_factory(settings.sales_agent_db_url)
    engine = factory.kw["bind"]
    logger.info("predeploy milestone=database_connected")
    with _migration_lock(engine):
        init_database(factory)
        ensure_job_lease_schema(engine)
        ensure_website_ops_storage_schema(engine)
        configured_owner = (
            settings.building_default_lead_owner
            or (
                settings.rbac_superadmin_emails[0]
                if settings.rbac_superadmin_emails
                else "building-operator"
            )
        )
        backfill_building_inquiry_assignments(
            factory,
            default_owner=configured_owner,
            response_sla_hours=settings.building_response_sla_hours,
        )
        from sales_support_agent.services.access import store as access_store
        from sales_support_agent.services.building_arena_agreement_seed import (
            ensure_arena_review_template,
        )

        access_store.seed_superadmins(
            getattr(settings, "rbac_superadmin_emails", ())
        )
        arena_template_result = ensure_arena_review_template(factory)
        logger.info(
            "predeploy milestone=arena_agreement_template result=%s",
            arena_template_result,
        )
    logger.info(
        "predeploy milestone=schema_ready elapsed_ms=%.1f",
        (time.perf_counter() - started) * 1000,
    )


if __name__ == "__main__":
    main()
