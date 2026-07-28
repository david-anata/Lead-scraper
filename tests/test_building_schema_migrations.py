from sqlalchemy import create_engine, inspect, text

from sales_support_agent.models.database import (
    Base,
    _ensure_building_columns,
    _ensure_building_tables,
    _register_models,
)


def test_building_campaign_compat_migration_keeps_all_column_generations() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE building_campaigns (id VARCHAR(64) PRIMARY KEY)"))

    _ensure_building_columns(engine)
    columns = {column["name"] for column in inspect(engine).get_columns("building_campaigns")}

    assert {
        "content_version",
        "template_reference",
        "content_checksum",
        "content_classification",
        "private_content_approval_evidence",
        "reviewed_by",
        "reviewed_at",
        "sender_identity",
        "scheduled_at",
        "scheduled_by",
        "sent_by",
    } <= columns

    # The production pre-deploy command can safely retry the additive migration.
    _ensure_building_columns(engine)


def test_building_table_bootstrap_uses_complete_model_registry() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    _register_models()

    _ensure_building_tables(engine)
    table_names = set(inspect(engine).get_table_names())

    assert "building_billing_adjustments" in table_names
    assert {
        name for name in Base.metadata.tables if name.startswith("building_")
    } <= table_names

    # Existing production tables remain untouched on retries.
    _ensure_building_tables(engine)
