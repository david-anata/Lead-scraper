from sqlalchemy import create_engine, inspect, text

from sales_support_agent.models.database import _ensure_building_columns


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
