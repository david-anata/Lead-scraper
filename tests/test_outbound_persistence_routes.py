from unittest.mock import patch

from sqlalchemy import create_engine

from sales_support_agent.api.outbound_router import outbound_brands_csv


def test_pull_is_blocked_before_storeleads_when_persistence_is_unavailable():
    engine = create_engine("sqlite://", future=True)
    with (
        patch("outbound_pipeline.load_config_from_env", return_value=("storeleads-key", "")),
        patch("sales_support_agent.models.database.get_engine", return_value=engine),
        patch("outbound_pipeline.run_storeleads_to_clay") as run_source,
    ):
        response = outbound_brands_csv(None, recipe="icp_baseline")

    assert response.status_code == 503
    assert b"no StoreLeads pull was started" in response.body
    run_source.assert_not_called()

