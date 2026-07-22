from types import SimpleNamespace

from sales_support_agent.api.cashflow_router import _finance_settings


def test_finance_prefers_full_agent_settings_over_root_app_settings():
    root_settings = SimpleNamespace(apollo_api_key="legacy-root")
    agent_settings = SimpleNamespace(
        plaid_client_id="client",
        plaid_secret="secret",
        plaid_token_secret="token-secret",
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(settings=root_settings, agent_settings=agent_settings)
        )
    )

    assert _finance_settings(request) is agent_settings


def test_finance_settings_falls_back_for_standalone_app():
    standalone_settings = SimpleNamespace(plaid_client_id="client")
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(settings=standalone_settings))
    )

    assert _finance_settings(request) is standalone_settings
