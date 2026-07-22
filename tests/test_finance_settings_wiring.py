import asyncio
from types import SimpleNamespace

from sales_support_agent.api import cashflow_router
from sales_support_agent.api.cashflow_router import _finance_settings, _plaid_client_user_id


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


def test_plaid_user_id_is_stable_and_contains_no_email_address():
    first = _plaid_client_user_id({"email": "David@AnataInc.com"})
    second = _plaid_client_user_id({"email": "david@anatainc.com"})

    assert first == second
    assert first.startswith("finance-")
    assert "@" not in first
    assert "david" not in first


def test_plaid_link_loader_is_served_as_first_party_javascript(monkeypatch):
    class FakeResponse:
        content = b"window.Plaid = {};"

        def raise_for_status(self):
            return None

    cashflow_router._load_plaid_link_sdk.cache_clear()
    monkeypatch.setattr(cashflow_router.requests, "get", lambda *args, **kwargs: FakeResponse())

    response = asyncio.run(cashflow_router.plaid_link_sdk())

    assert response.body == b"window.Plaid = {};"
    assert response.media_type == "text/javascript"
    assert response.headers["cache-control"].startswith("public")
