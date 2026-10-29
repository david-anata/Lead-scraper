"""Shared LLM client: gateway first on Vercel, then the direct Anthropic key."""

from types import SimpleNamespace

import pytest

from sales_support_agent.services import llm_client as llm


class _Msg:
    def __init__(self) -> None:
        self.content = [SimpleNamespace(text="ok", type="text")]
        self.model = "anthropic/claude-haiku-4.5"


def _default_create(**kwargs):
    _FakeAnthropic.kwargs.append(kwargs)
    return _Msg()


class _FakeAnthropic:
    calls = []
    kwargs = []

    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key
        self.base_url = base_url
        type(self).calls.append(self)

    class messages:
        create = staticmethod(_default_create)


@pytest.fixture
def fake_sdk(monkeypatch):
    import types
    _FakeAnthropic.calls = []
    _FakeAnthropic.kwargs = []
    _FakeAnthropic.messages.create = staticmethod(_default_create)
    module = types.ModuleType("anthropic")
    module.Anthropic = _FakeAnthropic
    monkeypatch.setitem(__import__("sys").modules, "anthropic", module)
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    return _FakeAnthropic


def test_direct_key_is_used_off_vercel(fake_sdk):
    client = llm.llm_client(caller="outbound_openers", api_key="direct-key")
    msg = client.messages.create(model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}])
    assert msg.content[0].text == "ok"
    assert fake_sdk.calls[0].api_key == "direct-key"
    assert fake_sdk.calls[0].base_url is None
    assert fake_sdk.kwargs[0]["model"] == llm.HAIKU
    assert fake_sdk.kwargs[0]["extra_headers"]["ai-reporting-tags"] == "caller:outbound_openers"


def test_gateway_is_used_on_vercel_and_tags_the_caller(fake_sdk, monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", "oidc-token")
    llm.llm_client(caller="abby_classify").messages.create(
        model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
    )
    assert fake_sdk.calls[0].api_key == "oidc-token"
    assert fake_sdk.calls[0].base_url == llm.GATEWAY_BASE_URL
    assert fake_sdk.kwargs[0]["model"] == "anthropic/claude-haiku-4.5"
    gateway = fake_sdk.kwargs[0]["extra_body"]["providerOptions"]["gateway"]
    assert gateway["tags"] == ["caller:abby_classify"]
    assert gateway["models"] == ["google/gemini-2.5-flash-lite"]


def test_gateway_error_falls_back_to_the_direct_key(fake_sdk, monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", "oidc-token")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "direct-key")
    calls = {"n": 0}

    def create(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("gateway down")
        return _Msg()

    fake_sdk.messages.create = staticmethod(create)
    llm.llm_client(caller="ads_narrative", api_key="direct-key").messages.create(
        model=llm.SONNET, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
    )
    assert calls["n"] == 2
    assert fake_sdk.calls[1].base_url is None
    assert fake_sdk.calls[1].api_key == "direct-key"


def test_both_failures_raise_a_clean_error(fake_sdk, monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("VERCEL_OIDC_TOKEN", "oidc-token")

    def create(**kwargs):
        raise RuntimeError("secret-token-should-not-leak")

    fake_sdk.messages.create = staticmethod(create)
    with pytest.raises(llm.LLMError, match="both failed") as caught:
        llm.llm_client(caller="finance_summary", api_key="direct-key").messages.create(
            model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
        )
    assert "secret-token" not in str(caught.value)


def test_missing_oidc_uses_the_direct_key(fake_sdk, monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    llm.llm_client(caller="content_copy", api_key="direct-key").messages.create(
        model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
    )
    assert fake_sdk.calls[0].base_url is None
    assert len(fake_sdk.calls) == 1


def test_header_without_env_var_uses_gateway(fake_sdk, monkeypatch):
    import types

    monkeypatch.setenv("VERCEL", "1")
    headers = types.ModuleType("vercel.headers")
    headers.get = lambda name: "header-token" if str(name).lower() == "x-vercel-oidc-token" else ""
    vercel_mod = types.ModuleType("vercel")
    vercel_mod.headers = headers
    monkeypatch.setitem(__import__("sys").modules, "vercel", vercel_mod)
    monkeypatch.setitem(__import__("sys").modules, "vercel.headers", headers)
    llm.llm_client(caller="llm_ping").messages.create(
        model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
    )
    assert fake_sdk.calls[0].api_key == "header-token"
    assert fake_sdk.calls[0].base_url == llm.GATEWAY_BASE_URL
    assert fake_sdk.kwargs[0]["extra_headers"]["ai-reporting-tags"] == "caller:llm_ping"


def test_request_context_without_env_var_uses_gateway(fake_sdk, monkeypatch):
    monkeypatch.setenv("VERCEL", "1")
    token = llm.bind_request_oidc("context-token")
    try:
        llm.llm_client(caller="llm_ping").messages.create(
            model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
        )
    finally:
        llm.reset_request_oidc(token)
    assert fake_sdk.calls[0].api_key == "context-token"
    assert fake_sdk.calls[0].base_url == llm.GATEWAY_BASE_URL


def test_background_thread_inherits_oidc_context():
    seen = []
    token = llm.bind_request_oidc("threaded-token")

    def read() -> None:
        seen.append(llm._oidc_token())

    try:
        thread = llm.start_background_thread(read)
        thread.join(timeout=2)
    finally:
        llm.reset_request_oidc(token)
    assert seen == ["threaded-token"]


def test_llm_ping_route_requires_internal_api_key(monkeypatch):
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from sales_support_agent.api.llm_ping import router

    def fake_ping():
        return {"route": "gateway", "model": "anthropic/claude-haiku-4.5", "ok": True}

    monkeypatch.setattr("sales_support_agent.api.llm_ping.llm_ping", fake_ping)
    app = FastAPI()
    app.state.settings = SimpleNamespace(internal_api_key="internal-test-key")
    app.include_router(router)
    client = TestClient(app)
    assert client.get("/api/admin/llm-ping").status_code == 401
    response = client.get(
        "/api/admin/llm-ping",
        headers={"X-Internal-Api-Key": "internal-test-key"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "route": "gateway",
        "model": "anthropic/claude-haiku-4.5",
        "ok": True,
    }


def test_no_credentials_raise(monkeypatch):
    monkeypatch.delenv("VERCEL", raising=False)
    monkeypatch.delenv("VERCEL_OIDC_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    class _Missing:
        def __init__(self, api_key=None, base_url=None):
            raise RuntimeError("Could not resolve authentication method")

    import types
    module = types.ModuleType("anthropic")
    module.Anthropic = _Missing
    monkeypatch.setitem(__import__("sys").modules, "anthropic", module)
    with pytest.raises(llm.LLMError, match="No AI Gateway token"):
        llm.llm_client(caller="marketing_intake").messages.create(
            model=llm.HAIKU, max_tokens=10, messages=[{"role": "user", "content": "hi"}],
        )


def test_abby_caches_system_prompt_and_uses_sonnet_only_to_draft(monkeypatch):
    import abby

    prompts = []

    class Msg:
        def __init__(self, text):
            self.content = [SimpleNamespace(text=text, type="text")]

    def create(**kwargs):
        prompts.append(kwargs)
        if kwargs["model"] == abby.CLASSIFY_MODEL:
            return Msg('{"category": "interested", "confidence": 0.9, "reply": null}')
        return Msg('{"reply": "Thanks. Grab a time with David.", "cc_david": false}')

    monkeypatch.setattr(
        llm,
        "llm_client",
        lambda **kwargs: SimpleNamespace(messages=SimpleNamespace(create=create)),
    )
    raw = abby.call_model("Prospect said yes.")
    assert "interested" in raw
    assert "Thanks" in raw
    assert prompts[0]["model"] == abby.CLASSIFY_MODEL
    assert prompts[1]["model"] == abby.DRAFT_MODEL
    assert prompts[0]["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert prompts[1]["system"][0]["cache_control"] == {"type": "ephemeral"}
