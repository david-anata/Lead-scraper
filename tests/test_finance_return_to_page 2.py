"""Every action used to bounce back to the finance home page.

Dismissing one audit finding meant navigating back to Bill audit and scrolling
again. Actions now return to the page that submitted them.
"""

from types import SimpleNamespace

import pytest

from sales_support_agent.api import cashflow_router as router


class _FakeRequest:
    """Minimal stand-in: only the bits _safe_return_path actually reads."""

    def __init__(self, referer: str = "", host: str = "agent.anatainc.com", return_to: str = ""):
        self.headers = {"referer": referer} if referer else {}
        self.url = SimpleNamespace(netloc=host)
        self.state = SimpleNamespace()
        if return_to:
            self.state.finance_return_to = return_to


def _with(request):
    token = router._CURRENT_REQUEST.set(request)
    try:
        return router._safe_return_path()
    finally:
        router._CURRENT_REQUEST.reset(token)


def test_an_action_returns_to_the_page_it_came_from():
    assert _with(_FakeRequest("https://agent.anatainc.com/admin/finances/audit")) == "/admin/finances/audit"
    assert _with(_FakeRequest("https://agent.anatainc.com/admin/finances/bookkeeping")) == "/admin/finances/bookkeeping"
    assert _with(_FakeRequest("https://agent.anatainc.com/admin/finances/review")) == "/admin/finances/review"


def test_an_anchor_is_kept_so_you_land_on_the_same_section():
    assert _with(_FakeRequest(return_to="/admin/finances#update-money")) == "/admin/finances#update-money"


def test_an_explicit_return_wins_over_the_referring_page():
    request = _FakeRequest(
        referer="https://agent.anatainc.com/admin/finances",
        return_to="/admin/finances#update-money",
    )
    assert _with(request) == "/admin/finances#update-money"


def test_a_query_string_on_the_referrer_is_dropped_so_flashes_do_not_stack():
    result = _with(_FakeRequest("https://agent.anatainc.com/admin/finances/audit?flash=ok%3Adone"))
    assert result == "/admin/finances/audit"


@pytest.mark.parametrize("hostile", [
    "https://evil.example.com/admin/finances/audit",   # another host
    "https://agent.anatainc.com/admin/sales",          # outside finance
    "https://agent.anatainc.com/",                     # site root
    "javascript:alert(1)",                             # not a path at all
])
def test_a_destination_outside_finance_is_never_followed(hostile):
    assert _with(_FakeRequest(hostile)) == "/admin/finances"


def test_no_referrer_falls_back_to_the_finance_page():
    assert _with(_FakeRequest()) == "/admin/finances"
    assert router._safe_return_path() == "/admin/finances"  # nothing tracked at all


def test_the_flash_message_survives_the_redirect():
    request = _FakeRequest("https://agent.anatainc.com/admin/finances/audit")
    token = router._CURRENT_REQUEST.set(request)
    try:
        response = router._redirect_with_flash("Audit item dismissed.")
        error = router._redirect_with_flash("That failed.", level="err")
    finally:
        router._CURRENT_REQUEST.reset(token)

    assert response.status_code == 303
    assert response.headers["location"].startswith("/admin/finances/audit?flash=ok%3A")
    assert error.headers["location"].startswith("/admin/finances/audit?flash=err%3A")


def test_an_anchor_stays_after_the_flash_is_appended():
    request = _FakeRequest(return_to="/admin/finances#update-money")
    token = router._CURRENT_REQUEST.set(request)
    try:
        response = router._redirect_with_flash("Account updated.")
    finally:
        router._CURRENT_REQUEST.reset(token)

    location = response.headers["location"]
    assert location.startswith("/admin/finances?flash=")
    assert location.endswith("#update-money"), "the dialog must reopen where you were"
