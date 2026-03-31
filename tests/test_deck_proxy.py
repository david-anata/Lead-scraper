from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

PYTHON_SUPPORTED = sys.version_info >= (3, 10)

try:
    import requests
    from starlette.requests import Request

    REPO_ROOT = Path(__file__).resolve().parents[1]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    if PYTHON_SUPPORTED:
        import main

        FASTAPI_AVAILABLE = True
    else:
        FASTAPI_AVAILABLE = False
except ModuleNotFoundError as exc:
    if exc.name not in {"fastapi", "sqlalchemy", "requests"}:
        raise
    FASTAPI_AVAILABLE = False


def _fake_response(status_code: int, body: bytes = b"", content_type: str = "text/html; charset=utf-8") -> SimpleNamespace:
    return SimpleNamespace(
        status_code=status_code,
        content=body,
        headers={"Content-Type": content_type},
    )


@unittest.skipUnless(FASTAPI_AVAILABLE, "python>=3.10, fastapi, requests, and sqlalchemy are required for deck proxy tests")
class DeckProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = SimpleNamespace(sales_support_agent_url="https://sales-support-agent.onrender.com")

    def _request(self, path: str, query_string: bytes = b"") -> Request:
        return Request(
            {
                "type": "http",
                "method": "GET",
                "scheme": "https",
                "server": ("agent.anatainc.com", 443),
                "path": path,
                "query_string": query_string,
                "headers": [],
            }
        )

    def test_public_deck_proxy_passes_through_success(self) -> None:
        with mock.patch.object(main, "load_admin_dashboard_settings", return_value=self.settings), mock.patch.object(
            main.requests,
            "get",
            return_value=_fake_response(200, b"<html>deck ok</html>"),
        ) as get_mock:
            response = main.public_deck_proxy(
                self._request("/decks/test-deck/61/token-123", b"viewer=internal"),
                "test-deck",
                61,
                "token-123",
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("deck ok", response.body.decode())
        get_mock.assert_called_once_with(
            "https://sales-support-agent.onrender.com/decks/test-deck/61/token-123?viewer=internal",
            timeout=main.DECK_PROXY_TIMEOUT_SECONDS,
        )

    def test_public_deck_proxy_passes_through_not_found(self) -> None:
        with mock.patch.object(main, "load_admin_dashboard_settings", return_value=self.settings), mock.patch.object(
            main.requests,
            "get",
            return_value=_fake_response(404, b"Deck export not found."),
        ):
            response = main.public_deck_proxy(self._request("/decks/test-deck/61/token-123"), "test-deck", 61, "token-123")

        self.assertEqual(response.status_code, 404)
        self.assertIn("Deck export not found.", response.body.decode())

    def test_public_deck_proxy_retries_retryable_status_then_succeeds(self) -> None:
        with mock.patch.object(main, "load_admin_dashboard_settings", return_value=self.settings), mock.patch.object(
            main.requests,
            "get",
            side_effect=[
                _fake_response(502, b"bad gateway"),
                _fake_response(200, b"<html>deck ok</html>"),
            ],
        ) as get_mock, mock.patch.object(main.time, "sleep", return_value=None) as sleep_mock:
            response = main.public_deck_proxy(self._request("/decks/test-deck/61/token-123"), "test-deck", 61, "token-123")

        self.assertEqual(response.status_code, 200)
        self.assertIn("deck ok", response.body.decode())
        self.assertEqual(get_mock.call_count, 2)
        sleep_mock.assert_called_once_with(main.DECK_PROXY_RETRY_DELAYS_SECONDS[0])

    def test_public_deck_proxy_retries_exception_then_succeeds(self) -> None:
        with mock.patch.object(main, "load_admin_dashboard_settings", return_value=self.settings), mock.patch.object(
            main.requests,
            "get",
            side_effect=[
                requests.ConnectionError("upstream reset"),
                _fake_response(200, b"<html>deck ok</html>"),
            ],
        ) as get_mock, mock.patch.object(main.time, "sleep", return_value=None) as sleep_mock:
            response = main.public_deck_proxy(self._request("/decks/test-deck/61/token-123"), "test-deck", 61, "token-123")

        self.assertEqual(response.status_code, 200)
        self.assertIn("deck ok", response.body.decode())
        self.assertEqual(get_mock.call_count, 2)
        sleep_mock.assert_called_once_with(main.DECK_PROXY_RETRY_DELAYS_SECONDS[0])

    def test_public_deck_proxy_returns_controlled_502_after_repeated_failure(self) -> None:
        with mock.patch.object(main, "load_admin_dashboard_settings", return_value=self.settings), mock.patch.object(
            main.requests,
            "get",
            side_effect=[
                _fake_response(503, b"unavailable"),
                requests.Timeout("timed out"),
                _fake_response(502, b"bad gateway"),
            ],
        ) as get_mock, mock.patch.object(main.time, "sleep", return_value=None) as sleep_mock:
            response = main.public_deck_proxy(self._request("/decks/test-deck/61/token-123"), "test-deck", 61, "token-123")

        self.assertEqual(response.status_code, 502)
        body = response.body.decode()
        self.assertIn("Deck is temporarily unavailable.", body)
        self.assertIn("Retry in a few seconds.", body)
        self.assertEqual(get_mock.call_count, 3)
        self.assertEqual(
            sleep_mock.call_args_list,
            [
                mock.call(main.DECK_PROXY_RETRY_DELAYS_SECONDS[0]),
                mock.call(main.DECK_PROXY_RETRY_DELAYS_SECONDS[1]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
