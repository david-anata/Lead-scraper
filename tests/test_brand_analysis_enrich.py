"""Tests for the auto-enrich service (Brand Analysis)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_claude_response(json_text: str):
    content = MagicMock()
    content.text = json_text
    msg = MagicMock()
    msg.content = [content]
    return msg


def test_auto_enrich_returns_dict():
    """auto_enrich() returns a dict with _sources and _errors even on partial data."""
    with patch("sales_support_agent.services.brand_analysis.enrich._fetch") as mock_fetch, \
         patch("sales_support_agent.services.brand_analysis.enrich._claude_extract") as mock_claude, \
         patch("sales_support_agent.services.brand_analysis.social.discover_socials") as mock_discover:
        mock_discover.return_value = {
            "instagram": "https://instagram.com/testbrand",
            "youtube": "https://youtube.com/@testbrand",
        }
        mock_fetch.return_value = "<html>fake page</html>"
        mock_claude.return_value = {"followers": 12000}

        from sales_support_agent.services.brand_analysis.enrich import auto_enrich
        result = auto_enrich("Test Brand", "testbrand.com", {})

    assert isinstance(result, dict)
    assert "_sources" in result
    assert "_errors" in result
    # Should have populated instagram_url from discover_socials
    assert result.get("instagram_url") == "https://instagram.com/testbrand"


def test_auto_enrich_fails_open():
    """auto_enrich() returns a partial dict without raising when network fails."""
    with patch("sales_support_agent.services.brand_analysis.enrich._fetch",
               side_effect=Exception("network down")), \
         patch("sales_support_agent.services.brand_analysis.social.discover_socials",
               return_value={"instagram": "https://instagram.com/foo"}):
        from sales_support_agent.services.brand_analysis.enrich import auto_enrich
        result = auto_enrich("Broken Brand", "brokensite.com", {})

    assert isinstance(result, dict)
    assert "_errors" in result
    # Should not raise — must return something
    assert "instagram_url" in result  # URL is set before follower fetch fails


def test_auto_enrich_skips_amazon_on_error():
    """Amazon fetch failure is captured in _errors, not raised."""
    with patch("sales_support_agent.services.brand_analysis.enrich._fetch",
               side_effect=Exception("timeout")), \
         patch("sales_support_agent.services.brand_analysis.social.discover_socials",
               return_value={}):
        from sales_support_agent.services.brand_analysis.enrich import auto_enrich
        result = auto_enrich("TestBrand", "", {})

    assert "amazon" in " ".join(result.get("_errors", []))


def test_yt_subscribers_parses_ytInitialData():
    """YouTube subscriber count is parsed from embedded ytInitialData JSON (no API key)."""
    with patch("sales_support_agent.services.brand_analysis.enrich._fetch") as mock_fetch:
        mock_fetch.return_value = (
            'window["ytInitialData"] = {..., "subscriberCount":"125000", ...}'
        )
        from sales_support_agent.services.brand_analysis.enrich import _yt_subscribers
        result = _yt_subscribers("https://youtube.com/@testchannel")

    assert result == 125000


def test_yt_subscribers_parses_count_text():
    """YouTube subscriberCountText '1.23M subscribers' is parsed correctly."""
    with patch("sales_support_agent.services.brand_analysis.enrich._fetch") as mock_fetch:
        mock_fetch.return_value = (
            '"subscriberCountText":{"simpleText":"1.23M subscribers"}'
        )
        from sales_support_agent.services.brand_analysis.enrich import _yt_subscribers
        result = _yt_subscribers("https://youtube.com/@testchannel")

    assert result == 1_230_000


def test_parse_count_text():
    """_parse_count_text converts human-readable counts to integers."""
    from sales_support_agent.services.brand_analysis.enrich import _parse_count_text
    assert _parse_count_text("1.23M subscribers") == 1_230_000
    assert _parse_count_text("5.4K followers") == 5_400
    assert _parse_count_text("120,000") == 120_000
    assert _parse_count_text("2B") == 2_000_000_000
    assert _parse_count_text(None) is None


def test_parse_usd():
    from sales_support_agent.services.brand_analysis.deal_discovery import _parse_usd
    assert _parse_usd("$1.2M") == 120_000_000
    assert _parse_usd("500K") == 50_000_000
    assert _parse_usd("2500000") == 250_000_000_00 // 100  # noqa
    assert _parse_usd(None) is None
    assert _parse_usd("bad") is None
