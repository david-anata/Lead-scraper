"""Tests for the deal discovery service (Brand Analysis)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _mock_claude(json_text: str):
    content = MagicMock()
    content.text = json_text
    msg = MagicMock()
    msg.content = [content]
    return msg


def _mock_anthropic_client(json_text: str):
    """Returns a fake anthropic.Anthropic() client whose .messages.create() returns json_text."""
    import sys
    from unittest.mock import MagicMock as _MM
    fake_mod = _MM()
    fake_client = _MM()
    fake_client.messages.create.return_value = _mock_claude(json_text)
    fake_mod.Anthropic.return_value = fake_client
    return fake_mod


def _with_fake_anthropic(json_text: str, fn, *args, **kwargs):
    """Run fn(*args, **kwargs) with anthropic module replaced by a fake."""
    import sys
    import importlib
    fake_mod = _mock_anthropic_client(json_text)
    original = sys.modules.get("anthropic")
    sys.modules["anthropic"] = fake_mod
    try:
        return fn(*args, **kwargs)
    finally:
        if original is None:
            del sys.modules["anthropic"]
        else:
            sys.modules["anthropic"] = original


def test_qualify_listing_passes_strong_deal():
    """A $2M revenue, 45% margin listing should be qualified."""
    from sales_support_agent.services.brand_analysis.deal_discovery import qualify_listing
    listing = {
        "name": "HealthBrand FBA",
        "description": "Amazon FBA supplement brand, 5-star reviews, 45% EBITDA margin.",
        "_revenue_cents": 2_000_000_00,
        "_cashflow_cents": 900_000_00,
        "_asking_price_cents": 5_000_000_00,
        "listing_url": "https://bizbuysell.com/listing/12345",
        "_source": "bizbuysell",
    }
    result = _with_fake_anthropic(
        '{"score": 82, "gaps": [], "extracted": {"ebitda_margin_pct": 45}}',
        qualify_listing, listing,
    )
    assert result["qualified"] is True
    assert result["score"] >= 70
    assert result["criteria"]["revenue_ok"] is True
    assert result["criteria"]["margin_ok"] is True


def test_qualify_listing_fails_low_margin():
    """A 12% EBITDA listing should fail qualification."""
    from sales_support_agent.services.brand_analysis.deal_discovery import qualify_listing
    listing = {
        "name": "LowMargin FBA",
        "description": "Amazon FBA business, 12% EBITDA.",
        "_revenue_cents": 1_500_000_00,
        "_cashflow_cents": 180_000_00,
        "_asking_price_cents": 3_000_000_00,
        "listing_url": "https://bizbuysell.com/listing/99999",
        "_source": "bizbuysell",
    }
    result = _with_fake_anthropic(
        '{"score": 28, "gaps": ["EBITDA margin 12% well below 35% target"], "extracted": {}}',
        qualify_listing, listing,
    )
    assert result["qualified"] is False
    assert result["criteria"]["margin_ok"] is False


def test_qualify_listing_fails_low_revenue():
    """A sub-$1M revenue listing should not be qualified."""
    from sales_support_agent.services.brand_analysis.deal_discovery import qualify_listing
    listing = {
        "name": "Small FBA",
        "description": "Amazon FBA store with good reviews.",
        "_revenue_cents": 400_000_00,
        "_cashflow_cents": 160_000_00,
        "_asking_price_cents": 800_000_00,
        "listing_url": "https://bizbuysell.com/listing/55555",
        "_source": "bizbuysell",
    }
    result = _with_fake_anthropic(
        '{"score": 35, "gaps": ["Revenue below $1M threshold"], "extracted": {}}',
        qualify_listing, listing,
    )
    assert result["qualified"] is False
    assert result["criteria"]["revenue_ok"] is False


def test_create_pipeline_entry_sets_correct_fields():
    """create_pipeline_entry() passes correct fields to create_placeholder_entry."""
    listing = {
        "name": "GoodBrand",
        "_asking_price_cents": 3_000_000_00,
        "_revenue_cents": 1_200_000_00,
        "listing_url": "https://bizbuysell.com/listing/77777",
        "_source": "bizbuysell",
    }
    qualified = {"qualified": True, "score": 75, "gaps": ["No trademark info"]}

    captured = {}

    def fake_create_placeholder(**kwargs):
        captured.update(kwargs)
        return "fake-report-id"

    # create_placeholder_entry is imported inside the function from storage
    with patch(
        "sales_support_agent.services.brand_analysis.storage.create_placeholder_entry",
        side_effect=fake_create_placeholder,
        create=True,
    ):
        from sales_support_agent.services.brand_analysis.deal_discovery import create_pipeline_entry
        rid = create_pipeline_entry(listing, qualified)

    assert rid == "fake-report-id"
    assert captured["brand_name"] == "GoodBrand"
    assert captured["ask_price_cents"] == 3_000_000_00
    assert "bizbuysell.com/listing/77777" in captured["notes"]
    assert "75" in captured["notes"]


def test_dedup_skips_existing():
    """create_placeholder_entry returns None when the listing URL already exists."""
    with patch(
        "sales_support_agent.services.brand_analysis.storage._session"
    ) as mock_session_ctx:
        fake_session = MagicMock()
        fake_session.__enter__ = lambda s: fake_session
        fake_session.__exit__ = MagicMock(return_value=False)
        # Simulate a row with matching URL in notes
        from unittest.mock import patch as _patch
        import importlib
        import sales_support_agent.services.brand_analysis.storage as storage_mod

        with _patch.object(
            storage_mod, "_session",
            return_value=mock_session_ctx.return_value,
        ):
            fake_session.execute.return_value.fetchall.return_value = [
                ("existing-id", "Source: bizbuysell | URL: https://bizbuysell.com/listing/ABC | Score: 80"),
            ]
            mock_session_ctx.return_value.__enter__ = lambda s: fake_session
            mock_session_ctx.return_value.__exit__ = MagicMock(return_value=False)
            result = storage_mod.create_placeholder_entry(
                brand_name="Test",
                notes="Source: bizbuysell | URL: https://bizbuysell.com/listing/ABC | Score: 80",
            )

    assert result is None
