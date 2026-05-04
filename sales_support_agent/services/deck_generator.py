"""Compat shim. Real implementation now lives in sales_support_agent.services.deck.

Kept so external imports of the form
    from sales_support_agent.services.deck_generator import DeckGenerationService
continue to work unchanged.
"""

from __future__ import annotations

from sales_support_agent.services.deck import (  # noqa: F401
    DeckDataset,
    DeckGenerationResult,
    DeckGenerationService,
)
from sales_support_agent.services.deck.formatting import (  # noqa: F401
    _extract_listing_copy_points,
    _normalize_custom_offer_cards,
)

__all__ = [
    "DeckGenerationService",
    "DeckGenerationResult",
    "DeckDataset",
]
