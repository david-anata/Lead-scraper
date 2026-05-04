"""Deck generation package.

Public API:
- DeckGenerationService — orchestration class
- DeckGenerationResult, DeckDataset — return / payload types
"""

from __future__ import annotations

from sales_support_agent.services.deck.dataset import (
    DeckDataset,
    DeckGenerationResult,
)
from sales_support_agent.services.deck.service import DeckGenerationService

__all__ = [
    "DeckGenerationService",
    "DeckGenerationResult",
    "DeckDataset",
]
