"""Carrier name helpers for rate-sheet display and selection."""

from __future__ import annotations

import re


def normalize_carrier_key(carrier: str) -> str:
    """Canonical carrier key for matching aliases such as UniUni / Uni-Uni."""
    return re.sub(r"[^A-Z0-9]+", "", str(carrier or "").upper())
