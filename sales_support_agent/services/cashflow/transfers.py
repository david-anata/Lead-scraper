"""Recognise money moving between the operator's own accounts.

An internal transfer is not spending, not income, and not a bill. Counting one
as an expense inflates every total that touches it: the bill audit reports it as
a duplicate, bookkeeping demands a category for it, and vendor spend goes up.

The patterns are deliberately narrow and listed explicitly, because a real
payment worded like a transfer must not be dropped. "Transfer to Wise.com" is a
contractor payment and stays; "Transfer to S0050" is a share account and goes.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

# Share/sub-account moves and home-banking transfers seen in real statements.
_INTERNAL_PATTERNS = (
    r"\bto share\b",
    r"\bfrom share\b",
    r"\ba2a transfer\b",
    r"\btransfer (?:to|from) s\d{3,}\b",
    r"\bhome banking\b.*\btransfer\b",
    r"\binternal transfer\b",
)
_INTERNAL_RE = re.compile("|".join(_INTERNAL_PATTERNS), re.IGNORECASE)


def is_internal_transfer(row: Mapping[str, Any]) -> bool:
    """True when this row is the operator moving their own money."""
    if str(row.get("category") or "").lower() == "transfer":
        return True
    if row.get("is_transfer"):
        return True
    haystack = " ".join(
        str(row.get(field) or "")
        for field in ("name", "vendor_or_customer", "description")
    )
    return bool(_INTERNAL_RE.search(haystack))


def transfer_sql_filter(alias: str = "") -> str:
    """SQL fragment excluding internal transfers, for queries that pre-filter.

    Kept alongside the Python check so both stay in step; the Python version is
    authoritative for anything that has already been loaded.
    """
    prefix = f"{alias}." if alias else ""
    conditions = [f"LOWER(COALESCE({prefix}category,'')) <> 'transfer'"]
    for pattern in ("%to share%", "%a2a transfer%", "%home banking%transfer%"):
        conditions.append(
            f"LOWER(COALESCE({prefix}name,'') || ' ' || COALESCE({prefix}description,'')) NOT LIKE '{pattern}'"
        )
    return " AND ".join(conditions)
