"""Shared utilities for the cashflow module."""
from __future__ import annotations


def encode_flash(kind: str, message: str) -> str:
    """Encode a flash message for use as a query parameter.

    kind: 'ok' or 'err'
    Returns: 'ok:Message text' or 'err:Message text'
    """
    safe_kind = "ok" if kind == "ok" else "err"
    return f"{safe_kind}:{message}"


def decode_flash(raw: str) -> tuple[str, str]:
    """Decode a flash query parameter into (kind, message).

    Returns ('ok', message), ('err', message), or ('', '') if empty/invalid.
    """
    if not raw:
        return ("", "")
    if raw.startswith("ok:"):
        return ("ok", raw[3:])
    if raw.startswith("err:"):
        return ("err", raw[4:])
    return ("", raw)
