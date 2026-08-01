"""Read money the way an operator types it.

The rate-plan form asked for cents. On 2026-07-31 David typed `4` into a field
labelled "Internal unit price (cents)" for a room that rents at $175 an hour.
Saving that would have priced The Arena at four cents. Nobody types cents; the
form should not have asked for them.

These helpers accept what a person actually writes, including a dollar sign,
thousands separators and stray spaces, and refuse anything ambiguous rather
than guessing. Decimal, not float: 17.55 as a float is 17.549999... and rounds
the wrong way often enough to matter on money.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

# Above this, the operator almost certainly pasted cents into a dollars field
# or fat-fingered a zero. The Arena is $175/hour; nothing here is $1m a unit.
MAX_REASONABLE_DOLLARS = Decimal("1000000")


class MoneyError(ValueError):
    """Raised when a typed amount cannot be read as money."""


def dollars_to_cents(raw: str | int | float | Decimal | None) -> int:
    """Turn a typed dollar amount into whole cents.

    Accepts `175`, `175.00`, `$175`, `1,050.50`, and blank (zero). Rejects
    negatives, more than two decimal places, and anything non-numeric, because
    silently rounding somebody's price is worse than making them retype it.
    """

    if raw is None:
        return 0
    text = str(raw).strip()
    if not text:
        return 0

    cleaned = text.replace("$", "").replace(",", "").replace(" ", "")
    if not cleaned:
        return 0

    try:
        amount = Decimal(cleaned)
    except InvalidOperation as exc:
        raise MoneyError(
            f"{text!r} is not an amount. Enter a number of dollars, for example 175 or 1,050.50."
        ) from exc

    if amount < 0:
        raise MoneyError("An amount cannot be negative.")
    if amount.as_tuple().exponent < -2:
        raise MoneyError(
            f"{text!r} has more than two decimal places. Money stops at cents."
        )
    if amount > MAX_REASONABLE_DOLLARS:
        raise MoneyError(
            f"{text!r} is over ${MAX_REASONABLE_DOLLARS:,.0f}. "
            "Enter dollars, not cents."
        )
    return int(amount.scaleb(2))


def cents_to_dollars(cents: int | None) -> str:
    """Render stored cents for a form field: 17500 becomes '175.00'."""

    return f"{Decimal(int(cents or 0)) / 100:.2f}"


def parse_lines(raw: str | None) -> list[str]:
    """Split a one-per-line textarea into a clean list.

    Replaces the add-ons field that asked operators to write JSON. Blank lines
    and stray bullet characters are dropped so a pasted list still works.
    """

    if not raw:
        return []
    items: list[str] = []
    for line in str(raw).splitlines():
        cleaned = line.strip().lstrip("-*•").strip()
        if cleaned:
            items.append(cleaned)
    return items


def suggested_rate_plan_id(offering_id: str, version: int) -> str:
    """Build the stable id an operator should never have to invent."""

    base = "".join(
        char if char.isalnum() or char == "-" else "-"
        for char in (offering_id or "plan").strip().lower()
    ).strip("-")
    while "--" in base:
        base = base.replace("--", "-")
    return f"{base or 'plan'}-v{max(int(version or 1), 1)}"
