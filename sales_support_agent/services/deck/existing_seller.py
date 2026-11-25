"""Existing-seller copy for a prospect deck.

Used when Amazon search confirms the brand already sells there. The sections
are growth on the matched listings: which ASINs to focus on, rating and
review position, image work on those listings, and ads for the catalog that
is already selling. They do not describe a launch, an entry, or a page that
still has to be created.

The listing roster is rendered by ``catalog_html`` and nowhere else.
"""

from __future__ import annotations

import html
from typing import Any


def summary_text(brand: str) -> str:
    """Executive summary. Does not repeat "already sells on Amazon"."""

    name = (brand or "This brand").strip() or "This brand"
    return (
        f"The plan for {name} is growth on the live catalog: which ASINs to focus on, "
        "where the rating and review count stand, listing and image improvements on those listings, "
        "and an ads structure for the catalog that is already selling. "
        "Anata's software tracks conversion on each ASIN once Seller Central is connected."
    )


def _rows(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> list[dict[str, Any]]:
    return [
        item
        for item in listings or ()
        if isinstance(item, dict) and (str(item.get("asin") or "").strip() or str(item.get("title") or "").strip())
    ]


def focus_listing(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> dict[str, Any] | None:
    """The ASIN with the strongest review base, then the highest rating."""

    rows = _rows(listings)
    if not rows:
        return None

    def sort_key(item: dict[str, Any]) -> tuple[int, float]:
        reviews = item.get("review_count")
        rating = item.get("rating")
        return (
            int(reviews) if isinstance(reviews, int) else -1,
            float(rating) if isinstance(rating, (int, float)) else -1.0,
        )

    return max(rows, key=sort_key)


def listing_line(item: dict[str, Any]) -> str:
    """One listing, once: title, ASIN, rating, reviews, price, image count."""

    parts: list[str] = []
    title = str(item.get("title") or "").strip()
    asin = str(item.get("asin") or "").strip()
    if title:
        parts.append(title)
    if asin:
        parts.append(f"ASIN {asin}")
    rating = item.get("rating")
    if isinstance(rating, (int, float)):
        parts.append(f"{float(rating):.1f} stars")
    reviews = item.get("review_count")
    if isinstance(reviews, int):
        parts.append(f"{reviews:,} reviews")
    price = str(item.get("price") or "").strip()
    if price:
        parts.append(price)
    images = item.get("image_count")
    if isinstance(images, int):
        parts.append(f"{images} images")
    return ", ".join(parts)


def catalog_html(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> str:
    """The only listing summary in the deck, plus rating and review position."""

    rows = _rows(listings)[:4]
    if not rows:
        return (
            "<p>Amazon confirmed a live listing. Connecting Seller Central names the ASIN to focus on.</p>"
        )
    focus = focus_listing(rows) or rows[0]
    focus_asin = str(focus.get("asin") or "").strip()
    items = "".join(f"<li>{html.escape(listing_line(item))}</li>" for item in rows if listing_line(item))
    positions: list[str] = []
    for item in rows:
        asin = str(item.get("asin") or "").strip() or "This listing"
        rating = item.get("rating")
        reviews = item.get("review_count")
        if isinstance(rating, (int, float)) and isinstance(reviews, int):
            positions.append(f"{asin} is at {float(rating):.1f} stars from {reviews:,} reviews.")
        elif isinstance(rating, (int, float)):
            positions.append(f"{asin} is at {float(rating):.1f} stars.")
        elif isinstance(reviews, int):
            positions.append(f"{asin} has {reviews:,} reviews.")
    if focus_asin:
        lead = f"Focus on {focus_asin} first. It has the strongest review base in the matched catalog."
    else:
        lead = "Focus on the listing with the strongest review base in the matched catalog."
    position_html = f"<p>{html.escape(' '.join(positions))}</p>" if positions else ""
    return f"<p>{html.escape(lead)}</p><ul class=\"cklist\">{items}</ul>{position_html}"


def ads_html(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> str:
    """Ads for an existing catalog, and how the software tracks conversion."""

    focus = focus_listing(listings)
    asin = str((focus or {}).get("asin") or "").strip()
    target = f"ASIN {asin}" if asin else "the focus ASIN"
    return (
        "<ul class=\"cklist\">"
        "<li><strong>Catalog structure.</strong> Sponsored Products sit on the ASINs that are already selling. "
        f"Exact match and product targeting start on {html.escape(target)}, then the rest of the matched catalog.</li>"
        "<li><strong>Search terms.</strong> Keep brand terms and the terms already converting. "
        "Pause queries that spend without orders.</li>"
        "<li><strong>Conversion tracking.</strong> Anata's software reads sessions, unit session percentage, "
        "and ad-attributed orders for each of these ASINs once Seller Central is connected.</li>"
        "</ul>"
    )


def creative_html(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> str:
    """Image and listing improvements tied to the matched ASINs."""

    rows = _rows(listings)[:4]
    if not rows:
        return (
            "<p>Image work waits on the ASIN list from Seller Central. "
            "This deck does not invent a page that is not already selling.</p>"
        )
    items: list[str] = []
    for item in rows:
        asin = str(item.get("asin") or "").strip() or "This listing"
        images = item.get("image_count")
        if isinstance(images, int) and images < 7:
            items.append(
                f"{asin} shows {images} images. Add the open image slots on this live listing: "
                "one use frame, one proof frame, and one comparison frame."
            )
        elif isinstance(images, int):
            items.append(
                f"{asin} already has {images} images. Refresh the sequence so each frame makes one claim."
            )
        else:
            items.append(
                f"{asin} did not return an image count. Check the image stack on that live listing before changing creative."
            )
    lis = "".join(f"<li>{html.escape(line)}</li>" for line in items)
    return f"<ul class=\"cklist\">{lis}</ul>"


def strength_line(listings: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None) -> str:
    """A short finding. It is not the listing roster."""

    focus = focus_listing(listings)
    if not focus:
        return "Amazon confirmed this brand sells there, so the work starts from the live catalog."
    asin = str(focus.get("asin") or "").strip() or "The focus listing"
    rating = focus.get("rating")
    reviews = focus.get("review_count")
    if isinstance(rating, (int, float)) and isinstance(reviews, int):
        return (
            f"{asin} carries the strongest review base in the matched catalog, "
            f"at {float(rating):.1f} stars from {reviews:,} reviews."
        )
    if isinstance(reviews, int):
        return f"{asin} carries the strongest review base in the matched catalog, with {reviews:,} reviews."
    return f"{asin} is the listing to focus on in the matched catalog."
