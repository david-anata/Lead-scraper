"""Building content review, public projection, and offering readiness rules."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select

from sales_support_agent.models.entities import (
    BuildingLifestyleMedia,
    BuildingOffering,
    BuildingRatePlan,
    BuildingSpace,
    BuildingTenantLogo,
    BuildingTestimonial,
)

CONTENT_STATUSES = {"draft", "needs_review", "approved", "rejected", "retired"}
PUBLIC_URL_RE = re.compile(r"^(?:https://|/(?!/))")
PRIVATE_BENEFIT_RE = re.compile(
    r"\b(?:boom|private[- ]benefit|members?[- ]only benefit)\b",
    re.IGNORECASE,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def is_safe_public_url(value: str) -> bool:
    """Allow HTTPS and site-relative URLs; reject protocol-relative or script URLs."""

    return bool(PUBLIC_URL_RE.match(str(value or "").strip()))


def contains_private_benefit_language(*values: str) -> bool:
    """Keep internal Boom/private-benefit language out of public projections."""

    return any(PRIVATE_BENEFIT_RE.search(str(value or "")) for value in values)


def record_is_public(status: str, review_expires_on: date) -> bool:
    return status == "approved" and review_expires_on >= date.today()


def lifestyle_public_payload(row: BuildingLifestyleMedia) -> dict[str, Any]:
    return {
        "id": row.id,
        "title": row.title,
        "media_url": row.media_url,
        "media_kind": row.media_kind,
        "alt_text": row.alt_text,
        "caption": row.caption,
        "placement": row.placement,
        "updated_at": row.updated_at.isoformat(),
    }


def logo_public_payload(row: BuildingTenantLogo) -> dict[str, Any]:
    return {
        "id": row.id,
        "tenant_name": row.tenant_name,
        "asset_url": row.asset_url,
        "alt_text": row.alt_text,
        "destination_url": row.destination_url or None,
        "updated_at": row.updated_at.isoformat(),
    }


def testimonial_public_payload(row: BuildingTestimonial) -> dict[str, Any]:
    return {
        "id": row.id,
        "quote": row.quote,
        "attribution": {
            "name": row.attribution_name,
            "title": row.attribution_title or None,
            "company": row.attribution_company or None,
        },
        "rating": row.rating,
        "updated_at": row.updated_at.isoformat(),
    }


def public_content_projection(session) -> dict[str, Any]:
    """Return only current, approved, public-safe content fields."""

    lifestyle = [
        row
        for row in session.execute(
            select(BuildingLifestyleMedia).order_by(
                BuildingLifestyleMedia.placement, BuildingLifestyleMedia.title
            )
        ).scalars()
        if record_is_public(row.status, row.review_expires_on)
        and is_safe_public_url(row.media_url)
        and not contains_private_benefit_language(row.title, row.caption, row.alt_text)
    ]
    logos = [
        row
        for row in session.execute(
            select(BuildingTenantLogo).order_by(BuildingTenantLogo.tenant_name)
        ).scalars()
        if record_is_public(row.status, row.review_expires_on)
        and is_safe_public_url(row.asset_url)
        and (not row.destination_url or is_safe_public_url(row.destination_url))
        and not contains_private_benefit_language(row.tenant_name, row.alt_text)
    ]
    testimonials = [
        row
        for row in session.execute(
            select(BuildingTestimonial).order_by(BuildingTestimonial.updated_at.desc())
        ).scalars()
        if record_is_public(row.status, row.review_expires_on)
        and not contains_private_benefit_language(
            row.quote,
            row.attribution_name,
            row.attribution_title,
            row.attribution_company,
        )
    ]
    updated_values = [
        *(row.updated_at for row in lifestyle),
        *(row.updated_at for row in logos),
        *(row.updated_at for row in testimonials),
    ]
    return {
        "lifestyle_media": [lifestyle_public_payload(row) for row in lifestyle],
        "tenant_logos": [logo_public_payload(row) for row in logos],
        "testimonials": [testimonial_public_payload(row) for row in testimonials],
        "updated_at": max(updated_values).isoformat() if updated_values else None,
    }


def _legacy_space_has_approved_media(space: BuildingSpace) -> bool:
    for item in list(space.media_json or []):
        if (
            isinstance(item, dict)
            and item.get("approved") is True
            and str(item.get("alt") or "").strip()
            and is_safe_public_url(str(item.get("src") or ""))
        ):
            return True
    return False


def offering_publication_readiness(
    session,
    offering: BuildingOffering,
    *,
    today: date | None = None,
) -> list[dict[str, str]]:
    """Return deterministic blockers for publishing one offering."""

    current_date = today or date.today()
    blockers: list[dict[str, str]] = []
    if not offering.public_description.strip():
        blockers.append({"field": "public_description", "message": "Add a public description."})
    if not offering.call_to_action.strip():
        blockers.append({"field": "call_to_action", "message": "Choose a conversion action."})
    space = session.get(BuildingSpace, offering.space_id) if offering.space_id else None
    if space is None:
        blockers.append({"field": "space_id", "message": "Link a physical space."})
    else:
        if not space.is_public:
            blockers.append({"field": "space_id", "message": "The linked space is not public."})
        if not space.public_description.strip():
            blockers.append({"field": "space_id", "message": "Add the linked space description."})
        if space.status not in {
            "available", "soft_hold", "contract_pending", "occupied",
            "turnover", "maintenance", "unavailable",
        }:
            blockers.append({"field": "availability", "message": "Set a reviewed availability state."})
        if not _legacy_space_has_approved_media(space):
            blockers.append(
                {"field": "media", "message": "Approve at least one accessible image or video."}
            )
    approved_rate = session.execute(
        select(BuildingRatePlan).where(
            BuildingRatePlan.offering_id == offering.id,
            BuildingRatePlan.status == "approved",
            BuildingRatePlan.effective_from <= current_date,
            (
                BuildingRatePlan.effective_until.is_(None)
                | (BuildingRatePlan.effective_until >= current_date)
            ),
        )
    ).scalar_one_or_none()
    if not (
        (approved_rate and approved_rate.public_price_display.strip())
        or offering.price_display.strip()
    ):
        blockers.append(
            {"field": "pricing", "message": "Add reviewed public pricing or an active approved rate plan."}
        )
    return blockers


def validate_content_for_approval(kind: str, values: dict[str, Any]) -> list[str]:
    """Validate evidence and public-safe fields before approval."""

    errors: list[str] = []
    expiry = values.get("review_expires_on")
    if isinstance(expiry, str):
        try:
            expiry = date.fromisoformat(expiry)
        except ValueError:
            expiry = None
    if not isinstance(expiry, date) or expiry < date.today():
        errors.append("Review expiry must be today or later.")
    if not str(values.get("source_reference") or "").strip():
        errors.append("Source evidence is required.")
    if not str(values.get("consent_reference") or "").strip():
        errors.append("Consent evidence is required.")
    public_text = [
        str(values.get(key) or "")
        for key in (
            "title", "caption", "alt_text", "tenant_name", "quote",
            "attribution_name", "attribution_title", "attribution_company",
        )
    ]
    if contains_private_benefit_language(*public_text):
        errors.append("Remove Boom or private-benefit language from public fields.")
    if kind == "lifestyle_media":
        if values.get("media_kind") not in {"image", "video"}:
            errors.append("Media kind must be image or video.")
        if not is_safe_public_url(str(values.get("media_url") or "")):
            errors.append("Media URL must be HTTPS or site-relative.")
        if not str(values.get("alt_text") or "").strip():
            errors.append("Alt text is required.")
    elif kind == "tenant_logo":
        if not is_safe_public_url(str(values.get("asset_url") or "")):
            errors.append("Logo URL must be HTTPS or site-relative.")
        destination = str(values.get("destination_url") or "")
        if destination and not is_safe_public_url(destination):
            errors.append("Destination URL must be HTTPS or site-relative.")
        if not str(values.get("tenant_name") or "").strip():
            errors.append("Tenant display name is required.")
        if not str(values.get("alt_text") or "").strip():
            errors.append("Alt text is required.")
    elif kind == "testimonial":
        if not str(values.get("quote") or "").strip():
            errors.append("Testimonial text is required.")
        if not str(values.get("attribution_name") or "").strip():
            errors.append("Public attribution is required.")
        rating = values.get("rating")
        if rating is not None and rating not in {1, 2, 3, 4, 5}:
            errors.append("Rating must be between 1 and 5.")
    return errors
