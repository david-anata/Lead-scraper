"""Rate-sheet generation orchestration.

intake -> LLM profile extraction -> rate matrix (WMS or mock) -> savings math
-> narrative -> section flags -> rendered HTML -> persisted AutomationRun as a
DRAFT. David reviews/edits on the admin review page, then publishes — only
published ("completed") runs serve the public token-gated view.
"""

from __future__ import annotations

import inspect
import logging
import re
import secrets
from datetime import datetime, timezone
from typing import Optional

from sales_support_agent.config import Settings
from sales_support_agent.services.deck.formatting import _slugify
from sales_support_agent.services.fulfillment_deck import llm as llm_module
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.intake import (
    build_extraction_context,
    fetch_brand_assets,
)
from sales_support_agent.services.fulfillment_deck.llm import extract_prospect_profile
from sales_support_agent.services.fulfillment_deck.quote import build_fulfillment_quote
from sales_support_agent.services.fulfillment_deck.rendering import render_rate_sheet_html
from sales_support_agent.services.fulfillment_deck.rates import build_rate_matrix
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    ANATA_HQ_ZIP,
    NarrativeBlock,
    ProductSpec,
    ProspectProfile,
    RateMatrix,
    RATE_SOURCE_MOCK,
    clean_segment,
    clean_zip,
)
from sales_support_agent.services.fulfillment_deck.sections import decide_sections
from sales_support_agent.services.fulfillment_deck.us_map import (
    STATE_REP_ZIPS,
    state_zone_map,
)
from sales_support_agent.services.fulfillment_deck.wms_client import get_wms_client

logger = logging.getLogger(__name__)


def rate_sheet_slug(brand: str, when: Optional[datetime] = None) -> str:
    moment = when or datetime.now(timezone.utc)
    stamp = moment.strftime("%Y-%m-%d-%H%M")
    head = _slugify(brand or "") or "prospect"
    return f"{head}-x-anata-rate-sheet-{stamp}"


# ---------------------------------------------------------------------------
# Intake / LLM adapters (transitional shims while intake.py + llm.py are being
# rewritten concurrently — TODO: drop both once the new signatures land)
# ---------------------------------------------------------------------------


def _unpack_intake(result: tuple) -> tuple[str, Optional[list], list]:
    """Accept both the legacy 2-tuple (context, warnings) and the new 3-tuple
    (context, attachments, warnings) from build_extraction_context.

    TODO: remove once intake.py's 3-tuple signature has landed everywhere.
    """
    if len(result) == 3:
        context, attachments, warnings = result
        return context, attachments, list(warnings or [])
    context, warnings = result
    return context, None, list(warnings or [])


def _call_extract(context: str, attachments: Optional[list]):
    """Call extract_prospect_profile, passing attachments only when the new
    signature supports them. TODO: collapse once llm.py's rewrite lands."""
    try:
        params = inspect.signature(extract_prospect_profile).parameters
        accepts_attachments = "attachments" in params or any(
            p.kind == p.VAR_POSITIONAL for p in params.values()
        )
    except (TypeError, ValueError):
        accepts_attachments = False
    if accepts_attachments:
        return extract_prospect_profile(context, attachments)
    return extract_prospect_profile(context)


def _fallback_narrative(
    profile: ProspectProfile, matrix: RateMatrix, savings: Optional[dict]
) -> NarrativeBlock:
    """Deterministic narrative used until llm.generate_narrative exists (and
    as a safety net if it raises). Never blank."""
    # Kept punchy on purpose (v5): 2-3 sentences, under ~55 words — the
    # bullets carry the detail.
    name = profile.display_name
    pieces = [
        f"This rate sheet was prepared for {name}: "
        f"{len(matrix.products)} product configuration{'s' if len(matrix.products) != 1 else ''} "
        f"quoted across every US shipping zone from our Lehi, Utah dock."
    ]
    volume = profile.monthly_order_volume or sum(p.monthly_units or 0 for p in profile.products)
    if volume:
        pieces.append(
            f"At ~{volume:,} orders a month, carrier rates are your biggest "
            "fulfillment lever — every line below is rate-shopped at label time."
        )
    bullets = [
        "Utah origin puts the entire West in zones 1–5 with 2–4 day national ground coverage.",
        "Every order is rate-shopped across carriers at label time — you ship at these rates or better.",
    ]
    if profile.destinations_note:
        bullets.append(f"Quoted with your destination mix in mind: {profile.destinations_note[:140]}.")
    savings_text = ""
    if savings:
        savings_text = (
            f"Switching the blended sample rate of ${savings['anata_blended_per_parcel']:,.2f} in for "
            f"your current ${savings['current_per_parcel']:,.2f} per parcel pencils out to "
            f"${savings['monthly_savings']:,.0f}/month — ${savings['annual_savings']:,.0f} a year — "
            f"at {savings['monthly_orders']:,} orders."
        )
    return NarrativeBlock(
        executive_summary=" ".join(pieces),
        savings_text=savings_text,
        bullets=tuple(bullets[:4]),
        model="fallback",
    )


def _build_narrative(
    profile: ProspectProfile, matrix: RateMatrix, savings: Optional[dict]
) -> NarrativeBlock:
    gen = getattr(llm_module, "generate_narrative", None)
    if gen is not None:
        try:
            narrative = gen(profile, matrix, savings)
            if isinstance(narrative, NarrativeBlock) and narrative.executive_summary.strip():
                return narrative
        except Exception:  # noqa: BLE001 — narrative is decoration, never fatal
            logger.exception("[fulfillment_deck] generate_narrative failed — using fallback")
    return _fallback_narrative(profile, matrix, savings)


def _public_narrative(profile: ProspectProfile, matrix: RateMatrix) -> NarrativeBlock:
    """Deterministic public-funnel copy derived only from the returned matrix."""
    zones = {zone.zone for product in matrix.products for zone in product.zones if zone.quotes}
    products = [product for product in matrix.products if product.zones]
    product_label = "package configuration" if len(products) == 1 else "package configurations"
    zone_label = "representative shipping zone" if len(zones) == 1 else "representative shipping zones"
    bullets = [
        "Every displayed price is carrier postage returned by the live rate engine.",
        "The table shows the carrier service and returned transit time for each representative zone.",
        "Fulfillment service fees are not included in this public Rate Sheet.",
    ]
    if any(product.product.dims_estimated for product in products):
        bullets.append("Package assumptions marked estimated should be confirmed before shipping.")
    return NarrativeBlock(
        executive_summary=(
            f"Live carrier postage was prepared for {profile.display_name} using "
            f"{len(products)} recognized {product_label} across {len(zones)} {zone_label}. "
            "Review the returned service, transit time, and postage by zone below."
        ),
        bullets=tuple(bullets[:4]),
        model="public-deterministic",
    )


# ---------------------------------------------------------------------------
# Savings math
# ---------------------------------------------------------------------------

# Two-letter tokens in destinations_note that are real state codes drive the
# zone weighting (e.g. "shipping mostly to CA, TX, FL").
_STATE_CODE_RE = re.compile(r"\b([A-Z]{2})\b")

BLEND_METHOD_WEIGHTED = "weighted to your stated destination mix"
BLEND_METHOD_FLAT = "flat average across zones"


def _blended_rate(profile: ProspectProfile, matrix: RateMatrix) -> tuple[float, str]:
    """Blended best-rate per parcel across the matrix, plus the method label.

    Per product the cheapest rate per zone wins. ZONE weights: state codes
    parsed from ``profile.destinations_note``; with >=2 distinct known states
    each zone is weighted by how many of those states land in it (zones with
    zero weight excluded), else a flat average across quoted zones. PRODUCT
    weights: ``monthly_units`` share when any product has units, else equal.
    Returns (0.0, method) when the matrix holds no rates.
    """
    zone_weights = _zone_weights_for(profile, matrix)
    method = BLEND_METHOD_WEIGHTED if zone_weights else BLEND_METHOD_FLAT

    any_units = any(pr.product.monthly_units for pr in matrix.products)
    blends: list[tuple[float, float]] = []  # (per-product blend, product weight)
    for product_rates in matrix.products:
        cheapest: dict[int, float] = {}
        for zone in product_rates.zones:
            best = min((q.rate_usd for q in zone.quotes), default=None)
            if best is not None:
                cheapest[zone.zone] = best
        if not cheapest:
            continue
        if zone_weights:
            weight_total = sum(zone_weights.get(z, 0) for z in cheapest)
            if weight_total:
                product_blend = (
                    sum(rate * zone_weights.get(z, 0) for z, rate in cheapest.items())
                    / weight_total
                )
            else:  # product quoted only in zones outside the stated mix
                product_blend = sum(cheapest.values()) / len(cheapest)
        else:
            product_blend = sum(cheapest.values()) / len(cheapest)
        weight = float(product_rates.product.monthly_units or 0) if any_units else 1.0
        blends.append((product_blend, weight))

    if not blends:
        return 0.0, method
    weight_total = sum(weight for _blend, weight in blends)
    if not weight_total:  # only unit-less products carry rates -> equal weights
        return sum(blend for blend, _w in blends) / len(blends), method
    return sum(blend * weight for blend, weight in blends) / weight_total, method


def _zone_weights_for(profile: ProspectProfile, matrix: RateMatrix) -> dict[int, int]:
    """Zone weights from state codes in destinations_note (>=2 distinct known
    states required) — the SAME weighting _blended_rate uses."""
    zone_weights: dict[int, int] = {}
    states: list[str] = []
    for code in _STATE_CODE_RE.findall(profile.destinations_note or ""):
        if code in STATE_REP_ZIPS and code not in states:
            states.append(code)
    if len(states) >= 2:
        zones_by_state = state_zone_map(matrix.origin_zip)
        for code in states:
            zone = zones_by_state.get(code)
            if zone is not None:
                zone_weights[zone] = zone_weights.get(zone, 0) + 1
    return zone_weights


def _avg_transit_days(profile: ProspectProfile, matrix: RateMatrix) -> Optional[float]:
    """Weighted average of BEST-RATE transit days across zones, using the
    SAME zone weights as the blended rate (destination-mix states; flat when
    no usable mix) and the same units-share product weighting.

    Per product per zone the transit days of the cheapest quote count; zones
    whose cheapest quote has no transit estimate are skipped. Returns None
    when no quote anywhere carries transit days.
    """
    zone_weights = _zone_weights_for(profile, matrix)
    any_units = any(pr.product.monthly_units for pr in matrix.products)
    blends: list[tuple[float, float]] = []  # (per-product avg days, weight)
    for product_rates in matrix.products:
        days_by_zone: dict[int, int] = {}
        for zone in product_rates.zones:
            best = min(zone.quotes, key=lambda q: q.rate_usd, default=None)
            if best is not None and best.transit_days:
                days_by_zone[zone.zone] = best.transit_days
        if not days_by_zone:
            continue
        if zone_weights:
            weight_total = sum(zone_weights.get(z, 0) for z in days_by_zone)
            if weight_total:
                product_avg = (
                    sum(d * zone_weights.get(z, 0) for z, d in days_by_zone.items())
                    / weight_total
                )
            else:
                product_avg = sum(days_by_zone.values()) / len(days_by_zone)
        else:
            product_avg = sum(days_by_zone.values()) / len(days_by_zone)
        weight = float(product_rates.product.monthly_units or 0) if any_units else 1.0
        blends.append((product_avg, weight))
    if not blends:
        return None
    weight_total = sum(weight for _avg, weight in blends)
    if not weight_total:
        return sum(avg for avg, _w in blends) / len(blends)
    return sum(avg * weight for avg, weight in blends) / weight_total


def _compute_savings(
    profile: ProspectProfile, matrix: RateMatrix
) -> tuple[Optional[dict], list[str]]:
    """Deterministic monthly/annual savings vs the prospect's reported cost.

    Blended Anata rate comes from :func:`_blended_rate` (destination-mix zone
    weighting + units-share product weighting — same number the monthly-math
    section shows). Returns (savings_dict, warnings); savings is None when
    inputs are missing or the blended rate isn't actually below the
    prospect's current cost.
    """
    current = profile.current_cost_per_parcel_usd
    if not current:
        return None, []
    volume = profile.monthly_order_volume or sum(p.monthly_units or 0 for p in profile.products)
    if not volume:
        return None, []
    blended, method = _blended_rate(profile, matrix)
    if not blended:
        return None, []
    if blended >= current:
        return None, [
            "Blended sample rate not below prospect's current cost — savings section omitted"
        ]
    monthly_savings = round((current - blended) * volume, 2)
    return {
        "current_per_parcel": round(current, 2),
        "anata_blended_per_parcel": round(blended, 2),
        "monthly_orders": int(volume),
        "monthly_savings": round(monthly_savings, 2),
        "annual_savings": round(monthly_savings * 12, 2),
        "blend_method": method,
    }, []


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _opt_margin(value) -> Optional[float]:
    """Normalize a stored/posted quote margin override; None when unset."""
    try:
        margin = float(value)
    except (TypeError, ValueError):
        return None
    if margin <= 0:
        return None
    return margin


def _assemble(
    *,
    settings: Settings,
    profile: ProspectProfile,
    origin: str,
    warnings: list[str],
    view_path: str = "",
    quote_margin_override: Optional[float] = None,
    rate_overrides: Optional[dict] = None,
    rate_card_note: str = "",
    segment: str = "dfy",
    suppress_fulfillment_pricing: bool = False,
) -> dict:
    """Shared back half: rates -> savings -> quote -> narrative -> HTML.

    Returns the summary fields that change whenever the profile changes.
    """
    segment = clean_segment(segment)
    matrix, rate_warnings = build_rate_matrix(list(profile.products), origin, get_wms_client())
    warnings.extend(rate_warnings)
    if matrix.source == RATE_SOURCE_MOCK:
        warnings.append(
            "Live WMS carrier rates are not configured or unavailable — using sample rates "
            "(USPS, UPS, and FedEx only). Configure ANATA_WMS_* before quoting."
        )

    savings, savings_warnings = _compute_savings(profile, matrix)
    warnings.extend(savings_warnings)

    blended_rate, blend_method = _blended_rate(profile, matrix)
    avg_transit = _avg_transit_days(profile, matrix)
    fulfillment_quote = build_fulfillment_quote(
        profile, matrix, blended_rate,
        margin_override=quote_margin_override,
        rate_overrides=rate_overrides or {},
    )
    narrative = _public_narrative(profile, matrix) if suppress_fulfillment_pricing else _build_narrative(profile, matrix, savings)
    flags = decide_sections(profile, matrix)

    now = datetime.now(timezone.utc)
    origin_label = ANATA_HQ_ADDRESS if origin == ANATA_HQ_ZIP else f"ZIP {origin}"
    deck_html = render_rate_sheet_html(
        profile=profile,
        matrix=matrix,
        flags=flags,
        origin_label=origin_label,
        generated_on=now.strftime("%B %d, %Y"),
        settings=settings,
        narrative=narrative,
        savings=savings,
        requote_path=f"{view_path}/requote" if view_path else "",
        blended_rate=blended_rate,
        blend_method=blend_method,
        avg_transit_days=avg_transit,
        quote=fulfillment_quote,
        rate_overrides=rate_overrides or {},
        rate_card_note=rate_card_note or "",
        segment=segment,
        suppress_fulfillment_pricing=suppress_fulfillment_pricing,
    )
    return {
        "design_title": f"{profile.display_name} × Anata Rate Sheet",
        "segment": segment,
        "suppress_fulfillment_pricing": suppress_fulfillment_pricing,
        "prospect": profile.display_name,
        "deck_html": deck_html,
        "prospect_profile": profile.to_dict(),
        "rate_matrix": matrix.to_dict(),
        "sections_included": [key for key, on in flags.to_dict().items() if on],
        "origin_zip": origin,
        "rates_source": matrix.source,
        "narrative": narrative.to_dict(),
        "savings": savings,
        "fulfillment_quote": fulfillment_quote,
        "blend_method": blend_method,
        "blended_rate": round(blended_rate, 2) if blended_rate else None,
        "avg_transit_days": round(avg_transit, 2) if avg_transit else None,
        "warnings": warnings,
    }


def generate_rate_sheet(
    *,
    settings: Settings,
    notes: str,
    files: list[tuple[str, bytes]],
    website_url: str = "",
    origin_zip: str = "",
    brand_override: str = "",
    trigger: str = "admin_dashboard",
    segment: str = "dfy",
    max_products: int = 0,
    suppress_fulfillment_pricing: bool = False,
) -> dict:
    """Run the full pipeline; persists a DRAFT and returns the summary
    (incl. run_id + review_path).

    ``segment`` ("dfy"|"diy") drives the closer + invoice visibility and is
    stored on the run's summary_json. ``max_products`` > 0 caps how many
    products are rate-quoted for THIS render (a speed lever for the public
    self-serve teaser); the FULL extracted product list is still persisted so a
    later rerender/publish produces the complete sheet."""
    segment = clean_segment(segment)
    run_id = storage.create_run(
        trigger=trigger,
        metadata={
            "notes_chars": len(notes or ""),
            "file_names": [name for name, _ in files][:20],
            "website_url": (website_url or "")[:300],
            "origin_zip_input": (origin_zip or "")[:16],
        },
    )
    try:
        warnings: list[str] = []

        context, attachments, intake_warnings = _unpack_intake(
            build_extraction_context(notes, files, website_url)
        )
        warnings.extend(intake_warnings)

        profile, meta = _call_extract(context, attachments)
        warnings.extend(meta.warnings)
        if brand_override.strip():
            profile = ProspectProfile.from_dict(
                {**profile.to_dict(), "brand": brand_override.strip()}
            )

        # v7: personalize from the prospect's site — logo + identity. Part of
        # the intake step; failure is silent (warning at most). Prefer the
        # admin-supplied URL, else the one extraction pulled from the notes.
        known_website = (website_url or "").strip() or profile.website
        if known_website:
            try:
                assets = fetch_brand_assets(known_website)
            except Exception:  # noqa: BLE001 — never block generation
                logger.warning("[fulfillment_deck] brand asset fetch raised", exc_info=True)
                assets = {}
            if assets:
                merged = profile.to_dict()
                if assets.get("logo_data_uri"):
                    merged["brand_logo_data_uri"] = assets["logo_data_uri"]
                if assets.get("tagline") and not merged.get("brand_tagline"):
                    merged["brand_tagline"] = assets["tagline"]
                profile = ProspectProfile.from_dict(merged)
            else:
                warnings.append(
                    f"Could not pull brand logo/identity from {known_website[:80]} — "
                    "sheet renders without the prospect logo."
                )

        origin = clean_zip(origin_zip) or ANATA_HQ_ZIP
        if origin_zip and clean_zip(origin_zip) is None:
            warnings.append(f"Origin ZIP '{origin_zip}' not recognized — using Anata HQ ({ANATA_HQ_ZIP}).")

        now = datetime.now(timezone.utc)
        slug = rate_sheet_slug(profile.display_name, now)
        token = secrets.token_hex(16)
        view_path = f"/rate-sheets/{slug}/{run_id}/{token}"

        # Speed lever for the public teaser: quote only the first N products for
        # this render, but persist the full extracted catalog so a later
        # rerender/publish yields the complete sheet.
        assemble_profile = profile
        if max_products and max_products > 0 and len(profile.products) > max_products:
            assemble_profile = ProspectProfile.from_dict({
                **profile.to_dict(),
                "products": [p.to_dict() for p in profile.products[:max_products]],
            })

        assembled = _assemble(
            settings=settings, profile=assemble_profile, origin=origin,
            warnings=warnings, view_path=view_path, segment=segment,
            suppress_fulfillment_pricing=suppress_fulfillment_pricing,
        )
        # Persist the FULL profile even when the teaser render was truncated.
        assembled["prospect_profile"] = profile.to_dict()

        summary = {
            **assembled,
            "deck_slug": slug,
            "export_token": token,
            "view_path": view_path,
            "review_path": f"/admin/fulfillment/sales/runs/{run_id}/review",
            "status_note": "Draft — review, edit, and publish to activate the public link.",
            "llm_model": meta.model,
            "llm_input_tokens": meta.input_tokens,
            "llm_output_tokens": meta.output_tokens,
        }
        storage.save_draft(run_id, summary)
        return {"run_id": run_id, **summary}
    except Exception as exc:  # noqa: BLE001 — persist the failure for History
        logger.exception("[fulfillment_deck] rate sheet generation failed")
        storage.fail_run(run_id, str(exc))
        raise


def rerender_rate_sheet(run_id: int, *, settings: Settings) -> dict:
    """Rebuild rates/savings/narrative/HTML from the stored profile (used
    after review-page edits). Returns the patched summary."""
    run = storage.get_run(run_id)
    if run is None:
        raise ValueError(f"Rate sheet run {run_id} not found")
    summary = dict(run.summary_json or {})
    profile = ProspectProfile.from_dict(summary.get("prospect_profile") or {})
    origin = clean_zip(summary.get("origin_zip")) or ANATA_HQ_ZIP

    patch = _assemble(
        settings=settings, profile=profile, origin=origin, warnings=[],
        view_path=str(summary.get("view_path") or ""),
        quote_margin_override=_opt_margin(summary.get("quote_margin_override")),
        rate_overrides=dict(summary.get("rate_overrides") or {}),
        rate_card_note=str(summary.get("rate_card_note") or ""),
        segment=clean_segment(summary.get("segment")),
        suppress_fulfillment_pricing=bool(summary.get("suppress_fulfillment_pricing")),
    )
    storage.update_summary(run_id, patch)
    summary.update(patch)
    return {"run_id": run_id, **summary}


def apply_viewer_requote(
    run_id: int, products: list, origin_zip: str, *, settings: Settings,
    persist: bool = True,
) -> dict:
    """Requote a viewer's "Request rates" edit from the hosted sheet's map.

    Merges the posted dims onto the stored profile's products by name match
    (posted dims override; products not posted keep their stored values;
    ``dims_estimated`` clears for any posted product with a full spec — the
    viewer just confirmed real numbers), rebuilds rates/savings, regenerates
    the narrative via the DETERMINISTIC fallback only (no LLM call on viewer
    edits), and re-renders the deck at the SAME view/requote URL.

    Draft/admin preview requotes pass ``persist=True`` so confirmed package
    specs can land on the workbench. Published public sheets pass
    ``persist=False`` so a prospect or internal viewer cannot overwrite the
    canonical published rate matrix for everyone else.

    ``products`` are already-clamped ProductSpec objects. Posted names that
    don't match a stored product are ignored — a public token can't grow the
    stored catalog. Returns the patch dict (plus run_id and persisted flag).
    """
    run = storage.get_run(run_id)
    if run is None:
        raise ValueError(f"Rate sheet run {run_id} not found")
    summary = dict(run.summary_json or {})
    stored_profile = ProspectProfile.from_dict(summary.get("prospect_profile") or {})

    posted_by_name = {p.name: p for p in products if p.name}
    merged: list[dict] = []
    for product in stored_profile.products:
        posted = posted_by_name.get(product.name)
        if posted is None:
            merged.append(product.to_dict())
            continue
        data = product.to_dict()
        data.update(
            {
                "length_in": posted.length_in,
                "width_in": posted.width_in,
                "height_in": posted.height_in,
                "weight_lb": posted.weight_lb,
            }
        )
        if posted.has_full_package_spec:
            data["dims_estimated"] = False
        merged.append(data)
    profile = ProspectProfile.from_dict({**stored_profile.to_dict(), "products": merged})

    origin = (
        clean_zip(origin_zip)
        or clean_zip(summary.get("origin_zip"))
        or ANATA_HQ_ZIP
    )

    matrix, _rate_warnings = build_rate_matrix(list(profile.products), origin, get_wms_client())
    savings, _savings_warnings = _compute_savings(profile, matrix)
    blended_rate, blend_method = _blended_rate(profile, matrix)
    avg_transit = _avg_transit_days(profile, matrix)
    fulfillment_quote = build_fulfillment_quote(
        profile, matrix, blended_rate,
        margin_override=_opt_margin(summary.get("quote_margin_override")),
        rate_overrides=dict(summary.get("rate_overrides") or {}),
    )
    # Deterministic narrative only — viewer edits must never trigger an LLM call.
    suppress_fulfillment_pricing = bool(summary.get("suppress_fulfillment_pricing"))
    if suppress_fulfillment_pricing:
        narrative = _public_narrative(profile, matrix)
    else:
        narrative_fn = getattr(llm_module, "_fallback_narrative", None) or _fallback_narrative
        narrative = narrative_fn(profile, matrix, savings)
    flags = decide_sections(profile, matrix)

    view_path = str(summary.get("view_path") or "")
    origin_label = ANATA_HQ_ADDRESS if origin == ANATA_HQ_ZIP else f"ZIP {origin}"
    deck_html = render_rate_sheet_html(
        profile=profile,
        matrix=matrix,
        flags=flags,
        origin_label=origin_label,
        generated_on=datetime.now(timezone.utc).strftime("%B %d, %Y"),
        settings=settings,
        narrative=narrative,
        savings=savings,
        requote_path=f"{view_path}/requote" if view_path else "",
        blended_rate=blended_rate,
        blend_method=blend_method,
        avg_transit_days=avg_transit,
        quote=fulfillment_quote,
        rate_overrides=dict(summary.get("rate_overrides") or {}),
        rate_card_note=str(summary.get("rate_card_note") or ""),
        segment=clean_segment(summary.get("segment")),
        suppress_fulfillment_pricing=suppress_fulfillment_pricing,
    )
    patch = {
        "prospect_profile": profile.to_dict(),
        "origin_zip": origin,
        "rate_matrix": matrix.to_dict(),
        "savings": savings,
        "fulfillment_quote": fulfillment_quote,
        "blend_method": blend_method,
        "avg_transit_days": round(avg_transit, 2) if avg_transit else None,
        "narrative": narrative.to_dict(),
        "deck_html": deck_html,
        "rates_source": matrix.source,
        "sections_included": [key for key, on in flags.to_dict().items() if on],
    }
    if persist:
        storage.update_summary(run_id, patch)
    return {"run_id": run_id, "persisted": bool(persist), **patch}


def apply_profile_edits(run_id: int, edits: dict, *, settings: Settings) -> dict:
    """Merge review-page edits onto the stored ProspectProfile, then
    re-render. ``edits["products"]`` replaces the product list wholesale —
    a product absent from the list is deleted."""
    run = storage.get_run(run_id)
    if run is None:
        raise ValueError(f"Rate sheet run {run_id} not found")
    summary = dict(run.summary_json or {})
    stored = dict(summary.get("prospect_profile") or {})

    for key in ("brand", "destinations_note", "current_costs_note"):
        if key in edits:
            stored[key] = edits[key]
    for key in ("monthly_order_volume", "current_cost_per_parcel_usd"):
        if key in edits:
            stored[key] = edits[key]  # None clears the value
    if "products" in edits:
        stored["products"] = list(edits["products"] or [])

    profile = ProspectProfile.from_dict(stored)

    patch: dict = {"prospect_profile": profile.to_dict()}
    if "origin_zip" in edits:
        origin = clean_zip(edits.get("origin_zip"))
        if origin:
            patch["origin_zip"] = origin
    if "quote_margin_override" in edits:
        # None clears the override (back to automatic category margins).
        patch["quote_margin_override"] = _opt_margin(edits.get("quote_margin_override"))
    if "rate_overrides" in edits:
        patch["rate_overrides"] = dict(edits["rate_overrides"] or {})
    if "rate_card_note" in edits:
        patch["rate_card_note"] = str(edits.get("rate_card_note") or "").strip()
    if "sales_pricing" in edits:
        patch["sales_pricing"] = dict(edits.get("sales_pricing") or {})
    for key in ("hubspot_deal_id", "hubspot_deal_url"):
        if key in edits:
            patch[key] = str(edits.get(key) or "").strip()
    storage.update_summary(run_id, patch)

    return rerender_rate_sheet(run_id, settings=settings)
