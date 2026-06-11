"""Rate-sheet generation orchestration.

intake -> LLM profile extraction -> rate matrix (WMS or mock) -> savings math
-> narrative -> section flags -> rendered HTML -> persisted AutomationRun as a
DRAFT. David reviews/edits on the admin review page, then publishes — only
published ("completed") runs serve the public token-gated view.
"""

from __future__ import annotations

import inspect
import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

from sales_support_agent.config import Settings
from sales_support_agent.services.deck.formatting import _slugify
from sales_support_agent.services.fulfillment_deck import llm as llm_module
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.intake import build_extraction_context
from sales_support_agent.services.fulfillment_deck.llm import extract_prospect_profile
from sales_support_agent.services.fulfillment_deck.rendering import render_rate_sheet_html
from sales_support_agent.services.fulfillment_deck.rates import build_rate_matrix
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    ANATA_HQ_ZIP,
    NarrativeBlock,
    ProspectProfile,
    RateMatrix,
    clean_zip,
)
from sales_support_agent.services.fulfillment_deck.sections import decide_sections
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
    name = profile.display_name
    pieces = [
        f"This rate sheet was prepared for {name} from the details shared with Anata: "
        f"{len(matrix.products)} product configuration{'s' if len(matrix.products) != 1 else ''} "
        f"quoted across every US shipping zone from our Lehi, Utah dock."
    ]
    volume = profile.monthly_order_volume or sum(p.monthly_units or 0 for p in profile.products)
    if volume:
        pieces.append(
            f"At roughly {volume:,} orders a month, carrier rates are the single biggest "
            "lever in your fulfillment cost — every line below is rate-shopped at label time."
        )
    if savings:
        pieces.append(
            f"Against your current ~${savings['current_per_parcel']:,.2f} per parcel, the blended "
            f"sample rates here project about ${savings['monthly_savings']:,.0f} in monthly savings."
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


# ---------------------------------------------------------------------------
# Savings math
# ---------------------------------------------------------------------------


def _compute_savings(
    profile: ProspectProfile, matrix: RateMatrix
) -> tuple[Optional[dict], list[str]]:
    """Deterministic monthly/annual savings vs the prospect's reported cost.

    Blended Anata rate = mean of the cheapest quote per zone across products
    (same directional math the volume-economics section shows). Returns
    (savings_dict, warnings); savings is None when inputs are missing or the
    blended rate isn't actually below the prospect's current cost.
    """
    current = profile.current_cost_per_parcel_usd
    if not current:
        return None, []
    volume = profile.monthly_order_volume or sum(p.monthly_units or 0 for p in profile.products)
    if not volume:
        return None, []
    cheapest_rates: list[float] = []
    for product_rates in matrix.products:
        for zone in product_rates.zones:
            best = min((q.rate_usd for q in zone.quotes), default=None)
            if best is not None:
                cheapest_rates.append(best)
    if not cheapest_rates:
        return None, []
    blended = sum(cheapest_rates) / len(cheapest_rates)
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
    }, []


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def _assemble(
    *,
    settings: Settings,
    profile: ProspectProfile,
    origin: str,
    warnings: list[str],
) -> dict:
    """Shared back half: rates -> savings -> narrative -> flags -> HTML.

    Returns the summary fields that change whenever the profile changes.
    """
    matrix, rate_warnings = build_rate_matrix(list(profile.products), origin, get_wms_client())
    warnings.extend(rate_warnings)

    savings, savings_warnings = _compute_savings(profile, matrix)
    warnings.extend(savings_warnings)

    narrative = _build_narrative(profile, matrix, savings)
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
    )
    return {
        "design_title": f"{profile.display_name} × Anata Rate Sheet",
        "prospect": profile.display_name,
        "deck_html": deck_html,
        "prospect_profile": profile.to_dict(),
        "rate_matrix": matrix.to_dict(),
        "sections_included": [key for key, on in flags.to_dict().items() if on],
        "origin_zip": origin,
        "rates_source": matrix.source,
        "narrative": narrative.to_dict(),
        "savings": savings,
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
) -> dict:
    """Run the full pipeline; persists a DRAFT and returns the summary
    (incl. run_id + review_path)."""
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

        origin = clean_zip(origin_zip) or ANATA_HQ_ZIP
        if origin_zip and clean_zip(origin_zip) is None:
            warnings.append(f"Origin ZIP '{origin_zip}' not recognized — using Anata HQ ({ANATA_HQ_ZIP}).")

        assembled = _assemble(settings=settings, profile=profile, origin=origin, warnings=warnings)

        now = datetime.now(timezone.utc)
        slug = rate_sheet_slug(profile.display_name, now)
        token = secrets.token_hex(16)
        view_path = f"/rate-sheets/{slug}/{run_id}/{token}"

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

    patch = _assemble(settings=settings, profile=profile, origin=origin, warnings=[])
    storage.update_summary(run_id, patch)
    summary.update(patch)
    return {"run_id": run_id, **summary}


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
    storage.update_summary(run_id, patch)

    return rerender_rate_sheet(run_id, settings=settings)
