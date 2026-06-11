"""Rate-sheet generation orchestration.

intake -> LLM profile extraction -> rate matrix (WMS or mock) -> section
flags -> rendered HTML -> persisted AutomationRun. The public view re-serves
summary_json["deck_html"] exactly like sales decks do, token-gated.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

from sales_support_agent.config import Settings
from sales_support_agent.services.deck.formatting import _slugify
from sales_support_agent.services.fulfillment_deck import storage
from sales_support_agent.services.fulfillment_deck.intake import build_extraction_context
from sales_support_agent.services.fulfillment_deck.llm import extract_prospect_profile
from sales_support_agent.services.fulfillment_deck.rendering import render_rate_sheet_html
from sales_support_agent.services.fulfillment_deck.rates import build_rate_matrix
from sales_support_agent.services.fulfillment_deck.schema import (
    ANATA_HQ_ADDRESS,
    ANATA_HQ_ZIP,
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
    """Run the full pipeline; returns the persisted summary (incl. run_id)."""
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

        context, intake_warnings = build_extraction_context(notes, files, website_url)
        warnings.extend(intake_warnings)

        profile, meta = extract_prospect_profile(context)
        warnings.extend(meta.warnings)
        if brand_override.strip():
            profile = profile.__class__.from_dict({**profile.to_dict(), "brand": brand_override.strip()})

        origin = clean_zip(origin_zip) or ANATA_HQ_ZIP
        if origin_zip and clean_zip(origin_zip) is None:
            warnings.append(f"Origin ZIP '{origin_zip}' not recognized — using Anata HQ ({ANATA_HQ_ZIP}).")

        matrix, rate_warnings = build_rate_matrix(list(profile.products), origin, get_wms_client())
        warnings.extend(rate_warnings)

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
        )

        slug = rate_sheet_slug(profile.display_name, now)
        token = secrets.token_hex(16)
        view_path = f"/rate-sheets/{slug}/{run_id}/{token}"

        summary = {
            "design_title": f"{profile.display_name} × Anata Rate Sheet",
            "prospect": profile.display_name,
            "deck_slug": slug,
            "export_token": token,
            "view_path": view_path,
            "deck_html": deck_html,
            "prospect_profile": profile.to_dict(),
            "rate_matrix": matrix.to_dict(),
            "sections_included": [key for key, on in flags.to_dict().items() if on],
            "origin_zip": origin,
            "rates_source": matrix.source,
            "warnings": warnings,
            "llm_model": meta.model,
            "llm_input_tokens": meta.input_tokens,
            "llm_output_tokens": meta.output_tokens,
        }
        storage.complete_run(run_id, summary)
        return {"run_id": run_id, **summary}
    except Exception as exc:  # noqa: BLE001 — persist the failure for History
        logger.exception("[fulfillment_deck] rate sheet generation failed")
        storage.fail_run(run_id, str(exc))
        raise
