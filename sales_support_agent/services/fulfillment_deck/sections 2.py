"""Section selection for the Fulfillment Rate Sheet.

Pure function of the extracted profile + computed rate matrix: each optional
section renders only when the data behind it actually exists. Cover and the
About Anata closer always render.
"""

from __future__ import annotations

from sales_support_agent.services.fulfillment_deck.schema import (
    ProspectProfile,
    RateMatrix,
    SectionFlags,
)


def decide_sections(profile: ProspectProfile, matrix: RateMatrix) -> SectionFlags:
    has_rates = bool(matrix.products)
    has_volume = bool(
        profile.monthly_order_volume
        or any(p.monthly_units for p in profile.products)
    )
    return SectionFlags(
        cover=True,
        rate_matrix=has_rates,
        zone_map=has_rates,
        volume_economics=has_volume,
        cost_comparison=bool(profile.current_costs_note.strip()),
        destinations=bool(profile.destinations_note.strip()),
        about_anata=True,
    )
