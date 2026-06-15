"""LLM layer of the Fulfillment Rate Sheet generator.

Two jobs:
  * ``extract_prospect_profile`` — extract a ProspectProfile from the
    flattened intake context plus optional Claude-native PDF/image
    attachments (document/image content blocks). The model ESTIMATES typical
    shipped-package dims for products the source doesn't spec (flagged
    ``dims_estimated``) and parses the prospect's current $/parcel cost.
  * ``generate_narrative`` — write the prospect-specific rate-sheet prose
    (executive summary, savings sentence, fit bullets) from already-computed
    facts; a deterministic template fallback means it never returns blanks
    and never raises.

Mirrors brand_analysis/llm.py conventions: lazy ``import anthropic``,
ANTHROPIC_API_KEY env, JSON-only system prompts, tolerant outermost-braces
parse, deterministic fallbacks so the pipeline never crashes. Default model
is ``claude-sonnet-4-6`` (override via the FULFILLMENT_DECK_MODEL env var).

Schema (ProspectProfile.from_dict / NarrativeBlock.from_dict) does all
clamping/validation; this layer never trusts the model's numbers directly.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.fulfillment_deck.schema import (
    NarrativeBlock,
    ProspectProfile,
)

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-sonnet-4-6"


def _resolve_model(model: Optional[str]) -> str:
    return model or os.environ.get("FULFILLMENT_DECK_MODEL") or _DEFAULT_MODEL


@dataclass
class ExtractionMeta:
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0
    warnings: list = field(default_factory=list)


_SYSTEM = (
    "You are extracting a structured fulfillment-prospect profile for a 3PL "
    "sales team from raw sales notes, uploaded files (including PDF brand "
    "decks / line sheets and product images), and website text. "
    "Return ONLY a JSON object with keys: company, brand, website, "
    "contact_name, contact_email, products (array of {name, length_in, "
    "width_in, height_in, weight_lb, monthly_units, notes, dims_estimated, "
    "product_category, fragile} — numbers in inches and pounds, convert "
    "from cm/kg/oz if the source uses those; product_category is one of "
    "\"beauty\", \"supplements\", \"apparel\", \"food\", \"electronics\", "
    "\"home\", \"other\"; fragile is true only when the item is glass, "
    "ceramic, liquid-in-glass, or otherwise breakable), "
    "monthly_order_volume, volume_basis, volume_provenance, "
    "estimated_sku_count (integer — the TOTAL number of distinct SKUs the "
    "brand sells, stated outright or reasonably estimated from the source; "
    "null when there is no basis), sku_count_basis (short string explaining "
    "the count, e.g. \"100 SKUs stated in RFP deck\" or \"estimated from ~6 "
    "product lines\"; use \"not stated\" when nothing supports a count — never "
    "invent a precise count without a basis), brand_tagline (the brand's "
    "positioning line / tagline if the source states one, else \"\"), "
    "destinations_note, "
    "current_carrier, "
    "current_costs_note, current_cost_per_parcel_usd (numeric average the "
    "prospect pays per parcel today, parsed from any mention such as "
    "\"$9.80/parcel\" or \"about 10 bucks a label\"; null if unknown), "
    "source_confidence (\"low\"/\"medium\"/\"high\" — how complete and "
    "reliable the source material is), raw_notes_excerpt (a quote of at most "
    "2 sentences of the most load-bearing source line; when summarizing, "
    "refer to the organization, never to an individual contact by name). "
    "Volume rules: monthly_order_volume MUST be a total the source states "
    "outright, or the sum of explicitly stated per-channel volumes — never "
    "inferred from revenue, never invented. Return volume_basis as ONLY the "
    "arithmetic behind the number, at most 8 words (e.g. \"74 DTC Shopify + "
    "64 B2B wholesale\") — no commentary, no sourcing; empty string when no "
    "volume is stated. Return volume_provenance as WHERE the number came "
    "from (e.g. \"RFP deck p.2 orders table\" or \"email thread, Apr 12\"); "
    "empty string when no volume is stated. If sources conflict, use the "
    "most explicit figure and note the conflict in volume_provenance. "
    "Product dims rules: when the source provides dims/weight for a product, "
    "use them and set dims_estimated false. When the source gives NO "
    "dims/weight for a product, ESTIMATE typical shipped-package dimensions "
    "and weight from the product type and set dims_estimated true — "
    "estimates must reflect the SHIPPING BOX the item ships in, not the bare "
    "product. Only return a product with null dims if you genuinely cannot "
    "tell what the product is. No markdown, no prose outside the JSON."
)


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Tolerate code fences / leading prose: grab the outermost {...}.
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
    return None


# ---------------------------------------------------------------------------
# Deterministic no-key fallback
# ---------------------------------------------------------------------------

_KV_FIELDS = {
    "company": "company",
    "brand": "brand",
    "website": "website",
    "url": "website",
    "contact": "contact_name",
    "contact name": "contact_name",
    "name": "contact_name",
    "email": "contact_email",
    "contact email": "contact_email",
    "carrier": "current_carrier",
    "current carrier": "current_carrier",
    "destinations": "destinations_note",
    "current costs": "current_costs_note",
    "costs": "current_costs_note",
}

_KV_RE = re.compile(r"^\s*([A-Za-z][A-Za-z ]{0,30})\s*:\s*(.+?)\s*$")
_VOLUME_KEY_RE = re.compile(r"volume|monthly\s+orders?|orders?\s*(/|per)?\s*(mo|month)", re.IGNORECASE)
_INT_RE = re.compile(r"(\d[\d,]*)")

# e.g. "Super Serum — 4 x 4 x 6 in, 1.2 lb, ~3000 units/mo" or "Widget 12x8x4 in 2.5 lbs"
_DIMS_RE = re.compile(
    r"(?P<name>[^\n]+?)[\s,—:-]+"
    r"(?P<l>\d+(?:\.\d+)?)\s*[xX×]\s*(?P<w>\d+(?:\.\d+)?)\s*[xX×]\s*(?P<h>\d+(?:\.\d+)?)\s*(?:in(?:ch(?:es)?)?\.?\b)?"
    r"(?P<rest>[^\n]*)"
)
_WEIGHT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(lbs?|oz)\b", re.IGNORECASE)
_UNITS_RE = re.compile(r"~?\s*(\d[\d,]*)\s*units?\s*(?:/|per)?\s*(?:mo(?:nth)?)?", re.IGNORECASE)
# e.g. "$9.80/parcel", "$9.80 per parcel", "$10 per label/order/package"
_COST_PER_PARCEL_RE = re.compile(
    r"\$\s*(\d+(?:\.\d+)?)\s*(?:/|per)\s*(?:parcel|label|order|package|shipment)",
    re.IGNORECASE,
)


def _fallback_profile(context: str) -> ProspectProfile:
    """Best-effort text parse used when no API key is configured."""
    payload: dict = {}
    products: list[dict] = []

    for line in (context or "").splitlines():
        line = line.strip()
        if not line or line.startswith("==="):
            continue

        m = _KV_RE.match(line)
        if m:
            key = m.group(1).strip().lower()
            value = m.group(2).strip()
            target = _KV_FIELDS.get(key)
            if target and not payload.get(target):
                payload[target] = value
                continue
            if _VOLUME_KEY_RE.search(key) and payload.get("monthly_order_volume") is None:
                n = _INT_RE.search(value)
                if n:
                    payload["monthly_order_volume"] = int(n.group(1).replace(",", ""))
                continue

        d = _DIMS_RE.search(line)
        if d:
            rest = d.group("rest") or ""
            weight = None
            wm = _WEIGHT_RE.search(rest)
            if wm:
                weight = float(wm.group(1))
                if wm.group(2).lower() == "oz":
                    weight = round(weight / 16.0, 2)
            units = None
            um = _UNITS_RE.search(rest)
            if um:
                units = int(um.group(1).replace(",", ""))
            name = re.sub(r"[\s,—:-]+$", "", d.group("name")).strip()
            products.append({
                "name": name,
                "length_in": float(d.group("l")),
                "width_in": float(d.group("w")),
                "height_in": float(d.group("h")),
                "weight_lb": weight,
                "monthly_units": units,
            })

    cost = _COST_PER_PARCEL_RE.search(context or "")
    if cost:
        payload["current_cost_per_parcel_usd"] = float(cost.group(1))

    payload["products"] = products
    payload["source_confidence"] = "low"
    return ProspectProfile.from_dict(payload)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _attachment_blocks(attachments: Optional[list]) -> list:
    """Intake attachment dicts -> Claude content blocks (document/image)."""
    blocks: list = []
    for att in attachments or []:
        if not isinstance(att, dict):
            continue
        kind = att.get("kind")
        data = att.get("data_b64") or ""
        if kind == "pdf":
            blocks.append({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": att.get("media_type") or "application/pdf",
                    "data": data,
                },
            })
        elif kind == "image":
            blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": att.get("media_type") or "image/png",
                    "data": data,
                },
            })
    return blocks


def extract_prospect_profile(
    context: str,
    attachments: Optional[list] = None,
    *,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> tuple[ProspectProfile, ExtractionMeta]:
    """Extract a ProspectProfile from intake context (+ PDF/image attachments).

    ``attachments`` is the list of dicts returned by
    ``intake.build_extraction_context``. Never raises.
    """
    meta = ExtractionMeta()
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    model = _resolve_model(model)

    profile: Optional[ProspectProfile] = None
    if key:
        try:
            import anthropic

            blocks = _attachment_blocks(attachments)
            if blocks:
                content: object = blocks + [{"type": "text", "text": context}]
            else:
                # Back-compat: plain string content when there's nothing to attach.
                content = context

            client = anthropic.Anthropic(api_key=key)
            message = client.messages.create(
                model=model,
                max_tokens=4000,
                system=_SYSTEM,
                messages=[{"role": "user", "content": content}],
            )
            text = (message.content[0].text if message.content else "").strip()
            data = _parse_json(text)
            if data:
                profile = ProspectProfile.from_dict(data)
                meta.model = message.model
                meta.input_tokens = message.usage.input_tokens
                meta.output_tokens = message.usage.output_tokens
            else:
                meta.warnings.append("LLM returned unparseable output — used basic text parsing instead.")
        except Exception:  # noqa: BLE001 — extraction must never crash the pipeline
            logger.warning("[fulfillment_deck] extraction LLM call failed; using fallback parser", exc_info=True)
            meta.warnings.append("LLM extraction failed — used basic text parsing instead.")

    if profile is None:
        profile = _fallback_profile(context)
        if not key:
            meta.warnings.append(
                "ANTHROPIC_API_KEY not set — used basic text parsing; set the key for full extraction."
            )

    estimated = [
        p.name or "(unnamed product)"
        for p in profile.products
        if p.dims_estimated and p.has_full_package_spec
    ]
    if estimated:
        meta.warnings.append(
            "Estimated package specs for: " + ", ".join(estimated) + " — confirm with prospect"
        )
    missing_dims = [p.name or "(unnamed product)" for p in profile.products if not p.has_full_package_spec]
    if missing_dims:
        meta.warnings.append(
            "No usable dims/weight for: " + ", ".join(missing_dims) + " — rate matrix will need them"
        )
    return profile, meta


# ---------------------------------------------------------------------------
# Narrative generation
# ---------------------------------------------------------------------------

_NARRATIVE_SYSTEM = (
    "You are a senior 3PL sales copywriter for Anata, a fulfillment provider "
    "shipping from Lehi, Utah. You are given ALREADY-COMPUTED facts about a "
    "prospect: their profile, a summary of the custom rate matrix built for "
    "them (zones covered, cheapest carrier per zone), and deterministic "
    "savings math. Return ONLY a JSON object with keys: executive_summary "
    "(2-3 sentences, 55 words MAXIMUM, punchy — addressed to the brand by "
    "name about THEIR products, volume, and situation; the bullets carry "
    "the detail, so keep the summary tight; when the profile carries a "
    "brand_tagline or clear positioning, weave it in NATURALLY in the "
    "brand's own voice — never quote it verbatim as a slogan), savings_text "
    "(1-2 sentences "
    "citing ONLY the "
    "provided savings numbers; an empty string if no savings math is "
    "provided), bullets (array of 2-4 short prospect-specific reasons Anata "
    "is a fit). Address the company, never an individual contact by name — "
    "write to the organization (\"the Evre team\", the brand name), even if "
    "the profile includes a contact_name. NEVER invent numbers — cite only "
    "figures present in the input. No markdown, no prose outside the JSON."
)


def _matrix_summary(matrix) -> dict:
    """Compact rate-matrix facts for the narrative prompt."""
    zones: set = set()
    cheapest: dict = {}
    for product in getattr(matrix, "products", ()) or ():
        for zone_rates in getattr(product, "zones", ()) or ():
            zones.add(zone_rates.zone)
            for quote in zone_rates.quotes:
                current = cheapest.get(zone_rates.zone)
                if current is None or quote.rate_usd < current["rate_usd"]:
                    cheapest[zone_rates.zone] = {
                        "carrier": quote.carrier,
                        "service": quote.service,
                        "rate_usd": quote.rate_usd,
                    }
    return {
        "zones_covered": sorted(zones),
        "cheapest_by_zone": {str(zone): cheapest[zone] for zone in sorted(cheapest)},
    }


def _fallback_narrative(profile: ProspectProfile, matrix, savings: Optional[dict]) -> NarrativeBlock:
    """Deterministic template narrative — never blank, never raises."""
    name = profile.display_name
    n_products = len(profile.products)

    clauses = []
    if profile.monthly_order_volume:
        clauses.append(f"ships ~{profile.monthly_order_volume:,} orders a month")
    if n_products:
        plural = "s" if n_products != 1 else ""
        clauses.append(f"across {n_products} product{plural}" if clauses else f"sells {n_products} product{plural}")
    summary = f"{name} {' '.join(clauses)}." if clauses else f"{name} is evaluating fulfillment options."
    summary += (
        " This rate sheet shows Anata's carrier rates from our Lehi, UT warehouse "
        "for the exact packages you ship, so you can compare line by line."
    )

    savings_text = ""
    if savings:
        try:
            savings_text = (
                f"At {int(savings['monthly_orders']):,} orders/month, moving from "
                f"${float(savings['current_per_parcel']):.2f} to Anata's blended "
                f"${float(savings['anata_blended_per_parcel']):.2f} per parcel saves about "
                f"${float(savings['monthly_savings']):,.0f}/month "
                f"(${float(savings['annual_savings']):,.0f}/year)."
            )
        except (KeyError, TypeError, ValueError):
            savings_text = ""

    bullets: list = []
    if profile.monthly_order_volume:
        bullets.append(f"Capacity for {profile.monthly_order_volume:,}+ orders/month from day one")
    if profile.current_carrier:
        bullets.append(f"Multi-carrier rate shopping vs. {profile.current_carrier}-only pricing")
    zones = {
        zone_rates.zone
        for product in getattr(matrix, "products", ()) or ()
        for zone_rates in getattr(product, "zones", ()) or ()
    }
    if zones:
        bullets.append(f"Single Lehi, UT origin covering zones {min(zones)}-{max(zones)} nationwide")
    bullets.append("Transparent per-parcel pricing with no hidden surcharges")
    if len(bullets) < 2:
        bullets.append("Same-day pick/pack with 2-4 day ground delivery to most of the US")

    return NarrativeBlock(
        executive_summary=summary,
        savings_text=savings_text,
        bullets=tuple(bullets[:4]),
        model="none",
    )


def generate_narrative(
    profile: ProspectProfile,
    matrix,
    savings: Optional[dict],
    *,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
) -> NarrativeBlock:
    """Write the prospect-specific rate-sheet prose. Never blank, never raises.

    ``savings`` is the deterministic math computed by the service:
    {current_per_parcel, anata_blended_per_parcel, monthly_orders,
    monthly_savings, annual_savings} — or None when unknown.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    model = _resolve_model(model)

    if key:
        try:
            import anthropic

            facts = json.dumps(
                {
                    "profile": profile.to_dict(),
                    "rate_matrix_summary": _matrix_summary(matrix),
                    "savings": savings,
                },
                sort_keys=True,
            )
            client = anthropic.Anthropic(api_key=key)
            message = client.messages.create(
                model=model,
                max_tokens=1500,
                system=_NARRATIVE_SYSTEM,
                messages=[{"role": "user", "content": facts}],
            )
            text = (message.content[0].text if message.content else "").strip()
            data = _parse_json(text)
            if data and str(data.get("executive_summary") or "").strip():
                parsed = NarrativeBlock.from_dict(data)
                return NarrativeBlock(
                    executive_summary=parsed.executive_summary,
                    savings_text="" if savings is None else parsed.savings_text,
                    bullets=parsed.bullets,
                    model=getattr(message, "model", model),
                    input_tokens=message.usage.input_tokens,
                    output_tokens=message.usage.output_tokens,
                )
            logger.warning("[fulfillment_deck] narrative LLM returned unparseable output; using template")
        except Exception:  # noqa: BLE001 — narrative must never crash the pipeline
            logger.warning("[fulfillment_deck] narrative LLM call failed; using template", exc_info=True)

    return _fallback_narrative(profile, matrix, savings)
