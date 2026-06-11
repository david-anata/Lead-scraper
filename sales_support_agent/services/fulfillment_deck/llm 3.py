"""LLM extraction of a ProspectProfile from the flattened intake context.

Mirrors brand_analysis/llm.py: same key/model convention (lazy ``import
anthropic``, ANTHROPIC_API_KEY env, JSON-only system prompt, tolerant
outermost-braces parse) and a deterministic fallback parser so the pipeline
never crashes — without an API key you still get a best-effort profile from
simple "key: value" and dims-pattern lines.

Schema (ProspectProfile.from_dict) does all clamping/validation; this layer
never trusts the model's numbers directly.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.fulfillment_deck.schema import (
    ProspectProfile,
)

logger = logging.getLogger(__name__)


@dataclass
class ExtractionMeta:
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0
    warnings: list = field(default_factory=list)


_SYSTEM = (
    "You are extracting a structured fulfillment-prospect profile for a 3PL "
    "sales team from raw sales notes, uploaded files, and website text. "
    "Return ONLY a JSON object with keys: company, brand, website, "
    "contact_name, contact_email, products (array of {name, length_in, "
    "width_in, height_in, weight_lb, monthly_units, notes} — numbers in "
    "inches and pounds, convert from cm/kg/oz if the source uses those, null "
    "when unknown — never guess dimensions), monthly_order_volume, "
    "destinations_note, current_carrier, current_costs_note, "
    "source_confidence (\"low\"/\"medium\"/\"high\" — how complete and "
    "reliable the source material is), raw_notes_excerpt (a quote of at most "
    "2 sentences of the most load-bearing source line). No markdown, no "
    "prose outside the JSON."
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
_VOLUME_KEY_RE = re.compile(r"volume|orders?\s*(/|per)?\s*(mo|month)", re.IGNORECASE)
_INT_RE = re.compile(r"(\d[\d,]*)")

# e.g. "Super Serum — 4 x 4 x 6 in, 1.2 lb, ~3000 units/mo" or "Widget 12x8x4 in 2.5 lbs"
_DIMS_RE = re.compile(
    r"(?P<name>[^\n]+?)[\s,—:-]+"
    r"(?P<l>\d+(?:\.\d+)?)\s*[xX×]\s*(?P<w>\d+(?:\.\d+)?)\s*[xX×]\s*(?P<h>\d+(?:\.\d+)?)\s*(?:in(?:ch(?:es)?)?\.?\b)?"
    r"(?P<rest>[^\n]*)"
)
_WEIGHT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(lbs?|oz)\b", re.IGNORECASE)
_UNITS_RE = re.compile(r"~?\s*(\d[\d,]*)\s*units?\s*(?:/|per)?\s*(?:mo(?:nth)?)?", re.IGNORECASE)


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

    payload["products"] = products
    payload["source_confidence"] = "low"
    return ProspectProfile.from_dict(payload)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_prospect_profile(
    context: str,
    *,
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
) -> tuple[ProspectProfile, ExtractionMeta]:
    """Extract a ProspectProfile from intake context. Never raises."""
    meta = ExtractionMeta()
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    profile: Optional[ProspectProfile] = None
    if key:
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=key)
            message = client.messages.create(
                model=model,
                max_tokens=2000,
                system=_SYSTEM,
                messages=[{"role": "user", "content": context}],
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

    missing_dims = [p.name or "(unnamed product)" for p in profile.products if not p.has_full_package_spec]
    if missing_dims:
        meta.warnings.append(
            "No usable dims/weight for: " + ", ".join(missing_dims) + " — rate matrix will need them"
        )
    return profile, meta
