"""Canonical data contract for the Fulfillment Rate Sheet generator.

Conventions (mirroring services/brand_analysis/schema.py):
  * money is float USD here (carrier rates are quoted in dollars+cents and we
    never aggregate them into ledgers — display only)
  * dimensions are inches, weights are pounds
  * a *missing* input is ``None`` (distinct from a real 0) so section logic can
    tell "absent" from "zero"
  * parsers never raise — bad/blank values degrade to None
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional

ANATA_HQ_ZIP = "84043"
ANATA_HQ_ADDRESS = "1657 N. State Street, Lehi, UT 84043"

RATE_SOURCE_WMS = "wms"
RATE_SOURCE_MOCK = "mock"

# Sanity clamps for LLM-extracted product specs. Anything outside is dropped
# to None (parcel network limits, roughly).
MAX_DIM_IN = 108.0
MAX_WEIGHT_LB = 150.0

# Whitelist for LLM-extracted product categories (drives the fulfillment
# quote's category multiplier). Unknown non-empty values clamp to "other";
# blank stays blank ("no claim").
PRODUCT_CATEGORIES = (
    "beauty", "supplements", "apparel", "food", "electronics", "home", "other",
)

# Prospect funnel segment. "dfy" (done-for-you) = full Anata 3PL: we ship from
# our dock, the sheet shows the line-item fulfillment invoice. "diy"
# (do-it-yourself) = same negotiated carrier rates, the prospect ships from
# their OWN origin ZIP on Anata Shipping OS; the invoice section is hidden and
# the closer leads with the try-free Shipping OS offer.
SEGMENTS = ("dfy", "diy")
DEFAULT_SEGMENT = "dfy"


def clean_segment(value: object) -> str:
    """Normalize a funnel segment to one of SEGMENTS; unknown -> DEFAULT_SEGMENT."""
    v = str(value or "").strip().lower()
    return v if v in SEGMENTS else DEFAULT_SEGMENT


def clean_zip(value: object) -> Optional[str]:
    """Normalize a US ZIP to 5 digits, or None."""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    return None


def _pos_float(value: object, maximum: float) -> Optional[float]:
    try:
        f = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if f <= 0 or f > maximum:
        return None
    return round(f, 2)


@dataclass(frozen=True)
class ProductSpec:
    """One sellable product / package configuration from the prospect."""

    name: str = ""
    length_in: Optional[float] = None
    width_in: Optional[float] = None
    height_in: Optional[float] = None
    weight_lb: Optional[float] = None
    monthly_units: Optional[int] = None
    notes: str = ""
    # True when the dims/weight were ESTIMATED from the product type rather
    # than provided by the prospect — rendered with a visible "estimated" tag.
    dims_estimated: bool = False
    # Fulfillment-quote inputs: category drives the quote margin multiplier,
    # fragile adds a handling bump. Never rendered on the public sheet.
    product_category: str = ""  # one of PRODUCT_CATEGORIES, or "" for unknown
    fragile: bool = False

    @property
    def has_full_package_spec(self) -> bool:
        return None not in (self.length_in, self.width_in, self.height_in, self.weight_lb)

    @property
    def dims_key(self) -> tuple:
        """Identity for deduping identical package specs across products."""
        return (self.length_in, self.width_in, self.height_in, self.weight_lb)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(payload: dict) -> "ProductSpec":
        units = payload.get("monthly_units")
        try:
            units = int(units) if units is not None else None
        except (TypeError, ValueError):
            units = None
        if units is not None and units <= 0:
            units = None
        category = str(payload.get("product_category") or "").strip().lower()
        if category and category not in PRODUCT_CATEGORIES:
            category = "other"
        return ProductSpec(
            name=str(payload.get("name") or "").strip()[:120],
            length_in=_pos_float(payload.get("length_in"), MAX_DIM_IN),
            width_in=_pos_float(payload.get("width_in"), MAX_DIM_IN),
            height_in=_pos_float(payload.get("height_in"), MAX_DIM_IN),
            weight_lb=_pos_float(payload.get("weight_lb"), MAX_WEIGHT_LB),
            monthly_units=units,
            notes=str(payload.get("notes") or "").strip()[:300],
            dims_estimated=bool(payload.get("dims_estimated", False)),
            product_category=category,
            fragile=bool(payload.get("fragile", False)),
        )


@dataclass(frozen=True)
class ProspectProfile:
    """Everything the LLM (or fallback parser) could extract about a prospect."""

    company: str = ""
    brand: str = ""
    website: str = ""
    contact_name: str = ""
    contact_email: str = ""
    products: tuple = ()  # tuple[ProductSpec, ...]
    monthly_order_volume: Optional[int] = None
    destinations_note: str = ""        # e.g. "mostly West Coast, some Canada"
    current_carrier: str = ""
    current_costs_note: str = ""       # e.g. "paying ~$9.80 avg per parcel"
    # Parsed numeric form of the prospect's average cost per parcel, when
    # known — drives the deterministic savings math.
    current_cost_per_parcel_usd: Optional[float] = None
    source_confidence: str = "low"     # low | medium | high
    raw_notes_excerpt: str = ""
    # How monthly_order_volume was derived — ONLY the arithmetic, e.g.
    # "74 DTC Shopify + 64 B2B wholesale". Empty when no stated volume.
    volume_basis: str = ""
    # WHERE the volume number came from (vetting hint for the review page),
    # e.g. "RFP deck p.2 orders table". Never rendered on the public sheet.
    volume_provenance: str = ""
    # v7: brand identity scraped from the prospect's website (intake), used to
    # personalize the hero. logo is an inlined data-URI (size-bounded); tagline
    # is the brand's positioning line. Both empty when unavailable.
    brand_logo_data_uri: str = ""
    brand_tagline: str = ""
    # Estimated total SKUs the brand sells (warehouse-approval signal), plus
    # the basis for the estimate. None when there's no basis to estimate from.
    estimated_sku_count: Optional[int] = None
    sku_count_basis: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["products"] = [p.to_dict() for p in self.products]
        return d

    @staticmethod
    def from_dict(payload: dict) -> "ProspectProfile":
        products = tuple(
            ProductSpec.from_dict(p) for p in (payload.get("products") or []) if isinstance(p, dict)
        )
        volume = payload.get("monthly_order_volume")
        try:
            volume = int(volume) if volume is not None else None
        except (TypeError, ValueError):
            volume = None
        if volume is not None and volume <= 0:
            volume = None
        confidence = str(payload.get("source_confidence") or "low").lower()
        if confidence not in ("low", "medium", "high"):
            confidence = "low"
        sku_count = payload.get("estimated_sku_count")
        try:
            sku_count = int(sku_count) if sku_count is not None else None
        except (TypeError, ValueError):
            sku_count = None
        if sku_count is not None and sku_count <= 0:
            sku_count = None
        # The logo data-URI is stored as-is but length-guarded (~700KB); only
        # keep it when it actually looks like an inlined data-URI.
        logo = str(payload.get("brand_logo_data_uri") or "")
        if not logo.startswith("data:image/") or len(logo) > 700 * 1024:
            logo = ""
        return ProspectProfile(
            company=str(payload.get("company") or "").strip()[:160],
            brand=str(payload.get("brand") or "").strip()[:120],
            current_cost_per_parcel_usd=_pos_float(payload.get("current_cost_per_parcel_usd"), 1000.0),
            website=str(payload.get("website") or "").strip()[:300],
            contact_name=str(payload.get("contact_name") or "").strip()[:120],
            contact_email=str(payload.get("contact_email") or "").strip()[:200],
            products=products,
            monthly_order_volume=volume,
            destinations_note=str(payload.get("destinations_note") or "").strip()[:400],
            current_carrier=str(payload.get("current_carrier") or "").strip()[:120],
            current_costs_note=str(payload.get("current_costs_note") or "").strip()[:400],
            source_confidence=confidence,
            raw_notes_excerpt=str(payload.get("raw_notes_excerpt") or "").strip()[:600],
            volume_basis=str(payload.get("volume_basis") or "").strip()[:200],
            volume_provenance=str(payload.get("volume_provenance") or "").strip()[:200],
            brand_logo_data_uri=logo,
            brand_tagline=str(payload.get("brand_tagline") or "").strip()[:200],
            estimated_sku_count=sku_count,
            sku_count_basis=str(payload.get("sku_count_basis") or "").strip()[:200],
        )

    @property
    def display_name(self) -> str:
        return self.brand or self.company or "Prospect"


@dataclass(frozen=True)
class RateQuote:
    carrier: str = ""
    service: str = ""
    rate_usd: float = 0.0
    transit_days: Optional[int] = None
    zone: Optional[int] = None
    source: str = RATE_SOURCE_MOCK  # "wms" | "mock"

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(payload: dict) -> "RateQuote":
        try:
            rate = round(float(payload.get("rate_usd") or 0.0), 2)
        except (TypeError, ValueError):
            rate = 0.0
        transit = payload.get("transit_days")
        try:
            transit = int(transit) if transit is not None else None
        except (TypeError, ValueError):
            transit = None
        zone = payload.get("zone")
        try:
            zone = int(zone) if zone is not None else None
        except (TypeError, ValueError):
            zone = None
        source = str(payload.get("source") or RATE_SOURCE_MOCK)
        if source not in (RATE_SOURCE_WMS, RATE_SOURCE_MOCK):
            source = RATE_SOURCE_MOCK
        return RateQuote(
            carrier=str(payload.get("carrier") or "").strip()[:60],
            service=str(payload.get("service") or "").strip()[:80],
            rate_usd=rate,
            transit_days=transit,
            zone=zone,
            source=source,
        )


@dataclass(frozen=True)
class ZoneRates:
    """All quotes for one product spec to one representative destination."""

    zone: int = 0
    dest_zip: str = ""
    dest_label: str = ""  # e.g. "Atlanta, GA"
    quotes: tuple = ()    # tuple[RateQuote, ...]

    def to_dict(self) -> dict:
        return {
            "zone": self.zone,
            "dest_zip": self.dest_zip,
            "dest_label": self.dest_label,
            "quotes": [q.to_dict() for q in self.quotes],
        }

    @staticmethod
    def from_dict(payload: dict) -> "ZoneRates":
        try:
            zone = int(payload.get("zone") or 0)
        except (TypeError, ValueError):
            zone = 0
        return ZoneRates(
            zone=zone,
            dest_zip=str(payload.get("dest_zip") or ""),
            dest_label=str(payload.get("dest_label") or ""),
            quotes=tuple(RateQuote.from_dict(q) for q in (payload.get("quotes") or []) if isinstance(q, dict)),
        )


@dataclass(frozen=True)
class ProductRates:
    """The full zone matrix for one product spec."""

    product: ProductSpec = field(default_factory=ProductSpec)
    zones: tuple = ()  # tuple[ZoneRates, ...] ordered by zone

    def to_dict(self) -> dict:
        return {"product": self.product.to_dict(), "zones": [z.to_dict() for z in self.zones]}

    @staticmethod
    def from_dict(payload: dict) -> "ProductRates":
        return ProductRates(
            product=ProductSpec.from_dict(payload.get("product") or {}),
            zones=tuple(ZoneRates.from_dict(z) for z in (payload.get("zones") or []) if isinstance(z, dict)),
        )


@dataclass(frozen=True)
class RateMatrix:
    origin_zip: str = ANATA_HQ_ZIP
    products: tuple = ()  # tuple[ProductRates, ...]

    @property
    def source(self) -> str:
        """"wms" only when at least one quote exists and every quote came from
        the real WMS; else "mock"."""
        saw_quote = False
        for product in self.products:
            for zone in product.zones:
                for quote in zone.quotes:
                    saw_quote = True
                    if quote.source != RATE_SOURCE_WMS:
                        return RATE_SOURCE_MOCK
        return RATE_SOURCE_WMS if saw_quote else RATE_SOURCE_MOCK

    def to_dict(self) -> dict:
        return {"origin_zip": self.origin_zip, "products": [p.to_dict() for p in self.products]}

    @staticmethod
    def from_dict(payload: dict) -> "RateMatrix":
        return RateMatrix(
            origin_zip=str(payload.get("origin_zip") or ANATA_HQ_ZIP),
            products=tuple(ProductRates.from_dict(p) for p in (payload.get("products") or []) if isinstance(p, dict)),
        )


@dataclass(frozen=True)
class NarrativeBlock:
    """LLM-written prospect-specific prose for the rate sheet. Deterministic
    fallback text is used when no API key is available — never blank."""

    executive_summary: str = ""
    savings_text: str = ""
    bullets: tuple = ()  # tuple[str, ...] — 2-4 short "why this works for you" bullets
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["bullets"] = list(self.bullets)
        return d

    @staticmethod
    def from_dict(payload: dict) -> "NarrativeBlock":
        def _i(key: str) -> int:
            try:
                return int(payload.get(key) or 0)
            except (TypeError, ValueError):
                return 0
        return NarrativeBlock(
            executive_summary=str(payload.get("executive_summary") or "").strip()[:2000],
            savings_text=str(payload.get("savings_text") or "").strip()[:2000],
            bullets=tuple(str(b).strip()[:300] for b in (payload.get("bullets") or []) if str(b).strip()),
            model=str(payload.get("model") or "none"),
            input_tokens=_i("input_tokens"),
            output_tokens=_i("output_tokens"),
        )


@dataclass(frozen=True)
class SectionFlags:
    """Which rate-sheet sections render, decided from available data."""

    cover: bool = True
    rate_matrix: bool = False
    zone_map: bool = False
    volume_economics: bool = False
    cost_comparison: bool = False
    destinations: bool = False
    about_anata: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(payload: dict) -> "SectionFlags":
        defaults = SectionFlags()
        return SectionFlags(**{
            key: bool(payload.get(key, getattr(defaults, key)))
            for key in defaults.to_dict()
        })
