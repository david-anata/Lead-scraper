"""Canonical data contract for the advertising audit.

Every other module in this package speaks in these dataclasses and constants.
Conventions (mirroring the cashflow service):
  * money is integer cents
  * percentages/rates are integer basis points (25.0% -> 2500 bps)
  * normalizers never raise; bad values fall back to safe defaults
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

AD_TYPES = ("SP", "SB", "SD", "STV", "DSP")
AD_TYPE_LABELS = {
    "SP": "Sponsored Products",
    "SB": "Sponsored Brands",
    "SD": "Sponsored Display",
    "STV": "Sponsored TV / Streaming TV",
    "DSP": "Amazon DSP",
}

# Ad types where a downloadable bulk sheet can be round-tripped. STV/DSP have
# only partial bulk support, so their recommendations land in the burn list as
# manual tasks. See bulk_sheets.py.
BULK_SUPPORTED = ("SP", "SB", "SD")

ENTITY_CAMPAIGN = "campaign"
ENTITY_AD_GROUP = "ad_group"
ENTITY_KEYWORD = "keyword"
ENTITY_TARGET = "target"
ENTITY_SEARCH_TERM = "search_term"
ENTITY_PLACEMENT = "placement"
ENTITY_PRODUCT_AD = "product_ad"

# Recommendation categories
CAT_BID_DOWN = "bid_down"
CAT_BID_UP = "bid_up"
CAT_NEGATIVE = "negative_keyword"
CAT_NEW_KEYWORD = "new_keyword"
CAT_BUDGET = "budget"
CAT_STRUCTURE = "structure"
CAT_PLACEMENT = "placement"
CAT_DAYPARTING = "dayparting"
CAT_EXTERNAL = "external"
CAT_MANUAL = "manual"

SEV_HIGH = "high"
SEV_MEDIUM = "medium"
SEV_LOW = "low"

EXTERNAL_CHANNELS = ("meta", "tiktok", "influencer", "google", "other")


@dataclass(frozen=True)
class Thresholds:
    """Tunable knobs for the deterministic rules. Defaults are conservative
    industry starting points; surfaced here so they're testable and overridable
    per-run later without touching rule code.
    """

    # A click bucket is "significant" once it clears this many clicks.
    min_clicks_significant: int = 10
    # Spend (cents) with zero orders that flags a wasted-spend negative.
    wasted_spend_cents: int = 1500  # $15 with no sales
    # ACoS this many bps over target -> bid-down. (default +50% of target)
    bid_down_over_target_ratio: float = 1.5
    # ACoS this far under target with volume -> bid-up opportunity.
    bid_up_under_target_ratio: float = 0.6
    # Bid adjustment step, applied to current bid.
    bid_down_factor: float = 0.85
    bid_up_factor: float = 1.15
    # Floor/ceiling for any proposed bid (cents).
    min_bid_cents: int = 20
    max_bid_cents: int = 1000
    # A converting search term promotes to an exact keyword after this many orders.
    promote_keyword_min_orders: int = 2
    # Fallback target ACoS (bps) when the goal sets none, used for bid math.
    default_acos_target_bps: int = 3000  # 30%


# ---------------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------------


@dataclass
class Goals:
    revenue_target_cents: Optional[int] = None
    acos_target_bps: Optional[int] = None
    tacos_target_bps: Optional[int] = None
    units_target: Optional[int] = None
    period: str = "monthly"
    label: str = ""

    def effective_acos_bps(self, thresholds: Thresholds) -> int:
        """ACoS target to drive bid math, falling back to the default."""
        if self.acos_target_bps and self.acos_target_bps > 0:
            return self.acos_target_bps
        return thresholds.default_acos_target_bps

    def to_dict(self) -> dict:
        return {
            "revenue_target_cents": self.revenue_target_cents,
            "acos_target_bps": self.acos_target_bps,
            "tacos_target_bps": self.tacos_target_bps,
            "units_target": self.units_target,
            "period": self.period,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Goals":
        data = data or {}
        return cls(
            revenue_target_cents=data.get("revenue_target_cents"),
            acos_target_bps=data.get("acos_target_bps"),
            tacos_target_bps=data.get("tacos_target_bps"),
            units_target=data.get("units_target"),
            period=data.get("period", "monthly"),
            label=data.get("label", ""),
        )


# ---------------------------------------------------------------------------
# Normalized ingest rows
# ---------------------------------------------------------------------------


@dataclass
class AdRow:
    ad_type: str
    entity_level: str
    campaign_name: str = ""
    ad_group_name: str = ""
    campaign_id: str = ""
    ad_group_id: str = ""
    keyword_id: str = ""
    target_id: str = ""  # Product Targeting ID (auto-target expressions + ASIN targets)
    entity_text: str = ""
    match_type: str = ""
    impressions: int = 0
    clicks: int = 0
    spend_cents: int = 0
    sales_cents: int = 0
    orders: int = 0
    units: int = 0
    bid_cents: Optional[int] = None
    raw: dict = field(default_factory=dict)

    @property
    def acos_bps(self) -> Optional[int]:
        return acos_bps(self.spend_cents, self.sales_cents)

    @property
    def cpc_cents(self) -> Optional[int]:
        if self.clicks <= 0:
            return None
        return round(self.spend_cents / self.clicks)


@dataclass
class SalesRow:
    asin: str = ""
    sku: str = ""
    title: str = ""
    sessions: int = 0
    page_views: int = 0
    units: int = 0
    ordered_product_sales_cents: int = 0
    buy_box_pct_bps: Optional[int] = None
    conversion_bps: Optional[int] = None
    raw: dict = field(default_factory=dict)


@dataclass
class MarketRow:
    search_query: str = ""
    asin: str = ""
    search_query_volume: int = 0
    impressions_total: int = 0
    impression_share_bps: Optional[int] = None
    clicks_total: int = 0
    click_share_bps: Optional[int] = None
    purchases_total: int = 0
    purchase_share_bps: Optional[int] = None
    raw: dict = field(default_factory=dict)


@dataclass
class ExternalCostRow:
    channel: str = "other"
    cost_type: str = "ad_spend"
    label: str = ""
    amount_cents: int = 0
    note: str = ""


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


@dataclass
class Recommendation:
    category: str
    title: str
    ad_type: str = ""
    severity: str = SEV_MEDIUM
    detail: str = ""
    rationale: str = ""
    entity_ref: str = ""
    current_value: str = ""
    proposed_value: str = ""
    projected_impact: dict = field(default_factory=dict)
    bulk_row: dict = field(default_factory=dict)
    is_bulk_actionable: bool = False
    # Internal score used only for ranking; not persisted.
    score: float = 0.0

    def to_dict(self) -> dict:
        return {
            "category": self.category,
            "title": self.title,
            "ad_type": self.ad_type,
            "severity": self.severity,
            "detail": self.detail,
            "rationale": self.rationale,
            "entity_ref": self.entity_ref,
            "current_value": self.current_value,
            "proposed_value": self.proposed_value,
            "projected_impact": self.projected_impact,
            "bulk_row": self.bulk_row,
            "is_bulk_actionable": self.is_bulk_actionable,
        }


# ---------------------------------------------------------------------------
# Numeric helpers — all integer-safe
# ---------------------------------------------------------------------------


def acos_bps(spend_cents: int, sales_cents: int) -> Optional[int]:
    """Advertising Cost of Sale in basis points. None when there are no sales
    (ACoS is undefined / infinite — callers treat that as "all wasted")."""
    if not sales_cents:
        return None
    return round(spend_cents * 10000 / sales_cents)


def tacos_bps(ad_spend_cents: int, total_sales_cents: int) -> Optional[int]:
    """Total ACoS — ad spend over *total* sales, in basis points."""
    if not total_sales_cents:
        return None
    return round(ad_spend_cents * 10000 / total_sales_cents)


def parse_cents(value: object) -> int:
    """Parse a money string/number into integer cents. Strips $ , and spaces,
    handles parenthesized negatives, never raises -> 0 on junk."""
    if value is None:
        return 0
    if isinstance(value, (int,)):
        return int(value) * 100
    if isinstance(value, float):
        return int(round(value * 100))
    text = str(value).strip()
    if not text:
        return 0
    negative = text.startswith("(") and text.endswith(")")
    cleaned = text.replace("$", "").replace(",", "").replace("(", "").replace(")", "").replace("%", "").strip()
    if not cleaned or cleaned in {"-", "."}:
        return 0
    try:
        cents = int((Decimal(cleaned) * 100).to_integral_value())
    except (InvalidOperation, ValueError):
        return 0
    return -cents if negative else cents


def parse_int(value: object) -> int:
    """Parse an integer-ish value (impressions, clicks, units). Never raises."""
    if value is None:
        return 0
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        return int(round(value))
    text = str(value).strip().replace(",", "")
    if not text:
        return 0
    try:
        return int(round(float(text)))
    except (ValueError, TypeError):
        return 0


def parse_bps(value: object) -> Optional[int]:
    """Parse a percentage value (e.g. "27.5%", "27.5", 0.275) into basis points.

    Heuristic: values <= 1.0 are treated as fractions (0.275 -> 2750 bps);
    larger values are treated as percent (27.5 -> 2750 bps). Returns None on
    blank/junk so 'unknown' stays distinct from 0%."""
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        num = float(text)
    except (ValueError, TypeError):
        return None
    if -1.0 <= num <= 1.0 and "%" not in str(value):
        return round(num * 10000)
    return round(num * 100)


def fmt_money(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.2f}"


def fmt_pct(bps: Optional[int]) -> str:
    if bps is None:
        return "—"
    return f"{bps / 100:.1f}%"
