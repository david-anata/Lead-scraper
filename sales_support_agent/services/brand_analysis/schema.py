"""Canonical data contract for Brand Analysis.

Every module in this package speaks in these dataclasses and constants.
Conventions (mirroring services/advertising):
  * money is integer cents
  * percentages / rates / margins are integer basis points (25.0% -> 2500 bps)
  * a *missing* input is ``None`` (distinct from a real 0) so the
    missing-data / confidence logic can tell "absent" from "zero".
  * parsers and metric math never raise — bad/blank values degrade to None.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Optional

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

# Business-model tags — pick benchmark ranges per category. Default DTC.
CATEGORY_DTC = "dtc"
CATEGORY_RETAIL = "retail"
CATEGORY_SAAS = "saas"
CATEGORY_OTHER = "other"

CATEGORY_LABELS = {
    CATEGORY_DTC: "DTC e-commerce",
    CATEGORY_RETAIL: "Retail / CPG",
    CATEGORY_SAAS: "SaaS / subscription",
    CATEGORY_OTHER: "Other",
}

SEV_CRITICAL = "Critical"
SEV_HIGH = "High"
SEV_MEDIUM = "Medium"
_SEV_ORDER = {SEV_CRITICAL: 0, SEV_HIGH: 1, SEV_MEDIUM: 2}

# Grade points: A=4 … F=0, rebased to /100 by scoring.py.
GRADE_POINTS = {"A": 4.0, "B": 3.0, "C": 2.0, "D": 1.0, "F": 0.0}


def letter_from_score(score_100: float) -> str:
    """Letter grade from the rebased /100 score — standard academic scale."""
    if score_100 >= 90:
        return "A"
    if score_100 >= 80:
        return "B"
    if score_100 >= 70:
        return "C"
    if score_100 >= 60:
        return "D"
    return "F"


# Sentinel for a dimension we could not assess from the supplied data. It scores
# ZERO points (a deliberate penalty — incomplete submissions trend toward F
# until more is supplied) but renders distinctly from a real "F" so a reader
# sees "not assessed", not "assessed and failed".
NOT_ASSESSED = "NA"


# ---------------------------------------------------------------------------
# Weighted dimensions (spec table) — weights sum to 1.00
# ---------------------------------------------------------------------------

# FINANCIAL track — the deterministic acquisition grade. Two-track model:
# brand/social is scored SEPARATELY (see BRAND_SOCIAL_DIMENSIONS) and never
# folded into this grade, so the headline number stays hard-numbers-only.
# Reweighted to sum 1.00 after moving "brand" to its own track.
DIMENSIONS = (
    ("revenue", "Revenue trajectory & growth", 0.26),
    ("profitability", "Profitability & net margin", 0.16),
    ("marketing", "Marketing efficiency (MER)", 0.16),
    ("acquisition", "Acquisition mix & dependency", 0.13),
    ("media", "Media mix & concentration", 0.11),
    ("contribution", "Contribution / unit economics", 0.10),
    ("balance", "Balance sheet & earnings quality", 0.08),
)
DIMENSION_LABELS = {k: label for k, label, _ in DIMENSIONS}
DIMENSION_WEIGHTS = {k: w for k, _, w in DIMENSIONS}

# BRAND & SOCIAL track — a separate A–F shown alongside the financial grade.
# For Ascend this tracks ACQUISITION OPPORTUNITY: absence of social/DTC is a
# positive signal (maximum build runway), not a penalty. Weights sum to 1.00.
BRAND_SOCIAL_DIMENSIONS = (
    ("brand_equity",    "Brand defensibility & moat",           0.25),
    ("dtc_opportunity", "DTC & email build opportunity",         0.25),
    ("social_oppty",    "Social channel opportunity",            0.25),
    ("product_signal",  "Product-market fit (reviews & demand)", 0.25),
)


# ---------------------------------------------------------------------------
# Category benchmarks (healthy ranges). bps unless noted; MER is a float x.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Benchmarks:
    product_gm_bps: tuple[int, int] = (5500, 7000)        # 55–70%
    marketing_pct_bps: tuple[int, int] = (2000, 3500)     # 20–35% of revenue
    blended_mer_min: float = 3.0                          # >= 3.0x
    contribution_margin_bps: tuple[int, int] = (1500, 3000)  # 15–30%
    net_margin_bps: tuple[int, int] = (800, 1500)         # 8–15%
    owned_pct_bps: tuple[int, int] = (2000, 3500)         # email/owned 20–35%
    discount_rate_bps: tuple[int, int] = (1000, 2000)     # 10–20%
    return_rate_max_bps: int = 800                        # < 8%
    yoy_growth_min_bps: int = 0                           # >= 0%


_BENCHMARKS_BY_CATEGORY = {
    CATEGORY_DTC: Benchmarks(),
    # Retail/CPG: thinner product margins, lower marketing %, returns matter less.
    CATEGORY_RETAIL: Benchmarks(
        product_gm_bps=(3000, 5000),
        marketing_pct_bps=(800, 1800),
        blended_mer_min=5.0,
        contribution_margin_bps=(1000, 2500),
        net_margin_bps=(500, 1200),
        owned_pct_bps=(1000, 2500),
        discount_rate_bps=(500, 1500),
        return_rate_max_bps=500,
    ),
    # SaaS: very high gross margin, heavier S&M, churn not returns.
    CATEGORY_SAAS: Benchmarks(
        product_gm_bps=(7000, 9000),
        marketing_pct_bps=(3000, 5000),
        blended_mer_min=2.0,
        contribution_margin_bps=(6000, 8000),
        net_margin_bps=(1000, 2500),
        owned_pct_bps=(2000, 4000),
        discount_rate_bps=(500, 1500),
        return_rate_max_bps=300,
    ),
    CATEGORY_OTHER: Benchmarks(),
}


def benchmarks_for(category: str) -> Benchmarks:
    return _BENCHMARKS_BY_CATEGORY.get((category or "").lower(), _BENCHMARKS_BY_CATEGORY[CATEGORY_DTC])


# ---------------------------------------------------------------------------
# Parsed financials — one period (cents; None = not found in the dump)
# ---------------------------------------------------------------------------

# Canonical P&L / balance-sheet line items the intake layer maps onto. The
# scoring layer reads only these names, so adding a synonym in intake never
# touches scoring. Every field is Optional — absence is meaningful.
PNL_FIELDS = (
    "gross_sales_cents", "discounts_cents", "returns_cents", "net_revenue_cents",
    "cogs_cents", "freight_3pl_cents", "marketing_total_cents",
    "customer_support_cents", "opex_cents", "other_income_cents",
    "reported_gross_profit_cents", "net_earnings_cents",
)
BALANCE_FIELDS = (
    "total_assets_cents", "cash_cents", "inventory_cents",
    "intercompany_cents", "total_equity_cents", "dividends_cents",
)


@dataclass
class PeriodFinancials:
    """Financial line items for a single period. cents; None = absent."""

    period_label: str = ""
    year: Optional[int] = None

    # P&L
    gross_sales_cents: Optional[int] = None
    discounts_cents: Optional[int] = None
    returns_cents: Optional[int] = None
    net_revenue_cents: Optional[int] = None
    cogs_cents: Optional[int] = None
    freight_3pl_cents: Optional[int] = None
    marketing_total_cents: Optional[int] = None
    marketing_by_channel: dict = field(default_factory=dict)  # channel -> cents
    customer_support_cents: Optional[int] = None
    opex_cents: Optional[int] = None
    other_income_cents: Optional[int] = None
    reported_gross_profit_cents: Optional[int] = None
    net_earnings_cents: Optional[int] = None

    # Balance sheet / earnings quality
    total_assets_cents: Optional[int] = None
    cash_cents: Optional[int] = None
    inventory_cents: Optional[int] = None
    intercompany_cents: Optional[int] = None
    total_equity_cents: Optional[int] = None
    dividends_cents: Optional[int] = None
    related_party_flag: bool = False

    # Acquisition / unit economics (often absent in a basic dump)
    new_customer_revenue_cents: Optional[int] = None
    returning_customer_revenue_cents: Optional[int] = None
    owned_channel_revenue_cents: Optional[int] = None  # email + SMS
    aov_cents: Optional[int] = None
    cac_cents: Optional[int] = None
    ltv_cents: Optional[int] = None

    # Monthly revenue trajectory (label, cents) in calendar order, if derivable.
    monthly_revenue: list = field(default_factory=list)

    def net_revenue_or_derived(self) -> Optional[int]:
        """Net revenue, deriving from gross − discounts − returns if absent."""
        if self.net_revenue_cents is not None:
            return self.net_revenue_cents
        if self.gross_sales_cents is not None:
            return self.gross_sales_cents - (self.discounts_cents or 0) - (self.returns_cents or 0)
        return None

    def to_dict(self) -> dict:
        return {
            "period_label": self.period_label,
            "year": self.year,
            "marketing_by_channel": self.marketing_by_channel,
            "monthly_revenue": self.monthly_revenue,
            "related_party_flag": self.related_party_flag,
            "new_customer_revenue_cents": self.new_customer_revenue_cents,
            "returning_customer_revenue_cents": self.returning_customer_revenue_cents,
            "owned_channel_revenue_cents": self.owned_channel_revenue_cents,
            "aov_cents": self.aov_cents,
            "cac_cents": self.cac_cents,
            "ltv_cents": self.ltv_cents,
            **{f: getattr(self, f) for f in PNL_FIELDS},
            **{f: getattr(self, f) for f in BALANCE_FIELDS},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PeriodFinancials":
        data = data or {}
        obj = cls()
        for key, value in data.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
        return obj


# ---------------------------------------------------------------------------
# Derived metrics
# ---------------------------------------------------------------------------


@dataclass
class Metrics:
    """Derived KPIs for one period. bps unless suffixed; MER is a float x."""

    net_revenue_cents: Optional[int] = None
    cogs_cents: Optional[int] = None
    product_gross_profit_cents: Optional[int] = None
    product_gm_bps: Optional[int] = None
    marketing_total_cents: Optional[int] = None
    marketing_pct_bps: Optional[int] = None
    blended_mer: Optional[float] = None
    reported_gross_profit_cents: Optional[int] = None
    contribution_margin_bps: Optional[int] = None
    opex_cents: Optional[int] = None
    net_earnings_cents: Optional[int] = None
    net_margin_bps: Optional[int] = None
    operating_result_ex_other_cents: Optional[int] = None
    discount_rate_bps: Optional[int] = None
    return_rate_bps: Optional[int] = None
    owned_pct_bps: Optional[int] = None

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "Metrics":
        obj = cls()
        for key, value in (data or {}).items():
            if hasattr(obj, key):
                setattr(obj, key, value)
        return obj


# ---------------------------------------------------------------------------
# Scorecard / red flags / benchmarks / gaps
# ---------------------------------------------------------------------------


@dataclass
class DimensionGrade:
    key: str
    label: str
    weight: float
    letter: str
    points: float
    reason: str
    assessed: bool = True  # False = data not supplied; scores 0 (penalised)

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @classmethod
    def from_dict(cls, data: dict) -> "DimensionGrade":
        data = dict(data or {})
        # Tolerate older rows persisted before `assessed` existed.
        data.setdefault("assessed", True)
        return cls(**data)


@dataclass
class Scorecard:
    dimensions: list = field(default_factory=list)  # list[DimensionGrade]
    score_100: int = 0
    letter: str = "F"
    verdict: str = ""

    def to_dict(self) -> dict:
        return {
            "dimensions": [d.to_dict() for d in self.dimensions],
            "score_100": self.score_100,
            "letter": self.letter,
            "verdict": self.verdict,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Scorecard":
        data = data or {}
        return cls(
            dimensions=[DimensionGrade.from_dict(d) for d in data.get("dimensions", [])],
            score_100=data.get("score_100", 0),
            letter=data.get("letter", "F"),
            verdict=data.get("verdict", ""),
        )


@dataclass
class RedFlag:
    severity: str  # Critical | High | Medium
    title: str
    detail: str = ""

    def to_dict(self) -> dict:
        return dict(self.__dict__)


@dataclass
class BenchmarkRow:
    kpi: str
    healthy: str        # human-readable healthy range
    actual: str         # human-readable brand actual
    passed: Optional[bool]  # True=PASS, False=FAIL, None=data gap

    def to_dict(self) -> dict:
        return dict(self.__dict__)


# ---------------------------------------------------------------------------
# Full report
# ---------------------------------------------------------------------------


@dataclass
class BrandReport:
    brand: str = ""
    detected_brands: list = field(default_factory=list)
    category: str = CATEGORY_DTC
    prepared_date: str = ""
    period_current_label: str = ""
    period_prior_label: str = ""
    has_yoy: bool = False

    current: Metrics = field(default_factory=Metrics)
    prior: Metrics = field(default_factory=Metrics)
    yoy_revenue_growth_bps: Optional[int] = None
    monthly_revenue: list = field(default_factory=list)  # [(label, cents)]

    # Raw section inputs surfaced for the Media Mix / Balance Sheet sections
    media_mix: dict = field(default_factory=dict)        # channel -> cents (current)
    media_mix_prior: dict = field(default_factory=dict)  # channel -> cents (prior)
    balance_sheet: list = field(default_factory=list)    # [(label, cents)] current
    related_party_flag: bool = False

    scorecard: Scorecard = field(default_factory=Scorecard)
    red_flags: list = field(default_factory=list)        # list[RedFlag]
    benchmarks: list = field(default_factory=list)        # list[BenchmarkRow]

    # Missing-data block (short, under the grade) + full gaps checklist.
    missing_data: list = field(default_factory=list)      # list[str]
    confidence: str = "Low"                               # High | Medium | Low
    data_sufficient: bool = False
    data_gaps: list = field(default_factory=list)         # list[str]

    # Raw acquisition fields (PeriodFinancials passthrough — not in Metrics)
    acquisition_current: dict = field(default_factory=dict)
    acquisition_prior: dict = field(default_factory=dict)

    # Narrative (LLM-augmented, deterministic fallback)
    executive_summary: str = ""
    stands_out: list = field(default_factory=list)        # list[str]
    verdict_text: str = ""
    recommendation: str = ""                              # e.g. "Conditional Buy"
    narrative_model: str = "none"

    intake_summary: str = ""

    # ---- Investor-package additions ----------------------------------------
    # How much of the material input set was actually present (0–100); drives
    # the data-completeness meter and gates the valuation ranges.
    data_completeness_pct: int = 0
    # Provenance from the LLM classifier: field -> {sources:[...], confidence}.
    account_mappings: dict = field(default_factory=dict)
    unmapped_accounts: list = field(default_factory=list)
    classifier_model: str = ""
    # Indicative valuation ranges (ValuationRange.to_dict()) — caveated.
    valuation: dict = field(default_factory=dict)
    # Investor narrative: thesis "for", risks "against".
    investment_thesis: list = field(default_factory=list)  # list[str]
    key_risks: list = field(default_factory=list)          # list[str]
    # Exec-summary callout ribbon: list of {label, value, tone}.
    info_ribbon: list = field(default_factory=list)
    # Branding pulled from the brand's site (logo + product imagery).
    brand_website: str = ""
    logo_data_uri: str = ""
    product_images: list = field(default_factory=list)     # list[url|data-uri]
    brand_tagline: str = ""
    # Free-text context the analyst accumulates across reruns.
    context_notes: str = ""

    # ---- Brand & Social track (separate A–F, NOT in the financial grade) ----
    brand_social: dict = field(default_factory=dict)   # Scorecard.to_dict() + confidence + caveats
    email_list_size: int = 0                            # owned-audience size (analyst-supplied)
    social_handles: dict = field(default_factory=dict)  # platform -> url
    social_signals: dict = field(default_factory=dict)  # measured signals + measured/estimated flags

    # Analyst overrides: canonical field -> exact dollar value, applied over the
    # parsed numbers (the escape hatch when a value is mis-parsed). Persisted.
    overrides: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "brand": self.brand,
            "detected_brands": self.detected_brands,
            "category": self.category,
            "prepared_date": self.prepared_date,
            "period_current_label": self.period_current_label,
            "period_prior_label": self.period_prior_label,
            "has_yoy": self.has_yoy,
            "current": self.current.to_dict(),
            "prior": self.prior.to_dict(),
            "yoy_revenue_growth_bps": self.yoy_revenue_growth_bps,
            "monthly_revenue": self.monthly_revenue,
            "media_mix": self.media_mix,
            "media_mix_prior": self.media_mix_prior,
            "balance_sheet": self.balance_sheet,
            "related_party_flag": self.related_party_flag,
            "scorecard": self.scorecard.to_dict(),
            "red_flags": [r.to_dict() for r in self.red_flags],
            "benchmarks": [b.to_dict() for b in self.benchmarks],
            "missing_data": self.missing_data,
            "confidence": self.confidence,
            "data_sufficient": self.data_sufficient,
            "data_gaps": self.data_gaps,
            "acquisition_current": self.acquisition_current,
            "acquisition_prior": self.acquisition_prior,
            "executive_summary": self.executive_summary,
            "stands_out": self.stands_out,
            "verdict_text": self.verdict_text,
            "recommendation": self.recommendation,
            "narrative_model": self.narrative_model,
            "intake_summary": self.intake_summary,
            "data_completeness_pct": self.data_completeness_pct,
            "account_mappings": self.account_mappings,
            "unmapped_accounts": self.unmapped_accounts,
            "classifier_model": self.classifier_model,
            "valuation": self.valuation,
            "investment_thesis": self.investment_thesis,
            "key_risks": self.key_risks,
            "info_ribbon": self.info_ribbon,
            "brand_website": self.brand_website,
            "logo_data_uri": self.logo_data_uri,
            "product_images": self.product_images,
            "brand_tagline": self.brand_tagline,
            "context_notes": self.context_notes,
            "brand_social": self.brand_social,
            "email_list_size": self.email_list_size,
            "social_handles": self.social_handles,
            "social_signals": self.social_signals,
            "overrides": self.overrides,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BrandReport":
        data = data or {}
        return cls(
            brand=data.get("brand", ""),
            detected_brands=data.get("detected_brands", []),
            category=data.get("category", CATEGORY_DTC),
            prepared_date=data.get("prepared_date", ""),
            period_current_label=data.get("period_current_label", ""),
            period_prior_label=data.get("period_prior_label", ""),
            has_yoy=data.get("has_yoy", False),
            current=Metrics.from_dict(data.get("current")),
            prior=Metrics.from_dict(data.get("prior")),
            yoy_revenue_growth_bps=data.get("yoy_revenue_growth_bps"),
            monthly_revenue=data.get("monthly_revenue", []),
            media_mix=data.get("media_mix", {}),
            media_mix_prior=data.get("media_mix_prior", {}),
            balance_sheet=data.get("balance_sheet", []),
            related_party_flag=data.get("related_party_flag", False),
            scorecard=Scorecard.from_dict(data.get("scorecard")),
            red_flags=[RedFlag(**r) for r in data.get("red_flags", [])],
            benchmarks=[BenchmarkRow(**b) for b in data.get("benchmarks", [])],
            missing_data=data.get("missing_data", []),
            confidence=data.get("confidence", "Low"),
            data_sufficient=data.get("data_sufficient", False),
            data_gaps=data.get("data_gaps", []),
            acquisition_current=data.get("acquisition_current", {}),
            acquisition_prior=data.get("acquisition_prior", {}),
            executive_summary=data.get("executive_summary", ""),
            stands_out=data.get("stands_out", []),
            verdict_text=data.get("verdict_text", ""),
            recommendation=data.get("recommendation", ""),
            narrative_model=data.get("narrative_model", "none"),
            intake_summary=data.get("intake_summary", ""),
            data_completeness_pct=data.get("data_completeness_pct", 0),
            account_mappings=data.get("account_mappings", {}),
            unmapped_accounts=data.get("unmapped_accounts", []),
            classifier_model=data.get("classifier_model", ""),
            valuation=data.get("valuation", {}),
            investment_thesis=data.get("investment_thesis", []),
            key_risks=data.get("key_risks", []),
            info_ribbon=data.get("info_ribbon", []),
            brand_website=data.get("brand_website", ""),
            logo_data_uri=data.get("logo_data_uri", ""),
            product_images=data.get("product_images", []),
            brand_tagline=data.get("brand_tagline", ""),
            context_notes=data.get("context_notes", ""),
            brand_social=data.get("brand_social", {}),
            email_list_size=data.get("email_list_size", 0),
            social_handles=data.get("social_handles", {}),
            social_signals=data.get("social_signals", {}),
            overrides=data.get("overrides", {}),
        )


# ---------------------------------------------------------------------------
# Numeric helpers — all integer-safe, never raise (mirror advertising/schema)
# ---------------------------------------------------------------------------


def parse_cents(value: object) -> Optional[int]:
    """Parse a money string/number into integer cents. Strips $ , % and spaces,
    handles parenthesized negatives. Returns None on blank/junk so 'absent'
    stays distinct from a real 0."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value * 100
    if isinstance(value, float):
        return int(round(value * 100))
    text = str(value).strip()
    if not text:
        return None
    negative = text.startswith("(") and text.endswith(")")
    cleaned = (
        text.replace("$", "").replace(",", "").replace("(", "").replace(")", "")
        .replace("%", "").strip()
    )
    if not cleaned or cleaned in {"-", ".", "—"}:
        return None
    try:
        cents = int((Decimal(cleaned) * 100).to_integral_value())
    except (InvalidOperation, ValueError):
        return None
    return -cents if negative else cents


def safe_div(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    if numer is None or denom is None or denom == 0:
        return None
    return numer / denom


def margin_bps(numer: Optional[int], denom: Optional[int]) -> Optional[int]:
    """A ratio expressed in basis points; None when either side is absent/zero."""
    r = safe_div(numer, denom)
    if r is None:
        return None
    return round(r * 10000)


def fmt_money(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.0f}"


def fmt_money_precise(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    return f"${cents / 100:,.2f}"


def fmt_pct(bps: Optional[int]) -> str:
    if bps is None:
        return "—"
    return f"{bps / 100:.1f}%"


def fmt_mult(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.2f}x"


def fmt_delta_bps(bps: Optional[int]) -> str:
    """Signed delta in points (pp) for YoY columns."""
    if bps is None:
        return "—"
    sign = "+" if bps >= 0 else "−"
    return f"{sign}{abs(bps) / 100:.1f} pp"


def sort_red_flags(flags: list) -> list:
    return sorted(flags, key=lambda f: _SEV_ORDER.get(f.severity, 9))
