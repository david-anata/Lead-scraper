"""LLM-assisted financial line-item classification.

The deterministic intake (intake.py) matches *labelled P&L rows* by substring —
great for a clean income statement, but it falls apart on a real **trial
balance** or **general ledger**, where revenue is spread across dozens of GL
accounts ("Sales – Shopify", "Sales – Amazon", "Refunds", …), expenses are
itemised, and there is no single row literally called "Net Revenue".

This layer fixes that. It takes the *raw* parsed rows, asks Claude to classify
and SUM each account into the canonical buckets the scoring layer understands,
and returns the mapped values plus provenance (which source accounts fed each
bucket), per-bucket confidence, and the accounts it could not place.

It is gap-filling by design: the deterministic pass runs first and the LLM is
asked to confirm/complete the mapping, so a clean P&L stays free + instant and
only messier dumps pay for a classification call. Numbers come straight from
the supplied line items — the model categorises and adds, it never invents.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from sales_support_agent.services.brand_analysis.schema import (
    BALANCE_FIELDS,
    PNL_FIELDS,
    PeriodFinancials,
    parse_cents,
)

logger = logging.getLogger(__name__)

# Canonical buckets the model maps onto. Kept in sync with PeriodFinancials.
_NUMERIC_FIELDS: tuple[str, ...] = tuple(PNL_FIELDS) + tuple(BALANCE_FIELDS) + (
    "new_customer_revenue_cents",
    "returning_customer_revenue_cents",
    "owned_channel_revenue_cents",
    "aov_cents",
    "cac_cents",
    "ltv_cents",
)
_MARKETING_CHANNELS = ("meta", "google", "tiktok", "amazon", "email_sms", "influencer", "other_marketing")

# A material P&L field being absent after the deterministic pass is the signal
# that the dump is GL/trial-balance shaped and worth an LLM classification.
_MATERIAL_FOR_TRIGGER = ("net_revenue_cents", "cogs_cents", "marketing_total_cents", "opex_cents")

_MAX_ROWS = 400  # cap the prompt; trial balances rarely exceed this


@dataclass
class ClassificationResult:
    """Per-period mapped values + provenance, as returned by the model."""

    current: dict = field(default_factory=dict)          # field -> cents
    prior: dict = field(default_factory=dict)            # field -> cents
    marketing_by_channel: dict = field(default_factory=dict)        # channel -> cents (current)
    marketing_by_channel_prior: dict = field(default_factory=dict)  # channel -> cents (prior)
    provenance: dict = field(default_factory=dict)       # field -> [source account labels]
    confidence: dict = field(default_factory=dict)       # field -> "high"|"medium"|"low"
    unmapped: list = field(default_factory=list)         # [label] accounts left unplaced
    related_party_flag: bool = False
    period_current_label: str = ""
    period_prior_label: str = ""
    model: str = "none"
    input_tokens: int = 0
    output_tokens: int = 0

    def to_dict(self) -> dict:
        return dict(self.__dict__)


def should_classify(period: PeriodFinancials) -> bool:
    """True when the deterministic pass left a material P&L bucket empty — the
    tell-tale of a GL/trial-balance dump the substring matcher couldn't fold."""
    return any(getattr(period, f, None) is None for f in _MATERIAL_FOR_TRIGGER)


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM = (
    "You are a financial-data normaliser for an M&A analysis tool. You receive "
    "the raw line items of a brand's financial export — which may be a clean "
    "P&L, or a TRIAL BALANCE / GENERAL LEDGER with many granular GL accounts. "
    "Your job is to classify and SUM those accounts into canonical buckets. "
    "Critical rules:\n"
    "- Revenue is spread across many accounts (e.g. 'Sales - Shopify', 'Sales "
    "- Amazon', 'Wholesale revenue'). SUM all true sales into net_revenue, and "
    "subtract contra-revenue (discounts, refunds/returns) so net_revenue is "
    "net of them. Report discounts and returns separately too.\n"
    "- In a trial balance, revenue/credits may appear as negative or in a "
    "credit column. NORMALISE every bucket to a POSITIVE magnitude, except "
    "net_earnings which keeps its real sign (negative = loss).\n"
    "- COGS, freight/3PL, marketing, customer support, and other operating "
    "costs each have their own buckets — sum the matching accounts into each.\n"
    "- Marketing: also break out by channel where the account names it "
    "(meta/facebook, google, tiktok, amazon ads, email_sms/klaviyo, "
    "influencer/affiliate, else other_marketing).\n"
    "- Only map an account you are confident about. Put anything ambiguous in "
    "'unmapped' rather than forcing it. Never fabricate a value not present in "
    "the line items.\n"
    "- If two periods (columns) are present, map both as current (latest) and "
    "prior. If one period, leave prior empty.\n"
    "Return ONLY a JSON object, no prose."
)

_SCHEMA_HINT = (
    "{\n"
    '  "period_current_label": "FY2024 or similar",\n'
    '  "period_prior_label": "FY2023 or empty",\n'
    '  "current": { <bucket>: <number in dollars>, ... },\n'
    '  "prior":   { <bucket>: <number in dollars>, ... },\n'
    '  "marketing_by_channel": { "meta": <dollars>, "google": <dollars>, ... },\n'
    '  "marketing_by_channel_prior": { ... },\n'
    '  "provenance": { <bucket>: ["source account label", ...], ... },\n'
    '  "confidence": { <bucket>: "high|medium|low", ... },\n'
    '  "related_party_flag": true|false,\n'
    '  "unmapped": ["account label not placed", ...]\n'
    "}\n"
    "Valid buckets: " + ", ".join(_NUMERIC_FIELDS) + ".\n"
    "marketing channels: " + ", ".join(_MARKETING_CHANNELS) + "."
)


def _serialise_tables(tables, *, lines: list, max_rows: int) -> None:
    for t in tables:
        header = [str(c).strip() for c in (t.header or [])]
        if header and any(header):
            lines.append(f"# sheet: {t.source} | columns: {' | '.join(h for h in header if h)}")
        for row in t.rows:
            cells = [str(c).strip() for c in row]
            if not any(cells):
                continue
            label = " ".join(c for c in cells if c and not _looks_numeric(c))
            nums = [c for c in cells if _looks_numeric(c)]
            if not label or not nums:
                continue
            lines.append(f"{label} :: {' | '.join(nums)}")
            if len(lines) >= max_rows:
                return


def _serialise_rows(tables, *, max_rows: int = _MAX_ROWS) -> str:
    """Compact label→values text of every parsed row across all tables."""
    lines: list[str] = []
    _serialise_tables(tables, lines=lines, max_rows=max_rows)
    return "\n".join(lines)


def _serialise_groups(file_groups, *, max_rows: int = _MAX_ROWS) -> str:
    """Like _serialise_rows but bucketed by file, with each file's fiscal year
    marked — so the model can map current-vs-prior across SEPARATE files
    (e.g. a 2024 workbook + 2025 workbooks), not just year-labelled columns."""
    lines: list[str] = []
    for filename, year, tables in file_groups:
        yr = f" — fiscal year {year}" if year else ""
        lines.append(f"\n===== FILE: {filename}{yr} =====")
        _serialise_tables(tables, lines=lines, max_rows=max_rows)
        if len(lines) >= max_rows:
            break
    return "\n".join(lines)


def _looks_numeric(cell: str) -> bool:
    return parse_cents(cell) is not None and any(ch.isdigit() for ch in str(cell))


def _to_cents(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(round(value * 100))
    return parse_cents(value)


def _clean_period(raw: dict) -> dict:
    out: dict = {}
    for k, v in (raw or {}).items():
        if k not in _NUMERIC_FIELDS:
            continue
        cents = _to_cents(v)
        if cents is None:
            continue
        # Normalise magnitude for everything except net_earnings (keeps sign).
        out[k] = cents if k == "net_earnings_cents" else abs(cents)
    return out


def _clean_channels(raw: dict) -> dict:
    out: dict = {}
    for k, v in (raw or {}).items():
        key = str(k).strip().lower()
        if key not in _MARKETING_CHANNELS:
            continue
        cents = _to_cents(v)
        if cents is not None:
            out[key] = abs(cents)
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def classify(
    tables,
    *,
    file_groups=None,
    current_year: Optional[int] = None,
    prior_year: Optional[int] = None,
    context_notes: str = "",
    api_key: Optional[str] = None,
    model: str = "claude-haiku-4-5-20251001",
) -> Optional[ClassificationResult]:
    """Classify raw parsed rows into canonical buckets via the LLM.

    ``file_groups`` is an optional list of ``(filename, year, tables)`` so the
    model can map current-vs-prior across SEPARATE files (e.g. a 2024 workbook
    alongside 2025 ones). ``context_notes`` is analyst guidance to disambiguate
    accounts. Returns None without an API key or on failure — callers keep the
    deterministic mapping. Never raises.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    body = _serialise_groups(file_groups) if file_groups else _serialise_rows(tables)
    if not body.strip():
        return None

    period_block = ""
    if current_year:
        period_block = (
            f"\nPERIODS: current = fiscal year {current_year}"
            + (f", prior = fiscal year {prior_year}" if prior_year else " (single period)")
            + ". Map each file's lines to the period matching its fiscal year; "
            "SUM revenue/expense accounts within each period separately.\n"
        )
    context_block = (
        f"\nANALYST CONTEXT (use to disambiguate, never to fabricate numbers):\n{context_notes.strip()}\n"
        if context_notes and context_notes.strip() else ""
    )
    prompt = (
        "Classify these financial line items into the JSON schema below.\n\n"
        f"SCHEMA:\n{_SCHEMA_HINT}\n{period_block}{context_block}\nLINE ITEMS:\n{body}\n\nReturn the JSON now."
    )
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=key)
        message = client.messages.create(
            model=model,
            max_tokens=1500,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = (message.content[0].text if message.content else "").strip()
        data = _parse_json(text)
        if not data:
            return None
        return ClassificationResult(
            current=_clean_period(data.get("current")),
            prior=_clean_period(data.get("prior")),
            marketing_by_channel=_clean_channels(data.get("marketing_by_channel")),
            marketing_by_channel_prior=_clean_channels(data.get("marketing_by_channel_prior")),
            provenance={k: [str(x) for x in (v or [])] for k, v in (data.get("provenance") or {}).items()
                        if k in _NUMERIC_FIELDS},
            confidence={k: str(v).lower() for k, v in (data.get("confidence") or {}).items()
                        if k in _NUMERIC_FIELDS},
            unmapped=[str(x) for x in (data.get("unmapped") or [])][:50],
            related_party_flag=bool(data.get("related_party_flag")),
            period_current_label=str(data.get("period_current_label") or "").strip(),
            period_prior_label=str(data.get("period_prior_label") or "").strip(),
            model=message.model,
            input_tokens=getattr(message.usage, "input_tokens", 0),
            output_tokens=getattr(message.usage, "output_tokens", 0),
        )
    except Exception:  # noqa: BLE001 — never block intake on the LLM
        logger.warning("[brand_analysis] LLM classification failed; keeping deterministic map", exc_info=True)
        return None


def merge_into(
    period: PeriodFinancials,
    result: ClassificationResult,
    mapped: dict,
    *,
    prior: bool = False,
) -> dict:
    """Fill any field the deterministic pass left empty with the LLM value.

    The deterministic mapping is trusted where present (it's exact); the LLM
    only *completes* gaps. Returns the accumulated provenance dict for storage.
    """
    values = result.prior if prior else result.current
    channels = result.marketing_by_channel_prior if prior else result.marketing_by_channel
    for field_name, cents in values.items():
        if getattr(period, field_name, None) is None:
            setattr(period, field_name, cents)
            if field_name in result.provenance:
                mapped.setdefault(field_name, {
                    "sources": result.provenance.get(field_name, []),
                    "confidence": result.confidence.get(field_name, "medium"),
                })
    # Marketing channels fill in if the deterministic pass found none.
    if channels and not period.marketing_by_channel:
        period.marketing_by_channel = dict(channels)
    if period.marketing_total_cents is None and period.marketing_by_channel:
        period.marketing_total_cents = sum(period.marketing_by_channel.values())
    if result.related_party_flag:
        period.related_party_flag = True
    return mapped


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except json.JSONDecodeError:
                return None
    return None
