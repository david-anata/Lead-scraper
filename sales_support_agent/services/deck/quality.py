"""Content checks for a prospect strategy deck.

A deck is not fit to send when it shows an empty market as if it were
measured, names a retired client, or uses a placeholder category. The
gate reports those problems. It does not invent replacement numbers.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

_ZERO_DOLLAR_RE = re.compile(r"\$0(?!\.\d*[1-9])\b")
_THIS_CATEGORY_RE = re.compile(r"this category", re.IGNORECASE)
_TOP_ZERO_RE = re.compile(r"\bTop\s+0\b", re.IGNORECASE)
_ZANTREX_RE = re.compile(r"\bZantrex\b", re.IGNORECASE)
_REDO_RE = re.compile(r"\bRedo\b")
_ZERO_METRIC_RE = re.compile(
    r"(?:"
    r"\b0\s+listings\b"
    r"|\btop\s+0\s+brands\b"
    r"|\bUnits sold\b(?:\s|<[^>]*>){0,40}\b0\b"
    r"|\b30-day units sold\b(?:\s|<[^>]*>){0,40}\b0\b"
    r"|\b0\s*/\s*0\b"
    r"|\bmonthly\s*(?:·|&#183;|&middot;)\s*top\s+0\b"
    r")",
    re.IGNORECASE,
)
_STYLE_OR_SCRIPT_RE = re.compile(
    r"(<style\b[^>]*>.*?</style>|<script\b[^>]*>.*?</script>)",
    re.IGNORECASE | re.DOTALL,
)
_PLACEHOLDER_DASH_RE = re.compile(r">\s*[\u2014\u2013]\s*<")
_NUMBER_RANGE_RE = re.compile(r"(\d)\s*\u2013\s*(\$?\d)")
_EM_DASH_RE = re.compile(r"\s*\u2014\s*")
_EN_DASH_RE = re.compile(r"\s*\u2013\s*")


def is_zero_dollar(value: str) -> bool:
    """True for a displayed zero amount, not for a non-zero price such as $0.15."""

    return bool(re.fullmatch(r"\$0(?:\.0+)?", str(value or "").strip()))


def market_metrics_renderable(xray_report: Any) -> bool:
    """True only when the export has listings and a non-zero revenue or unit total."""

    if xray_report is None:
        return False
    if not bool(getattr(xray_report, "market_evidence_sufficient", False)):
        return False
    status = str(getattr(xray_report, "evidence_status", "") or "")
    if status not in {"ready", "directional"}:
        return False
    products = list(getattr(xray_report, "products", []) or [])
    if not products:
        return False
    revenue = float(getattr(xray_report, "total_revenue", 0) or 0)
    units = float(getattr(xray_report, "total_units_sold", 0) or 0)
    return revenue > 0 or units > 0


def normalize_market_evidence(xray_report: Any) -> Any:
    """Mark an empty or all-zero export as missing instead of ready."""

    if xray_report is None:
        return xray_report
    products = list(getattr(xray_report, "products", []) or [])
    revenue = float(getattr(xray_report, "total_revenue", 0) or 0)
    units = float(getattr(xray_report, "total_units_sold", 0) or 0)
    has_totals = bool(products) and (revenue > 0 or units > 0)
    status = str(getattr(xray_report, "evidence_status", "") or "")
    sufficient = bool(getattr(xray_report, "market_evidence_sufficient", False))
    if has_totals and sufficient and status in {"ready", "directional"}:
        return xray_report
    if has_totals and not sufficient:
        return xray_report
    reason = str(getattr(xray_report, "market_evidence_reason", "") or "").strip()
    if not reason:
        reason = (
            "No competitor market export was supplied, so category revenue, units, and brand counts are withheld."
            if not products
            else "The market export has no usable revenue or unit totals, so those figures are withheld."
        )
    next_status = status if status not in {"", "ready"} else "missing"
    if next_status == status and not sufficient and str(getattr(xray_report, "market_evidence_reason", "") or "").strip():
        return xray_report
    return replace(
        xray_report,
        evidence_status=next_status,
        market_evidence_sufficient=False,
        market_evidence_reason=reason,
    )


def scrub_dash_copy(text: str) -> str:
    """Remove em and en dashes from visible deck copy.

    Style and script blocks stay untouched. A dash used as an empty cell
    becomes the word Unavailable. A number range becomes the word "to".
    """

    if not text:
        return text

    def _scrub_chunk(chunk: str) -> str:
        chunk = _PLACEHOLDER_DASH_RE.sub("><", chunk)
        chunk = _NUMBER_RANGE_RE.sub(r"\1 to \2", chunk)
        chunk = _EM_DASH_RE.sub(", ", chunk)
        chunk = _EN_DASH_RE.sub(", ", chunk)
        chunk = re.sub(r"\s+,", ",", chunk)
        chunk = re.sub(r",(?:\s*,)+", ", ", chunk)
        return chunk

    parts = _STYLE_OR_SCRIPT_RE.split(text)
    scrubbed: list[str] = []
    for part in parts:
        if part.lower().startswith("<style") or part.lower().startswith("<script"):
            scrubbed.append(part)
        else:
            scrubbed.append(_scrub_chunk(part))
    return "".join(scrubbed)


_COVERAGE_ZERO_RE = re.compile(r"\b0\s+covered\b|\b0\s+missing\b", re.IGNORECASE)
_EMPTY_METRIC_VALUE_RE = re.compile(
    r"class=['\"]val['\"]>\s*(?:\$?0(?:\.0+)?|n/?a|unavailable|unknown|not supplied)?\s*</",
    re.IGNORECASE,
)
_APPAREL_ADVICE_RE = re.compile(r"materials,\s*size and fit", re.IGNORECASE)
_INGREDIENT_ADVICE_RE = re.compile(
    r"ingredient context|clinical or generic packaging",
    re.IGNORECASE,
)
_IMAGE_CLAIM_RE = re.compile(r"usable primary image", re.IGNORECASE)
_REVIEW_CLAIM_RE = re.compile(
    r"credible review signal|lighter review base|review moat is still light",
    re.IGNORECASE,
)
_PRICE_CLAIM_RE = re.compile(
    r"Price on the site|The DTC price|The current price point",
    re.IGNORECASE,
)
_META_CONTENT_RE = re.compile(
    r'<meta\s+name="(?P<name>anata-scraped-support|anata-store-category)"\s+content="(?P<value>[^"]*)"',
    re.IGNORECASE,
)
_EMPTY_METRIC_TOKENS = {
    "",
    "0",
    "0.0",
    "0.00",
    "$0",
    "$0.0",
    "$0.00",
    "n/a",
    "na",
    "unavailable",
    "unknown",
    "not supplied",
    "none",
}


def metric_value_is_empty(value: str) -> bool:
    """True when a displayed metric is zero, blank, or a placeholder."""

    token = re.sub(r"\s+", " ", str(value or "")).strip().lower()
    if token in _EMPTY_METRIC_TOKENS:
        return True
    if re.fullmatch(r"\$?0(?:\.0+)?", token):
        return True
    if re.fullmatch(r"0\s*/\s*0", token):
        return True
    return False


def _meta_values(html: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for match in _META_CONTENT_RE.finditer(html or ""):
        found[match.group("name").lower()] = match.group("value").strip()
    return found


def _support_tokens(html: str) -> set[str]:
    raw = _meta_values(html).get("anata-scraped-support", "")
    return {part.strip().lower() for part in raw.split() if part.strip()}


def assess_deck_content(html: str) -> list[str]:
    """Phrases that make a deck unfit to send, in a stable order."""

    text = html or ""
    findings: list[str] = []
    if _ZANTREX_RE.search(text):
        findings.append("Zantrex")
    if _REDO_RE.search(text):
        findings.append("Redo")
    if _THIS_CATEGORY_RE.search(text):
        findings.append("this category")
    if _TOP_ZERO_RE.search(text):
        findings.append("Top 0")
    if _ZERO_DOLLAR_RE.search(text):
        findings.append("$0")
    if _ZERO_METRIC_RE.search(text) or _EMPTY_METRIC_VALUE_RE.search(text):
        findings.append("zero metric")
    if _COVERAGE_ZERO_RE.search(text):
        findings.append("coverage zero")
    from sales_support_agent.services.deck.dataset import category_family

    family = category_family(_meta_values(text).get("anata-store-category", ""))
    if _APPAREL_ADVICE_RE.search(text) and family != "apparel":
        findings.append("category mismatch")
    if _INGREDIENT_ADVICE_RE.search(text) and family != "ingredient":
        findings.append("category mismatch")
    support = _support_tokens(text)
    if _IMAGE_CLAIM_RE.search(text) and "image" not in support:
        findings.append("unsupported image")
    if _REVIEW_CLAIM_RE.search(text) and "reviews" not in support:
        findings.append("unsupported review")
    if _PRICE_CLAIM_RE.search(text) and "price" not in support:
        findings.append("unsupported price")
    return findings


def public_deck_block_reason(summary: Any) -> str:
    """Why the public deck page would not return 200.

    An empty string means the page would return 200. DTC mode, a deck with
    no Amazon market export, is sendable when the content gate passes.
    Amazon mode stays under review when that export was expected and is
    missing. Either mode stays under review when the content gate fails.
    """

    data = dict(summary or {})
    findings = [
        str(item).strip()
        for item in (data.get("content_gate_findings") or ())
        if str(item).strip()
    ]
    passed = bool(data.get("content_gate_passed", True)) and not findings
    if not passed:
        detail = ", ".join(findings)
        if detail:
            return f"Content gate failed: {detail}."
        return "Content gate failed."
    mode = str(data.get("market_mode") or "").strip().lower()
    if mode == "dtc":
        return ""
    sufficient = bool(data.get("market_evidence_sufficient", True))
    status = str(data.get("evidence_status") or "ready").strip().lower()
    if sufficient and status not in {"missing", "blocked"}:
        return ""
    reason = str(data.get("market_evidence_reason") or "").strip()
    return reason or "The market comparison needs an internal review."


def public_deck_visitor_message(reason: str) -> str:
    """The sentence an external visitor sees on the under-review page."""

    text = str(reason or "").strip()
    if text.lower().startswith("content gate"):
        return "The deck still needs an internal review before it can be shared."
    return text or "The market comparison needs an internal review."
