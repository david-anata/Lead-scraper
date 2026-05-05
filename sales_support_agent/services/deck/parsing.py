"""Target / competitor product input parsing — ASIN, Amazon URL, Shopify URL."""

from __future__ import annotations

import base64
import csv
import html
import io
import json
import mimetypes
import re
import secrets
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Any

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from sales_support_agent.config import Settings
from sales_support_agent.integrations.amazon_sp_api import AmazonSpApiClient
from sales_support_agent.models.entities import AutomationRun
from sales_support_agent.services.audit import AuditService
from sales_support_agent.services.helium10 import (
    CerebroKeywordInsight,
    DistributionSlice,
    Helium10CerebroReport,
    Helium10KeywordReport,
    Helium10XrayReport,
    KeywordInsight,
    WordFrequencyReport,
    XrayProduct,
    parse_cerebro_csv,
    parse_keyword_csv,
    parse_keyword_csvs,
    parse_word_frequency_csv,
    parse_xray_csv,
    parse_xray_csvs,
)
from sales_support_agent.services.product_research import EnrichedHeroProduct, ProductResearchService

from sales_support_agent.services.deck.formatting import (  # noqa: F401
    _titleize_slug,
)


def _parse_target_product_input(value: str) -> dict[str, str]:
    cleaned = str(value or "").strip()
    if not cleaned:
        return {
            "source_type": "",
            "source_url": "",
            "domain": "",
            "brand_name": "",
            "product_handle": "",
            "product_name": "",
            "asin": "",
        }
    amazon_candidate = _parse_competitor_reference(cleaned)
    if _looks_like_amazon_target(cleaned, amazon_candidate["asin"]):
        return {
            "source_type": "amazon",
            "source_url": amazon_candidate["source_url"],
            "domain": "amazon.com",
            "brand_name": "",
            "product_handle": amazon_candidate["asin"],
            "product_name": "",
            "asin": amazon_candidate["asin"],
        }
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    if parsed.scheme and parsed.netloc:
        path_parts = [segment for segment in parsed.path.split("/") if segment]
        inferred_name = path_parts[-1].replace("-", " ").replace("_", " ").strip().title() if path_parts else ""
        domain = (parsed.netloc or "").strip().lower()
        brand_name = re.sub(r"^www\.", "", domain).split(".")[0].replace("-", " ").replace("_", " ").strip().title()
        return {
            "source_type": "website",
            "source_url": parsed.geturl(),
            "domain": domain,
            "brand_name": brand_name,
            "product_handle": path_parts[-1] if path_parts else "",
            "product_name": inferred_name,
            "asin": "",
        }
    if cleaned:
        inferred_name = cleaned.replace("-", " ").replace("_", " ").strip().title()
        return {
            "source_type": "website",
            "source_url": cleaned if "://" in cleaned else f"https://{cleaned}",
            "domain": "",
            "brand_name": "",
            "product_handle": "",
            "product_name": inferred_name,
            "asin": "",
        }
    return {
        "source_type": "",
        "source_url": "",
        "domain": "",
        "brand_name": "",
        "product_handle": "",
        "product_name": "",
        "asin": "",
    }
def _looks_like_amazon_target(raw_value: str, asin: str) -> bool:
    cleaned = str(raw_value or "").strip()
    if not cleaned or not asin:
        return False
    upper = cleaned.upper()
    if re.fullmatch(r"[A-Z0-9]{10}", upper):
        return True
    parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
    host = (parsed.netloc or "").lower()
    return "amazon." in host or host.endswith("amzn.to")
def _parse_competitor_reference(value: str) -> dict[str, str]:
    cleaned = str(value or "").strip()
    asin_match = re.search(r"\b([A-Z0-9]{10})\b", cleaned.upper())
    parsed = urlparse(cleaned if "://" in cleaned else "")
    path = parsed.path if parsed.scheme else ""
    url_candidate = cleaned if parsed.scheme else ""
    name = ""
    if path:
        for pattern in (r"/dp/([A-Z0-9]{10})", r"/gp/product/([A-Z0-9]{10})", r"/([^/?#]+)/dp/[A-Z0-9]{10}"):
            path_match = re.search(pattern, path, flags=re.IGNORECASE)
            if path_match and pattern.startswith("/("):
                name = _titleize_slug(path_match.group(1))
                break
    asin = asin_match.group(1) if asin_match else ""
    identifier = asin or cleaned
    if not name:
        if asin:
            name = f"ASIN {asin}"
        else:
            name = _titleize_slug(cleaned.rsplit("/", 1)[-1]) or cleaned
    source_url = url_candidate or (f"https://www.amazon.com/dp/{asin}" if asin else cleaned)
    return {
        "name": name[:120],
        "identifier": identifier[:160],
        "source_url": source_url[:255],
        "asin": asin,
    }
