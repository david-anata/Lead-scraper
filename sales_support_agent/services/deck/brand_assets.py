"""Brand asset and stylesheet path resolution."""

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
from sales_support_agent.integrations.shopify import ShopifyStorefrontClient
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



def _candidate_brand_paths(settings: Settings, relative_path: str) -> list[Path]:
    configured_root = Path(str(getattr(settings, "shared_brand_package_path", "") or "")).expanduser()
    repo_root = Path(__file__).resolve().parents[2]
    candidates: list[Path] = []
    if str(configured_root):
        candidates.append(configured_root / relative_path)
    candidates.append(repo_root / "shared" / "anata_brand" / relative_path)
    return candidates
def _candidate_brand_asset_paths(settings: Settings, relative_path: str) -> list[Path]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for base_path in _candidate_brand_paths(settings, relative_path):
        stem_path = base_path.with_suffix("")
        prioritized: list[Path] = []
        normalized = str(relative_path).replace("\\", "/")
        if normalized.endswith("assets/monogram.png"):
            prioritized.append(base_path.with_name("1.png"))
        if normalized.endswith("assets/wordmark.png"):
            prioritized.append(base_path.with_name("anata wordmark logo - black.png"))
        for candidate in prioritized:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
        for suffix in (".png", ".svg", ".webp", ".jpg", ".jpeg"):
            candidate = stem_path.with_suffix(suffix)
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates
