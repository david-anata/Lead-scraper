import csv
import io
import json
import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

app = FastAPI()


# ========= EXTERNAL ENDPOINTS =========
STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
APOLLO_PEOPLE_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
APOLLO_BULK_PEOPLE_MATCH_URL = "https://api.apollo.io/api/v1/people/bulk_match"
SLACK_CHAT_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD_URL = "https://slack.com/api/files.completeUploadExternal"
INSTANTLY_ADD_LEADS_URL = "https://api.instantly.ai/api/v2/leads/add"


# ========= RUNTIME CONFIG =========
REQUEST_TIMEOUT_SECONDS = 60
MAX_STORELEADS_PAGES = 10
STORELEADS_PAGE_SIZE = 200
MAX_APOLLO_DOMAINS_PER_RUN = 40
APOLLO_SLEEP_SECONDS = 1.2
MAX_CONTACTS_PER_DOMAIN = 2
APOLLO_SEARCH_CANDIDATES_PER_DOMAIN = 10

GENERIC_EMAIL_PREFIXES = (
    "info@",
    "support@",
    "hello@",
    "contact@",
    "admin@",
    "team@",
    "sales@",
    "marketing@",
    "office@",
    "care@",
    "service@",
    "help@",
    "noreply@",
    "no-reply@",
)

TARGET_CAMPAIGN_NAME = "Amazon | DTC Brands | Performance Marketing | Mar 2026"
APOLLO_TARGET_TITLES = (
    "founder",
    "co-founder",
    "owner",
    "ceo",
    "chief executive officer",
    "chief operating officer",
    "coo",
    "chief of staff",
    "president",
    "head of operations",
    "operations",
    "operator",
    "ecommerce",
    "e-commerce",
    "brand",
)
APOLLO_TARGET_SENIORITIES = (
    "c_suite",
    "vp",
    "head",
    "director",
    "manager",
)
APOLLO_DEBUG_RAW = os.getenv("APOLLO_DEBUG_RAW", "").strip().lower() in {"1", "true", "yes", "on"}
APP_VERSION = os.getenv("APP_VERSION", "apollo-people-search-v2")
RENDER_GIT_COMMIT = os.getenv("RENDER_GIT_COMMIT", "").strip()
RENDER_GIT_BRANCH = os.getenv("RENDER_GIT_BRANCH", "").strip()
PROCESSED_DOMAINS_FILE = os.getenv("PROCESSED_DOMAINS_FILE", "processed_domains.csv").strip()
DAILY_IMPORT_LOG_FILE = os.getenv("DAILY_IMPORT_LOG_FILE", "daily_import_counts.csv").strip()
DAILY_NEW_LEAD_LIMIT = int((os.getenv("DAILY_NEW_LEAD_LIMIT", "0") or "0").strip() or 0)
ENABLE_WEEKDAY_ONLY_IMPORTS = os.getenv("ENABLE_WEEKDAY_ONLY_IMPORTS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


# ========= REQUEST / SETTINGS MODELS =========
class ICPBuildRequest(BaseModel):
    date: str
    max_domains: int = Field(default=150)


@dataclass(frozen=True)
class Settings:
    storeleads_api_key: str
    apollo_api_key: str
    slack_bot_token: str
    slack_channel_id: str
    instantly_campaign_id: str
    instantly_api_key: str


# ========= CONFIGURATION =========
def configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)


def load_settings() -> Settings:
    return Settings(
        storeleads_api_key=os.getenv("STORELEADS_API_KEY", "").strip(),
        apollo_api_key=os.getenv("APOLLO_API_KEY", "").strip(),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", "").strip(),
        slack_channel_id=os.getenv("SLACK_CHANNEL_ID", "").strip(),
        instantly_campaign_id=os.getenv("INSTANTLY_CAMPAIGN_ID", "").strip(),
        instantly_api_key=(os.getenv("INSTANTLY_API_KEY") or os.getenv("INSTANTLY_AI") or "").strip(),
    )


def get_missing_required_settings(settings: Settings) -> list[str]:
    missing: list[str] = []

    if not settings.storeleads_api_key:
        missing.append("STORELEADS_API_KEY")
    if not settings.apollo_api_key:
        missing.append("APOLLO_API_KEY")
    if not settings.slack_bot_token:
        missing.append("SLACK_BOT_TOKEN")
    if not settings.slack_channel_id:
        missing.append("SLACK_CHANNEL_ID")

    return missing


def build_missing_settings_message(missing: list[str]) -> str:
    return (
        "Missing required environment variables: "
        + ", ".join(missing)
        + ". Set them before starting the API."
    )


def validate_required_settings(settings: Settings) -> None:
    missing = get_missing_required_settings(settings)
    if missing:
        raise HTTPException(status_code=500, detail=build_missing_settings_message(missing))


def validate_settings_on_startup(settings: Settings) -> None:
    missing = get_missing_required_settings(settings)
    if missing:
        message = build_missing_settings_message(missing)
        logger.error(message)
        raise RuntimeError(message)


@app.on_event("startup")
def startup() -> None:
    configure_logging()
    settings = load_settings()
    app.state.settings = settings
    validate_settings_on_startup(settings)
    logger.info(
        "[Startup] app_version=%s render_git_branch=%s render_git_commit=%s apollo_mode=people_search_enrichment",
        APP_VERSION,
        RENDER_GIT_BRANCH or "unknown",
        RENDER_GIT_COMMIT or "unknown",
    )


# ========= GENERAL HELPERS =========
def normalize_domain(domain: str) -> str:
    return (
        str(domain or "")
        .replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def parse_monthly_sales(store: dict[str, Any]) -> float | None:
    try:
        value = store.get("estimated_sales")
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_average_product_price_usd(store: dict[str, Any]) -> float | None:
    for field_name in ("avg_price_usd", "average_product_price_usd", "avgppusd"):
        try:
            value = store.get(field_name)
            if value is None:
                continue
            parsed_value = float(value)
            # StoreLeads price values are in minor currency units.
            return parsed_value / 100.0
        except (TypeError, ValueError):
            continue
    return None


def split_full_name(full_name: str) -> tuple[str, str]:
    name_parts = full_name.split()
    first_name = name_parts[0] if name_parts else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first_name, last_name


def rows_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def processed_domains_path() -> Path:
    configured_path = Path(PROCESSED_DOMAINS_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def load_processed_domains() -> set[str]:
    path = processed_domains_path()
    if not path.exists():
        return set()

    processed_domains: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            domain = normalize_domain((row or {}).get("domain", ""))
            if domain:
                processed_domains.add(domain)

    return processed_domains


def append_processed_domains(domains: set[str], run_date: str) -> None:
    if not domains:
        return

    path = processed_domains_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["domain", "date_added"])
        if not file_exists:
            writer.writeheader()

        for domain in sorted(domains):
            writer.writerow({"domain": domain, "date_added": run_date})


def daily_import_log_path() -> Path:
    configured_path = Path(DAILY_IMPORT_LOG_FILE)
    if configured_path.is_absolute():
        return configured_path
    return Path(__file__).resolve().parent / configured_path


def load_daily_import_counts() -> dict[str, int]:
    path = daily_import_log_path()
    if not path.exists():
        return {}

    counts_by_date: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            date_key = ((row or {}).get("date") or "").strip()
            try:
                imported_count = int((row or {}).get("imported_count", 0) or 0)
            except (TypeError, ValueError):
                imported_count = 0

            if date_key:
                counts_by_date[date_key] = counts_by_date.get(date_key, 0) + max(imported_count, 0)

    return counts_by_date


def append_daily_import_count(run_date: str, imported_count: int) -> None:
    if imported_count <= 0:
        return

    path = daily_import_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open("a", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=["date", "imported_count"])
        if not file_exists:
            writer.writeheader()

        writer.writerow({"date": run_date, "imported_count": imported_count})


def clean_company_name(company_name: str) -> str:
    cleaned = (company_name or "").strip()
    if not cleaned:
        return ""

    mojibake_replacements = {
        "‚Ä¢": " • ",
        "â€¢": " • ",
        "Â®": "",
        "Â™": "",
        "â„¢": "",
        "Ã©": "e",
        "Ã¨": "e",
        "Ã": "",
    }

    for source, target in mojibake_replacements.items():
        cleaned = cleaned.replace(source, target)

    cleaned = unicodedata.normalize("NFKC", cleaned)

    for separator in (" • ", " | ", " — ", " – ", " :: ", " - "):
        if separator in cleaned:
            cleaned = cleaned.split(separator, 1)[0]
            break

    cleaned = cleaned.replace("&amp;", "&")
    cleaned = re.sub(r"[^\w\s&'\-.,]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|•,")
    return cleaned


def clean_role_name(role_name: str) -> str:
    cleaned = (role_name or "").strip()
    if not cleaned:
        return ""

    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.title()

    acronym_replacements = {
        "Ceo": "CEO",
        "Coo": "COO",
        "Cfo": "CFO",
        "Cmo": "CMO",
        "Cto": "CTO",
        "Cro": "CRO",
        "Cso": "CSO",
        "Cpo": "CPO",
        "Vp": "VP",
    }
    for source, target in acronym_replacements.items():
        cleaned = re.sub(rf"\b{source}\b", target, cleaned)

    cleaned = re.sub(r"\bEcommerce\b", "Ecommerce", cleaned)
    cleaned = re.sub(r"\bE-Commerce\b", "E-commerce", cleaned)
    return cleaned.strip()


def clean_platform_name(platform_name: str) -> str:
    cleaned = (platform_name or "").strip()
    if not cleaned:
        return ""

    normalized = cleaned.lower()
    if "shopify" in normalized:
        return "Shopify"
    if "woocommerce" in normalized:
        return "WooCommerce"
    if "bigcommerce" in normalized:
        return "BigCommerce"
    if "magento" in normalized:
        return "Magento"
    if "wordpress" in normalized:
        return "WordPress"

    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title().strip()


def derive_department(role_name: str) -> str:
    normalized_role = (role_name or "").strip().lower()
    if not normalized_role:
        return ""

    department_keywords = (
        ("operations", "Operations"),
        ("operator", "Operations"),
        ("supply chain", "Operations"),
        ("logistics", "Operations"),
        ("fulfillment", "Operations"),
        ("ecommerce", "Ecommerce"),
        ("e-commerce", "Ecommerce"),
        ("digital", "Ecommerce"),
        ("growth", "Growth"),
        ("marketing", "Marketing"),
        ("brand", "Brand"),
        ("partnership", "Partnerships"),
        ("sales", "Sales"),
        ("revenue", "Revenue"),
        ("founder", "Leadership"),
        ("owner", "Leadership"),
        ("chief", "Leadership"),
        ("president", "Leadership"),
        ("ceo", "Leadership"),
        ("coo", "Leadership"),
    )

    for keyword, department in department_keywords:
        if keyword in normalized_role:
            return department

    return "Leadership"


def format_money_bucket(amount: float | None) -> str:
    if amount is None:
        return ""

    normalized_amount = int(round(max(amount, 0)))
    if normalized_amount == 0:
        return "$0"

    if normalized_amount < 1_000:
        bucket_size = 100
    elif normalized_amount < 100_000:
        bucket_size = 10_000
    elif normalized_amount < 1_000_000:
        bucket_size = 100_000
    elif normalized_amount < 10_000_000:
        bucket_size = 1_000_000
    else:
        bucket_size = 10_000_000

    bucketed_amount = round(normalized_amount / bucket_size) * bucket_size
    bucketed_amount = max(bucketed_amount, bucket_size)

    return f"${int(bucketed_amount):,}"


def estimate_monthly_orders(revenue: float | None, average_product_price_usd: float | None) -> int | None:
    if revenue is None or average_product_price_usd is None or average_product_price_usd <= 0:
        return None

    return max(int(round(revenue / average_product_price_usd)), 0)


def format_orders_bucket(order_count: int | None) -> str:
    if order_count is None:
        return ""

    if order_count == 0:
        return "0"

    if order_count < 100:
        bucket_size = 10
    elif order_count < 1_000:
        bucket_size = 100
    elif order_count < 10_000:
        bucket_size = 1_000
    else:
        bucket_size = 10_000

    bucketed_orders = round(order_count / bucket_size) * bucket_size
    bucketed_orders = max(bucketed_orders, bucket_size)

    return f"{int(bucketed_orders):,}"


def build_location_name(city: str, state: str, country_code: str) -> str:
    parts = [part.strip() for part in (city, state, country_code) if str(part or "").strip()]
    if not parts:
        return ""

    if len(parts) >= 2 and parts[0].lower() == parts[1].lower():
        parts = [parts[0]] + parts[2:]

    return ", ".join(parts)


def apply_daily_import_limit(
    rows: list[dict[str, Any]],
    run_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    scheduler_status = {
        "enabled": False,
        "weekday_only": ENABLE_WEEKDAY_ONLY_IMPORTS,
        "daily_limit": DAILY_NEW_LEAD_LIMIT,
        "already_imported_today": 0,
        "remaining_capacity": None,
        "run_date": run_date,
        "status": "disabled",
    }

    if not rows:
        scheduler_status["status"] = "no_rows"
        return rows, scheduler_status

    if DAILY_NEW_LEAD_LIMIT <= 0 and not ENABLE_WEEKDAY_ONLY_IMPORTS:
        return rows, scheduler_status

    scheduler_status["enabled"] = True

    current_date = datetime.now().date()
    if ENABLE_WEEKDAY_ONLY_IMPORTS and current_date.weekday() >= 5:
        scheduler_status["status"] = "weekend_blocked"
        scheduler_status["remaining_capacity"] = 0
        return [], scheduler_status

    if DAILY_NEW_LEAD_LIMIT <= 0:
        scheduler_status["status"] = "weekday_only_passthrough"
        return rows, scheduler_status

    daily_counts = load_daily_import_counts()
    today_key = current_date.isoformat()
    already_imported_today = daily_counts.get(today_key, 0)
    remaining_capacity = max(DAILY_NEW_LEAD_LIMIT - already_imported_today, 0)

    scheduler_status["already_imported_today"] = already_imported_today
    scheduler_status["remaining_capacity"] = remaining_capacity

    if remaining_capacity <= 0:
        scheduler_status["status"] = "daily_capacity_reached"
        return [], scheduler_status

    scheduler_status["status"] = "limited" if len(rows) > remaining_capacity else "within_capacity"
    return rows[:remaining_capacity], scheduler_status


# ========= STORELEADS =========
def build_storeleads_query() -> dict[str, Any]:
    # This payload is intentionally preserved because it defines the current ICP.
    return {
        "must": {
            "conjuncts": [
                {
                    "field": "tech",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": (
                        "Wordpress Cloudflare Cloudflare...CDN Google...Ads...Pixel "
                        "Facebook...Pixel Apple...Pay Google...Pay Shop...Pay "
                        "PayPal...Express...Checkout Yoast Google...Analytics "
                        "Google...Analytics...4 Judge.me TikTok...Pixel Klaviyo "
                        "Mailchimp Shop Klarna Stripe Hotjar Omnisend ReCharge "
                        "Yotpo ShareASale Aftership Affirm Route Faire Reviews.io "
                        "HubSpot"
                    ),
                },
                {
                    "field": "an",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": (
                        "1.judgeme 1.product-reviews 1.loox 1.tt-reviewimport "
                        "1.yotpo-social-reviews 1.ryviu 1.sealapps-product-review "
                        "1.product-reviews-addon 1.vitals 1.air-reviews"
                    ),
                },
                {
                    "field": "it",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "4 7 3 2 8 1 10 13",
                },
                {
                    "field": "p",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "1",
                },
                {
                    "field": "empc",
                    "min": None,
                    "max": 200,
                    "inclusive_min": True,
                    "inclusive_max": True,
                },
                {
                    "field": "er",
                    "min": None,
                    "max": 100000000,
                    "inclusive_min": True,
                    "inclusive_max": True,
                },
                {
                    "field": "cc",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "Unknown US GB CA AU",
                },
                {
                    "field": "scs",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "8 1 2 9",
                },
            ]
        },
        "must_not": {
            "disjuncts": [
                {
                    "field": "tech",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "Printful Printify teelaunch Calendly",
                },
                {
                    "field": "an",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "1.printful 1.printify 1.gelato-print-on-demand",
                },
                {
                    "field": "scs",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "12 10 14 13 3 11",
                },
                {
                    "field": "cat",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": (
                        "/People...&...Society /Autos...&...Vehicles "
                        "/Business...&...Industrial/Business...Services "
                        "/People...&...Society/Religion...&...Belief "
                        "/Autos...&...Vehicles/Parts...&...Services "
                        "/People...&...Society/Family...&...Relationships "
                        "/Business...&...Industrial/Industrial...Materials...&...Equipment "
                        "/People...&...Society/Family...&...Relationships/Family "
                        "/Business...&...Industrial/Agriculture...&...Forestry "
                        "/People...&...Society/Social...Issues...&...Advocacy "
                        "/Business...&...Industrial/Business...Services/Office...Supplies "
                        "/Travel/Hotels...&...Accommodations "
                        "/Business...&...Industrial/Construction...&...Maintenance "
                        "/Business...&...Industrial/Chemicals...Industry "
                        "/Business...&...Industrial/Business...Operations "
                        "/Autos...&...Vehicles/Motor...Vehicles "
                        "/People...&...Society/Family...&...Relationships/Marriage "
                        "/Finance/Investing "
                        "/Autos...&...Vehicles/Motor...Vehicles/Motorcycles...&...Scooters "
                        "/People...&...Society/Social...Issues...&...Advocacy/Charity...&...Philanthropy "
                        "/Business...&...Industrial/Metals...&...Mining "
                        "/Autos...&...Vehicles/Boats...&...Watercraft "
                        "/Business...&...Industrial/Renewable...&...Alternative...Energy "
                        "/Autos...&...Vehicles/Repair...&...Maintenance "
                        "/Travel/Car...Rental...&...Taxi...Services "
                        "/Business...&...Industrial/Packaging "
                        "/Business...&...Industrial/Manufacturing "
                        "/Business...&...Industrial/Industrial...Materials...&...Equipment/Heavy...Machinery "
                        "/Travel/Air...Travel "
                        "/Computers/Software/Business...&...Productivity...Software "
                        "/People...&...Society/Social...Issues...&...Advocacy/Green...Living...&...Environmental...Issues "
                        "/Business...&...Industrial/Business...Services/E-Commerce...Services "
                        "/Business...&...Industrial/Pharmaceuticals...&...Biotech "
                        "/People...&...Society/Kids...&...Teens "
                        "/Business...&...Industrial/Chemicals...Industry/Plastics...&...Polymers "
                        "/Business...&...Industrial/Agriculture...&...Forestry/Agricultural...Equipment "
                        "/Business...&...Industrial/Mail...&...Package...Delivery "
                        "/Finance/Investing/Currencies...&...Foreign...Exchange "
                        "/Business...&...Industrial/Retail...Equipment...&...Technology "
                        "/People...&...Society/Politics "
                        "/Business...&...Industrial/Metals...&...Mining/Precious...Metals "
                        "/Business...&...Industrial/Business...Services/Consulting "
                        "/Business...&...Industrial/Business...Services/Corporate...Events "
                        "/Business...&...Industrial/Agriculture...&...Forestry/Livestock "
                        "/Travel/Cruises...&...Charters "
                        "/Autos...&...Vehicles/Motor...Vehicles/Off-Road "
                        "/Autos...&...Vehicles/Campers...&...RVs "
                        "/Autos...&...Vehicles/Motor...Vehicles/Trucks...&...SUVs "
                        "/People...&...Society/Social...Networks "
                        "/Consumer...Electronics/Mobile...&...Wireless/Mobile...Apps...&...Add-Ons "
                        "/Business...&...Industrial/Moving...&...Relocation "
                        "/Autos...&...Vehicles/Motor...Vehicles/Electric...&...Alternative "
                        "/Travel/Air...Travel/Airport...Parking...&...Transportation "
                        "/Travel/Bus...&...Rail "
                        "/Business...&...Industrial/Agriculture...&...Forestry/Wood...&...Forestry "
                        "/Autos...&...Vehicles/Safety "
                        "/Business...&...Industrial/Business...Finance "
                        "/Business...&...Industrial/Agriculture...&...Forestry/Beekeeping "
                        "/Business...&...Industrial/Business...Services/Office...Services "
                        "/Jobs...&...Education/Business "
                        "/People...&...Society/Family...&...Relationships/Troubled...Relationships "
                        "/Autos...&...Vehicles/Classic...Vehicles "
                        "/Business...&...Industrial/Advertising...&...Marketing/Public...Relations "
                        "/Business...&...Industrial/Advertising...&...Marketing "
                        "/Business...&...Industrial/Business...Services/Writing...&...Editing...Services "
                        "/Finance/Investing/Stocks...&...Bonds "
                        "/Business...&...Industrial/Printing...&...Publishing "
                        "/Computers"
                    ),
                },
            ]
        },
    }


def matches_icp(_store: dict[str, Any]) -> bool:
    # StoreLeads already applies the full ICP query above.
    return True


def fetch_storeleads_page(page: int, settings: Settings) -> list[dict[str, Any]]:
    payload = {
        "page": page,
        "page_size": STORELEADS_PAGE_SIZE,
        "bq": json.dumps(build_storeleads_query()),
        "fields": ",".join(
            [
                "name",
                "title",
                "platform",
                "country_code",
                "state",
                "city",
                "estimated_sales",
                "avg_price_usd",
            ]
        ),
    }

    response = requests.post(
        STORELEADS_URL,
        headers={
            "Authorization": f"Bearer {settings.storeleads_api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    data = response.json()
    domains = data.get("domains", []) or []
    logger.info("[StoreLeads] page=%s returned %s domains", page, len(domains))
    return domains


def collect_domains(max_domains: int, settings: Settings) -> tuple[list[dict[str, Any]], int]:
    qualified_domains: list[dict[str, Any]] = []
    seen_domains: set[str] = set()
    raw_scanned = 0

    for page in range(MAX_STORELEADS_PAGES):
        domains = fetch_storeleads_page(page, settings)
        if not domains:
            break

        raw_scanned += len(domains)

        for store in domains:
            normalized_domain = normalize_domain(store.get("name", ""))
            if not normalized_domain or normalized_domain in seen_domains:
                continue

            seen_domains.add(normalized_domain)

            if matches_icp(store):
                qualified_domains.append(store)

            if len(qualified_domains) >= max_domains:
                return qualified_domains, raw_scanned

    return qualified_domains, raw_scanned


# ========= APOLLO =========
def is_personal_email(email: str) -> bool:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return False

    return not any(normalized_email.startswith(prefix) for prefix in GENERIC_EMAIL_PREFIXES)


def extract_contact_email(contact: dict[str, Any]) -> str:
    direct_email = (contact.get("email") or "").strip().lower()
    if direct_email:
        return direct_email

    emails = contact.get("emails") or []
    if emails and isinstance(emails, list):
        first_email = ((emails[0] or {}).get("email") or "").strip().lower()
        if first_email:
            return first_email

    return ""


def extract_contact_name(contact: dict[str, Any]) -> str:
    full_name = (contact.get("name") or "").strip()
    if full_name:
        return full_name

    first_name = (contact.get("first_name") or "").strip()
    last_name = (contact.get("last_name") or "").strip()
    return " ".join(part for part in (first_name, last_name) if part).strip()


def email_matches_store(email: str, store_domain: str) -> bool:
    email_domain = normalize_domain(email.split("@")[-1])
    normalized_store_domain = normalize_domain(store_domain)
    return email_domain == normalized_store_domain or email_domain.endswith("." + normalized_store_domain)


def score_contact_title(title: str) -> int:
    normalized_title = (title or "").strip().lower()
    if not normalized_title:
        return 0

    weighted_keywords = (
        ("founder", 100),
        ("co-founder", 100),
        ("owner", 90),
        ("chief executive officer", 85),
        ("ceo", 85),
        ("president", 80),
        ("chief operating officer", 75),
        ("coo", 75),
        ("chief of staff", 70),
        ("head of", 65),
        ("vp", 55),
        ("vice president", 55),
        ("director", 45),
        ("operations", 40),
        ("operator", 40),
        ("manager", 30),
    )

    for keyword, score in weighted_keywords:
        if keyword in normalized_title:
            return score

    return 10


def search_apollo_people(
    domain: str,
    settings: Settings,
    *,
    max_results: int = APOLLO_SEARCH_CANDIDATES_PER_DOMAIN,
) -> list[dict[str, Any]]:
    search_params = {
        "page": 1,
        "per_page": max_results,
        "include_similar_titles": "true",
        "person_titles[]": list(APOLLO_TARGET_TITLES),
        "person_seniorities[]": list(APOLLO_TARGET_SENIORITIES),
        "q_organization_domains_list[]": [domain],
    }

    try:
        response = requests.post(
            APOLLO_PEOPLE_SEARCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": settings.apollo_api_key,
            },
            params=search_params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[Apollo] people search request error for domain=%s: %s", domain, exc)
        return []

    if response.status_code != 200:
        failure_class = "request_shape_or_unknown"
        if response.status_code in (401, 403):
            failure_class = "api_key_permission_or_scope"
        elif response.status_code == 422:
            failure_class = "request_shape"
        elif response.status_code >= 500:
            failure_class = "apollo_server_error"
        logger.warning(
            "[Apollo] people search non-200 for domain=%s status=%s failure_class=%s body=%s",
            domain,
            response.status_code,
            failure_class,
            response.text,
        )
        if APOLLO_DEBUG_RAW:
            logger.debug("[Apollo] people search params for domain=%s: %s", domain, search_params)
        return []

    try:
        data = response.json()
    except ValueError:
        logger.warning("[Apollo] invalid people search JSON for domain=%s", domain)
        return []

    if APOLLO_DEBUG_RAW:
        logger.debug("[Apollo] people search response for domain=%s: %s", domain, data)

    people = data.get("people", []) or []
    logger.info("[Apollo] raw people for domain=%s: %s", domain, len(people))
    return people


def enrich_apollo_people(people: list[dict[str, Any]], settings: Settings) -> list[dict[str, Any]]:
    if not people:
        return []

    details = [{"id": person["id"]} for person in people if person.get("id")]
    if not details:
        return []

    enrich_payload = {"details": details}

    try:
        response = requests.post(
            APOLLO_BULK_PEOPLE_MATCH_URL,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": settings.apollo_api_key,
            },
            params={
                "reveal_personal_emails": "false",
                "reveal_phone_number": "false",
            },
            json=enrich_payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.warning("[Apollo] people enrichment request error: %s", exc)
        return []

    if response.status_code != 200:
        failure_class = "request_shape_or_unknown"
        if response.status_code in (401, 403):
            failure_class = "api_key_permission_or_scope"
        elif response.status_code == 422:
            failure_class = "request_shape"
        elif response.status_code >= 500:
            failure_class = "apollo_server_error"
        logger.warning(
            "[Apollo] people enrichment non-200 status=%s failure_class=%s body=%s",
            response.status_code,
            failure_class,
            response.text,
        )
        if APOLLO_DEBUG_RAW:
            logger.debug("[Apollo] people enrichment payload: %s", enrich_payload)
        return []

    try:
        data = response.json()
    except ValueError:
        logger.warning("[Apollo] invalid people enrichment JSON")
        return []

    if APOLLO_DEBUG_RAW:
        logger.debug("[Apollo] people enrichment response: %s", data)

    matches = [match for match in (data.get("matches", []) or []) if isinstance(match, dict)]
    logger.info("[Apollo] enriched people returned: %s", len(matches))
    return matches


def search_apollo_contacts(
    domain: str,
    settings: Settings,
    *,
    max_per_domain: int = MAX_CONTACTS_PER_DOMAIN,
) -> tuple[list[dict[str, Any]], dict[str, int | str]]:
    people = search_apollo_people(domain, settings)
    if not people:
        logger.info(
            "[ApolloPipeline] domain=%s stage=people_search result=empty likely_root_cause=request_shape_permission_or_no_results",
            domain,
        )
        return [], {
            "domain": domain,
            "people_search_candidates": 0,
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
        }

    enriched_people = enrich_apollo_people(people, settings)
    if not enriched_people:
        logger.info(
            "[ApolloPipeline] domain=%s stage=enrichment result=empty likely_root_cause=permission_shape_or_no_matches people_search_candidates=%s",
            domain,
            len(people),
        )
        return [], {
            "domain": domain,
            "people_search_candidates": len(people),
            "enrichment_matches": 0,
            "candidates_with_any_email": 0,
            "candidates_with_brand_domain_email": 0,
        }

    candidates_with_any_email = sum(1 for person in enriched_people if extract_contact_email(person))

    filtered_people = [
        person
        for person in enriched_people
        if email_matches_store(extract_contact_email(person), domain)
    ]
    filtered_people.sort(key=lambda person: score_contact_title(person.get("title", "")), reverse=True)
    return filtered_people[: max_per_domain * 6], {
        "domain": domain,
        "people_search_candidates": len(people),
        "enrichment_matches": len(enriched_people),
        "candidates_with_any_email": candidates_with_any_email,
        "candidates_with_brand_domain_email": len(filtered_people),
    }


# ========= LEAD OUTPUT BUILDERS =========
def determine_offer(revenue: float | None) -> str:
    if revenue and revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


def build_csv_rows(
    domains: list[dict[str, Any]],
    run_date: str,
    settings: Settings,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    instantly_rows: list[dict[str, Any]] = []
    linkedin_rows: list[dict[str, Any]] = []
    successful_contacts = 0
    apollo_hits = 0
    seen_emails_global: set[str] = set()

    max_apollo_domains = min(MAX_APOLLO_DOMAINS_PER_RUN, len(domains))

    for index, store in enumerate(domains):
        domain = normalize_domain(store.get("name", ""))
        if not domain or index >= max_apollo_domains:
            continue

        contacts, apollo_debug_stats = search_apollo_contacts(
            domain,
            settings,
            max_per_domain=MAX_CONTACTS_PER_DOMAIN,
        )
        time.sleep(APOLLO_SLEEP_SECONDS)

        if contacts:
            apollo_hits += 1

        revenue = parse_monthly_sales(store)
        formatted_revenue = format_money_bucket(revenue)
        average_product_price_usd = parse_average_product_price_usd(store)
        formatted_average_product_price = format_money_bucket(average_product_price_usd)
        estimated_monthly_orders = estimate_monthly_orders(revenue, average_product_price_usd)
        formatted_estimated_monthly_orders = format_orders_bucket(estimated_monthly_orders)
        offer = determine_offer(revenue)
        accepted_for_domain = 0

        for contact in contacts:
            if accepted_for_domain >= MAX_CONTACTS_PER_DOMAIN:
                break

            full_name = extract_contact_name(contact)
            email = extract_contact_email(contact)

            if not email:
                continue
            if not is_personal_email(email):
                continue
            if not email_matches_store(email, domain):
                continue
            if email in seen_emails_global:
                continue

            seen_emails_global.add(email)
            accepted_for_domain += 1

            first_name, last_name = split_full_name(full_name)
            linkedin_url = contact.get("linkedin_url", "") or ""
            store_title = clean_company_name(store.get("title", "") or "")
            role = clean_role_name(contact.get("title", "") or "")
            department = derive_department(role)
            platform = clean_platform_name(store.get("platform", "") or "")
            location = build_location_name(
                store.get("city", "") or "",
                store.get("state", "") or "",
                store.get("country_code", "") or "",
            )

            instantly_rows.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "role": role,
                    "department": department,
                    "linkedin_url": linkedin_url,
                    "company_name": store_title,
                    "website": domain,
                    "platform": platform,
                    "location": location,
                    "city": store.get("city", ""),
                    "state": store.get("state", ""),
                    "revenue": formatted_revenue,
                    "average_product_price": formatted_average_product_price,
                    "estimated_monthly_orders": formatted_estimated_monthly_orders,
                    "campaign_name": TARGET_CAMPAIGN_NAME,
                    "campaign_id": settings.instantly_campaign_id,
                    "custom_offer": offer,
                }
            )

            linkedin_rows.append(
                {
                    "name": full_name,
                    "role": role,
                    "department": department,
                    "linkedin_url": linkedin_url,
                    "company": store_title,
                    "website": domain,
                    "email": email,
                    "platform": platform,
                    "location": location,
                    "city": store.get("city", ""),
                    "state": store.get("state", ""),
                    "revenue": formatted_revenue,
                    "average_product_price": formatted_average_product_price,
                    "estimated_monthly_orders": formatted_estimated_monthly_orders,
                    "date_added": run_date,
                }
            )

            successful_contacts += 1

        logger.info(
            "[ApolloPipeline] domain=%s people_search_candidates=%s enrichment_matches=%s "
            "candidates_with_any_email=%s candidates_with_brand_domain_email=%s "
            "final_contacts_selected=%s",
            apollo_debug_stats["domain"],
            apollo_debug_stats["people_search_candidates"],
            apollo_debug_stats["enrichment_matches"],
            apollo_debug_stats["candidates_with_any_email"],
            apollo_debug_stats["candidates_with_brand_domain_email"],
            accepted_for_domain,
        )

    return instantly_rows, linkedin_rows, successful_contacts, apollo_hits


# ========= SLACK =========
def build_slack_headers(settings: Settings) -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.slack_bot_token}"}


def upload_file_to_slack(filename: str, content: str, settings: Settings) -> dict[str, Any]:
    if not content:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    content_bytes = content.encode("utf-8")
    if len(content_bytes) <= 1:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    upload_details_response = requests.post(
        SLACK_GET_UPLOAD_URL,
        headers=build_slack_headers(settings),
        data={
            "filename": filename,
            "length": str(len(content_bytes)),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    upload_details_response.raise_for_status()
    upload_details = upload_details_response.json()

    if not upload_details.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack getUploadURLExternal failed: {upload_details}",
        )

    upload_url = upload_details.get("upload_url")
    file_id = upload_details.get("file_id")
    if not upload_url or not file_id:
        raise HTTPException(
            status_code=500,
            detail=f"Slack upload URL missing from response: {upload_details}",
        )

    upload_response = requests.post(
        upload_url,
        data=content_bytes,
        headers={"Content-Type": "application/octet-stream"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    upload_response.raise_for_status()

    completion_response = requests.post(
        SLACK_COMPLETE_UPLOAD_URL,
        headers=build_slack_headers(settings),
        data={
            "files": json.dumps([{"id": file_id, "title": filename}]),
            "channel_id": settings.slack_channel_id,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    completion_response.raise_for_status()
    completion = completion_response.json()

    if not completion.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack completeUploadExternal failed: {completion}",
        )

    return completion


def import_leads_to_instantly(rows: list[dict[str, Any]], settings: Settings) -> dict[str, Any]:
    if not rows:
        return {"status": "skipped", "reason": "no_rows", "created_count": 0, "skipped_count": 0}

    if not settings.instantly_campaign_id:
        logger.warning("[Instantly] import skipped: INSTANTLY_CAMPAIGN_ID missing")
        return {"status": "skipped", "reason": "missing_campaign_id", "created_count": 0, "skipped_count": 0}

    if not settings.instantly_api_key:
        logger.warning("[Instantly] import skipped: INSTANTLY_API_KEY missing")
        return {"status": "skipped", "reason": "missing_api_key", "created_count": 0, "skipped_count": 0}

    leads = []
    for row in rows:
        leads.append(
            {
                "email": row.get("email", ""),
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "company_name": row.get("company_name", ""),
                "website": row.get("website", ""),
                "custom_variables": {
                    "custom_offer": row.get("custom_offer", ""),
                    "linkedin_url": row.get("linkedin_url", ""),
                    "role": row.get("role", ""),
                    "department": row.get("department", ""),
                    "platform": row.get("platform", ""),
                    "location": row.get("location", ""),
                    "city": row.get("city", ""),
                    "state": row.get("state", ""),
                    "revenue": row.get("revenue", ""),
                    "average_product_price": row.get("average_product_price", ""),
                    "estimated_monthly_orders": row.get("estimated_monthly_orders", ""),
                },
            }
        )

    response = requests.post(
        INSTANTLY_ADD_LEADS_URL,
        headers={
            "Authorization": f"Bearer {settings.instantly_api_key}",
            "Content-Type": "application/json",
        },
        json={
            "campaign_id": settings.instantly_campaign_id,
            "leads": leads,
            "verify_leads_on_import": False,
            "skip_if_in_workspace": True,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    result = response.json()

    created_count = len(result.get("created_leads", []) or [])
    skipped_count = int(result.get("skipped_count", 0) or 0)
    logger.info(
        "[Instantly] status=%s total_sent=%s created_count=%s skipped_count=%s invalid_email_count=%s",
        result.get("status", "unknown"),
        result.get("total_sent", len(rows)),
        created_count,
        skipped_count,
        result.get("invalid_email_count", 0),
    )
    return {
        "status": result.get("status", "unknown"),
        "reason": "",
        "created_count": created_count,
        "skipped_count": skipped_count,
        "total_sent": result.get("total_sent", len(rows)),
    }


def post_slack_summary(
    raw_scanned: int,
    qualified_domains: int,
    new_domains_considered: int,
    previously_processed_domains: int,
    apollo_domains_queried: int,
    apollo_hits: int,
    successful_contacts: int,
    instantly_import_result: dict[str, Any],
    scheduler_status: dict[str, Any],
    settings: Settings,
) -> None:
    contact_rate_per_apollo_domain = (
        round((successful_contacts / apollo_domains_queried) * 100, 2) if apollo_domains_queried else 0
    )
    contact_rate_per_apollo_hit = round((successful_contacts / apollo_hits) * 100, 2) if apollo_hits else 0

    scheduler_lines = ""
    if scheduler_status.get("enabled"):
        scheduler_lines = f"""
Scheduler status: {scheduler_status.get("status", "unknown")}
Daily new lead limit: {scheduler_status.get("daily_limit", 0)}
Already imported today: {scheduler_status.get("already_imported_today", 0)}
Remaining capacity: {scheduler_status.get("remaining_capacity", 0)}
"""

    message_text = f"""<!channel>

Lead build completed.

Domains scanned from StoreLeads: {raw_scanned}
ICP matches: {qualified_domains}
Previously processed domains skipped: {previously_processed_domains}
New domains considered: {new_domains_considered}
Domains queried in Apollo: {apollo_domains_queried}
Domains with Apollo candidates: {apollo_hits}
Final contacts selected: {successful_contacts}
Instantly import status: {instantly_import_result.get("status", "unknown")}
Instantly leads created: {instantly_import_result.get("created_count", 0)}
Instantly leads skipped: {instantly_import_result.get("skipped_count", 0)}
{scheduler_lines}

Contacts per Apollo-queried domain: {contact_rate_per_apollo_domain}%
Contacts per Apollo-positive domain: {contact_rate_per_apollo_hit}%

CSV file attached below.
"""

    response = requests.post(
        SLACK_CHAT_POST_MESSAGE_URL,
        headers={**build_slack_headers(settings), "Content-Type": "application/json"},
        json={
            "channel": settings.slack_channel_id,
            "text": message_text,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


# ========= ROUTES =========
@app.get("/")
def home() -> dict[str, str]:
    return {"status": "lead engine running"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run-lead-build", response_model=None)
def run(payload: ICPBuildRequest) -> JSONResponse | StreamingResponse:
    settings = load_settings()
    app.state.settings = settings
    validate_required_settings(settings)

    try:
        qualified_domains, raw_scanned = collect_domains(payload.max_domains, settings)
        processed_domains = load_processed_domains()
        filtered_domains = [
            domain_obj
            for domain_obj in qualified_domains
            if normalize_domain(domain_obj.get("name", "")) not in processed_domains
        ]
        previously_processed_domains = len(qualified_domains) - len(filtered_domains)
        apollo_domains_queried = min(MAX_APOLLO_DOMAINS_PER_RUN, len(filtered_domains))

        logger.info(
            "[Run] date=%s max_domains=%s raw_scanned=%s icp_matches=%s new_domains_considered=%s skipped_processed=%s apollo_domains_queried=%s",
            payload.date,
            payload.max_domains,
            raw_scanned,
            len(qualified_domains),
            len(filtered_domains),
            previously_processed_domains,
            apollo_domains_queried,
        )

        instantly_rows, linkedin_rows, successful_contacts, apollo_hits = build_csv_rows(
            filtered_domains,
            payload.date,
            settings,
        )

        instantly_rows, scheduler_status = apply_daily_import_limit(instantly_rows, payload.date)
        allowed_emails = {row.get("email", "") for row in instantly_rows if row.get("email")}
        linkedin_rows = [row for row in linkedin_rows if row.get("email", "") in allowed_emails]
        successful_contacts = len(instantly_rows)

        instantly_csv = rows_to_csv(instantly_rows)
        exported_domains = {normalize_domain(row.get("website", "")) for row in instantly_rows if row.get("website")}
        instantly_import_result = import_leads_to_instantly(instantly_rows, settings)
        if instantly_rows and instantly_import_result.get("status") != "error":
            append_processed_domains(exported_domains, payload.date)
        created_count = int(instantly_import_result.get("created_count", 0) or 0)
        if created_count > 0:
            append_daily_import_count(datetime.now().date().isoformat(), created_count)

        post_slack_summary(
            raw_scanned=raw_scanned,
            qualified_domains=len(qualified_domains),
            new_domains_considered=len(filtered_domains),
            previously_processed_domains=previously_processed_domains,
            apollo_domains_queried=apollo_domains_queried,
            apollo_hits=apollo_hits,
            successful_contacts=successful_contacts,
            instantly_import_result=instantly_import_result,
            scheduler_status=scheduler_status,
            settings=settings,
        )

        if instantly_rows:
            upload_file_to_slack(f"instantly_upload_{payload.date}.csv", instantly_csv, settings)

        if not instantly_rows:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "ok",
                    "message": "No valid personal contacts found for this run.",
                    "domains_scanned": raw_scanned,
                    "icp_matches": len(qualified_domains),
                    "apollo_contacts_found": apollo_hits,
                    "personal_contacts_found": successful_contacts,
                },
            )

        return StreamingResponse(
            iter([instantly_csv]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="instantly_upload_{payload.date}.csv"'
            },
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("[Run] unexpected error")
        return JSONResponse(
            status_code=500,
            content={"error_type": type(exc).__name__, "detail": str(exc)},
        )
