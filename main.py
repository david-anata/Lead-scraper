from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import csv
import io
import os
import json
import time
import requests

app = FastAPI()

# ========= ENV =========
STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID", "")

# ========= ENDPOINTS =========
STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
APOLLO_PEOPLE_SEARCH = "https://api.apollo.io/api/v1/mixed_people/api_search"
HUNTER_VERIFY = "https://api.hunter.io/v2/email-verifier"

SLACK_CHAT_POST_MESSAGE = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD = "https://slack.com/api/files.completeUploadExternal"

# ========= CONFIG =========
REQUEST_TIMEOUT = 60

MIN_REVENUE = 20000
MAX_REVENUE = 300000
MAX_EMPLOYEES = 25  # from old ICP

MAX_PAGES = 5       # match old behavior
PAGE_SIZE = 100

# shipping-tech match string from old code
TECH_MATCH = "Aftership ShipStation Easyship Pirate Ship Shippo ShippingEasy ShipHero"

# POD / dropship techs to exclude
POD_TECH = [
    "printful",
    "printify",
    "teelaunch",
    "shineon",
    "gearment",
    "spocket",
    "zendrop",
    "dsers",
    "modalyst",
]

# generic email prefixes
GENERIC_PREFIXES = (
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

# ========= REQUEST MODEL =========
class ICPBuildRequest(BaseModel):
    date: str
    max_domains: int = 150


# ========= HELPERS: ICP & TECH =========
def normalize_domain(domain: str) -> str:
    return (
        str(domain or "")
        .replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def get_technologies(domain_obj):
    techs = domain_obj.get("technologies") or []
    names = []

    for t in techs:
        if isinstance(t, dict):
            names.append((t.get("name") or "").lower())
        else:
            names.append(str(t).lower())

    return names


def is_dropship(techs):
    for tech in techs:
        for bad in POD_TECH:
            if bad in tech:
                return True
    return False


def monthly_sales(domain_obj):
    try:
        value = domain_obj.get("estimated_sales")
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def matches_icp(domain_obj) -> bool:
    # Same logic as old version, but used after StoreLeads bq filter
    platform = str(domain_obj.get("platform", "")).lower()
    if platform != "shopify":
        return False

    if str(domain_obj.get("country_code", "")).upper() != "US":
        return False

    revenue = monthly_sales(domain_obj)
    if revenue is None:
        return False

    if revenue < MIN_REVENUE or revenue > MAX_REVENUE:
        return False

    employees = domain_obj.get("employee_count")
    if employees:
        try:
            if int(employees) > MAX_EMPLOYEES:
                return False
        except Exception:
            pass

    techs = get_technologies(domain_obj)
    if is_dropship(techs):
        return False

    return True


def build_storeleads_bq():
    # Directly ported from old file
    return {
        "must": {
            "conjuncts": [
                {
                    "field": "p",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "1",  # Shopify
                },
                {
                    "field": "tech",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": TECH_MATCH,
                },
                {
                    "field": "cc",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "US",
                },
                {
                    "field": "er",
                    "min": MIN_REVENUE,
                    "max": MAX_REVENUE,
                    "inclusive_min": True,
                    "inclusive_max": True,
                },
            ]
        }
    }


# ========= STORELEADS =========
def fetch_storeleads_page(page: int):
    if not STORELEADS_API_KEY:
        raise HTTPException(status_code=500, detail="STORELEADS_API_KEY missing")

    headers = {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "page": page,
        "page_size": PAGE_SIZE,
        "bq": json.dumps(build_storeleads_bq()),
        "fields": ",".join(
            [
                "name",
                "title",
                "platform",
                "country_code",
                "state",
                "city",
                "employee_count",
                "estimated_sales",
                "technologies",
            ]
        ),
    }

    r = requests.post(
        STORELEADS_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    r.raise_for_status()
    data = r.json()
    domains = data.get("domains", []) or []

    print(f"[StoreLeads] page={page} returned {len(domains)} domains")
    return domains


def collect_domains(max_domains: int):
    results = []
    seen = set()
    raw_scanned = 0

    for page in range(MAX_PAGES):
        domains = fetch_storeleads_page(page)

        if not domains:
            break

        raw_scanned += len(domains)

        for d in domains:
            domain = normalize_domain(d.get("name", ""))

            if not domain or domain in seen:
                continue

            seen.add(domain)

            if matches_icp(d):
                results.append(d)

            if len(results) >= max_domains:
                return results, raw_scanned

    return results, raw_scanned


# ========= APOLLO & HUNTER =========
def apollo_people_search(domain: str):
    if not APOLLO_API_KEY:
        raise HTTPException(status_code=500, detail="APOLLO_API_KEY missing")

    headers =
