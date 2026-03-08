from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import csv
import io
import os
import json
import time
import requests
from typing import Any, Dict, List, Optional, Tuple

app = FastAPI()

STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")

STORELEADS_BASE = "https://storeleads.app/json/api/v1/all/domain"
HUNTER_DOMAIN_SEARCH = "https://api.hunter.io/v2/domain-search"
HUNTER_EMAIL_VERIFY = "https://api.hunter.io/v2/email-verifier"

REQUEST_TIMEOUT = 60

MIN_REVENUE = 20000
MAX_REVENUE = 300000
MAX_EMPLOYEES = 25

MAX_STORELEADS_PAGES = 10
STORELEADS_PAGE_SIZE = 100

SHIPPING_TECH = [
    "shipstation",
    "shippo",
    "easyship",
    "shippingeasy",
    "shiphero",
    "aftership",
    "pirate ship",
]

TEAM_MATURITY_TECH = [
    "klaviyo",
    "attentive",
    "gorgias",
    "recharge",
    "loop returns",
]

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

AMAZON_SIGNAL_KEYWORDS = [
    "amazon",
    "marketplace connect",
    "codisto",
    "cedcommerce",
    "buy with prime",
    "amazon mcf",
]

PRIMARY_TITLE_KEYWORDS = [
    "founder",
    "co-founder",
    "cofounder",
    "ceo",
    "owner",
    "head of ecommerce",
    "head of e-commerce",
    "head of operations",
    "vp marketing",
    "director of marketing",
    "ecommerce lead",
    "e-commerce lead",
]

ACCEPT_EMAIL_PREFIX = (
    "hello@",
    "support@",
    "contact@",
    "team@",
)

REJECT_EMAIL_PREFIX = (
    "info@",
    "admin@",
    "noreply@",
    "no-reply@",
)


class ICPBuildRequest(BaseModel):
    date: str
    sheet_name: str = "ICP Export"
    max_stores: int = 20
    first_page_only: bool = False


def storeleads_headers() -> Dict[str, str]:
    if not STORELEADS_API_KEY:
        raise HTTPException(status_code=500, detail="Missing STORELEADS_API_KEY")
    return {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def safe_get(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def normalize_domain(domain: str) -> str:
    return (
        domain.replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def get_technologies(domain: Dict[str, Any]) -> List[str]:
    techs = domain.get("technologies") or []
    names = []
    for t in techs:
        if isinstance(t, dict):
            names.append((t.get("name") or "").lower())
        else:
            names.append(str(t).lower())
    return names


def has_any_tech(techs: List[str], targets: List[str]) -> bool:
    for tech in techs:
        for target in targets:
            if target in tech:
                return True
    return False


def is_dropship_or_pod(techs: List[str]) -> bool:
    return has_any_tech(techs, POD_TECH)


def uses_shipping_tech(techs: List[str]) -> bool:
    return has_any_tech(techs, SHIPPING_TECH)


def has_team_maturity_tech(techs: List[str]) -> bool:
    return has_any_tech(techs, TEAM_MATURITY_TECH)


def monthly_sales_usd(domain: Dict[str, Any]) -> Optional[float]:
    value = safe_get(domain, "estimated_sales")
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def infer_amazon_tier(domain: Dict[str, Any], techs: List[str]) -> Tuple[str, bool]:
    sales_channels = domain.get("sales_channels") or []
    description = str(domain.get("description") or "").lower()

    if any(str(ch).lower() == "amazon" for ch in sales_channels):
        return "A", False

    for tech in techs:
        if any(sig in tech for sig in AMAZON_SIGNAL_KEYWORDS):
            return "A", False

    if "amazon" in description:
        return "A", True

    return "B", True


def matches_icp(domain: Dict[str, Any]) -> bool:
    platform = str(safe_get(domain, "platform") or "").lower()
    country = str(safe_get(domain, "country_code", "country") or "").upper()
    employees = safe_get(domain, "employee_count")
    revenue = monthly_sales_usd(domain)
    techs = get_technologies(domain)

    if platform != "shopify":
        return False

    if country != "US":
        return False

    if employees is not None:
        try:
            if int(employees) > MAX_EMPLOYEES:
                return False
        except Exception:
            pass

    if revenue is None:
        return False

    if revenue < MIN_REVENUE or revenue > MAX_REVENUE:
        return False

    if is_dropship_or_pod(techs):
        return False

    if not uses_shipping_tech(techs):
        return False

    if not has_team_maturity_tech(techs):
        return False

    return True


def build_storeleads_bq() -> Dict[str, Any]:
    tech_query = " ".join([
        "Aftership",
        "ShipStation",
        "Easyship",
        "Pirate Ship",
        "Shippo",
        "ShippingEasy",
        "ShipHero",
        "Klaviyo",
        "Attentive",
        "Gorgias",
        "Recharge",
        "Loop Returns",
    ])

    return {
        "must": {
            "conjuncts": [
                {
                    "field": "p",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "1"
                },
                {
                    "field": "tech",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": tech_query
                },
                {
                    "field": "cc",
                    "operator": "or",
                    "analyzer": "advanced",
                    "match": "US"
                },
                {
                    "field": "er",
                    "min": MIN_REVENUE,
                    "max": MAX_REVENUE,
                    "inclusive_min": True,
                    "inclusive_max": True
                }
            ]
        }
    }


def fetch_storeleads_page(page: int, page_size: int) -> List[Dict[str, Any]]:
    payload = {
        "page": page,
        "page_size": page_size,
        "bq": json.dumps(build_storeleads_bq()),
        "fields": ",".join([
            "name",
            "title",
            "platform",
            "country_code",
            "state",
            "city",
            "employee_count",
            "estimated_sales",
            "description",
            "sales_channels",
            "shipping_carriers",
            "technologies",
            "tags",
        ]),
    }

    response = requests.post(
        STORELEADS_BASE,
        headers=storeleads_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    data = response.json()
    return data.get("domains") or []


def collect_candidate_domains(max_stores: int, first_page_only: bool) -> List[Dict[str, Any]]:
    matched = []
    seen_domains = set()

    pages_to_scan = 1 if first_page_only else MAX_STORELEADS_PAGES

    for page in range(0, pages_to_scan):
        domains = fetch_storeleads_page(page=page, page_size=STORELEADS_PAGE_SIZE)
        if not domains:
            break

        for d in domains:
            domain_name = normalize_domain(str(safe_get(d, "name") or ""))
            if not domain_name or domain_name in seen_domains:
                continue

            seen_domains.add(domain_name)

            if matches_icp(d):
                matched.append(d)

            if len(matched) >= max_stores:
                return matched

    return matched


def hunter_domain_search(domain: str) -> List[Dict[str, Any]]:
    try:
        response = requests.get(
            HUNTER_DOMAIN_SEARCH,
            params={
                "domain": domain,
                "limit": 10,
                "api_key": HUNTER_IO_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code in (403, 429):
            return []

        response.raise_for_status()
        return response.json().get("data", {}).get("emails", []) or []
    except Exception:
        return []


def validate_email(email: str) -> bool:
    try:
        response = requests.get(
            HUNTER_EMAIL_VERIFY,
            params={
                "email": email,
                "api_key": HUNTER_IO_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code in (403, 429):
            return False

        response.raise_for_status()
        result = response.json().get("data", {}).get("result", "")
        return result in ["deliverable", "risky"]
    except Exception:
        return False


def pick_contact(domain: str, contacts: List[Dict[str, Any]]) -> Tuple[str, str, str, str]:
    # Tier 1: decision-maker-ish contacts with confidence
    for c in contacts:
        email = str(c.get("value") or "").strip().lower()
        confidence = int(c.get("confidence") or 0)
        position = str(c.get("position") or "").strip()
        first_name = str(c.get("first_name") or "").strip()
        last_name = str(c.get("last_name") or "").strip()
        linkedin_url = str(c.get("linkedin") or "").strip()

        if not email:
            continue
        if not email.endswith("@" + domain):
            continue
        if email.startswith(REJECT_EMAIL_PREFIX):
            continue
        if confidence < 70:
            continue

        title_lower = position.lower()
        if any(k in title_lower for k in PRIMARY_TITLE_KEYWORDS):
            if validate_email(email):
                return (
                    f"{first_name} {last_name}".strip(),
                    position,
                    email,
                    linkedin_url,
                )

    # Tier 2: fallback inboxes if validated
    for c in contacts:
        email = str(c.get("value") or "").strip().lower()
        if not email:
            continue
        if not email.endswith("@" + domain):
            continue
        if email.startswith(ACCEPT_EMAIL_PREFIX):
            if validate_email(email):
                return ("", "", email, "")

    return ("", "", "", "")


def determine_primary_offer(revenue: float) -> str:
    if revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


def build_rows(domains: List[Dict[str, Any]], run_date: str) -> List[Dict[str, Any]]:
    rows = []
    seen = set()

    for d in domains:
        domain = normalize_domain(str(safe_get(d, "name") or ""))
        if not domain:
            continue

        techs = get_technologies(d)
        revenue = monthly_sales_usd(d) or 0
        amazon_tier, amazon_uncertain = infer_amazon_tier(d, techs)

        contacts = hunter_domain_search(domain)
        contact_name, contact_title, contact_email, linkedin_url = pick_contact(domain, contacts)

        if not contact_email:
            continue

        dedupe_key = f"{domain}|{contact_email}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        rows.append({
            "domain": domain,
            "brand_name": d.get("title", ""),
            "state": d.get("state", ""),
            "city": d.get("city", ""),
            "revenue": revenue,
            "employees": d.get("employee_count", ""),
            "shipping_stack": ",".join(techs),
            "contact_name": contact_name,
            "contact_title": contact_title,
            "contact_email": contact_email,
            "linkedin": linkedin_url,
            "primary_offer": determine_primary_offer(revenue),
            "date_added": run_date,
        })

        time.sleep(0.2)

    return rows


@app.get("/")
def home():
    return {"status": "Agent server running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):
    domains = collect_candidate_domains(
        max_stores=payload.max_stores,
        first_page_only=payload.first_page_only
    )

    rows = build_rows(domains, payload.date)

    output = io.StringIO()
    fieldnames = [
        "domain",
        "brand_name",
        "state",
        "city",
        "revenue",
        "employees",
        "shipping_stack",
        "contact_name",
        "contact_title",
        "contact_email",
        "linkedin",
        "primary_offer",
        "date_added",
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="icp_export_{payload.date}.csv"'
        },
    )
