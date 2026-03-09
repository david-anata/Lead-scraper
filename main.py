from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import csv
import io
import os
import json
import time
import requests

app = FastAPI()

STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")

STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
HUNTER_DOMAIN_SEARCH = "https://api.hunter.io/v2/domain-search"
HUNTER_VERIFY = "https://api.hunter.io/v2/email-verifier"

REQUEST_TIMEOUT = 60

MIN_REVENUE = 20000
MAX_REVENUE = 300000
MAX_EMPLOYEES = 25

MAX_PAGES = 5
PAGE_SIZE = 100

TECH_MATCH = "Aftership ShipStation Easyship Pirate Ship Shippo ShippingEasy ShipHero"

POD_TECH = [
    "printful",
    "printify",
    "teelaunch",
    "shineon",
    "gearment",
    "spocket",
    "zendrop",
    "dsers",
    "modalyst"
]

REJECT_EMAIL_PREFIX = (
    "info@",
    "admin@",
    "noreply@",
    "no-reply@"
)

FALLBACK_EMAIL_PREFIX = (
    "hello@",
    "support@",
    "contact@",
    "team@"
)


class ICPBuildRequest(BaseModel):
    date: str
    max_stores: int = 25
    first_page_only: bool = False


def normalize_domain(domain: str) -> str:
    return (
        domain.replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def get_technologies(domain):
    techs = domain.get("technologies") or []
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


def monthly_sales(domain):
    value = domain.get("estimated_sales")
    if value is None:
        return None

    try:
        return float(value)
    except:
        return None


def matches_icp(domain):
    platform = str(domain.get("platform", "")).lower()
    if platform != "shopify":
        return False

    if str(domain.get("country_code", "")).upper() != "US":
        return False

    revenue = monthly_sales(domain)
    if revenue is None:
        return False

    if revenue < MIN_REVENUE or revenue > MAX_REVENUE:
        return False

    employees = domain.get("employee_count")
    if employees:
        try:
            if int(employees) > MAX_EMPLOYEES:
                return False
        except:
            pass

    techs = get_technologies(domain)
    if is_dropship(techs):
        return False

    return True


def build_storeleads_bq():
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
                    "match": TECH_MATCH
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


def fetch_storeleads_page(page_number):
    headers = {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "page": page_number,
        "page_size": PAGE_SIZE,
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
            "tags"
        ])
    }

    r = requests.post(
        STORELEADS_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    return r.json().get("domains", [])


def collect_domains(max_stores, first_page_only):
    pages = 1 if first_page_only else MAX_PAGES
    collected = []
    seen = set()

    for page in range(0, pages):
        domains = fetch_storeleads_page(page)
        if not domains:
            break

        for d in domains:
            domain = normalize_domain(d.get("name", ""))
            if not domain or domain in seen:
                continue

            seen.add(domain)

            if matches_icp(d):
                collected.append(d)

            if len(collected) >= max_stores:
                return collected

    return collected


def hunter_search(domain):
    try:
        r = requests.get(
            HUNTER_DOMAIN_SEARCH,
            params={
                "domain": domain,
                "limit": 10,
                "api_key": HUNTER_IO_API_KEY
            },
            timeout=REQUEST_TIMEOUT
        )

        if r.status_code in [403, 429]:
            return []

        r.raise_for_status()

        return r.json().get("data", {}).get("emails", [])

    except:
        return []


def validate_email(email):
    try:
        r = requests.get(
            HUNTER_VERIFY,
            params={
                "email": email,
                "api_key": HUNTER_IO_API_KEY
            },
            timeout=REQUEST_TIMEOUT
        )

        if r.status_code in [403, 429]:
            return False

        r.raise_for_status()

        result = r.json().get("data", {}).get("result", "")
        return result in ["deliverable", "risky"]

    except:
        return False


def pick_contact(domain, contacts):
    # Tier 1: non-generic emails
    for c in contacts:
        email = (c.get("value") or "").strip().lower()
        confidence = int(c.get("confidence") or 0)
        first_name = (c.get("first_name") or "").strip()
        last_name = (c.get("last_name") or "").strip()
        title = (c.get("position") or "").strip()
        linkedin = (c.get("linkedin") or "").strip()

        if not email:
            continue
        if not email.endswith("@" + domain):
            continue
        if email.startswith(REJECT_EMAIL_PREFIX):
            continue
        if confidence < 60:
            continue

        if validate_email(email):
            return first_name, last_name, title, email, linkedin, "validated_person"

    # Tier 2: fallback inboxes
    for c in contacts:
        email = (c.get("value") or "").strip().lower()

        if not email:
            continue
        if not email.endswith("@" + domain):
            continue
        if email.startswith(FALLBACK_EMAIL_PREFIX):
            if validate_email(email):
                return "", "", "", email, "", "validated_fallback"

    return "", "", "", "", "", "no_valid_email"


def build_rows(domains, run_date):
    rows = []

    for d in domains:
        domain = normalize_domain(d.get("name", ""))
        contacts = hunter_search(domain)
        first_name, last_name, title, email, linkedin, email_status = pick_contact(domain, contacts)

        rows.append({
            "domain": domain,
            "brand_name": d.get("title", ""),
            "state": d.get("state", ""),
            "city": d.get("city", ""),
            "revenue": monthly_sales(d),
            "employees": d.get("employee_count", ""),
            "tech_stack": ",".join(get_technologies(d)),
            "hunter_email_count": len(contacts),
            "contact_name": (first_name + " " + last_name).strip(),
            "contact_title": title,
            "contact_email": email,
            "linkedin": linkedin,
            "email_status": email_status,
            "date_added": run_date
        })

        time.sleep(0.2)

    return rows


@app.get("/")
def home():
    return {"status": "agent running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):
    if not STORELEADS_API_KEY:
        raise HTTPException(status_code=500, detail="Missing STORELEADS_API_KEY")

    if not HUNTER_IO_API_KEY:
        raise HTTPException(status_code=500, detail="Missing HUNTER_IO_API_KEY")

    domains = collect_domains(
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
        "tech_stack",
        "hunter_email_count",
        "contact_name",
        "contact_title",
        "contact_email",
        "linkedin",
        "email_status",
        "date_added"
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="icp_export_{payload.date}.csv"'}
    )
