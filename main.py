from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import csv
import io
import os
import requests

app = FastAPI()

STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")

STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
HUNTER_DOMAIN_SEARCH = "https://api.hunter.io/v2/domain-search"
HUNTER_VERIFY = "https://api.hunter.io/v2/email-verifier"

MIN_REVENUE = 20000
MAX_REVENUE = 300000
MAX_EMPLOYEES = 25
REQUEST_TIMEOUT = 60

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


class ICPBuildRequest(BaseModel):
    date: str
    max_stores: int = 25


def normalize_domain(domain):
    return domain.replace("https://", "").replace("http://", "").replace("www.", "").strip("/").lower()


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


def fetch_domains():
    headers = {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "page": 0,
        "page_size": 100
    }

    r = requests.post(STORELEADS_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("domains", [])


def hunter_domain_search(domain):
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
        r.raise_for_status()
        return r.json().get("data", {}).get("emails", [])
    except Exception:
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
        r.raise_for_status()
        result = r.json().get("data", {}).get("result", "")
        return result in ["deliverable", "risky"]
    except Exception:
        return False


def build_debug_rows(domains, max_stores):
    rows = []
    count = 0

    for d in domains:
        if not matches_icp(d):
            continue

        domain = normalize_domain(d.get("name", ""))
        hunter_results = hunter_domain_search(domain)

        first_email = ""
        second_email = ""
        validated_first = False

        if len(hunter_results) > 0:
            first_email = (hunter_results[0].get("value") or "").lower()

        if len(hunter_results) > 1:
            second_email = (hunter_results[1].get("value") or "").lower()

        if first_email:
            validated_first = validate_email(first_email)

        rows.append({
            "domain": domain,
            "brand_name": d.get("title", ""),
            "state": d.get("state", ""),
            "city": d.get("city", ""),
            "revenue": monthly_sales(d),
            "employees": d.get("employee_count", ""),
            "tech_stack": ",".join(get_technologies(d)),
            "hunter_email_count": len(hunter_results),
            "first_email": first_email,
            "second_email": second_email,
            "first_email_valid": validated_first,
            "date_added": "debug"
        })

        count += 1
        if count >= max_stores:
            break

    return rows


@app.get("/")
def home():
    return {"status": "agent running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):
    domains = fetch_domains()
    rows = build_debug_rows(domains, payload.max_stores)

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
        "first_email",
        "second_email",
        "first_email_valid",
        "date_added"
    ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename=icp_debug_{payload.date}.csv'}
    )
