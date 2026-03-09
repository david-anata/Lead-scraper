from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import csv
import io
import os
import requests
import time

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

SHIPPING_TECH = [
    "shipstation",
    "shippo",
    "easyship",
    "shippingeasy",
    "shiphero",
    "aftership",
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
    "modalyst"
]

ACCEPT_EMAIL_PREFIX = (
    "hello@",
    "support@",
    "contact@",
    "team@"
)

REJECT_EMAIL_PREFIX = (
    "info@",
    "admin@",
    "noreply@",
    "no-reply@"
)


class ICPBuildRequest(BaseModel):
    date: str
    max_stores: int = 50


def normalize_domain(domain):
    return domain.replace("https://","").replace("http://","").replace("www.","").strip("/").lower()


def get_technologies(domain):
    techs = domain.get("technologies") or []
    names = []

    for t in techs:
        if isinstance(t,dict):
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


def hunter_domain_search(domain):

    try:

        r = requests.get(
            HUNTER_DOMAIN_SEARCH,
            params={
                "domain":domain,
                "limit":10,
                "api_key":HUNTER_IO_API_KEY
            },
            timeout=REQUEST_TIMEOUT
        )

        return r.json()["data"]["emails"]

    except:

        return []


def validate_email(email):

    try:

        r = requests.get(
            HUNTER_VERIFY,
            params={
                "email":email,
                "api_key":HUNTER_IO_API_KEY
            },
            timeout=REQUEST_TIMEOUT
        )

        result = r.json()["data"]["result"]

        return result in ["deliverable","risky"]

    except:

        return False


def pick_contact(domain,contacts):

    for c in contacts:

        email = (c.get("value") or "").lower()
        confidence = c.get("confidence",0)

        if not email.endswith("@"+domain):
            continue

        if email.startswith(REJECT_EMAIL_PREFIX):
            continue

        if confidence < 60:
            continue

        if validate_email(email):

            name = (c.get("first_name","")+" "+c.get("last_name","")).strip()
            title = c.get("position","")

            return name,title,email

    for c in contacts:

        email = (c.get("value") or "").lower()

        if email.startswith(ACCEPT_EMAIL_PREFIX):

            if validate_email(email):

                return "", "", email

    return None,None,None


def matches_icp(domain):

    platform = str(domain.get("platform","")).lower()

    if platform != "shopify":
        return False

    if str(domain.get("country_code","")).upper() != "US":
        return False

    revenue = monthly_sales(domain)

    if revenue is None:
        return False

    if revenue < MIN_REVENUE or revenue > MAX_REVENUE:
        return False

    employees = domain.get("employee_count")

    if employees and int(employees) > MAX_EMPLOYEES:
        return False

    techs = get_technologies(domain)

    if is_dropship(techs):
        return False

    return True


def fetch_domains():

    headers = {
        "Authorization":f"Bearer {STORELEADS_API_KEY}",
        "Content-Type":"application/json"
    }

    payload = {
        "page":0,
        "page_size":100
    }

    r = requests.post(STORELEADS_URL,headers=headers,json=payload)

    return r.json().get("domains",[])


def build_rows(domains,date):

    rows = []

    for d in domains:

        if not matches_icp(d):
            continue

        domain = normalize_domain(d.get("name",""))

        contacts = hunter_domain_search(domain)

        name,title,email = pick_contact(domain,contacts)

        if not email:
            continue

        rows.append({

            "domain":domain,
            "brand_name":d.get("title",""),
            "state":d.get("state",""),
            "city":d.get("city",""),
            "revenue":monthly_sales(d),
            "employees":d.get("employee_count",""),
            "contact_name":name,
            "contact_title":title,
            "contact_email":email,
            "primary_offer":"Shipping Optimization",
            "date_added":date

        })

        time.sleep(.25)

    return rows


@app.get("/")
def home():

    return {"status":"agent running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):

    domains = fetch_domains()

    rows = build_rows(domains,payload.date)

    if not rows:

        rows = []

    output = io.StringIO()

    fieldnames = [
        "domain",
        "brand_name",
        "state",
        "city",
        "revenue",
        "employees",
        "contact_name",
        "contact_title",
        "contact_email",
        "primary_offer",
        "date_added"
    ]

    writer = csv.DictWriter(output,fieldnames=fieldnames)

    writer.writeheader()

    writer.writerows(rows)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition":f'attachment; filename=icp_export_{payload.date}.csv'}
    )
