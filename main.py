from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import csv
import io
import os
import json
import time
import requests

app = FastAPI()

STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

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
    except Exception:
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
        except Exception:
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

        if r.status_code in [403, 429]:
            return False

        r.raise_for_status()

        result = r.json().get("data", {}).get("result", "")
        return result in ["deliverable", "risky"]

    except Exception:
        return False


def pick_emails(domain, contacts):
    valid_person = []
    valid_fallback = []

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

        if not validate_email(email):
            continue

        record = {
            "email": email,
            "name": (first_name + " " + last_name).strip(),
            "title": title,
            "linkedin": linkedin,
            "confidence": confidence
        }

        if email.startswith(FALLBACK_EMAIL_PREFIX):
            valid_fallback.append(record)
        else:
            valid_person.append(record)

    valid_person.sort(key=lambda x: x["confidence"], reverse=True)
    valid_fallback.sort(key=lambda x: x["confidence"], reverse=True)

    all_valid = valid_person + valid_fallback
    return all_valid[:3]


def determine_primary_offer(revenue):
    if revenue and revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


def build_rows(domains, run_date):
    master_rows = []
    instantly_rows = []
    seen_sendable = set()

    for d in domains:
        domain = normalize_domain(d.get("name", ""))
        contacts = hunter_search(domain)
        valid_contacts = pick_emails(domain, contacts)

        if not valid_contacts:
            continue

        revenue = monthly_sales(d)
        primary_offer = determine_primary_offer(revenue)
        tech_stack = ",".join(get_technologies(d))

        primary = valid_contacts[0]
        email_2 = valid_contacts[1]["email"] if len(valid_contacts) > 1 else ""
        email_3 = valid_contacts[2]["email"] if len(valid_contacts) > 2 else ""

        master_rows.append({
            "domain": domain,
            "brand_name": d.get("title", ""),
            "state": d.get("state", ""),
            "city": d.get("city", ""),
            "revenue": revenue,
            "employees": d.get("employee_count", ""),
            "tech_stack": tech_stack,
            "contact_name": primary["name"],
            "contact_title": primary["title"],
            "primary_email": primary["email"],
            "email_2": email_2,
            "email_3": email_3,
            "linkedin_url": primary["linkedin"],
            "primary_offer": primary_offer,
            "date_added": run_date
        })

        send_key = f"{domain}|{primary['email']}"
        if send_key not in seen_sendable:
            seen_sendable.add(send_key)
            instantly_rows.append({
                "email": primary["email"],
                "first_name": primary["name"].split(" ")[0] if primary["name"] else "",
                "last_name": " ".join(primary["name"].split(" ")[1:]) if primary["name"] else "",
                "company_name": d.get("title", ""),
                "website": domain,
                "custom_primary_offer": primary_offer,
                "custom_city": d.get("city", ""),
                "custom_state": d.get("state", ""),
                "custom_revenue": revenue,
                "custom_linkedin_url": primary["linkedin"]
            })

        time.sleep(0.2)

    return master_rows, instantly_rows


def rows_to_csv(rows, fieldnames):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise HTTPException(status_code=500, detail="Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)


def upload_csv_to_drive(filename, csv_text):
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise HTTPException(status_code=500, detail="Missing GOOGLE_DRIVE_FOLDER_ID")

    service = get_drive_service()

    file_metadata = {
        "name": filename,
        "parents": [GOOGLE_DRIVE_FOLDER_ID]
    }

    media = MediaIoBaseUpload(
        io.BytesIO(csv_text.encode("utf-8")),
        mimetype="text/csv",
        resumable=False
    )

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink"
    ).execute()

    return created


def post_to_slack(message):
    if not SLACK_WEBHOOK_URL:
        return

    try:
        requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=REQUEST_TIMEOUT
        )
    except Exception:
        pass


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

    master_rows, instantly_rows = build_rows(domains, payload.date)

    master_fields = [
        "domain",
        "brand_name",
        "state",
        "city",
        "revenue",
        "employees",
        "tech_stack",
        "contact_name",
        "contact_title",
        "primary_email",
        "email_2",
        "email_3",
        "linkedin_url",
        "primary_offer",
        "date_added"
    ]

    instantly_fields = [
        "email",
        "first_name",
        "last_name",
        "company_name",
        "website",
        "custom_primary_offer",
        "custom_city",
        "custom_state",
        "custom_revenue",
        "custom_linkedin_url"
    ]

    master_csv = rows_to_csv(master_rows, master_fields)
    instantly_csv = rows_to_csv(instantly_rows, instantly_fields)

    master_name = f"master_{payload.date}.csv"
    instantly_name = f"instantly_upload_{payload.date}.csv"

    master_file = upload_csv_to_drive(master_name, master_csv)
    instantly_file = upload_csv_to_drive(instantly_name, instantly_csv)

    slack_message = (
        f"Lead files ready for {payload.date}\n"
        f"Master CSV: {master_file.get('webViewLink', '')}\n"
        f"Instantly CSV: {instantly_file.get('webViewLink', '')}\n"
        f"Master rows: {len(master_rows)}\n"
        f"Instantly rows: {len(instantly_rows)}"
    )
    post_to_slack(slack_message)

    return JSONResponse({
        "status": "success",
        "master_rows": len(master_rows),
        "instantly_rows": len(instantly_rows),
        "master_file": master_file,
        "instantly_file": instantly_file
    })
