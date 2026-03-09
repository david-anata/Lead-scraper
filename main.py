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

STORELEADS_API_KEY = os.getenv("STORELEADS_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
HUNTER_DOMAIN_SEARCH = "https://api.hunter.io/v2/domain-search"
HUNTER_VERIFY = "https://api.hunter.io/v2/email-verifier"

SLACK_CHAT_POST_MESSAGE = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD = "https://slack.com/api/files.completeUploadExternal"

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
    "modalyst",
]

REJECT_EMAIL_PREFIX = (
    "info@",
    "admin@",
    "noreply@",
    "no-reply@",
)

FALLBACK_EMAIL_PREFIX = (
    "hello@",
    "support@",
    "contact@",
    "team@",
)


class ICPBuildRequest(BaseModel):
    date: str
    max_stores: int = 25
    first_page_only: bool = False
    output_type: str = "master"  # "master" or "instantly"


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
                    "match": "1",
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


def fetch_storeleads_page(page_number):
    headers = {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "page": page_number,
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
                "description",
                "sales_channels",
                "shipping_carriers",
                "technologies",
                "tags",
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
                "api_key": HUNTER_IO_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
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
                "api_key": HUNTER_IO_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
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
            "confidence": confidence,
        }

        if email.startswith(FALLBACK_EMAIL_PREFIX):
            valid_fallback.append(record)
        else:
            valid_person.append(record)

    valid_person.sort(key=lambda x: x["confidence"], reverse=True)
    valid_fallback.sort(key=lambda x: x["confidence"], reverse=True)

    return (valid_person + valid_fallback)[:3]


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

        master_rows.append(
            {
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
                "date_added": run_date,
            }
        )

        send_key = f"{domain}|{primary['email']}"
        if send_key not in seen_sendable:
            seen_sendable.add(send_key)
            instantly_rows.append(
                {
                    "email": primary["email"],
                    "first_name": primary["name"].split(" ")[0] if primary["name"] else "",
                    "last_name": " ".join(primary["name"].split(" ")[1:]) if primary["name"] else "",
                    "company_name": d.get("title", ""),
                    "website": domain,
                    "custom_primary_offer": primary_offer,
                    "custom_city": d.get("city", ""),
                    "custom_state": d.get("state", ""),
                    "custom_revenue": revenue,
                    "custom_linkedin_url": primary["linkedin"],
                }
            )

        time.sleep(0.2)

    return master_rows, instantly_rows


def rows_to_csv(rows, fieldnames):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def slack_headers():
    if not SLACK_BOT_TOKEN:
        raise HTTPException(status_code=500, detail="Missing SLACK_BOT_TOKEN")
    return {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}


def post_summary_to_slack(master_count, instantly_count):
    if not SLACK_CHANNEL_ID:
        raise HTTPException(status_code=500, detail="Missing SLACK_CHANNEL_ID")

    message = (
        f"Today's lead build is ready.\n\n"
        f"I found {master_count} qualified leads and {instantly_count} campaign-ready contacts.\n"
        f"The CSV files are posted below.\n"
        f"Use the Instantly file for outreach and the master file for reference."
    )

    r = requests.post(
        SLACK_CHAT_POST_MESSAGE,
        headers={**slack_headers(), "Content-Type": "application/json; charset=utf-8"},
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": message,
        },
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=500, detail=f"Slack chat.postMessage failed: {data.get('error')}")
    return data


def upload_file_to_slack(filename, file_content, title=None):
    if not SLACK_CHANNEL_ID:
        raise HTTPException(status_code=500, detail="Missing SLACK_CHANNEL_ID")

    content_bytes = file_content.encode("utf-8")

    # Step 1: get upload URL from Slack
    step1 = requests.post(
        SLACK_GET_UPLOAD_URL,
        headers=slack_headers(),
        data={
            "filename": filename,
            "length": str(len(content_bytes)),
        },
        timeout=REQUEST_TIMEOUT,
    )
    step1.raise_for_status()
    step1_data = step1.json()
    if not step1_data.get("ok"):
        raise HTTPException(status_code=500, detail=f"Slack getUploadURLExternal failed: {step1_data.get('error')}")

    upload_url = step1_data["upload_url"]
    file_id = step1_data["file_id"]

    # Step 2: upload the raw file bytes
    upload_resp = requests.post(
        upload_url,
        data=content_bytes,
        headers={"Content-Type": "text/csv"},
        timeout=REQUEST_TIMEOUT,
    )
    upload_resp.raise_for_status()

    # Step 3: complete upload and share to channel
    step3 = requests.post(
        SLACK_COMPLETE_UPLOAD,
        headers=slack_headers(),
        data={
            "files": json.dumps([{"id": file_id, "title": title or filename}]),
            "channel_id": SLACK_CHANNEL_ID,
        },
        timeout=REQUEST_TIMEOUT,
    )
    step3.raise_for_status()
    step3_data = step3.json()
    if not step3_data.get("ok"):
        raise HTTPException(status_code=500, detail=f"Slack completeUploadExternal failed: {step3_data.get('error')}")

    return step3_data


@app.get("/")
def home():
    return {"status": "agent running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):
    try:
        if not STORELEADS_API_KEY:
            raise HTTPException(status_code=500, detail="Missing STORELEADS_API_KEY")
        if not HUNTER_IO_API_KEY:
            raise HTTPException(status_code=500, detail="Missing HUNTER_IO_API_KEY")
        if not SLACK_BOT_TOKEN:
            raise HTTPException(status_code=500, detail="Missing SLACK_BOT_TOKEN")
        if not SLACK_CHANNEL_ID:
            raise HTTPException(status_code=500, detail="Missing SLACK_CHANNEL_ID")

        domains = collect_domains(
            max_stores=payload.max_stores,
            first_page_only=payload.first_page_only,
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
            "date_added",
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
            "custom_linkedin_url",
        ]

        master_csv = rows_to_csv(master_rows, master_fields)
        instantly_csv = rows_to_csv(instantly_rows, instantly_fields)

        master_name = f"master_{payload.date}.csv"
        instantly_name = f"instantly_upload_{payload.date}.csv"

        # Upload files first
        upload_file_to_slack(master_name, master_csv, title=f"Master Lead File {payload.date}")
        upload_file_to_slack(instantly_name, instantly_csv, title=f"Instantly Upload File {payload.date}")

        # Then post the summary message
        post_summary_to_slack(len(master_rows), len(instantly_rows))

        if payload.output_type == "instantly":
            return StreamingResponse(
                iter([instantly_csv]),
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{instantly_name}"'},
            )

        return StreamingResponse(
            iter([master_csv]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{master_name}"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error_type": type(e).__name__, "detail": str(e)},
        )
