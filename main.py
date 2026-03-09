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
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
HUNTER_IO_API_KEY = os.getenv("HUNTER_IO_API_KEY")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID")

STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
APOLLO_PEOPLE_SEARCH = "https://api.apollo.io/v1/mixed_people/search"
HUNTER_VERIFY = "https://api.hunter.io/v2/email-verifier"

SLACK_CHAT_POST_MESSAGE = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD = "https://slack.com/api/files.completeUploadExternal"

REQUEST_TIMEOUT = 60

MIN_REVENUE = 20000
MAX_REVENUE = 300000

MAX_PAGES = 10
PAGE_SIZE = 200

GENERIC_PREFIX = (
    "info@","support@","hello@","contact@",
    "admin@","team@","sales@","marketing@"
)


class ICPBuildRequest(BaseModel):
    date: str
    max_domains: int = 150


def normalize_domain(domain):
    return (
        domain.replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def monthly_sales(domain):
    try:
        return float(domain.get("estimated_sales"))
    except:
        return None


def matches_icp(domain):

    if domain.get("platform","").lower() != "shopify":
        return False

    if domain.get("country_code","").upper() != "US":
        return False

    revenue = monthly_sales(domain)

    if revenue is None:
        return False

    if revenue < MIN_REVENUE or revenue > MAX_REVENUE:
        return False

    return True


def fetch_storeleads_page(page):

    headers = {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "page": page,
        "page_size": PAGE_SIZE,
        "fields": "name,title,state,city,estimated_sales"
    }

    r = requests.post(
        STORELEADS_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )

    r.raise_for_status()

    return r.json().get("domains", [])


def collect_domains(max_domains):

    results = []
    seen = set()

    for page in range(MAX_PAGES):

        domains = fetch_storeleads_page(page)

        if not domains:
            break

        for d in domains:

            domain = normalize_domain(d.get("name",""))

            if not domain or domain in seen:
                continue

            seen.add(domain)

            if matches_icp(d):
                results.append(d)

            if len(results) >= max_domains:
                return results

    return results


def apollo_people_search(domain):

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": APOLLO_API_KEY
    }

    payload = {
        "q_organization_domains": [domain],
        "page": 1,
        "per_page": 5
    }

    r = requests.post(
        APOLLO_PEOPLE_SEARCH,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT
    )

    if r.status_code != 200:
        return []

    data = r.json()

    return data.get("people",[])


def is_personal_email(email):

    if not email:
        return False

    email = email.lower()

    for prefix in GENERIC_PREFIX:
        if email.startswith(prefix):
            return False

    return True


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

        result = r.json()["data"]["result"]

        return result in ["deliverable","risky"]

    except:
        return False


def find_contact(domain):

    people = apollo_people_search(domain)

    for p in people:

        email = p.get("email")

        if not is_personal_email(email):
            continue

        if not validate_email(email):
            continue

        return {
            "name": p.get("name",""),
            "title": p.get("title",""),
            "email": email,
            "linkedin": p.get("linkedin_url","")
        }

    return None


def determine_offer(revenue):

    if revenue and revenue > 150000:
        return "Fulfillment"

    return "Shipping Optimization"


def build_rows(domains, run_date):

    instantly_rows = []
    linkedin_rows = []

    success = 0

    for d in domains:

        domain = normalize_domain(d.get("name",""))

        contact = find_contact(domain)

        if not contact:
            continue

        revenue = monthly_sales(d)

        offer = determine_offer(revenue)

        instantly_rows.append({

            "email": contact["email"],
            "first_name": contact["name"].split(" ")[0] if contact["name"] else "",
            "company_name": d.get("title",""),
            "website": domain,
            "custom_offer": offer,
            "custom_city": d.get("city",""),
            "custom_state": d.get("state",""),
            "custom_revenue": revenue

        })

        linkedin_rows.append({

            "name": contact["name"],
            "linkedin_url": contact["linkedin"],
            "company": d.get("title",""),
            "website": domain

        })

        success += 1

        time.sleep(0.2)

    return instantly_rows, linkedin_rows, success


def rows_to_csv(rows):

    if not rows:
        return ""

    output = io.StringIO()

    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))

    writer.writeheader()

    writer.writerows(rows)

    return output.getvalue()


def slack_headers():

    return {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}"
    }


def upload_file(filename, content):

    content_bytes = content.encode()

    step1 = requests.post(
        SLACK_GET_UPLOAD_URL,
        headers=slack_headers(),
        data={
            "filename": filename,
            "length": len(content_bytes)
        }
    ).json()

    upload_url = step1["upload_url"]
    file_id = step1["file_id"]

    requests.post(
        upload_url,
        data=content_bytes,
        headers={"Content-Type":"application/octet-stream"}
    )

    requests.post(
        SLACK_COMPLETE_UPLOAD,
        headers=slack_headers(),
        data={
            "files": json.dumps([{"id":file_id,"title":filename}]),
            "channel_id": SLACK_CHANNEL_ID
        }
    )


def slack_summary(total_domains, success):

    rate = round((success/total_domains)*100,2) if total_domains else 0

    text = f"""@channel

Lead build completed.

Domains scanned: {total_domains}
Personal contacts found: {success}

Success rate: {rate}%

Files attached below.
"""

    requests.post(
        SLACK_CHAT_POST_MESSAGE,
        headers={**slack_headers(),"Content-Type":"application/json"},
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": text
        }
    )


@app.get("/")
def home():
    return {"status":"lead engine running"}


@app.post("/run-lead-build")

def run(payload: ICPBuildRequest):

    if not STORELEADS_API_KEY:
        raise HTTPException(500,"STORELEADS_API_KEY missing")

    domains = collect_domains(payload.max_domains)

    instantly_rows, linkedin_rows, success = build_rows(domains, payload.date)

    instantly_csv = rows_to_csv(instantly_rows)

    linkedin_csv = rows_to_csv(linkedin_rows)

    upload_file(f"instantly_upload_{payload.date}.csv", instantly_csv)

    upload_file(f"linkedin_targets_{payload.date}.csv", linkedin_csv)

    slack_summary(len(domains), success)

    return StreamingResponse(
        iter([instantly_csv]),
        media_type="text/csv",
        headers={"Content-Disposition":f'attachment; filename="instantly_upload_{payload.date}.csv"'}
    )
