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
HUNTER_DOMAIN_SEARCH = "https://api.hunter.io/v2/domain-search"

SLACK_CHAT_POST_MESSAGE = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD = "https://slack.com/api/files.completeUploadExternal"

# ========= CONFIG =========
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
        print("[Apollo] missing APOLLO_API_KEY, skipping Apollo for this run")
        return []

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": APOLLO_API_KEY,
        "Accept": "application/json",
        "Cache-Control": "no-cache",
    }

    payload = {
        "q_organization_domains": [domain],
        "page": 1,
        "per_page": 5,
    }

    try:
        r = requests.post(
            APOLLO_PEOPLE_SEARCH,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"[Apollo] request error for domain={domain}: {e}")
        return []

    if r.status_code != 200:
        print(f"[Apollo] non-200 for domain={domain}: {r.status_code} {r.text}")
        return []

    data = r.json()
    people = data.get("people", []) or []
    if not people:
        print(f"[Apollo] no people for domain={domain}")
    return people


def validate_email(email: str) -> bool:
    try:
        if not HUNTER_IO_API_KEY:
            raise HTTPException(status_code=500, detail="HUNTER_IO_API_KEY missing")

        r = requests.get(
            HUNTER_VERIFY,
            params={
                "email": email,
                "api_key": HUNTER_IO_API_KEY,
            },
            timeout=REQUEST_TIMEOUT,
        )

        if r.status_code in [403, 429]:
            print(f"[Hunter] rate/permission issue for {email}: {r.status_code}")
            return False

        r.raise_for_status()

        result = r.json().get("data", {}).get("result", "")
        return result in ["deliverable", "risky"]

    except HTTPException:
        raise
    except Exception as e:
        print(f"[Hunter] validate_email error for {email}: {e}")
        return False


def is_personal_email(email: str) -> bool:
    if not email:
        return False

    email = email.strip().lower()

    for prefix in GENERIC_PREFIXES:
        if email.startswith(prefix):
            return False

    return True


def hunter_search(domain: str):
    try:
        if not HUNTER_IO_API_KEY:
            print("[Hunter] missing HUNTER_IO_API_KEY, skipping domain search")
            return []

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
            print(f"[Hunter] domain search rate/permission issue for {domain}: {r.status_code}")
            return []

        r.raise_for_status()
        return r.json().get("data", {}).get("emails", [])
    except Exception as e:
        print(f"[Hunter] domain search error for {domain}: {e}")
        return []


def pick_emails_from_hunter(domain: str, contacts):
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

    return (valid_person + valid_fallback)[:1]  # we only need one contact per domain


def determine_offer(revenue):
    if revenue and revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


# ========= BUILD ROWS WITH APOLLO + HUNTER FALLBACK =========
def build_rows(domains, run_date: str):
    instantly_rows = []
    linkedin_rows = []

    success = 0
    apollo_hits = 0

    for d in domains:
        domain = normalize_domain(d.get("name", ""))
        if not domain:
            continue

        contact = None

        # 1) Try Apollo first
        people = apollo_people_search(domain)

        if people:
            apollo_hits += 1

            for p in people:
                email = (p.get("email") or "").strip().lower()

                if not is_personal_email(email):
                    continue

                if not validate_email(email):
                    continue

                contact = {
                    "name": p.get("name", "") or "",
                    "title": p.get("title", "") or "",
                    "email": email,
                    "linkedin": p.get("linkedin_url", "") or "",
                    "source": "apollo",
                }
                break

        # 2) If Apollo gave nothing usable, fall back to Hunter
        if not contact:
            hunter_contacts = hunter_search(domain)
            picked = pick_emails_from_hunter(domain, hunter_contacts)

            if picked:
                h = picked[0]
                contact = {
                    "name": h["name"],
                    "title": h["title"],
                    "email": h["email"],
                    "linkedin": h["linkedin"],
                    "source": "hunter",
                }

        if not contact:
            continue

        revenue = monthly_sales(d)
        offer = determine_offer(revenue)

        full_name = contact["name"].strip()
        name_parts = full_name.split()
        first_name = name_parts[0] if name_parts else ""
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        instantly_rows.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "email": contact["email"],
                "role": contact["title"],
                "linkedin_url": contact["linkedin"],
                "company_name": d.get("title", ""),
                "website": domain,
                "city": d.get("city", ""),
                "state": d.get("state", ""),
                "revenue": revenue,
                "campaign_name": TARGET_CAMPAIGN_NAME,
                "campaign_id": INSTANTLY_CAMPAIGN_ID,
                "custom_offer": offer,
                "source": contact["source"],
            }
        )

        linkedin_rows.append(
            {
                "name": contact["name"],
                "role": contact["title"],
                "linkedin_url": contact["linkedin"],
                "company": d.get("title", ""),
                "website": domain,
                "email": contact["email"],
                "city": d.get("city", ""),
                "state": d.get("state", ""),
                "revenue": revenue,
                "date_added": run_date,
                "source": contact["source"],
            }
        )

        success += 1
        time.sleep(0.2)

    return instantly_rows, linkedin_rows, success, apollo_hits


def rows_to_csv(rows):
    if not rows:
        return ""

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


# ========= SLACK =========
def slack_headers():
    if not SLACK_BOT_TOKEN:
        raise HTTPException(status_code=500, detail="SLACK_BOT_TOKEN missing")
    return {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}


def upload_file(filename: str, content: str):
    if not content:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    content_bytes = content.encode("utf-8")
    if len(content_bytes) <= 1:
        return {"ok": True, "skipped": True, "reason": "empty_file"}

    step1_resp = requests.post(
        SLACK_GET_UPLOAD_URL,
        headers=slack_headers(),
        data={
            "filename": filename,
            "length": str(len(content_bytes)),
        },
        timeout=REQUEST_TIMEOUT,
    )

    step1_resp.raise_for_status()
    step1 = step1_resp.json()

    if not step1.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack getUploadURLExternal failed: {step1}",
        )

    upload_url = step1.get("upload_url")
    file_id = step1.get("file_id")

    if not upload_url or not file_id:
        raise HTTPException(
            status_code=500,
            detail=f"Slack upload URL missing from response: {step1}",
        )

    upload_resp = requests.post(
        upload_url,
        data=content_bytes,
        headers={"Content-Type": "application/octet-stream"},
        timeout=REQUEST_TIMEOUT,
    )
    upload_resp.raise_for_status()

    step3_resp = requests.post(
        SLACK_COMPLETE_UPLOAD,
        headers=slack_headers(),
        data={
            "files": json.dumps([{"id": file_id, "title": filename}]),
            "channel_id": SLACK_CHANNEL_ID,
        },
        timeout=REQUEST_TIMEOUT,
    )

    step3_resp.raise_for_status()
    step3 = step3_resp.json()

    if not step3.get("ok"):
        raise HTTPException(
            status_code=500,
            detail=f"Slack completeUploadExternal failed: {step3}",
        )

    return step3


def slack_summary(raw_scanned: int, qualified_domains: int, apollo_hits: int, success: int):
    contact_hit_rate = round((success / qualified_domains) * 100, 2) if qualified_domains else 0
    pipeline_success_rate = round((success / raw_scanned) * 100, 2) if raw_scanned else 0

    text = f"""@channel

Lead build completed.

Domains scanned: {raw_scanned}
ICP matches: {qualified_domains}
Apollo contacts found: {apollo_hits}
Personal contacts found: {success}

Contact hit rate: {contact_hit_rate}%
Pipeline success rate: {pipeline_success_rate}%

Files attached below.
"""

    r = requests.post(
        SLACK_CHAT_POST_MESSAGE,
        headers={**slack_headers(), "Content-Type": "application/json"},
        json={
            "channel": SLACK_CHANNEL_ID,
            "text": text,
        },
        timeout=REQUEST_TIMEOUT,
    )

    r.raise_for_status()


# ========= ROUTES =========
@app.get("/")
def home():
    return {"status": "lead engine running"}


@app.post("/run-lead-build")
def run(payload: ICPBuildRequest):
    if not STORELEADS_API_KEY:
        raise HTTPException(status_code=500, detail="STORELEADS_API_KEY missing")
    if not HUNTER_IO_API_KEY:
        raise HTTPException(status_code=500, detail="HUNTER_IO_API_KEY missing")
    if not SLACK_BOT_TOKEN:
        raise HTTPException(status_code=500, detail="SLACK_BOT_TOKEN missing")
    if not SLACK_CHANNEL_ID:
        raise HTTPException(status_code=500, detail="SLACK_CHANNEL_ID missing")

    try:
        domains, raw_scanned = collect_domains(payload.max_domains)

        print(
            f"[Run] date={payload.date} max_domains={payload.max_domains} "
            f"raw_scanned={raw_scanned} icp_matches={len(domains)}"
        )

        instantly_rows, linkedin_rows, success, apollo_hits = build_rows(domains, payload.date)

        instantly_csv = rows_to_csv(instantly_rows)
        linkedin_csv = rows_to_csv(linkedin_rows)

        if instantly_rows:
            upload_file(f"instantly_upload_{payload.date}.csv", instantly_csv)

        if linkedin_rows:
            upload_file(f"linkedin_targets_{payload.date}.csv", linkedin_csv)

        slack_summary(raw_scanned, len(domains), apollo_hits, success)

        if not instantly_rows:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "ok",
                    "message": "No valid personal contacts found for this run.",
                    "domains_scanned": raw_scanned,
                    "icp_matches": len(domains),
                    "apollo_contacts_found": apollo_hits,
                    "personal_contacts_found": success,
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
    except Exception as e:
        print(f"[Run] unexpected error: {type(e).__name__} {e}")
        return JSONResponse(
            status_code=500,
            content={"error_type": type(e).__name__, "detail": str(e)},
        )
