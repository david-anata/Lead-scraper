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
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
INSTANTLY_CAMPAIGN_ID = os.getenv("INSTANTLY_CAMPAIGN_ID", "")

# ========= ENDPOINTS =========
STORELEADS_URL = "https://storeleads.app/json/api/v1/all/domain"
APOLLO_CONTACTS_SEARCH = "https://api.apollo.io/api/v1/contacts/search"

SLACK_CHAT_POST_MESSAGE = "https://slack.com/api/chat.postMessage"
SLACK_GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
SLACK_COMPLETE_UPLOAD = "https://slack.com/api/files.completeUploadExternal"

# ========= CONFIG =========
REQUEST_TIMEOUT = 60

MAX_PAGES = 10
PAGE_SIZE = 200

MAX_APOLLO_DOMAINS_PER_RUN = 40
APOLLO_SLEEP_SECONDS = 1.2  # stay under ~50 calls/minute

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

MAX_CONTACTS_PER_DOMAIN = 2

TARGET_CAMPAIGN_NAME = "Amazon | DTC Brands | Performance Marketing | Mar 2026"


# ========= REQUEST MODEL =========
class ICPBuildRequest(BaseModel):
    date: str
    max_domains: int = 150


# ========= HELPERS =========
def normalize_domain(domain: str) -> str:
    return (
        str(domain or "")
        .replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def monthly_sales(domain_obj):
    try:
        value = domain_obj.get("estimated_sales")
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def build_storeleads_bq():
    # Mirrors your StoreLeads POST bq exactly.
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
                    "match": "1",  # Shopify
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


def matches_icp(domain_obj) -> bool:
    # StoreLeads already enforced ICP via bq.
    return True


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
                "estimated_sales",
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


# ========= APOLLO CONTACTS =========
def is_personal_email(email: str) -> bool:
    if not email:
        return False

    email = email.strip().lower()

    for prefix in GENERIC_PREFIXES:
        if email.startswith(prefix):
            return False

    return True


def extract_email_from_contact(c: dict) -> str:
    email = (c.get("email") or "").strip().lower()
    return email or ""


def email_matches_store(email: str, store_domain: str) -> bool:
    email_domain = normalize_domain(email.split("@")[-1])
    store_domain = normalize_domain(store_domain)
    # exact match or subdomain of store
    return email_domain == store_domain or email_domain.endswith("." + store_domain)


def org_matches_store(contact: dict, store_domain: str, store_title: str) -> bool:
    store_domain = normalize_domain(store_domain)
    store_title = (store_title or "").lower()

    org_name = (contact.get("organization_name") or "").lower()
    if store_title and store_title.split(" ")[0] in org_name:
        return True

    # if Apollo ever exposes an org domain/website here, check that too
    org_domain = normalize_domain(contact.get("organization_domain", ""))
    if org_domain and (org_domain == store_domain or store_domain in org_domain or org_domain in store_domain):
        return True

    return False


def apollo_contacts_search(domain: str, max_per_domain: int = MAX_CONTACTS_PER_DOMAIN):
    """
    Ask Apollo for contacts for this domain.
    Let Apollo handle ranking; we just require that contacts have emails.
    """
    if not APOLLO_API_KEY:
        print("[Apollo] missing APOLLO_API_KEY, skipping Apollo for this run")
        return []

    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": APOLLO_API_KEY,
    }

    payload = {
        "page": 1,
        "per_page": max_per_domain * 6,  # grab extras so we can filter
        "domain": domain,
        "has_personal_emails": True,
        "has_valid_emails": True,
    }

    try:
        r = requests.post(
            APOLLO_CONTACTS_SEARCH,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        print(f"[Apollo] contacts request error for domain={domain}: {e}")
        return []

    if r.status_code != 200:
        print(f"[Apollo] contacts non-200 for domain={domain}: {r.status_code} {r.text}")
        return []

    data = r.json()
    contacts = data.get("contacts", []) or []

    print(f"[Apollo] raw contacts for domain={domain}: {len(contacts)}")

    # Keep the debug logging for now while we tune.
    if contacts:
        try:
            sample = json.dumps(contacts[0])[:1200]
            print(f"[Apollo debug] domain={domain} sample_contact={sample}")
        except Exception as e:
            print(f"[Apollo debug] error serializing sample contact for {domain}: {e}")

    return contacts


def determine_offer(revenue):
    if revenue and revenue >= 150000:
        return "Fulfillment"
    return "Shipping Optimization"


def build_rows(domains, run_date: str):
    instantly_rows = []
    linkedin_rows = []

    success = 0          # total personal contacts generated
    apollo_hits = 0      # domains where Apollo returned >=1 contact
    seen_emails_global = set()  # de-dupe across the entire run

    max_apollo_domains = min(MAX_APOLLO_DOMAINS_PER_RUN, len(domains))

    for idx, d in enumerate(domains):
        domain = normalize_domain(d.get("name", ""))
        if not domain:
            continue

        if idx >= max_apollo_domains:
            # beyond Apollo budget for this run
            continue

        contacts = apollo_contacts_search(domain, max_per_domain=MAX_CONTACTS_PER_DOMAIN)
        time.sleep(APOLLO_SLEEP_SECONDS)

        if contacts:
            apollo_hits += 1

        revenue = monthly_sales(d)
        offer = determine_offer(revenue)

        accepted_for_domain = 0

        for c in contacts:
            if accepted_for_domain >= MAX_CONTACTS_PER_DOMAIN:
                break

            full_name = (c.get("name") or "").strip()
            email = extract_email_from_contact(c)

            if not email:
                continue
            if not is_personal_email(email):
                continue
            if email in seen_emails_global:
                continue

            # NEW: ensure this contact actually belongs to this store
            store_title = d.get("title", "") or ""
            if not (email_matches_store(email, domain) or org_matches_store(c, domain, store_title)):
                continue

            # accept this contact
            seen_emails_global.add(email)
            accepted_for_domain += 1

            name_parts = full_name.split()
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            linkedin_url = c.get("linkedin_url", "") or ""

            instantly_rows.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email,
                    "role": c.get("title", "") or "",
                    "linkedin_url": linkedin_url,
                    "company_name": store_title,
                    "website": domain,
                    "city": d.get("city", ""),
                    "state": d.get("state", ""),
                    "revenue": revenue,
                    "campaign_name": TARGET_CAMPAIGN_NAME,
                    "campaign_id": INSTANTLY_CAMPAIGN_ID,
                    "custom_offer": offer,
                }
            )

            linkedin_rows.append(
                {
                    "name": full_name,
                    "role": c.get("title", "") or "",
                    "linkedin_url": linkedin_url,
                    "company": store_title,
                    "website": domain,
                    "email": email,
                    "city": d.get("city", ""),
                    "state": d.get("state", ""),
                    "revenue": revenue,
                    "date_added": run_date,
                }
            )

            success += 1

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
    if not APOLLO_API_KEY:
        raise HTTPException(status_code=500, detail="APOLLO_API_KEY missing")
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
