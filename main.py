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

REQUEST_TIMEOUT = 60

# SMB-focused ICP
MIN_MONTHLY_REVENUE = 10000
MAX_MONTHLY_REVENUE = 300000
MAX_EMPLOYEES = 25

# Search controls
MAX_STORELEADS_PAGES = 10
STORELEADS_PAGE_SIZE = 100

# Tech OR search for self-shipping / shipping workflow signals
TECH_MATCH = "Aftership ShipStation Easyship Pirate Ship Shippo ShippingEasy ShipHero"

AMAZON_SIGNAL_KEYWORDS = [
    "amazon",
    "marketplace connect",
    "codisto",
    "cedcommerce",
    "buy with prime",
    "amazon mcf",
]

CONTACT_TITLE_PRIORITY = [
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

GENERIC_PREFIXES = ("info@", "hello@", "support@", "contact@", "admin@")


class ICPBuildRequest(BaseModel):
    date: str
    sheet_name: str = "ICP Export"
    max_stores: int = 100
    first_page_only: bool = False


def storeleads_headers() -> Dict[str, str]:
    if not STORELEADS_API_KEY:
        raise HTTPException(status_code=500, detail="Missing STORELEADS_API_KEY")
    return {
        "Authorization": f"Bearer {STORELEADS_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def hunter_headers() -> Dict[str, str]:
    if not HUNTER_IO_API_KEY:
        raise HTTPException(status_code=500, detail="Missing HUNTER_IO_API_KEY")
    return {
        "X-API-KEY": HUNTER_IO_API_KEY,
        "Accept": "application/json",
    }


def safe_get(d: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def normalize_domain(value: str) -> str:
    return (
        value.replace("https://", "")
        .replace("http://", "")
        .replace("www.", "")
        .strip("/")
        .lower()
    )


def get_tech_names(domain: Dict[str, Any]) -> List[str]:
    techs = domain.get("technologies") or []
    names: List[str] = []

    for t in techs:
        if isinstance(t, dict):
            name = t.get("name")
            if name:
                names.append(str(name))
        elif isinstance(t, str):
            names.append(str(t))

    return names


def infer_amazon_tier(domain: Dict[str, Any]) -> Tuple[str, bool]:
    sales_channels = domain.get("sales_channels") or []
    tech_names = [t.lower() for t in get_tech_names(domain)]
    description = str(domain.get("description") or "").lower()

    if any(str(ch).lower() == "amazon" for ch in sales_channels):
        return "A", False

    for tech in tech_names:
        if any(sig in tech for sig in AMAZON_SIGNAL_KEYWORDS):
            return "A", False

    if "amazon" in description:
        return "A", True

    return "B", True


def monthly_sales_usd(domain: Dict[str, Any]) -> Optional[float]:
    value = safe_get(domain, "estimated_sales")
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def matches_icp(domain: Dict[str, Any]) -> bool:
    platform = str(safe_get(domain, "platform") or "").lower()
    country = str(safe_get(domain, "country_code", "country") or "").upper()
    employees = safe_get(domain, "employee_count")
    sales = monthly_sales_usd(domain)

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

    if sales is None:
        return False

    if sales < MIN_MONTHLY_REVENUE or sales > MAX_MONTHLY_REVENUE:
        return False

    return True


def build_storeleads_bq() -> Dict[str, Any]:
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
                    "min": MIN_MONTHLY_REVENUE,
                    "max": MAX_MONTHLY_REVENUE,
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

    try:
        response = requests.post(
            STORELEADS_BASE,
            headers=storeleads_headers(),
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.HTTPError as e:
        detail = f"StoreLeads error {e.response.status_code}: {e.response.text[:500]}"
        raise HTTPException(status_code=502, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"StoreLeads request failed: {str(e)}")

    data = response.json()
    return data.get("domains") or []


def collect_candidate_domains(max_stores: int, first_page_only: bool) -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
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
    params = {
        "domain": domain,
        "limit": 10,
    }

    try:
        response = requests.get(
            HUNTER_DOMAIN_SEARCH,
            headers=hunter_headers(),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        # Hunter docs: 403 rate limit, 429 usage limit
        if response.status_code in (403, 429):
            return []

        response.raise_for_status()
    except Exception:
        return []

    data = response.json().get("data", {})
    emails = data.get("emails") or []
    return emails


def filter_hunter_contacts(domain: str, contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered = []

    for c in contacts:
        email = str(c.get("value") or c.get("email") or "").strip().lower()
        if not email:
            continue
        if email.startswith(GENERIC_PREFIXES):
            continue
        if not email.endswith("@" + domain):
            continue

        position = str(c.get("position") or "").strip()
        first_name = str(c.get("first_name") or "").strip()
        last_name = str(c.get("last_name") or "").strip()
        linkedin_url = str(c.get("linkedin") or c.get("linkedin_url") or "").strip()

        filtered.append(
            {
                "name": f"{first_name} {last_name}".strip(),
                "title": position,
                "email": email,
                "linkedin_url": linkedin_url,
            }
        )

    def title_rank(item: Dict[str, str]) -> int:
        title = str(item.get("title") or "").lower()
        for i, pref in enumerate(CONTACT_TITLE_PRIORITY):
            if pref in title:
                return i
        return 999

    filtered.sort(key=title_rank)
    return filtered[:2]


def build_rows(domains: List[Dict[str, Any]], run_date: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()

    for d in domains:
        domain_name = normalize_domain(str(safe_get(d, "name") or ""))
        if not domain_name:
            continue

        # Small delay to be polite / avoid bursts
        time.sleep(0.2)

        amazon_tier, amazon_uncertain = infer_amazon_tier(d)
        hunter_contacts = hunter_domain_search(domain_name)
        contacts = filter_hunter_contacts(domain_name, hunter_contacts)

        if contacts:
            for c in contacts:
                email = str(c.get("email") or "").strip().lower()
                dedupe_key = f"{domain_name}|{email}"
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                rows.append(
                    {
                        "domain": domain_name,
                        "brand_name": safe_get(d, "title", "name") or domain_name,
                        "country": safe_get(d, "country_code", "country") or "",
                        "state": d.get("state") or "",
                        "city": d.get("city") or "",
                        "revenue_band": monthly_sales_usd(d) or "",
                        "employee_count": d.get("employee_count") or "",
                        "categories": ", ".join(d.get("tags") or []),
                        "uses_shipstation": True,
                        "amazon_tier": amazon_tier,
                        "amazon_uncertain": amazon_uncertain,
                        "decision_maker_name": c.get("name") or "",
                        "decision_maker_title": c.get("title") or "",
                        "decision_maker_email": email,
                        "decision_maker_linkedin_url": c.get("linkedin_url") or "",
                        "source": "StoreLeads+Hunter",
                        "date_added": run_date,
                    }
                )
        else:
            dedupe_key = f"{domain_name}|no_contact"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            rows.append(
                {
                    "domain": domain_name,
                    "brand_name": safe_get(d, "title", "name") or domain_name,
                    "country": safe_get(d, "country_code", "country") or "",
                    "state": d.get("state") or "",
                    "city": d.get("city") or "",
                    "revenue_band": monthly_sales_usd(d) or "",
                    "employee_count": d.get("employee_count") or "",
                    "categories": ", ".join(d.get("tags") or []),
                    "uses_shipstation": True,
                    "amazon_tier": amazon_tier,
                    "amazon_uncertain": amazon_uncertain,
                    "decision_maker_name": "",
                    "decision_maker_title": "",
                    "decision_maker_email": "",
                    "decision_maker_linkedin_url": "",
                    "source": "StoreLeads_only",
                    "date_added": run_date,
                }
            )

    return rows


def rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    fieldnames = [
        "domain",
        "brand_name",
        "country",
        "state",
        "city",
        "revenue_band",
        "employee_count",
        "categories",
        "uses_shipstation",
        "amazon_tier",
        "amazon_uncertain",
        "decision_maker_name",
        "decision_maker_title",
        "decision_maker_email",
        "decision_maker_linkedin_url",
        "source",
        "date_added",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@app.get("/")
def home():
    return {"status": "Agent server running"}


@app.post("/run-icp-build")
def run_icp_build(payload: ICPBuildRequest):
    candidate_domains = collect_candidate_domains(
        max_stores=payload.max_stores,
        first_page_only=payload.first_page_only
    )

    rows = build_rows(candidate_domains, payload.date)
    csv_text = rows_to_csv(rows)
    filename = f"icp_export_{payload.date}.csv"

    return StreamingResponse(
        iter([csv_text]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
