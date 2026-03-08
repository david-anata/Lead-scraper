from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
import os
import csv
import io

app = FastAPI()

API_SECRET = os.getenv("GPT_ACTION_SECRET")


class ICPBuildRequest(BaseModel):
    date: str
    sheet_name: str = "ICP Export"
    max_stores: int = 100
    first_page_only: bool = True


@app.get("/")
def home():
    return {"status": "Agent server running"}


@app.post("/run-icp-build")
def run_icp_build(
    payload: ICPBuildRequest,
    authorization: str | None = Header(default=None)
):
    if API_SECRET and authorization != f"Bearer {API_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Placeholder sample data for now
    rows = [
        {
            "domain": "brandone.com",
            "brand_name": "Brand One",
            "country": "US",
            "amazon_tier": "A",
            "decision_maker_name": "Jane Smith",
            "decision_maker_title": "Founder",
            "decision_maker_email": "jane@brandone.com",
            "date_added": payload.date
        },
        {
            "domain": "brandone.com",
            "brand_name": "Brand One",
            "country": "US",
            "amazon_tier": "A",
            "decision_maker_name": "Jane Smith",
            "decision_maker_title": "Founder",
            "decision_maker_email": "jane@brandone.com",
            "date_added": payload.date
        },
        {
            "domain": "brandtwo.com",
            "brand_name": "Brand Two",
            "country": "US",
            "amazon_tier": "B",
            "decision_maker_name": "Mike Lee",
            "decision_maker_title": "Head of Ecommerce",
            "decision_maker_email": "mike@brandtwo.com",
            "date_added": payload.date
        }
    ]

    # Deduplicate by domain + email
    seen = set()
    deduped_rows = []
    for row in rows:
        email = row.get("decision_maker_email", "").strip().lower()
        domain = row.get("domain", "").strip().lower()

        if not email or not domain:
            continue

        key = f"{domain}|{email}"
        if key in seen:
            continue

        seen.add(key)
        deduped_rows.append(row)

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "domain",
            "brand_name",
            "country",
            "amazon_tier",
            "decision_maker_name",
            "decision_maker_title",
            "decision_maker_email",
            "date_added"
        ]
    )
    writer.writeheader()
    writer.writerows(deduped_rows)
    output.seek(0)

    filename = f"icp_export_{payload.date}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
