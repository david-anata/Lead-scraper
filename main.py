from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os

app = FastAPI()

API_SECRET = os.getenv("GPT_ACTION_SECRET")


class ICPBuildRequest(BaseModel):
    date: str
    sheet_name: str
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

    return {
        "stores_processed": 0,
        "amazon_tier_a": 0,
        "amazon_tier_b": 0,
        "contacts_enriched": 0,
        "sheet_name": payload.sheet_name,
        "sheet_url": "",
        "limit_hit": False,
        "limit_reason": ""
    }
