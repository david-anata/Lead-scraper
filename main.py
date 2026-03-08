from fastapi import FastAPI, Header, HTTPException
import os

app = FastAPI()

API_SECRET = os.getenv("GPT_ACTION_SECRET")

@app.get("/")
def home():
    return {"status": "Agent server running"}

@app.post("/run-icp-build")
def run_icp_build(authorization: str | None = Header(default=None)):

    if authorization != f"Bearer {API_SECRET}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Placeholder pipeline
    stores_processed = 0
    amazon_tier_a = 0
    amazon_tier_b = 0
    contacts_enriched = 0

    return {
        "stores_processed": stores_processed,
        "amazon_tier_a": amazon_tier_a,
        "amazon_tier_b": amazon_tier_b,
        "contacts_enriched": contacts_enriched,
        "sheet_name": "",
        "sheet_url": "",
        "limit_hit": False,
        "limit_reason": ""
    }
