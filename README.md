# Lead Scraper Service

This project is a single-file FastAPI service that builds outbound lead lists for ecommerce brands.

The service:

- queries StoreLeads for domains that match a fixed ICP
- enriches those domains with contact data from Apollo
- filters for personal emails that match the store domain
- generates CSV output for Instantly and LinkedIn workflows
- uploads the generated CSV files to Slack
- posts a summary message to Slack

## Environment Variables

Copy `.env.example` to `.env` or export the variables directly in your shell before starting the API.

Required:

- `STORELEADS_API_KEY`: authenticates requests to StoreLeads when fetching domains that match the ICP
- `APOLLO_API_KEY`: authenticates requests to Apollo when enriching domains with contact data
- `SLACK_BOT_TOKEN`: authenticates Slack API calls for file uploads and summary messages
- `SLACK_CHANNEL_ID`: Slack channel that receives uploaded CSV files and the run summary

Optional:

- `INSTANTLY_CAMPAIGN_ID`: included in the generated Instantly CSV rows as the campaign ID field

If any required variables are missing, the app fails clearly at startup.

## Project Structure

- `main.py`: FastAPI application entrypoint and all service logic
- `requirements.txt`: Python dependencies

## Run Locally

1. Create and activate a virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `.env.example` to `.env` or export the required environment variables.
4. Start the FastAPI server with Uvicorn.

Example:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env

export STORELEADS_API_KEY="your-storeleads-key"
export APOLLO_API_KEY="your-apollo-key"
export SLACK_BOT_TOKEN="your-slack-bot-token"
export SLACK_CHANNEL_ID="your-slack-channel-id"
export INSTANTLY_CAMPAIGN_ID="your-instantly-campaign-id"
```

## Start the FastAPI Server

Run:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Available endpoints:

- `GET /`
- `GET /health`
- `POST /run-lead-build`

## Call `/run-lead-build`

Send a JSON body with:

- `date`: string used in output filenames and LinkedIn rows
- `max_domains`: optional integer, defaults to `150`

Example request:

```bash
curl -X POST "http://127.0.0.1:8000/run-lead-build" \
  -H "Content-Type: application/json" \
  -d '{
    "date": "2026-03-11",
    "max_domains": 150
  }'
```

## Expected Output

Successful run with contacts found:

- returns a CSV response as the HTTP body
- response content type is `text/csv`
- response includes a `Content-Disposition` header with a filename like `instantly_upload_2026-03-11.csv`
- uploads the Instantly CSV to Slack
- uploads the LinkedIn CSV to Slack when LinkedIn rows exist
- posts a Slack summary with scanned domain and contact counts

Successful run with no valid personal contacts found:

- returns JSON like:

```json
{
  "status": "ok",
  "message": "No valid personal contacts found for this run.",
  "domains_scanned": 0,
  "icp_matches": 0,
  "apollo_contacts_found": 0,
  "personal_contacts_found": 0
}
```

Health check:

```bash
curl http://127.0.0.1:8000/health
```

Response:

```json
{
  "status": "ok"
}
```
