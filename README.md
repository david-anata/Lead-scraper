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
- `INSTANTLY_API_KEY`: when set with `INSTANTLY_CAMPAIGN_ID`, the app also pushes accepted leads directly into the Instantly campaign API
- `INSTANTLY_AI`: supported as an alias for `INSTANTLY_API_KEY` if your existing deployment already uses that env var name
- `DAILY_NEW_LEAD_LIMIT`: caps how many new leads can be added to Instantly in a single day; recommended production default is `15`
- `ENABLE_WEEKDAY_ONLY_IMPORTS`: when `true`, blocks scheduled imports on Saturday and Sunday
- `STATE_BACKEND`: state storage backend for processed domains and daily import counts; use `github` to avoid Render local-disk resets
- `GITHUB_STATE_TOKEN`: GitHub token with contents write access, required when `STATE_BACKEND=github`
- `GITHUB_STATE_REPO`: repo used for durable state storage, for example `david-anata/Lead-scraper`
- `GITHUB_STATE_BRANCH`: branch used only for state commits; recommended value is `state`
- `GITHUB_STATE_BASE_BRANCH`: branch to copy from if the state branch does not exist yet; usually `main`
- `GITHUB_STATE_PROCESSED_DOMAINS_PATH`: path of the processed-domain CSV on the state branch
- `GITHUB_STATE_DAILY_IMPORTS_PATH`: path of the daily import counter CSV on the state branch
- `PROCESSED_DOMAINS_FILE`: optional override for the temporary processed-domain state file
- `DAILY_IMPORT_LOG_FILE`: optional override for the temporary daily import counter file

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
export INSTANTLY_API_KEY="your-instantly-api-key"
# or, if your deploy already uses this name:
export INSTANTLY_AI="your-instantly-api-key"
export DAILY_NEW_LEAD_LIMIT="15"
export ENABLE_WEEKDAY_ONLY_IMPORTS="true"
export STATE_BACKEND="github"
export GITHUB_STATE_TOKEN="your-github-token"
export GITHUB_STATE_REPO="david-anata/Lead-scraper"
export GITHUB_STATE_BRANCH="state"
export GITHUB_STATE_BASE_BRANCH="main"
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
- posts a Slack summary with scanned domain counts, pacing information, scheduler source, and Instantly import counts

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

## Daily Automation

Recommended production scheduler:

- primary: Render Cron
- backup/manual rerun: GitHub Actions

### Render Cron

Configure Render Cron to send:

- method: `POST`
- URL: `https://agent.anatainc.com/run-lead-build?scheduler_source=render_cron`
- body:

```json
{
  "date": "YYYY-MM-DD",
  "max_domains": 150
}
```

Recommended schedule:

- weekdays at `8:00 AM America/Denver`

The app will still enforce:

- `DAILY_NEW_LEAD_LIMIT=15`
- `ENABLE_WEEKDAY_ONLY_IMPORTS=true`

### Durable State Without Postgres

Render free services do not provide durable local disk for the processed-domain and daily import counters. To keep moving forward through new domains without paying for Postgres, switch the app to GitHub-backed state:

```txt
STATE_BACKEND=github
GITHUB_STATE_TOKEN=your-github-token-with-contents-write-access
GITHUB_STATE_REPO=david-anata/Lead-scraper
GITHUB_STATE_BRANCH=state
GITHUB_STATE_BASE_BRANCH=main
```

Recommended setup:

- create a fine-grained GitHub token with repository contents read/write access
- store state on a dedicated `state` branch so routine state commits do not touch `main`
- let the app auto-create the `state` branch from `main` if it does not exist yet

### GitHub Actions Backup Runner

The repo includes a manual/backup workflow at [`.github/workflows/daily-lead-build.yml`](/Users/davidnarayan/Documents/Playground/Lead-scraper/.github/workflows/daily-lead-build.yml).

Set this GitHub Actions secret exactly:

```txt
LEAD_BUILD_URL=https://lead-scraper-jb3u.onrender.com
```

No quotes, no trailing slash, and no extra spaces.
