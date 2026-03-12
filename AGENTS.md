# AGENTS.md

## Project Purpose

This repository contains a single FastAPI service for outbound lead generation.

The service:

- queries StoreLeads for domains that match a fixed ICP
- enriches those domains with Apollo contact data
- filters for valid personal emails that match the company domain
- generates CSV output for Instantly and LinkedIn workflows
- uploads output files to Slack
- posts a Slack summary after each run

This is currently a single-service, single-file FastAPI project. Treat it as an integration-heavy operational tool, not a generic starter API.

## Current File Structure

At the time of writing, the project is intentionally small:

- `main.py`: FastAPI app entrypoint and all lead-build logic
- `requirements.txt`: Python dependencies
- `README.md`: local setup and usage instructions
- `AGENTS.md`: guidance for future coding agents

Do not assume additional architecture exists. Inspect the repository before proposing abstractions.

## How To Run The API

From the project directory:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Current routes:

- `GET /`
- `GET /health`
- `POST /run-lead-build`

## Required Environment Variables

These variables are required for the service to function:

- `STORELEADS_API_KEY`
- `APOLLO_API_KEY`
- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`

Optional:

- `INSTANTLY_CAMPAIGN_ID`

The application is expected to fail clearly when required variables are missing.

## Change Guardrails

Preserve business logic unless the user explicitly asks to change it.

That includes:

- the current lead selection flow
- StoreLeads query behavior
- Apollo filtering behavior
- CSV output shape
- Slack upload and summary behavior
- route paths and response behavior

If you refactor code, the default expectation is behavior-preserving cleanup only.

## Refactor Guidance

Future refactors should be incremental.

Preferred approach:

1. Keep changes small and easy to review.
2. Preserve endpoint contracts and output behavior unless explicitly asked otherwise.
3. Avoid splitting into multiple files unless the user explicitly asks for it.
4. Verify syntax and affected behavior after edits.
5. Document new operational behavior in `README.md` when relevant.

Do not introduce large architectural changes speculatively. This repo is still in an early, compact form.
