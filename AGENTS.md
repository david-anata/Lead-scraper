# AGENTS.md

## Project Mission

This repo now supports two related operational tools:

1. the original outbound lead-scraper service
2. the ClickUp sales support agent that keeps existing leads moving after creation

The sales support agent powers an internal sales support automation flow that ensures Account Executives follow up on time, maintain CRM hygiene in ClickUp, and do not let opportunities go stale.

## Product Priorities

1. Reliability over cleverness
2. Clear auditability of automated actions
3. CRM hygiene and data completeness
4. Actionable notifications for the AE
5. Modular integrations and simple deployment

## Current File Structure

- `main.py`: original single-file lead-scraper FastAPI app
- `sales_support_agent/`: modular sales support agent app
- `requirements.txt`: shared Python dependencies
- `README.md`: repo-level instructions
- `sales_support_agent/README.md`: sales support agent setup and architecture
- `AGENTS.md`: guidance for future coding agents

## How To Run The API

From the project directory:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Lead-scraper routes:

- `GET /`
- `GET /health`
- `POST /run-lead-build`

Sales support agent:

```bash
cd /Users/davidnarayan/Documents/Playground/Lead-scraper
uvicorn sales_support_agent.main:app --host 0.0.0.0 --port 8010 --reload
```

## Required Environment Variables

Lead-scraper required variables:

- `STORELEADS_API_KEY`
- `APOLLO_API_KEY`
- `SLACK_BOT_TOKEN`
- `SLACK_CHANNEL_ID`

Sales support agent required variables:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`

Common optional variables:

- `INSTANTLY_CAMPAIGN_ID`
- `SALES_AGENT_INTERNAL_API_KEY`
- `SLACK_AE_MAP_JSON`

Each app should fail clearly when its own required variables are missing.

## Change Guardrails

Preserve business logic unless the user explicitly asks to change it.

That includes:

- the current lead selection flow
- StoreLeads query behavior
- Apollo filtering behavior
- CSV output shape
- Slack upload and summary behavior
- route paths and response behavior

For the sales support agent:

- Use Python and FastAPI
- Prefer deterministic business rules for follow-up logic
- Use AI only for summarization and recommendation layers
- Keep integrations isolated behind service classes
- Log all external writes
- Never silently update CRM records without an audit trail
- Add docstrings and type hints
- Keep env vars in `.env`
- Use background jobs or scheduled triggers for recurring checks
- Do not introduce unnecessary framework complexity

## Business Logic

- ClickUp is the source of truth for lead records and statuses
- The agent works only on existing ClickUp tasks after they are created upstream
- “Meaningful touch” includes outbound email, inbound email, completed call, completed meeting, or logged note
- “Follow-up due” means no meaningful touch within the configured status threshold
- “Overdue” means the lead has passed the due threshold and still lacks the next step
- Slack alerts should include owner, lead name, current status, last touch, and recommended next action
- Exclude `LOST`, `LOST - NOT QUALIFIED`, and `WON - CANCELED` from follow-up enforcement

## Refactor Guidance

Future refactors should be incremental.

Preferred approach:

1. Keep changes small and easy to review.
2. Preserve endpoint contracts and output behavior unless explicitly asked otherwise.
3. Do not refactor the lead-scraper into the sales support app unless explicitly asked.
4. Verify syntax and affected behavior after edits.
5. Document new operational behavior in `README.md` when relevant.

## Expected Outputs

When making changes:

1. Explain assumptions
2. List affected files
3. Implement cleanly
4. Provide validation steps
5. Suggest next phase improvements
