# Anata Agent

Anata Agent is Anata's internal operating workspace. It combines Sales, Website Ops,
Finance, Advertising, Executive, Fulfillment, HR, Building, and outbound operations in
one authenticated application.

## Outbound system

The supported lead workflow is:

1. StoreLeads supplies candidate brands.
2. Agent applies recipes, ICP qualification, scoring, and never-contact-twice memory.
3. Agent records Amazon findings and prepares approved brands.
4. Clay enriches and verifies contacts.
5. Instantly and HeyReach deliver approved outreach.

The scheduled entry point is `POST /api/jobs/outbound-morning/run`. Lead Ops pages and
APIs live under `/admin/outbound` and `/admin/api/outbound`.

## Run locally

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Set the environment variables required for the sections you use. The primary database is
configured with `SALES_AGENT_DB_URL`; outbound sourcing requires `STORELEADS_API_KEY`.
See `sales_support_agent/README.md` and the documents under `docs/` for section-specific
configuration and operating procedures.

## Verification

```bash
python -m pytest
```

Production runs on Vercel. Follow `docs/vercel-cutover-rollback-runbook.md` and the
repository deployment rules for releases and rollback.
