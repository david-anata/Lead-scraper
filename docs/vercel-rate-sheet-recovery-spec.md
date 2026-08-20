# Vercel rate-sheet recovery specification

Status: approved for implementation
Prepared: August 20, 2026

## Outcome

Restore the production FastAPI application so an authenticated operator can open `/admin/fulfillment/sales`, connect a recent rate sheet to a HubSpot deal, and receive a persistent success state or an actionable integration error.

## Verified failure

- The production Rate Sheets route returned Vercel `500 FUNCTION_INVOCATION_FAILED` before FastAPI rendered it.
- Deployment `dpl_BaBMPWU6hbMKjADJ2g2xVX3pKtWx` was marked Ready even though its build warned that it produced no `functions` or `static` output.
- The named-service routing configuration did not emit a deployable Python function.
- The existing rate-sheet handler already supports selected and manually entered HubSpot deal IDs; this recovery does not change that business logic.

## Approved scope

1. Use the Vercel Services FastAPI configuration with `api/index.py` as the explicit service entrypoint.
2. Route all existing public paths to the FastAPI function without changing URLs.
3. Preserve authentication, HubSpot behavior, rate-sheet data, audit history, cron paths, memory, duration, and region settings.
4. Add regression coverage for the deployment boundary.
5. Verify a preview build, then deploy and verify the exact production commit and custom domain.

## Non-goals

- Rate-sheet UI redesign.
- HubSpot matching or business-rule changes.
- Data migrations or provider changes.
- Changes to unrelated admin workflows.

## Acceptance criteria

1. Deployment metadata reports one Python runtime and the service routes requests through `api/index.py`. The generic no-output warning is not a release blocker for a verified Vercel Services deployment.
2. Health and readiness respond with their intended status codes.
3. Authenticated `/admin` and `/admin/fulfillment/sales` requests no longer return a Vercel function failure.
4. `agent.anatainc.com` serves the promoted commit from the intended Vercel project.
5. Selecting or manually entering a valid HubSpot deal persists after reload and records audit history.
6. A HubSpot integration failure is actionable and does not crash the page or erase the rate-sheet edit.
7. Failed build, readiness, authentication, or representative-route checks block promotion or trigger rollback.

## Validation and rollback

- Run the deployment-config regression tests plus existing health and fulfillment tests.
- Inspect the preview build output and test health, login, admin, and Rate Sheets routes.
- Promote only the verified artifact; do not rebuild a different artifact for production.
- After promotion, verify desktop and phone layouts and scan production runtime errors.
- If health, authentication, or Rate Sheets fails, restore the prior known-good production alias. Configuration changes are backward-compatible and do not require data rollback.

## Definition of done

The exact verified production commit is served by `agent.anatainc.com`, the authenticated Rate Sheets page renders, and a safe test record can complete the HubSpot connection workflow with persistent state and audit evidence.
