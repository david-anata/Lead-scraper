# Vercel environment parity receipt

Reviewed: August 14, 2026

Project: `anata-agent-staging`

This receipt records variable names and configuration intent only. It never records values.

## Verified

- Vercel exposes 152 production-scoped variable names for the staging project;
  names were re-audited after the Supabase cutover rehearsal.
- `SALES_AGENT_DB_URL` points to Supabase's session pooler through the dedicated
  non-owner `agent_app` role.
- The legacy Neon marketplace resource is disconnected from this Vercel
  project. All 16 Neon-provided environment names (`DATABASE_URL`, `PG*`,
  `POSTGRES_*`, and `NEON_PROJECT_ID`) were verified absent before rebuilding
  the final candidate. The Neon resource was retained, not deleted.
- `AGENT_RUNTIME_SCHEMA_MAINTENANCE=false` prevents ordinary Vercel requests
  from attempting owner-only DDL. Schema changes run through the controlled
  pre-deploy migration instead.
- `VERCEL_STAGING` is present.
- `VERCEL_CRON_WRITES_ENABLED` is present and staging writes remain disabled.
- `CRON_SECRET` is present.
- `CLICKUP_API_KEY` satisfies the application's supported `CLICKUP_API_TOKEN` alias.
- `INSTANTLY_AI` satisfies the application's supported `INSTANTLY_API_KEY` alias.
- `ANTHROPIC_API_KEY` is present for configured AI paths; `OPENAI_API_KEY` is optional for the selected provider paths.
- `ADMIN_DASHBOARD_SESSION_SECRET` safely backs the optional dedicated Building campaign-token secret.
- Explicit staging values were added for `QB_REDIRECT_URI`, `PLAID_WEBHOOK_URL`, and `PLAID_REDIRECT_URI` so those flows cannot fall back to production-domain defaults.
- `DECK_PUBLIC_BASE_URL` explicitly uses `https://agent-staging.anatainc.com`,
  so staging-generated deck links do not fall back to the production hostname.
- Google sign-in, Gmail, QuickBooks, Plaid, and Resend credential names are
  present. Provider-side registration and controlled receipts remain separate
  from environment presence.

## Intentionally local or defaulted

- `DAILY_NEW_LEAD_LIMIT` and `ENABLE_WEEKDAY_ONLY_IMPORTS` use documented application defaults.
- `CLICKUP_DISCOVERY_SNAPSHOT_PATH` points at an ephemeral local cache and is not a durable-state requirement.
- `SLACK_AE_MAP_JSON` is optional mapping data.
- `AMAZON_PROFIT_API_BASE_URL` uses the current documented default until the upstream service is separately migrated.
- HR out-of-office Google Calendar variables are optional and not part of the launched HR flows.

## Provider configuration still missing or unproven

These are not safe to invent or copy without validating the owning provider account:

- `STRIPE_SECRET_KEY` and `STRIPE_WEBHOOK_SECRET`: Building payment processing cannot be certified until the owner selects the correct Stripe environment and webhook endpoint.
- `INSTANTLY_WEBHOOK_SECRET`: the allowed-event and secret-header settings
  exist, but the signing secret itself is absent and must be supplied or
  confirmed in the Instantly console.
- Provider-side allowlists for Google, Gmail, QuickBooks, Plaid, Stripe, Resend, and Instantly still need the staging hostname registered and tested.

## Recheck command

Run `vercel env ls production --json`, compare only the returned `key` fields with `.env.example`, and classify aliases/defaults before treating a name difference as a missing dependency. Never print or commit the returned values.

Any change to environment variables requires a new immutable deployment and a repeat of readiness, authentication, callback, and browser QA.
