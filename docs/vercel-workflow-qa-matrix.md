# Vercel staging workflow QA matrix

Candidate baseline: `8e5dcfb` / immutable deployment
`dpl_4ZcSCzu27ANS5HbtpmzDEmhCvcDz`

Staging: `https://agent-staging.anatainc.com`

Safety boundary: provider writes, money movement, payroll changes, customer
communications, CRM mutation, and production publishing remained disabled.

| Area | Hosted happy/read path | Hosted rejected/error path | Automated coverage | Still requires provider or owner action |
| --- | --- | --- | --- | --- |
| Authentication and access | Fallback administrator login, logout, workspace home, and permission-filtered navigation pass | Unknown/deleted restricted identity has no workspace grant; direct Finance access for a Sales-only identity returns friendly `No access`; invalid OAuth state is rejected | Access, invite, email-link, Google-session, direct-route, and recent-history permission tests pass | Register Google staging redirect and repeat Google login; repeat one-time email delivery through the live mail provider |
| Sales | Deal Board and control/report reads return 200 with current Supabase mirror data | Missing provider configuration and unauthorized route tests fail closed | HubSpot sync, ClickUp rules, Gmail signals, stale lead, Sales operator, and Slack receipt tests pass | Controlled ClickUp/HubSpot/Slack/Gmail source-system write receipts |
| Website Ops | Today page, publishing, site health, and report library render; 144 retained files are readable | Empty/error renderer and storage-hash rejection tests pass | Storage durability, site-health, action, report, and canonical-shell tests pass | One provider-backed plan execution and recovery receipt after callback/provider approval |
| Content | Control Room and retained artifacts render; scheduler shadow confirms Riverside boundary configured | Publishing remains in shadow and refuses live destinations without approval | Ingestion, transcript, artifact, approval, channel playbook, learning-loop, and publishing-gate tests pass | Signed Riverside/provider ingestion plus platform-specific shadow delivery receipts |
| Finance | Finance Today and read APIs render from Supabase in 6.36–6.79 seconds cold and 252 ms warm; Finance categories remain distinct | Invalid QuickBooks state and unsigned Plaid webhook reject; runtime role cannot execute DDL | Plaid, QuickBooks, reconciliation, evidence, preview/apply/undo, permissions, renderer, and request-reuse tests pass | Register callbacks and run sandbox/read-only QuickBooks and Plaid flows; no money movement is authorized |
| Building | Today page and 21 inquiry records render; scheduler shadow reads the operational tables | Unsigned Stripe and Resend webhooks reject; workflow validation tests cover invalid transitions | Intake, inquiry, tour, campaign, contract, billing, privacy, communication, and lifecycle suites pass | Select provider environments and capture signed Stripe/Resend sandbox receipts |
| Advertising | Burn List renders with canonical shell and no overflow | Invalid/mismatched upload and preflight cases reject | Upload parsing, audit, COGS, profitability, correction, and renderer suites pass | Optional staging-safe upload walkthrough; no external correction export was sent |
| Executive and shared reports | Owner Overview, Fulfillment reports, Website Ops reports, and HR reports render | Restricted direct routes deny data access | Executive access, source freshness, report renderer, empty-state, and canonical-navigation tests pass | Owner review of current source meaning; no provider write required |
| Fulfillment | Prospects & Assets and CS report library render; durable report table is available | Invalid upload, handoff, and missing-report states are covered | Rate sheet, assets, quote/deck, CS queue, durable report storage, retry, and renderer suites pass | Controlled HubSpot/Slack/email handoff receipt using a staging-safe record |
| HR | Dashboard, Time/PTO, and reports render; six employee records are readable | Restricted routes deny access; inactive/suspended employee flows fail closed | Employee access, onboarding, time, PTO, policy, payroll-readiness, retention, compliance, and report tests pass | Read-only provider connection confirmation if payroll/calendar integrations are included at cutover |
| Scheduled execution | Synthetic health, queue recovery, and all 11 write-schedule shadow receipts pass in Vercel and Supabase | Missing cron credential rejects; writes-enabled shadow/probe rejects; overlap and replay are refused | Cron manifest, auth, lease, retry, idempotency, and durable-background-task enforcement tests pass | Stop Render writers and enable Vercel jobs one at a time only during approved cutover |

## Current conclusion

Application, database, permission, durable scheduling, read workflows, and
failure boundaries are verified on staging. Two complete 12-page desktop passes
and one Sales-only restricted-role pass succeeded on the exact candidate, with
no overflow or direct-route data leak. Provider-console registration and
controlled source-system receipts remain the integration-parity work that
cannot be completed solely from the application workspace.
