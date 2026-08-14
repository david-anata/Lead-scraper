# Vercel staging performance receipt

Measured: August 13–14, 2026

Release commit: `8e5dcfb`

Deployments: `dpl_Ja1nJeiWUp5gpWTa1KpqL9rFpdUW`,
`dpl_4Bf1fXa9ncFMRCAkJixiz8L6ixuE`, and
`dpl_4ZcSCzu27ANS5HbtpmzDEmhCvcDz`

Region: Vercel function `iad1`; Supabase session pooler `us-west-1`.

## Results

| Check | Result | Gate |
| --- | --- | --- |
| Five concurrent readiness requests, deployment 1 | 653–1,821 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 2 | 867–1,088 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 3 | 801–956 ms, all 200 | Pass |
| Twelve authenticated representative pages, first navigation | 450–6,793 ms, all expected 200 | Pass: every page under the ten-second cold navigation ceiling |
| Warm normal pages | 450–1,083 ms | Pass: under the two-second normal-page target |
| Warm heavy pages (Website Ops, Building, Advertising) | 2,282–2,844 ms | Pass: under the four-second heavy-page target |
| Finance cold then warm | 6,366 ms then 252 ms | Pass |
| Restricted-role direct URLs | Sales 200; Finance, Executive, and HR friendly 403 | Pass: no permission leak |
| Exact-deployment runtime errors | No error group attributed to `dpl_4ZcSCzu27ANS5HbtpmzDEmhCvcDz` | Pass |

The 24-hour project-level metric includes earlier failed development deployments and deliberate negative tests: eight 500 responses and two 503 responses before the current release candidate. These are not attributed to the latest deployment; the latest deployment log scan is clean.

## Finding and decision

The gate was repeated after the fresh-session audit found Finance at 10.3
seconds. The brief had been re-reading the same ledger classification and
repeating account, settings, payroll, and bill-pattern work inside one request.
The revised request reuses the already computed trust-gate result and one
request-scoped evidence snapshot without changing Plaid, payroll, settlement,
or money-classification semantics.

Against Supabase, the isolated service profile fell from 186 queries / 11.8
seconds to 27 queries / 2.7 seconds. On the exact deployed function, the cold
Finance request fell from 174 queries / 9.66 seconds server time to 103 queries /
6.36 seconds; database time fell from 5.15 to 3.19 seconds. A subsequent warm
Finance navigation completed in 252 ms. Three independent deployments of the
same commit passed readiness, and two complete authenticated desktop passes had
no 502/504, overflow, missing landmark, or permission leak. The exact final
deployment had no 5xx runtime log entry during the test window.

The migration performance gate is closed for the current staging candidate. Provisioned concurrency is not required for cutover based on current evidence. It remains an optional capacity improvement if post-cutover traffic or Vercel telemetry shows renewed cold-start pressure.

Any material runtime, dependency, database-region, or Vercel compute configuration change resets this evidence and requires the three rounds again.
