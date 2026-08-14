# Vercel staging performance receipt

Measured: August 13–14, 2026

Release commit: `1a05919`

Deployments: `dpl_7FX2Jc1k7NqbvSjRe42sKjjCt2jo`,
`dpl_FwNVpiRqasdQqcM3MyDgGoL1kgHe`, and
`dpl_F2zLkQvKDqGhvmwo1o2iYrQVir5w`

Region: Vercel function `pdx1`; Supabase session pooler `us-west-1`.

## Results

| Check | Result | Gate |
| --- | --- | --- |
| Five concurrent readiness requests, deployment 1 | 867–978 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 2 | 799–1,194 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 3 | 921–2,795 ms, all 200 | Pass; one cold-start outlier |
| Thirteen authenticated representative pages, two passes | All expected 200 with canonical main/skip targets and no horizontal overflow | Pass |
| Warm normal pages | 450–1,083 ms | Pass: under the two-second normal-page target |
| Warm heavy pages (Website Ops, Building, Advertising) | 2,282–2,844 ms | Pass: under the four-second heavy-page target |
| Finance cold then warm | 6,366 ms then 252 ms | Pass |
| Restricted-role direct URLs | Sales 200; Finance, Executive, and HR friendly 403 | Pass: no permission leak |
| Exact-deployment runtime errors | No error-level, HubSpot-background, or 5xx entry attributed to `dpl_F2zLkQvKDqGhvmwo1o2iYrQVir5w` | Pass |

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

The final candidate also removes HubSpot background-thread work from ordinary
Vercel Deal Board reads. A page read completed in 159 ms warm with four queries;
the explicit operator Sync action uses a tested synchronous request path instead
of pretending a detached serverless thread will finish.

The migration performance gate is closed for the current staging candidate. Provisioned concurrency is not required for cutover based on current evidence. It remains an optional capacity improvement if post-cutover traffic or Vercel telemetry shows renewed cold-start pressure.

Any material runtime, dependency, database-region, or Vercel compute configuration change resets this evidence and requires the three rounds again.
