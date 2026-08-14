# Vercel staging performance receipt

Measured: August 13–14, 2026

Release commit: `2ce079e`

Deployments: `dpl_BS3E65mVM6e7vZshsJbzow94YZKt`,
`dpl_76py4CEJjar3Bvq8ouZKKQC2Hvcd`, and
`dpl_6EQpH2fHQTt2kpzxoKN4QXqCFH4D`

Region: Vercel function `pdx1`; Supabase session pooler `us-west-1`.

## Results

| Check | Result | Gate |
| --- | --- | --- |
| Ten sequential readiness requests on the final candidate | 272–1,035 ms, all 200 | Pass: warm responses remain near or under one second |
| Five concurrent readiness requests, deployment 1 | 687–1,269 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 2 | 661–1,691 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 3 | 710–946 ms, all 200 | Pass |
| Twelve authenticated representative pages, first navigation | 260–9,701 ms, all 200 | Pass: every page under the ten-second cold navigation ceiling |
| Warm normal pages | 221–689 ms | Pass: under the two-second normal-page target |
| Warm heavy pages (Website Ops, Building, Advertising) | 2,199–2,638 ms | Pass: under the four-second heavy-page target |
| Browser console on the release candidate | No warning or error entries | Pass |

The 24-hour project-level metric includes earlier failed development deployments and deliberate negative tests: eight 500 responses and two 503 responses before the current release candidate. These are not attributed to the latest deployment; the latest deployment log scan is clean.

## Finding and decision

The gate was repeated because removing request-time schema DDL materially
changed application startup. Three independent deployments of the same release
commit passed the five-second readiness target and the ten-second authenticated
navigation target without a 502/504 or database-pool error. Website Ops was the
slowest first authenticated navigation at 9.70 seconds and Finance followed at
9.23 seconds; both remained under the cold ceiling. Website Ops then stabilized
at 2.20 seconds and Finance at 0.48 seconds. The exact final deployment had no
5xx runtime log entry during the test window.

The migration performance gate is closed for the current staging candidate. Provisioned concurrency is not required for cutover based on current evidence. It remains an optional capacity improvement if post-cutover traffic or Vercel telemetry shows renewed cold-start pressure.

Any material runtime, dependency, database-region, or Vercel compute configuration change resets this evidence and requires the three rounds again.
