# Vercel staging performance receipt

Measured: August 13, 2026

Release commit: `d21d58b`

Deployments: `dpl_BKU8CpgwVtjHW6HCsUityRESQ3kQ`, `dpl_FjJJwrNTK2Bndq5yX1L6o6dKmpY6`, and `dpl_7nnkYPTsADtEDAC7vsDyF12o6zmR`

Region: Vercel function `pdx1`; Neon is configured in the same migration region.

## Results

| Check | Result | Gate |
| --- | --- | --- |
| Ten sequential readiness requests on the first candidate | 308–3,949 ms, all 200 | Pass: every request under five seconds |
| Five concurrent readiness requests, deployment 1 | 623 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 2 | 1,082–1,084 ms, all 200 | Pass |
| Five concurrent readiness requests, deployment 3 | 971–972 ms, all 200 | Pass |
| Ten authenticated representative pages, first navigation | 244–5,550 ms | Pass: every page under the ten-second cold navigation ceiling |
| Warm Sales / Website Ops / Finance repeats | 228–1,506 ms after warm-up | Pass: under the two-second normal-page target |
| Browser console on the release candidate | No warning or error entries | Pass |

The 24-hour project-level metric includes earlier failed development deployments and deliberate negative tests: eight 500 responses and two 503 responses before the current release candidate. These are not attributed to the latest deployment; the latest deployment log scan is clean.

## Finding and decision

The prior 6–7 second burst-cold result did not recur on the hardened release. Three independent deployments of the same release commit passed the strict five-second readiness target and the ten-second authenticated-navigation target without a 502/504 or database-pool error. Website Ops was the slowest first authenticated navigation at 5.55 seconds, then stabilized at 1.3–1.5 seconds.

The migration performance gate is closed for the current staging candidate. Provisioned concurrency is not required for cutover based on current evidence. It remains an optional capacity improvement if post-cutover traffic or Vercel telemetry shows renewed cold-start pressure.

Any material runtime, dependency, database-region, or Vercel compute configuration change resets this evidence and requires the three rounds again.
