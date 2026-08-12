# Vercel staging performance receipt

Measured: August 12, 2026

Deployment: `dpl_4Bux8yHmJqCq7X9ZKs1h7hB8eDRf`

Region: Vercel function `pdx1`; Neon is configured in the same migration region.

## Results

| Check | Result | Gate |
| --- | --- | --- |
| Ten sequential readiness navigations | 264–799 ms | Pass: every request under one second |
| Five-request concurrent readiness burst | 360, 423, 436, 6,484, and 6,965 ms | Reliability pass under ten seconds; strict cold-readiness target fails for two newly provisioned instances |
| Vercel one-hour FastAPI p95 TTFB | 1,407 ms | Pass for normal authenticated-page target |
| Latest deployment runtime error scan | No error-level entries returned | Pass |
| Latest-hour 5xx scan | No 5xx recorded in the latest deployment test window | Pass |

The 24-hour project-level metric includes earlier failed development deployments and deliberate negative tests: eight 500 responses and two 503 responses before the current release candidate. These are not attributed to the latest deployment; the latest deployment log scan is clean.

## Finding

The application is fast when warm, but burst concurrency can force independent Python cold starts. Two of five concurrent instances exceeded the specification's aspirational five-second cold-readiness threshold while remaining below the ten-second user-navigation/502 prevention ceiling. This is a capacity characteristic, not a database failure: warm requests are sub-second and no pool or function error occurred.

## Required decision before cutover

Choose one of these evidence-backed controls:

1. Configure Vercel provisioned concurrency/minimum warm instances for the FastAPI service if the selected Vercel plan supports it, then repeat three cold rounds; or
2. Accept the measured 6–7 second burst cold start as a documented severity-3 limitation because it remains below the ten-second navigation ceiling and produced no 502/504.

Do not weaken readiness or remove security/database checks to hide cold-start time. The strict performance gate remains open until one option is selected and verified.
