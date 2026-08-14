# Agent Vercel release-candidate sign-off

Status: engineering candidate ready for provider registration and timed rehearsal; production cutover not approved

## Candidate identity

- Branch: `codex/vercel-agent-duplicate`
- Candidate application commit: `8e5dcfb`
- Full GitHub release gate: run `31770662065`, passed in 5m07s
- Final immutable performance deployment: `dpl_4ZcSCzu27ANS5HbtpmzDEmhCvcDz`
- Immutable URL: `https://anata-agent-staging-6vnk7c9sk-david-narayans-projects.vercel.app`
- Stable staging URL: `https://agent-staging.anatainc.com`
- Database: Supabase project `vfcmljqakphwhslxtfzv`, `us-west-1`, dedicated restricted `agent_app` role

## Passed engineering gates

- Full hosted regression gate passed for `8e5dcfb`.
- Tracked-secret scan: 797 tracked files, zero findings. The scan reports only
  file, line, and pattern name and never prints a candidate value.
- Dependency audit: 46 declared/runtime dependencies, zero known vulnerabilities.
- Route inventory regenerated: 689 mounted FastAPI routes.
- Source/target database parity: 171 tables, 223,016 rows, full-row fingerprints,
  and 61 sequence states match the rehearsal snapshot.
- Artifact parity: 144 Website Ops files, 124,124,830 bytes; Render has no
  persistent disk outside database-backed retained state.
- Hosted Vercel scheduler authentication and read-only synthetic health pass.
- Hosted durable queue recovery passes failure, retry, overlap, and replay tests
  with no external effect.
- All 11 write schedules have successful Supabase shadow receipts with readable
  database inputs, configured declared dependencies, and no external writes.
- Three separate deployments passed concurrent readiness; 12 authenticated
  representative pages passed two complete desktop passes at 1280px. Finance
  completed in 6.36–6.79 seconds cold and 252 ms warm after its query fix.
- Administrator and fresh-session passes found every expected heading, main
  landmark, skip target, and no horizontal overflow. A temporary restricted
  user could access Sales only and received a friendly denial for Finance,
  Executive, and HR. Its user and token records were deleted and verified zero.
- The saved rollback archive restored into isolated PostgreSQL 17 in 12.89
  seconds and recovered 172 tables / 223,018 rows, including all 144 retained
  Website Ops files. The isolated databases were stopped and securely removed.
- Vercel schedules remain globally write-disabled. Render production remains unchanged.

## Required before asking for cutover approval

1. Register and test staging callbacks/webhooks in Google, Gmail, QuickBooks,
   Plaid, Instantly, Stripe, and Resend as applicable.
2. Capture controlled provider happy-path receipts using sandbox or staging-safe
   records; do not move money, publish content, or contact customers merely for QA.
3. During the approved maintenance window, capture and apply the final
   production delta, rerun the parity audit, and record the current source
   snapshot identifier. The base restore and rollback archive restore are timed
   and proven; only the live final delta remains.
4. Name the go/no-go owner, rollback owner, migration operator, and business
   verification owner.
5. Obtain explicit approval for the production maintenance window, domain move,
   provider callback change, and one-at-a-time scheduler enablement.

## Production safety statement

This record does not authorize changing `agent.anatainc.com`, disabling Render,
enabling Vercel writer schedules, moving callbacks, or running a final production
delta. Those actions require the explicit owner approval described above.
