# CONTEXT HANDOFF — Anata Building / The Arena booking system — 2026-08-01

## MISSION

Get anatabuilding.com's event booking flow working end to end: a customer enquires
on the public site, Anata quotes them, they sign, they pay, and Agent confirms the
date. The public site and the operating system behind it are built. What remains is
provider connections and cutting the setup down to a size that fits a small business.

## YOUR WORKING POSTURE

Keep building. Do not wait for permission between steps. Verify by running the
thing, not by reading the code. When you claim something works, have evidence.
Default PR workflow: push, `gh pr create`, `gh pr merge --squash`, pull. Exception:
anything touching auth, billing or a schema change waits for David.

**David's communication rules, which he has enforced repeatedly this session:**
- Short. He has asked three times for shorter replies. Lead with the answer.
- Plain English, no jargon. He is not technical.
- No em dashes anywhere, messages or customer copy.
- Never claim done without evidence. He caught an assumption this session.
- When he pushes back, check before defending. He was right about QuickBooks and
  right about the settings being overbuilt.

## WHERE WE ARE RIGHT NOW

### Repository
- Repo: `/Users/davidnarayan/Documents/Playground/Lead-scraper` (public,
  `david-anata/Lead-scraper`). This is "Agent", the FastAPI app behind
  agent.anatainc.com.
- Branch: `main`, clean of tracked changes. 35 untracked files exist, most are
  iCloud duplicate artifacts named `... 2.py`. Do not commit those.
- Two old stashes exist from other work. Leave them.
- The public site is a separate repo: `david-anata/anata-building`, cloned at
  `~/Developer/anata-building`. Live on Vercel at anatabuilding.com.

### Live state, verified in the browser this session
Building settings shows **8 of 10 launch requirements complete**.

| Row | State |
|---|---|
| Business rules | Ready |
| Arena pricing | Ready |
| Tax determination | Ready |
| Old booking-page copy | Ready |
| Reusable event agreement | Ready |
| Electronic signatures | Ready |
| Customer payments | Ready |
| **Dedicated Arena calendar** | **Outside setup** |
| **Customer email** | **Outside setup** |
| Customer booking launch | Unlocks automatically |

### What is known to work
- Public site: 13 pages live, event enquiry, cost estimator, status page.
- The whole booking lifecycle, proven end to end by
  `scripts/arena_event_flow_walkthrough.py` — **26 of 26 steps pass** against a
  throwaway database. Run it, it is the fastest way to see the system work.
- Discount on a quote flows to the bill without anyone retyping it, and an invoice
  refuses to send if the quote changed underneath it.
- 237 building tests pass.

### What is not done
1. **Calendar.** Env vars are set on Render and writes are on, but the
   `event_calendar` decision is not recorded, so the row stays red. David tried and
   could not; the row has since been fixed to say which condition is missing.
   Read what it now says before advising him.
2. **Customer email.** Needs `RESEND_WEBHOOK_SECRET` on the Render service. The
   endpoint to register in Resend is
   `https://agent.anatainc.com/api/integrations/resend/webhook`.
3. **Settings page is still overbuilt.** David's words: "it's overbuilt and requires
   approval as if we are a large business." One pass has landed (PR #313). He was
   asked whether to cut further and answered by requesting this handoff, so **the
   next session should assume yes and keep cutting.**

## DEPLOYMENT

- Service: `sales-support-agent` on Render, auto-deploys on commit to `main`.
- Build: `pip install -r requirements.txt`
- Pre-deploy: `python scripts/predeploy_agent.py` ← **this failed once this session**
- Start: `uvicorn sales_support_agent.main:app --host 0.0.0.0 --port $PORT`
- Database: Postgres. Local tests use SQLite, so timezone-naive datetimes pass
  locally and 500 in production. Always use timezone-aware UTC.

**The pre-deploy failure to remember:** editing
`docs/building/agreements/arena-event-agreement-business-terms-v2.md` changes its
checksum, and the checksum is part of the seeded template's identity. The seed
refuses to overwrite and the deploy dies. If you change that document, bump it to
v3 and update the constants in `building_arena_agreement_seed.py` and
`building_agreement_readiness_router.py`. Do not overwrite the version in place.

## KEY DECISIONS THIS SESSION, HONOR THESE

- **David is the approver.** No lawyer. He said "we are too small of a business for
  this. I, the owner, approve." Do not reintroduce a legal-review step.
- **QuickBooks is the payment rail**, not Stripe. QuickBooks issues the invoice and
  holds the payment of record. Stripe is only an optional webhook. David corrected
  me on this and he was right.
- **Arena rental is not taxable.** His decision, not an accountant's. It carries the
  risk of uncollected tax and he accepted that in writing.
- **Building mail** sends from building@anatainc.com and always copies
  david@anatainc.com and val@anatainc.com.
- **Ceremony vs guards.** Typed passphrases were removed from setup actions. They
  stay on anything that reaches a customer, moves money, or cannot be undone. Keep
  that line if you cut further.
- **Money is entered in dollars, never cents.** David nearly priced The Arena at
  four cents because a field asked for cents.

## TASK QUEUE

### Resume immediately
1. Open agent.anatainc.com/admin/building/settings, read the calendar row, and tell
   David in one or two sentences exactly what it now says is missing.
2. Keep cutting the settings page. It is still far too many panels for a building
   with one bookable room. Start by hiding anything already complete and showing
   only what is outstanding.

### Queued
- Verify PR #313 actually simplified what David sees, in the browser, not the code.
- The other building products (coworking, private offices, warehouse, golf, two
  conference rooms) exist as an unpublished draft with no prices, blocked from
  publishing. Only The Arena is near ready. Ask before pricing them.
- Calendar choice worth raising: `anata Events`
  (`c_0e16476c25a1a45fe0ff1b15619e1d6fee639fdbc4682c26fb19ad84b4a78862@group.calendar.google.com`)
  is a plain calendar and will not block double-booking. There is also a room
  resource, `The anata Building-1-Event Center (250)`
  (`c_18880uv9k32vgjm6gqcotea297bf2@resource.calendar.google.com`), which would.

### Blocked on David
- Recording the calendar decision. Needs his login and his permission.
- `RESEND_WEBHOOK_SECRET`. A credential, so it is his to enter.

### Known issues
- The Anata Events calendar currently publishes **full event details publicly**.
  Anyone who finds it can read who booked what. Should be busy/free only. Flagged
  to David, not yet fixed.
- Signing and payment recording are manual: the contract goes out of QuickBooks by
  hand and the cleared payment is recorded on the booking by hand.
- `~35` untracked `... 2.py` files are iCloud duplication artifacts. Never `git add -A`
  in this repo; always add explicit paths.

## COMMANDS

```bash
# Prove the whole booking flow works, 26 steps, touches nothing real
cd ~/Documents/Playground/Lead-scraper && PYTHONPATH=. python3 scripts/arena_event_flow_walkthrough.py
```

```bash
# Every building test
cd ~/Documents/Playground/Lead-scraper && python3 -m pytest tests/test_building_*.py -q
```

```bash
# The public site
curl -s -o /dev/null -w "%{http_code}\n" https://anatabuilding.com
```

## KEY FILES

- `sales_support_agent/api/building_booking_router.py` — the booking lifecycle
- `sales_support_agent/api/building_billing_router.py` — invoices, the quote-to-bill link
- `sales_support_agent/services/building_launch_status.py` — the 10-row checklist
- `sales_support_agent/services/building_page.py` — the settings page David complains about
- `docs/building/arena-team-runbook.md` — how his team books an event
- `docs/building/agreements/arena-event-agreement-v1.md` — the contract customers sign
- `scripts/arena_event_flow_walkthrough.py` — the end-to-end proof

## HOW TO BEHAVE

Read this, run `git log --oneline -5`, then start on the resume list. Do not
summarize this back to David. He wants the calendar row read and the settings page
cut down, in that order.
