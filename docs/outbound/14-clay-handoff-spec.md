# 14 - Spec: the Clay hand-off

Status: **awaiting approval**. Nothing built yet.
Blocked until: Clay Growth tier unlocks webhook sources (~6 Aug 2026).

---

## Section A - Plain English Summary

**What this is.** Right now the chain has one manual link in the middle. The app finds the
brands, but you have to download a file and import it into Clay by hand. This replaces that
with a button. You press **Send to Clay** on Lead Ops, the brands go straight into your Clay
table, and Clay does the rest. The file step disappears.

The Lead Ops page gains a second button next to each recipe's **Pull now**: **Send to Clay**.
Press it and you see, in plain words, how many brands went across and whether Clay accepted
them. The run history gains a column showing whether each pull was downloaded, pushed, or
both. Nothing else about your day changes: the same recipes, the same caps, the same
never-email-twice memory, the same tuning and version tracking.

**Why it matters.** Today the middle of the machine only moves when you personally move it.
That makes daily sending depend on you being at a computer, and it means a pull you forget
to import is a pull that did nothing. After this, sourcing runs on its own schedule and the
leads arrive where they need to be. It also closes the measurement loop: because the recipe
and settings version travel with each brand into Clay and on into Instantly, we finally get
to answer "which recipe books calls, under which settings" instead of guessing.

**What is changing.** New: a push-to-Clay path, a Send to Clay button, delivery tracking on
each run, and a return path so booked calls can be traced back to the recipe that sourced
them. Unchanged: how brands are found and scored, the ICP gate, the dedup memory, the
tuning and change log, the CSV download (it stays as a fallback and for spot checks), and
everything in Instantly. Nothing in this spec sends an email.

---

## Section B - Mockup

### B1. Lead Ops, with the new push action

```
┌────────────────────────────────────────────────────────────────────────┐
│  Lead ops                                                              │
│  Clay: ✅ connected   ·   4,120 of 50,000 submissions used             │
├────────────────────────────────────────────────────────────────────────┤
│  PULL RECIPES                                                          │
│  ┌──────┬────────────────────────────┬──────────┬─────┬──────────────┐ │
│  │ Tier │ Recipe / why now           │ Runs     │ Cap │ Actions      │ │
│  ├──────┼────────────────────────────┼──────────┼─────┼──────────────┤ │
│  │  A   │ Just installed a growth... │ Today    │ 40  │ [Pull now]   │ │
│  │      │ Added a tool in 30 days    │          │     │ [Send to Clay]│ │
│  └──────┴────────────────────────────┴──────────┴─────┴──────────────┘ │
│                                                                        │
│  After pressing Send to Clay:                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Sent 38 brands to Clay. 2 were skipped as already contacted.     │  │
│  │ Clay accepted all 38.                                            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  Error state:                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Clay rejected 6 of 38. They are saved and will retry on the next │  │
│  │ push. Nothing was lost and no brand was contacted twice.         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  Not connected state:                                                  │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Clay is not connected yet. Add the webhook address in Settings   │  │
│  │ to turn this on. Pull now and the CSV download still work.       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```
Caption: the same recipe table you have today, with one new button. The message after a push
is written in plain words and always says what happened to every brand.

### B2. Run history, with delivery

```
┌──────────────────────────────────────────────────────────────────────────┐
│  RECENT PULLS                                                            │
│  ┌────────────┬──────────────┬───────┬───────┬────────┬──────────┬─────┐ │
│  │ When       │ Recipe       │ Fresh │ Sent  │ Clay   │ Status   │ Set │ │
│  ├────────────┼──────────────┼───────┼───────┼────────┼──────────┼─────┤ │
│  │ 29 Jul 09  │ new_growth   │ 38    │ 38    │ ✅ ok   │ complete │ v2  │ │
│  │ 29 Jul 09  │ replatformed │ 25    │ 25    │ ✅ ok   │ complete │ v2  │ │
│  │ 28 Jul 14  │ icp_baseline │ 25    │ -     │ file   │ complete │ v1  │ │
│  └────────────┴──────────────┴───────┴───────┴────────┴──────────┴─────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```
Caption: every pull now records how it was delivered. "file" means downloaded by hand, so
you can tell at a glance which pulls actually reached Clay.

### B3. Scoreboard, by recipe (the payoff)

```
┌──────────────────────────────────────────────────────────────────┐
│  BY RECIPE (which pull earns its place)                          │
│  ┌────────────────┬───────┬──────────┬────────────┬───────────┐  │
│  │ Recipe         │ Sent  │ Positive │ vs average │ Settings  │  │
│  ├────────────────┼───────┼──────────┼────────────┼───────────┤  │
│  │ Just installed │ 180   │ 2.8%     │ 3.1x  ▲    │ v2        │  │
│  │ Replatformed   │ 300   │ 0.9%     │ 1.0x       │ v2        │  │
│  │ Core ICP       │ 500   │ 0.3%     │ 0.3x  ▼    │ v1, v2    │  │
│  └────────────────┴───────┴──────────┴────────────┴───────────┘  │
│  Empty: "Fills in once replies come back from Instantly."        │
└──────────────────────────────────────────────────────────────────┘
```
Caption: the reason the whole chain carries a recipe tag. This is what tells you to keep the
just-installed trigger and drop the ones that do not pay.

---

## Section C - Technical Specification

### C1. Architecture

```
StoreLeads ──> app (recipes, ICP gate, scoring, dedup)      [BUILT]
                    │
                    ├── download CSV            [BUILT, kept as fallback]
                    └── POST to Clay webhook    [NEW - phase 1]
                              │
                              ▼
                        Clay table
                          enrich, verify, Sales Fit, personalize
                              │  (Clay native push)
                              ▼
                        Instantly campaign      [BUILT]
                              │
                              ▼  replies + booked calls
                        outcome return path     [NEW - phase 3]
                              │
                              ▼
                        scoreboard: by recipe, by settings version
```

Two facts fixed the shape of this. First, **Clay's webhook source is capped at 50,000
submissions for the life of the source**, and deleting rows does not give the budget back.
So we push deliberately and count what we have used. Second, **Clay's docs do not state
whether the webhook body may be an array**, so phase 1 sends one brand per request and only
switches to batching if a live test proves batching works.

### C2. Phases

**Phase 1 - Push to Clay (the actual hand-off)**
1. Store the Clay webhook address and optional auth token as settings, entered by David.
   Treated as a secret: written, never displayed back, never logged.
2. A push function: POST one brand as JSON, retry on failure with backoff, respect a small
   pace so we never hammer Clay.
3. A **Send to Clay** action per recipe on Lead Ops, plus a combined "send today's plan".
4. Delivery is recorded on the run: attempted, accepted, rejected, and the delivery method.
5. Rejected brands are **not** marked as contacted, so they return on the next pull rather
   than being silently lost.

**Phase 2 - Make it run itself**
6. A scheduled daily job that runs the day's recipes and pushes them, honouring the existing
   cadence (triggers Tue/Wed, baseline weekdays, nothing at weekends) and caps.
7. A submission budget guard: warn on the Lead Ops page as the 50,000 cap approaches, and
   refuse to push past a configurable floor so the source is never silently exhausted.

**Phase 3 - Close the loop**
8. Outcome return: match Instantly replies and booked calls back to the brand domain.
9. Scoreboard gains **by recipe** and **by settings version**, which is the whole point of
   carrying those tags.

### C3. Dependencies
- **Clay Growth tier** for webhook sources (~6 Aug). Nothing in phase 1 can be tested before
  this; the code can be written and unit-tested, but not proven.
- A Clay table whose columns accept our fields: tier, brand, domain, niche, country, reason,
  recipe, score, revenue, categories, plus the settings version.
- Clay's own native push into Instantly, already the plan and unchanged by this work.
- For phase 3, per-lead reply outcomes from Instantly, which we do not have today.

### C4. Technical decisions

**One brand per request, not a batch.** Clay's docs describe posting JSON and a row
appearing; they do not document array support. Alternative: send an array and halve the
request count. Drawback of one-per-request: more calls, slower for large pulls. Chosen
because a wrong guess here silently mangles rows, and our caps (25 to 40 a pull) make the
cost trivial. Phase 1 includes a live check of whether an array works, and we switch if it does.

**Push is triggered by a button first, a schedule second.** Alternative: schedule from day
one. Drawback: a bug would push wrong data unattended. Chosen because you should watch the
first few pushes land in Clay before it runs on its own.

**Rejected brands stay un-contacted.** Alternative: mark on attempt, which is simpler.
Drawback of our choice: a brand may be pushed twice if Clay accepted it but replied with an
error. Chosen because losing a good brand forever is worse than a rare duplicate, and Clay
dedupes on domain anyway.

**The webhook address is a secret.** Anyone with that address can write rows into your table
and burn the 50,000 budget. It is write-only in the app and never rendered back to the page.

**Keep the CSV.** Alternative: remove it once push works. Drawback: none worth it. It stays
as the fallback when Clay is down and as the way to eyeball a batch before it moves.

### C5. Alternatives considered (whole approach)
- **Clay pulls from us** instead of us pushing: needs a public endpoint on our side and
  Clay's HTTP API, more moving parts, and it inverts control of the cap. Rejected.
- **Keep CSV forever**: zero build, but the middle of the machine stays manual and daily
  sending stays dependent on you. Rejected, this is the whole point.

### C6. Risks
- **The 50,000 cap is permanent per source.** At about 130 brands a week that is roughly
  seven years, so it is not urgent, but a runaway loop could eat it in an afternoon. The
  budget guard and the per-recipe caps both exist to prevent that.
- **Growth tier slips.** Everything except the live proof can still be built and tested.
- **Clay's body shape assumption is wrong.** Contained by the live check before we trust it.
- **Duplicate rows in Clay** if a push half-fails. Clay dedupes by domain and our memory
  still holds, so the blast radius is a wasted enrichment credit, not a double email.
- **Phase 3 depends on data we do not yet have.** Called out honestly rather than promised.

### C7. Success criteria
- One press of Send to Clay puts the exact brands from that pull into the Clay table, with
  the recipe, reason and settings version intact on every row.
- A rejected push loses nothing: the brands come back on the next pull.
- The run history shows how every pull was delivered.
- The submission budget is visible before it becomes a problem.
- The CSV download still works unchanged.
- Nothing in this work can cause an email to send.

### C8. Verification (the three-pass rule)
Three consecutive clean passes, any finding resets the count.

- **Pass 1, code:** unit tests for the push (success, rejection, retry, secret never logged,
  budget guard, rejected-brands-stay-fresh), plus the full suite with no new failures
  against the current baseline of 21 pre-existing failures.
- **Pass 2, integration:** in-process run of every new route and state (not connected,
  connected, partial failure, budget exhausted) reading responses from disk, not memory.
- **Pass 3, live walkthrough** in the browser, covering exactly:
  1. Lead Ops with Clay not connected: Send to Clay explains itself and does not appear broken.
  2. Connect Clay, press Send to Clay on one recipe, and confirm in **Clay's own table** that
     the rows arrived with the right recipe, reason and version.
  3. Force a failure (bad address) and confirm the message is honest and nothing is marked
     contacted.
  4. Run history shows the delivery correctly for both a pushed and a downloaded pull.
  5. Re-pull the same recipe and confirm the pushed brands are not offered again.
  At each step: does what is on screen make sense to the person reading it?

### C9. Testing strategy
Unit tests first because they need no Clay access, so the whole thing can be built before the
6 Aug unlock. Then a single live push of **one brand** to confirm the shape before any real
batch. Then a full recipe. Then the schedule. No step trusts the previous one's word for it.

---

> Does this spec look correct? Once approved, run /ship to begin building.

---

# BUILD STATUS - phase 1 shipped 25 Jul 2026

**Built and live.** Push module, Send to Clay action, delivery tracking, budget guard,
secret handling, and the un-contacted rule.

| Verification | Result |
|---|---|
| Pass 1, code | 2,059 tests passing, 0 new failures against the 21 pre-existing baseline |
| Pass 2, integration | every state exercised from a clean read: not connected, connected, unknown recipe, missing key |
| Pass 3, live walk | **partial - see below** |

## What the live walk proved
- Not-connected state explains itself in plain words and shows **no dead button**.
- The file path is untouched: a pull returned 25 brands and logged correctly.
- Run history now shows Delivered (file vs Clay) and the settings version.
- Dedup is visibly working: one pull showed 72 scanned, 51 fit ICP, 25 fresh, 25 already
  seen, and a repeat of an earlier recipe returned 0 fresh because all 11 were already sent.
- The reasons correctly read "in the last 30 days", matching the tuned window.

## What the live walk could NOT prove
**The connected path has not been exercised against real Clay**, because webhook sources
need Growth tier (~6 Aug) and no webhook address is set. Specifically unproven:
1. that Clay accepts our row shape,
2. whether a batch body would work (we deliberately send one row per request until tested),
3. rejection handling against Clay's real error responses.

These are covered by unit tests against a stubbed Clay, which is not the same as proof.

## To finish, when Growth unlocks
1. In Clay, add a **Webhook** source to the Anata table and copy the address.
2. On Render, set `CLAY_WEBHOOK_URL` (and `CLAY_WEBHOOK_TOKEN` if you enable the token).
   Nobody needs to paste it anywhere else; the app never displays it back.
3. On Lead Ops, press **Send to Clay** on one recipe with a small cap.
4. Confirm in Clay's own table that the rows arrived with recipe, reason and settings
   version intact. That is the check that matters.
5. Then, and only then, consider phase 2 (running it on a schedule).
