# 09 - Implementation and Testing Spec: Make Outbound Actually Send

Status: awaiting approval. Do not build until approved.

---

## Section A - Plain English Summary

**What this is.** Today the machine is half-built. Our app can find brands and hand
you a clean, deduped list. But the middle and the end are not connected: in Clay
the columns and your two prompts exist yet have never actually run, and there is no
step that hands finished leads to Instantly. Instantly has no campaign yet. So
nothing can send. This plan finishes the chain and, just as important, tests every
link so we know it works before a single real email goes out.

When this is done you will be able to: download a brand list in the app, watch Clay
find the contact, qualify them, and write a personalized opener, see those leads
land in an Instantly campaign automatically, approve the copy, send a tiny test to
our own inboxes, and only then turn it on. Your scoreboard will show not just how
many emails went out, but which signals are actually booking calls, and where the
machine is jammed (too few emails, too few people to work replies, or Clay running
low). People who ask to follow up or no-show a call will drop into a HubSpot nurture
so they are not lost.

**Why it matters.** Right now "are we ready" is a guess. After this, every link has
been run and watched, so readiness is a fact, not a hope. You also stop flying blind:
the scoreboard tells you what is working and what is stuck, so you can fix the real
bottleneck instead of guessing.

**What is changing.** New: the Clay-to-Instantly hand-off, the Instantly campaign,
signal scoring on the brand list, a by-signal and bottleneck view on the scoreboard,
and a HubSpot nurture for follow-up and no-show. Unchanged: the app's brand sourcing
and deduped CSV (already built and tested), your Clay prompts (kept as-is), your
warmed domains, and the Generate Sales Deck feature (not touched).

---

## Section B - Mockup

### B1. Scoreboard, with new Capacity and By-signal panels

```
┌──────────────────────────────────────────────────────────────┐
│  Outbound scoreboard                    [Refresh]             │
│  Your machine, and how it is performing.                     │
├──────────────────────────────────────────────────────────────┤
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐                 │
│  │ Sent   │ │ Reply% │ │ Pos%   │ │ Bounce%│                 │
│  │ 4,555  │ │ 0.4%   │ │ 0.1%   │ │ 1.9%   │                 │
│  └────────┘ └────────┘ └────────┘ └────────┘                 │
│  ~X emails per booked call                                   │
│                                                              │
│  CAPACITY AND BOTTLENECKS                                    │
│  ┌───────────────┬──────────┬──────────┬──────────────────┐ │
│  │ Stage         │ Have     │ Need     │ Status           │ │
│  ├───────────────┼──────────┼──────────┼──────────────────┤ │
│  │ Emails/day    │ 300      │ 600      │ ⚠ Under target   │ │
│  │ Reply capacity│ 2 people │ 5 people │ ⚠ Not enough team│ │
│  │ Clay credits  │ 1,200    │ 4,000    │ ⚠ Low for volume │ │
│  └───────────────┴──────────┴──────────┴──────────────────┘ │
│  Biggest bottleneck right now: Clay credits.                │
│                                                              │
│  BY SIGNAL (which to double down on)                        │
│  ┌────────────────────────┬────────┬─────────┬────────────┐ │
│  │ Signal                 │ Sent   │ Pos %   │ vs baseline│ │
│  ├────────────────────────┼────────┼─────────┼────────────┤ │
│  │ Added CRO app <45d     │ 220    │ 1.2%    │ +3x  ▲     │ │
│  │ Runs Meta+Google ads   │ 900    │ 0.5%    │ = 1.0x     │ │
│  │ Trending on TikTok      │ 140    │ 0.1%    │ -   ▼     │ │
│  └────────────────────────┴────────┴─────────┴────────────┘ │
│  Empty state: "Numbers fill in once leads are tagged and    │
│  emails have gone out."                                     │
└──────────────────────────────────────────────────────────────┘
```
Caption: same top tiles as today, plus two panels. Capacity names the single
biggest jam in plain words. By-signal shows which selection reasons actually book
calls, so you double down on winners and drop losers.

### B2. Brand List, with tiers and a live pipeline status strip

```
┌──────────────────────────────────────────────────────────────┐
│  Brand list                                                  │
│  Pipeline: StoreLeads ✓   Clay connected ✓   Instantly ✓ live │
│                                                              │
│  How many brands [ 100 ]     [ Download brand CSV ]          │
│                                                              │
│  Preview of this batch                                       │
│  ┌──────┬──────────────┬────────┬───────────────────────┐    │
│  │ Tier │ Brand        │ Niche  │ Why now (signal)      │    │
│  ├──────┼──────────────┼────────┼───────────────────────┤    │
│  │  A   │ acme.com     │ beauty │ Added Intelligems 3wk │    │
│  │  A   │ brightpet.co │ pets   │ Upgraded to Plus      │    │
│  │  B   │ luxehome.com │ home   │ Runs Meta+Google ads  │    │
│  │  C   │ tinytoys.com │ gifts  │ Fits ICP, thin signal │    │
│  └──────┴──────────────┴────────┴───────────────────────┘    │
│  Never email twice: brands you download are remembered.      │
└──────────────────────────────────────────────────────────────┘
```
Caption: the download works as it does today, but each brand now shows a Tier
(A hottest) and the reason it was picked. The top strip is a live readiness light
for each of the three links, so you can see at a glance if anything is down.

### B3. HubSpot nurture trigger (from a reply/outcome)

```
┌──────────────────────────────────────────────────────────────┐
│  Reply / call outcome                                        │
│  Contact: jane@acme.com  (acme.com, Tier A)                  │
│                                                              │
│  Outcome:  ( ) Booked   (•) Follow up later   ( ) No show    │
│                                                              │
│  [ Add to HubSpot nurture ]   → enrolls in "Outbound Nurture"│
│  Confirmation: "Added. They will get the nurture sequence."  │
└──────────────────────────────────────────────────────────────┘
```
Caption: when someone asks to follow up later or no-shows, one click puts them in
a HubSpot nurture so they keep hearing from us instead of going cold.

---

## Section C - Technical Specification

### C0. Best course of action (the decision)

Finish and prove the send path FIRST, smallest safe steps, before any build that
only measures. Measurement and nurture are fast-follows that do not block Monday.
So: Phase 1 makes it send-ready and tested; Phases 2 and 3 add the intelligence.

### C1. Architecture (the chain, and who owns each link)

```
StoreLeads ──> App (Brand List, deduped CSV)  [built + tested]
                     │  download CSV
                     ▼
                 Clay table "Anata // Claude Table"
                   - import CSV as a source            [to wire]
                   - find contact + verified email     [exists, never run]
                   - Sales Fit prompt                  [exists, never run]
                   - Personalized Cold Outreach prompt [exists, never run]
                   - Add to Instantly Campaign column  [MISSING - to build]
                     │  push qualified rows
                     ▼
                 Instantly campaign (warmed domains)   [MISSING - to build, draft]
                     │  send (only after approval + test)
                     ▼
                 Replies worked by David + Gabe
                   - Booked  -> keep
                   - Follow up / No show -> HubSpot nurture  [Phase 3]

App scoreboard reads Instantly + our outcome store   [built; extend in Phase 2]
```

Ownership: app code is mine. Clay and Instantly setup is browser-driven by me with
you watching, EXCEPT the one credential step (adding the Instantly API key inside
Clay), which only you can do because I never handle keys.

### C2. Phases

**Phase 1 - Send-ready (the critical path for Monday)**
1. Deploy latest app commit (dedup) - one Manual Deploy of `2b5abc7`.
2. Create the Instantly campaign in draft: sequence loaded from
   docs/outbound/01, warmed domains attached, daily cap set, sending OFF.
3. You add the Instantly API key inside Clay (your step). I then add the
   "Add to Instantly Campaign" column to the Clay table so qualified rows push.
4. Prove Clay on a tiny batch: download a 10-brand CSV, import to Clay, run
   enrichment on those 10, confirm email + Sales Fit + Personalization populate,
   confirm the 10 push into the Instantly campaign (still not sent).
5. Internal test send: 3 to 5 rows aimed at our OWN inboxes, confirm delivery and
   that spintax/links/opt-out rules from the playbook hold.
6. You approve the copy. Only then the campaign is switched on.

**Phase 2 - Measurement (fast-follow, does not block sending)**
7. Signal scoring in the app: score each brand, attach Tier A/B/C and the "why now"
   reason to the CSV and preview (uses the rubric in docs/outbound/08).
8. Per-signal efficacy: tag each pushed lead with its signals in the existing
   outcome store, join to Instantly outcomes, show the By-signal table.
9. Bottleneck panel: capacity math for emails/day, reply capacity, Clay credits;
   name the single biggest jam.

**Phase 3 - Nurture (fast-follow)**
10. HubSpot nurture: a "Add to nurture" action that creates/updates the contact in
    HubSpot and enrolls them in an "Outbound Nurture" sequence, triggered on
    follow-up or no-show.

### C3. Dependencies
- App: no new libraries. Reuses the existing database engine and scoreboard.
- Clay: the Launch plan (CSV import path, no webhook). Enough Clay credits for the
  volume we run. The Instantly API key (yours to enter).
- Instantly: an account with warmed domains (have) and API access for the Clay push.
- HubSpot: the connected HubSpot workspace + a nurture sequence to enroll into.

### C4. Technical decisions (why, alternatives, drawbacks)
- **Prove the pipe on 10 rows before scaling.** Why: it is the cheapest way to catch
  a broken prompt or a bad email-finder before spending credits on hundreds.
  Alternative: import 100 straight away (faster, but burns credit on an unproven
  pipe). Drawback of the 10-row path: one extra small step.
- **Clay pushes to Instantly with Clay's native column, not a webhook.** Why: matches
  the Launch plan and needs no code. Alternative: our app calls Instantly's API
  directly (more control, but more to build and maintain, and duplicates Clay).
  Drawback: depends on Clay's integration staying available.
- **Scoring lives in the app, not Clay.** Why: keeps our selection logic and the
  learning loop in one place we control and can test. Alternative: score inside Clay
  (harder to test, locked to Clay). Drawback: the app must fetch the richer
  StoreLeads fields (it already can).
- **Nurture in HubSpot, triggered by outcome.** Why: HubSpot is built for multi-touch
  nurture and is already connected. Alternative: nurture inside Instantly (blurs cold
  outreach with warm nurture and risks domain reputation). Drawback: a contact hop
  between tools, handled by the connector.

### C5. Alternatives considered (whole approach)
- Fully automate app-to-Instantly and skip Clay: rejected, Clay does the
  contact-finding and verification we do not want to rebuild.
- Wait for Clay Growth webhooks (Aug 6): rejected, we want to send Monday; CSV works
  now.

### C6. Risks
- Clay enrichment returns few verified emails: we size sends to verified-email
  supply and never email unverified. Test at 10 rows surfaces this early.
- Instantly reputation if we send too fast: daily cap + warmed domains + test batch.
- Clay credits run out mid-run: the bottleneck panel warns before it bites; the
  10-row test keeps early spend tiny.
- Wrong Clay table touched: confirmed the correct one is "Anata // Claude Table" ->
  Found Contacts; all work stays there.
- Sandbox Mode left on in Clay (runs would only simulate): checked as step 1 of the
  Clay work.

### C7. Success criteria
- A 10-brand test flows StoreLeads -> app -> Clay -> Instantly with real verified
  emails, real Sales Fit answers, real personalized openers, landing in the campaign.
- An internal test email arrives in our own inbox, correctly personalized, passing
  the playbook rules.
- The scoreboard shows live sent/reply/positive numbers, a By-signal table, and a
  named bottleneck.
- A follow-up/no-show contact appears enrolled in the HubSpot nurture.
- Never a second email to the same brand (dedup holds across runs).

### C8. Verification strategy (the three-pass rule)
The build cannot be called done until the full verification loop passes THREE times
in a row with zero findings. Any finding resets the count. The final pass is always
a live end-to-end walkthrough driven through the browser.

- **Automated tests (per link):**
  - App sourcing + dedup: existing tests (35 passing) plus new tests for the scoring
    function (tier boundaries, each signal, anti-signals) and the efficacy join.
  - Bottleneck math: tests for under/at/over capacity and "biggest jam" selection.
- **Manual link checks:**
  - Clay: 10-row run shows non-empty Work Email, Sales Fit, Personalization.
  - Clay -> Instantly: those 10 appear in the campaign, unsent.
  - Instantly: internal 3-5 test send arrives, personalized, rules intact.
  - HubSpot: a test outcome enrolls a contact in the nurture.
- **Pass 3 (end-to-end browser walkthrough) MUST cover these exact workflows:**
  1. App: open Brand List, confirm pipeline strip all-green, download a small CSV,
     confirm tiers and reasons render.
  2. Clay: import that CSV, run enrichment, read a row and confirm the opener makes
     sense to a human, confirm push to Instantly.
  3. Instantly: open the campaign, confirm the pushed contacts are present and the
     campaign is correctly configured (not accidentally sending).
  4. Scoreboard: confirm live numbers, the By-signal table, and the bottleneck line
     read correctly to the person looking at them.
  5. HubSpot: mark a test contact as follow-up, confirm the nurture enrollment.
  Each workflow is checked for "does the content make sense to the person seeing it,"
  not just "did it technically work."

### C9. Testing strategy summary
Test each link in isolation first (cheap, fast), then end to end on 10 rows, then an
internal-only send, then the three-pass loop, then and only then a real send after
your copy approval. Nothing sends to a real brand until the internal test passes and
you approve.

---

> Does this spec look correct? Once approved, run /ship to begin building.
