# Claude Code handoff - finish Tape and the sales call system

Owner: David Narayan (david@anatainc.com). Written 2026-10-26.

## The job

Take Tape and the live sales call system from "works in a demo" to "done". There are five workstreams across two repos, plus one shared extension release.

| # | Workstream | Repo |
|---|---|---|
| A | Bug sweep and fixes: Tape extension, web app, server | `david-anata/tape` |
| B | One extension: fold everything into a single store build that replaces both installed copies | `david-anata/tape` |
| C | Finish the live call coach: replay harness, shadow mode, auto recap, calendar hold, trial signal, retention | `david-anata/Lead-scraper` |
| D | GTM sync: $99 cold copy, prefilled booked calls, weekly outcome report | `david-anata/Lead-scraper` |
| E | Tape known gaps: AI cost caps, long-recording transcription, recording resume | `david-anata/tape` |

**Work autonomously.** David wants this done, not a running Q&A.

- Make the reasonable call, write down what you decided, and keep going.
- Stop and ask only before something that can't be undone and could reasonably go either way, such as:
  - a destructive migration
  - deleting user data
  - changing prices in Stripe
  - sending email to real prospects for the first time

## Context to read first

1. `tape/CLAUDE.md`: Tape's stack, voice rules, hard rules, and two-dev git workflow. Read it end to end.
2. `Lead-scraper/AGENTS.md` and `DESIGN.md`: the agent's guardrails. This repo is deploy-by-default: validate, then push.
3. `tape/docs/LIVE_CALL_COACH_SPEC.md`: the live coach spec. Builds 1-5 are shipped; C and D below finish it.

## Access

| What | How |
|---|---|
| Repos | `gh repo clone david-anata/tape` and `gh repo clone david-anata/Lead-scraper` (both private) |
| Tape local | `cp .env.example .env` (values from the shared 1Password vault), then `npm install && npm start` -> localhost:3000 |
| Tape prod | https://tape.anatainc.com/healthz (every flag true). Render service `srv-d8b08vml51nc7397fltg`. `main` auto-deploys in about 2 min. |
| Tape database | Supabase `owbbbyyzbgqhatujokbx`. Migrations live in `migrations/`: numbered, idempotent, applied in order. |
| Agent prod | https://agent.anatainc.com. Vercel project `anata-agent-staging` (`prj_zBdSkP3F3H2Co1A078cWHlh7xKKD`, team `team_qqS6bS1ZSO9aUphulwduRanV`). `main` auto-deploys. |
| Agent database | Supabase `vfcmljqakphwhslxtfzv`. Vercel skips DDL, so apply schema changes as Supabase migrations: RLS on, anon and authenticated revoked, grants to `agent_app`. Also add the matching `_ensure_*` in `models/database.py`. |
| Support tickets | `GET https://tape.anatainc.com/api/admin/support-tickets` with `SUPPORT_AGENT_TOKEN` (see `tape/docs/SUPPORT_AGENT.md`). Real user-reported bugs. |
| Chrome Web Store | David uploads the zip himself; that needs his sign-in. The extension he has installed is item `geoplahocdlbagmglochankoklleefhi`. |

Secrets live only in Render env, Vercel env and 1Password. Never in chat, commits or logs.

## A. Bug sweep and fixes (Tape)

There is no bug list. You build it.

**1. Find the bugs**

- Start with every open support ticket.
- Run `npm test`.
- Drive the core flows by hand and with Playwright (works headless):
  - Screen, Cam and Both.
  - Pause, Restart and Cut.
  - The upload lands in the reel with a transcript and the read.
  - Smart trim.
  - Send the cut, then open the share link logged out and at phone width. Check the password gate, comments, reactions and watch progress.
  - Onboarding.
  - Settings and billing.
  - The workspace switcher.
  - Deal rooms.
  - Courses.
- Read prod logs on Render for repeating errors.
- Read the hot code for:
  - races
  - unhandled rejections
  - missing `await`
  - listeners that leak
  - state that doesn't survive a service worker restart

**2. Hot spots**

Most past fixes landed in the extension:

- `background.js`: the state machine. It persists to `chrome.storage.session`.
- `offscreen.js`: capture, the cam composite, MediaRecorder and the IndexedDB blob handoff. requestAnimationFrame doesn't fire in offscreen documents.
- `dock.js`: the Shadow DOM dock and cam bubble, device flyouts, and the Sales mode panel.
- `import-receiver.js`

Bug families that keep coming back, from git history:

- restart not saving
- blank recordings
- the cam composited twice in Both mode
- stale mic device IDs (AirPods)
- duplicate docks on tab revisit
- blob handoff races
- dock position resets

Prove each family still holds in 1.1.0.

**3. Log every bug** in `tape/docs/BUG_LIST.md` with:

- an ID
- the area
- severity: P0 (data loss, can't record or upload, security), P1 (core flow broken), P2 (annoying), P3 (cosmetic)
- steps to reproduce
- evidence (a ticket, a log line, or a file and line)
- the fix and its commit

**4. Fix every P0, P1 and P2.** Fix P3s while you're in that code anyway.

- One bug per commit, with a test where practical (`test/` uses node's built-in runner).
- Preserve existing behavior unless the bug is the behavior.

## B. One extension

David has two Tape copies installed: store 1.0.87 and unpacked 1.1.0 (Sales mode). The goal is one.

1. Put all extension fixes from A on top of the 1.1.0 line (`chrome-extension/`). Bump the version above anything uploaded to the store.
2. Check the manifest:
   - The Sales mode content script `agent-bridge.js` is on `https://agent.anatainc.com/*`.
   - The `tabCapture` permission is there.
   - The relay is limited to tape.anatainc.com.
   - No localhost matches and no dev leftovers.
3. Build the zip into `dist/`.
4. Update `docs/CWS_SUBMISSION.md` with:
   - the version
   - a one-line reason for each permission (tabCapture: "Captures Google Meet tab audio for live sales call coaching on agent.anatainc.com")
   - David's upload steps for item `geoplahocdlbagmglochankoklleefhi`
5. Do this early. The store review takes 1 to 3 days and shouldn't hold up the rest. Upload a first zip once the P0s are fixed, then a follow-up release later.

## C. Finish the live call coach (Lead-scraper)

Live today:

- Tape relay -> Deepgram -> agent -> Haiku coach -> the Live block on `/admin/sales/call/{tracker}`.
- After-call: scored review, ClickUp Sales CRM, recap as a Gmail draft, and the day-10 task in ClickUp.
- Calls where the prospect never spoke skip the coach and all after-call steps.

Code lives in `sales_support_agent/services/sales/call_script/`:

- `live.py`
- `coach.py`
- `after_call.py`
- `assets/runtime_adapter.js`

The routers are `api/call_script_router.py` and `api/sales_call_live_router.py`. Tests are `tests/test_sales_call_*.py`.

1. **Replay harness.** Add an admin tool that plays a stored or pasted transcript into the coach at real speed, shows each pass, and scores it against David's marks:
   - beliefs, and when each was said
   - objections
   - the offer that fit
   - the outcome

   Pass bar from the spec: the coach matches the marks on 8 of 10 calls, never ticks a belief from the rep's own words, and never cites a case outside the approved list. Seed it with any recorded sales calls in Tape. Ask David which ones count only if none are found.
2. **Shadow mode.** Add a per-rep setting (default on until about 10 calls):
   - Coach ticks show as suggestions with one-click Accept.
   - Accepts and rejects are logged so accuracy can be measured.
   - Turning it off makes ticks automatic, still undoable.
3. **Auto recap.** Recaps are a Gmail draft today; make them send through Abby.
   - If the lead is in the Instantly campaign, reply in that thread. The client is already in `abby_runner.py`: `emails/reply` on Instantly API v2.
   - Otherwise send from an Abby inbox, with the rep CC'd.
   - If the review rates the outcome unclear, keep it as a draft.
   - Keep the idempotency guarantee: never two recaps for one session.
   - First live send: send one to David first, then turn it on.
4. **Day-10 calendar hold.** Add a hold on the rep's Google Calendar next to the ClickUp task. Reuse the agent's existing Google Calendar integration (`integrations/hr_google_calendar.py` or `building_google_calendar.py`). If the rep's calendar isn't authorized, fall back to the ClickUp task and show that in the after-call steps.
5. **Trial start signal.**
   - Find where Anata Intelligence records a Seller Central connection.
   - Add a hook or poll so the day-10 date moves to trial start.
   - Until then, the call date stays the fallback. Document what you found.
6. **Retention.** Delete transcript lines and coach events older than 12 months on a scheduled job. Keep the scored review.

## D. GTM sync (Lead-scraper)

Abby's reply rules (`abby.py`) already sell the $99 offer. What's left:

1. **Cold sequence copy.**
   - Read the live Instantly campaign through the API (the key is in the agent's env).
   - Rewrite every step that still pitches the old $500 offer or the free breakdown, so it leads to the $99 Intelligence trial: 14 days, no card, starts when Seller Central connects.
   - Keep Abby's voice, plain text, no opt-out footer (David's decision), and 4-5 steps.
   - Show David the before and after, then push it to Instantly. This is the one place to pause for his OK, because it hits real prospects.
2. **Booked call opens prefilled.**
   - When a lead books through the Google booking link, match the attendee email to the Instantly lead and the ClickUp Sales CRM task.
   - Create the call tracker before the call and link it to the task.
   - Add a prospect brief at the top of the Live block:
     - name and company
     - category and revenue band from StoreLeads
     - the outreach signals
     - what Abby promised in the thread
     - the case study to prep
3. **Weekly outcome report.** Build an agent page plus a Monday Slack post. By recipe and opener, it shows leads booked, reached Ready to pitch, and started a trial.

## E. Tape known gaps

1. **AI cost caps.**
   - Read `docs/COSTS.md` first.
   - Add a per-plan `ai_seconds` limit to `PLAN_LIMITS`, enforced in `/api/transcribe` and `/api/ai`, with film-room copy when the cap is hit.
   - Pick limits that keep gross margin at or above the COSTS.md target, and write the numbers down.
2. **Long recordings.** Whisper caps uploads at 25 MB. Split the audio in the browser with the already-loaded ffmpeg.wasm, transcribe the pieces, and stitch the timestamps back together.
3. **Recording resume.** Replay preserved IndexedDB chunks after a tab refresh, replacing today's recovery toast.

Out of scope: SAML SSO, SCORM export, and anything Agency-tier (see CLAUDE.md "Explicitly dropped").

## Order

1. A (P0s first), then B's first zip. Live calls depend on the extension.
2. C1-C2 (replay and shadow mode), then C3-C6.
3. D1 (pause for David's OK on copy), D2, D3.
4. The rest of A, then E, then B's final zip.

## Rules

- Tape:
  - no new npm dependencies
  - Tabler icons only
  - Barlow fonts only
  - film-room voice
  - `public/index.html` stays one file
- Branch per batch. Pull-rebase before every push. Never force-push `main`.
- After every deploy:
  - Check prod health (`/healthz` for Tape; for the agent, check the Vercel deployment is READY and has no runtime errors).
  - Screenshot anything visual. David's rule: verify, don't assert.
- Never sign in to an account for David, and never submit to the Chrome Web Store yourself.
- No em dashes in any copy, docs or emails. Use hyphens.
- Every external write the agent makes (ClickUp, email, calendar, Instantly) is logged with an audit trail.

## Done means

- `BUG_LIST.md` has every P0-P2 fixed and verified, with evidence.
- One extension zip is in `dist/`, with upload steps ready for David.
- The replay harness passes the 8-of-10 bar, and shadow mode is live.
- Recap sends, the calendar hold is created, the trial signal is wired or documented, and the retention job is running.
- The $99 cold copy is live in Instantly after David's OK. Booked calls open prefilled, and the weekly report posts.
- AI caps, long-recording transcription and recording resume are shipped.
- All tests pass and prod health is green in both apps.
- A final report goes to David covering:
  - what shipped
  - what's open and why
  - what needs his hands: the store upload, real Meet and AirPods checks, and calendar authorization if missing
