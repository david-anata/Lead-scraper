# Live Call Coach - Implementation Spec

Oct 23, 2026 · David Narayan. Source doc: https://claude.ai/artifact/DUhcJ8sVmWqgQXyD9NqQwJ (the same spec is committed to both `david-anata/Lead-scraper` and `david-anata/tape` at `docs/LIVE_CALL_COACH_SPEC.md`).

## Summary

Six builds remain to make the Call Script run itself during a live Google Meet call. No new app: the Tape Chrome extension captures the call, Tape's server turns audio into text, the agent's coach reads it, and the Call Script page updates on its own.

**Live today:** the Call Script at agent.anatainc.com/admin/sales/call. One tracker per lead, saved as you go, manual ticking, Ask Claude, call reviews, the 25-slide deck.

**Left to build**

| # | Build | Where it lives | What it adds |
| --- | --- | --- | --- |
| 1 | Tape Sales mode | Tape Chrome extension | Captures the Meet tab (prospect) and your mic (you) and streams both while you talk |
| 2 | Live relay | Tape server (Render) | Sends the audio to Deepgram and gets back text in about a second, labelled by speaker |
| 3 | Live coach | Agent | Claude reads the conversation every 10 to 15 seconds and decides what changed |
| 4 | Live block | Call Script page | Go live button, recording indicator, auto-ticks, tie-downs, next move, Ready to pitch |
| 5 | After the call | Agent | ClickUp update, Abby recap, day-10 review, scored review, all automatic |
| 6 | GTM sync | Agent + Instantly | Tracker opens prefilled from lead gen; outbound sells the same $99 offer |

**End state on a call**

1. Open the lead in Call Script and click **Go live**. Tape starts on your Meet tab.
2. Say the call is recorded. Talk.
3. The page ticks beliefs, marks discovery sections done, writes tie-downs in their words and shows the next question.
4. When the beliefs are in, the card flips to **Pitch**: which offer, the bridge line, the slides to show.
5. Click **End call**. The review, ClickUp update, Abby recap and day-10 review happen on their own.

## How it works

Audio leaves Meet through the Tape extension, becomes text on Tape's server, and the agent's coach turns that text into updates on your Call Script page.

```
Google Meet -> Tape extension (Build 1) -> Tape relay on Render (Build 2) <-> Deepgram
                                                   |
                                                   v  text in about 1 s
Call Script page (Build 4) <- Live coach, Claude (Build 3) <- Transcript lines by speaker
        ^                                |
        | prefilled tracker              | End call
Lead gen brief (Build 6)       After the call (Build 5) -> ClickUp + Abby (CRM, recap, day 10)
```

The top row runs during the call at about one second per hop. The coach runs every 10 to 15 seconds, and the page picks up its updates every 2 seconds. Lead gen feeds the tracker before the call; the after-call steps run when you click End call.

## Build 1: Tape Sales mode (Chrome extension)

The extension already captures a tab and the mic as separate streams. Sales mode reuses that capture and, instead of saving a file at the end, streams both audio tracks to Tape's server every second.

**Start and stop**

- **Go live** on the Call Script page asks the agent for a live session and passes its token to the extension through a small bridge script on agent.anatainc.com (new content script, `agent-bridge.js`).
- The extension finds the open Google Meet tab. Chrome only lets an extension capture a tab after a click on that tab, so the Tape dock on the Meet tab shows one button: **Start live call: Jordan Lee, Acme Socks**. That click starts capture. If this turns out to be avoidable in testing, the button goes away.
- The dock shows a consent reminder at start ("Say: this call is recorded") with a **Said it** button that logs the time on the call record.
- **End call** in the dock or on the Call Script page stops both streams. Closing the Meet tab ends the session too.

**Capture details**

- **Prospect track:** the Meet tab's audio via tab capture. Tab capture mutes the tab by default, so the stream is routed back to the speakers; you must still hear the call.
- **Rep track:** your mic via the existing offscreen document. Two tracks means speaker labels are exact, with no guessing who spoke.
- **Chunks:** one recorder per track, Opus audio in WebM, sliced every 1 second and POSTed with a sequence number. Up to 30 seconds is buffered and resent if the network blips.
- **No video** in Sales mode. Saving the full call to the Tape reel is a later add-on.

**Ships as** a new extension version through the Chrome Web Store. Adding the agent site to the content script list may trigger a store review, so submit early.

**Done when**

- [ ] A Meet call with two people produces two labelled audio streams on Tape's server within 2 seconds of speech.
- [ ] You can still hear the prospect while capture runs.
- [ ] Stopping from the page, the dock, or closing the tab ends the session cleanly.
- [ ] A 45-minute call runs without the extension dropping or the service worker losing state.

## Build 2: Live relay and transcription (Tape server)

Tape's server on Render stays always on, so it holds the live connection to Deepgram that the agent (on Vercel) can't. It receives audio chunks, streams them to Deepgram, and forwards finished sentences to the agent.

**How it works**

- **Audio in:** `POST /api/live/{session}/audio?track=prospect|rep&seq=N` with the 1-second chunk as the body. Chunks are applied in order by sequence number.
- **Auth:** the live token from the agent, an HMAC-signed token carrying session id, tracker id, rep email and a 3-hour expiry. Both services share one secret, `LIVE_SESSION_SECRET`, set on Render (Tape) and Vercel (agent). No Tape login is needed for Sales mode.
- **Transcription:** one Deepgram streaming connection per track (Nova-3, smart formatting, endpointing around 300 ms). Silence keep-alives hold the connection; 60 seconds with no audio closes the session.
- **Text out:** finished utterances (speaker, text, start and end time) are batched and POSTed to the agent every 1 to 2 seconds, server to server with the same shared secret.
- **No new packages.** Tape's rule is Express and jsonwebtoken only. Node 22 has a built-in WebSocket client, so Render's `NODE_VERSION` moves from 20 to 22.
- **Health:** `/healthz` adds `hasDeepgramKey` and the count of live sessions, which also confirms the key already added.
- **Usage:** minutes per session are added to Tape's existing usage counters, so transcription cost is visible.

**Resilience**

- Sessions live in memory on the one Render instance. If it restarts mid-call, the extension keeps resending and the relay reopens Deepgram; text already sent to the agent is kept.
- Two or three calls at once is fine on the current plan.

**Done when**

- [ ] Speech on either track shows up at the agent as a labelled line within 2 seconds.
- [ ] Accuracy on a test call is good enough that names, numbers and product terms come through (checked against Tape's existing after-call Whisper transcript).
- [ ] Killing the network for 10 seconds loses no text once it comes back.
- [ ] `/healthz` reports the Deepgram key.

## Build 3: Live sessions and the coach (agent)

The agent owns the call: it starts the live session, stores every line of transcript, and runs the coach that turns the conversation into tracker updates.

**Session lifecycle**

1. **Go live** creates a session for the tracker and returns the live token (Build 2).
2. Tape's server posts transcript lines to `POST /api/sales-call/live/{session}/lines`. This route sits outside /admin and is checked with the shared secret, not a login.
3. **End call** closes the session and hands the full transcript to the after-call steps (Build 5).

**When the coach runs**

The Call Script page asks for updates every 2 seconds. On one of those requests, if at least 10 seconds have passed and new lines arrived, the agent runs a coach pass (Claude takes about 2 to 5 seconds) and returns the result. A database lease stops two passes running at once. This page-driven loop fits Vercel, which can't run a background worker for the length of a call. If the page is closed, coaching pauses and the after-call review still covers the call.

**What the coach reads**

- **Fixed (cached once per call):** the 17 sections and their goals, the 7 beliefs, the pain, objection and reservation lists, the offer rules, the approved case studies, David's POV lines, and the coach rules from the Sophie walkthrough.
- **Changing:** the full transcript so far (a 45-minute call is about 12,000 tokens), the tracker as it stands including what the rep ticked or unticked, minutes elapsed, and the last pass's output.

**What the coach returns (one JSON update)**

| Field | What it drives |
| --- | --- |
| Sections done, with the line that covered each | Discovery checkmarks |
| Beliefs: said, partial or not yet, with the prospect's quote | Belief Ladder ticks |
| Numbers: revenue, ad spend, ACoS/TACoS, how long, goal and date | Gap fields and calculators |
| Pains, objections, reservations (existing keys) | Raised tab and talk flow |
| The three things: problem, gap, cost of doing nothing | Discovery status |
| Tie-downs in their words, with how many times each was used | Tie-down list |
| Next move: ask, handle, tie down, move on, pitch or close, with the line to say | Next move card |
| Pitch: ready or not, offer pick, bridge line, slides, case study | Pitch card |
| Temperature: asked or not, score, clear or vague, follow-up | Close guard |
| Warnings, e.g. pitching before minute 20 | Nudges |

**Rules checked in code after Claude answers**

- Only the prospect's track can tick a belief, and the quote must appear in a prospect line.
- The rep's clicks win: the coach never re-ticks something the rep unticked or unticks something the rep ticked.
- Numbers only fill empty fields; the rep can always overwrite.
- Proof lines must come from the approved case list. No em dashes.
- Every pass is stored with a revision number, so a call can be replayed and audited.

**Model and cost:** a fast model per pass (Haiku 4.5 by default, switchable to Sonnet in settings if replay tests show it reads calls better). With prompt caching, estimated at roughly $1 to $2 per 45-minute call, to be measured in testing.

**Done when**

- [ ] Updates reach the page within 15 seconds of the prospect saying something that matters.
- [ ] On the replay test set (see Testing and rollout), the coach catches the beliefs and objections David would mark himself on at least 8 of 10 calls.
- [ ] Manual ticks are never overridden.
- [ ] Two passes never run at once for the same call.

## Build 4: The Live block in the Call Script page

The script text never changes. What's new is a Live block at the top of the Talk flow tab and live state across the page, all driven by the coach's updates.

**The Live block**

- **Before the call:** a **Go live** button, a check that the Tape extension is installed (with an install link if not), and the prospect brief from lead gen (Build 6).
- **During the call:** a recording dot and timer, the consent reminder until the rep clicks Said it, minutes in discovery against the 20-minute target, and **End call**.
- **Next move card:** one action at a time, the line to say, and a button that jumps to that section of the script.
- **Pitch card:** appears when the beliefs are in. Shows the offer ($99 Starter, Growth, Pro or full service), the bridge line built from their pain and goal, the 2 to 4 slides to show (one click opens them in the audience window), and the case study for their category.
- **Close guard:** after the temperature check, shows the score and whether the answer was clear or vague. Vague or under 8 shows the follow-up and points back to objections.
- **Last 3 lines** of the live transcript, so the rep can see it's hearing the call.

**Live state across the page**

- Belief buttons tick with a small quote underneath. Click to undo; the coach then leaves that belief alone.
- Discovery sections 1 to 13 in the left sidebar get a checkmark when covered.
- Gap numbers and the calculators fill from what the prospect says.
- Tie-downs list each pain in their words with a counter (target 5 to 6 uses) and a final close line.
- Coach-filled items are marked so the rep can tell them from what they typed.

**How it's wired:** a small live module added to the adapter that already runs the script in the agent. It polls for updates every 2 seconds and applies them through the script's existing apply-call function (`SS.applyCall`), the same one the after-call review uses. If polling fails, the page keeps working manually and shows "Live paused".

**Done when**

- [ ] A whole call runs from one screen while sharing only the deck's audience window in Meet.
- [ ] Every coach change is visible, explainable (quote shown) and undoable in one click.
- [ ] Reloading the page mid-call reconnects to the same live session.

## Build 5: After the call

**End call** triggers four steps in order, each logged on the call record with a retry if a service is down. Nothing waits on the rep.

1. **Scored review.** The agent runs the same review the script's End call & review uses, on the full stored transcript, and saves it to the coaching dashboard: score, beliefs said before the pitch, minutes in discovery, temperature answer, what to fix next call.
2. **ClickUp Sales CRM.** Finds the lead's task in the Sales CRM list (anata Sales space, Sales folder) by the lead link on the tracker, then by email or company; creates one if none exists. Updates the custom columns: stage, outcome (trial started, full service, follow-up), offer quoted, beliefs said, pains, objections, next step, call link. Adds a comment with the call summary.
3. **Abby recap.** Abby writes a short recap in the prospect's own words with the Seller Central connect link to start (or finish) the 14-day trial, CC'ing the rep. If the lead came through the Instantly campaign it goes in the same email thread; otherwise from an Abby inbox. It sends automatically like Abby's other replies; if the coach rates the call outcome as unclear, it waits as a draft for the rep.
4. **Day-10 review.** A ClickUp task due 10 days after the trial starts, and a hold on the rep's calendar. The recap includes the booking link so the prospect can confirm the time.

**Trial start signal:** "10 days after the trial starts" needs Anata Intelligence to tell the agent when the prospect connects Seller Central. Until that signal exists, the call date is used.

**Done when**

- [ ] Within 2 minutes of End call, the review, ClickUp update, recap and day-10 task all exist, with no manual step.
- [ ] A failure in one step (say ClickUp is down) doesn't block the others and shows a retry on the call record.
- [ ] Re-ending or reloading never creates a duplicate ClickUp task or a second recap email.

## Build 6: GTM sync

One offer from first email to close: the cold email, Abby's replies, the booked call and the pitch all sell the $99 Intelligence trial, with full service as the step-up.

**Outbound on the $99 offer**

- Rewrite the live Instantly sequence and Abby's reply rules from the $500/mo offer to: Anata Intelligence from $99/mo, 14-day free trial, no card, starts when Seller Central connects.
- Abby's answers to price, trial and "what do I get" match the Call Script's pricing section word for word, so nothing on the call contradicts the email.

**Booked call opens prefilled**

- When a lead books through the Google booking link, the agent matches the attendee's email to the Instantly lead and the ClickUp Sales CRM task, and creates the tracker for that lead before the call.
- The prospect brief at the top of the Live block shows: name, company, category and revenue band from StoreLeads, the signals that triggered outreach, what Abby promised in the thread, and the case study to prep for their category.
- The tracker is linked to the ClickUp task, which is what Build 5 updates after the call.

**Outcomes back into lead gen**

A weekly report in the agent shows, by recipe and opener, how many leads booked, reached Ready to pitch, and started a trial. StoreLeads filters and Instantly copy get tuned on closes, not reply rate.

**Done when**

- [ ] A booked call has a prefilled tracker waiting, linked to its ClickUp task, before the call starts.
- [ ] Instantly copy and Abby's rules say $99 everywhere the offer appears.
- [ ] The weekly report shows booked, pitched and trial-started by recipe.

## Data and API contracts

Three new agent tables, added the same way as the Phase 1 tables (Supabase migration, row-level security on, access for the agent's database role only).

| Table | Key columns | Holds |
| --- | --- | --- |
| sales_call_live_sessions | id, tracker_id, owner_email, status, started_at, ended_at, consent_at, after_call_json | One live call: state, consent time, after-call step results |
| sales_call_transcript_lines | id, session_id, speaker (prospect or rep), text, start_ms, end_ms | Every finished line from Deepgram |
| sales_call_coach_events | session_id, rev, patch_json, model, latency_ms, created_at | Every coach pass, for replay and audit |

**Agent endpoints (new)**

| Method and path | Called by | Purpose |
| --- | --- | --- |
| POST /admin/sales/call/{tracker}/live | Call Script page | Start a session; returns the live token |
| GET /admin/sales/call/{tracker}/live/state?rev=N | Call Script page, every 2 s | Latest coach update and last lines; may run a coach pass |
| POST /admin/sales/call/{tracker}/live/consent | Page or extension | Log "Said it" |
| POST /admin/sales/call/{tracker}/live/end | Page or extension | End the session and start the after-call steps |
| POST /api/sales-call/live/{session}/lines | Tape server | Add transcript lines (shared secret) |
| POST /api/sales-call/live/{session}/ended | Tape server | Session closed from Tape's side |

**Tape server endpoints (new)**

| Method and path | Called by | Purpose |
| --- | --- | --- |
| POST /api/live/{session}/audio?track=&seq= | Tape extension | 1-second audio chunk |
| POST /api/live/{session}/stop | Tape extension | Close both Deepgram streams |

**Environment settings**

| Setting | Where | Status |
| --- | --- | --- |
| DEEPGRAM_API_KEY | Render (Tape) | Added Oct 23 |
| LIVE_SESSION_SECRET | Render (Tape) and Vercel (agent), same value | To add |
| AGENT_BASE_URL | Render (Tape) | To add (https://agent.anatainc.com) |
| NODE_VERSION = 22 | Render (Tape) | Change from 20 |
| SALES_CALL_LIVE_MODEL | Vercel (agent) | Optional; defaults to Haiku 4.5 |
| ANTHROPIC_API_KEY | Vercel (agent) | Confirm: the Call Script list page shows a notice if it's missing |

## Testing and rollout

The coach is tested on recorded calls before it ever runs on a live one, then runs in shadow mode before anyone relies on it.

**1. Replay harness (before any live call)**

- A test tool in the agent plays a recorded call's transcript into the coach at real speed, exactly as live lines would arrive, and shows each pass.
- Test set: 5 to 10 real sales calls (Tape recordings or others), each marked by David with the right answers: beliefs said and when, objections, the offer that fit, and the outcome.
- Pass bar: the coach matches those marks on at least 8 of 10 calls, never ticks a belief from the rep's own words, and never quotes a case study outside the approved list.

**2. Mock calls**

Three to five role-play calls on Meet with scenario cards (easy $99 close, "too expensive", partner, already using a tool or agency, full-service fit). This checks the whole chain: extension, relay, coach, page.

**3. Shadow mode on real calls**

The coach runs live, but its ticks show as suggestions the rep accepts with one click. After about 10 calls, compare: how often suggestions were accepted, how often they were early or wrong.

**4. Full live**

Suggestions become automatic ticks (still undoable). The scored reviews keep measuring: beliefs before the pitch, minutes in discovery, temperature answers, first-call closes.

**Automated tests** for every build: unit tests on token signing, line ordering, the coach rules and the after-call steps, and a browser test that runs a recorded call end to end.

## Build order, risks and open questions

Each step ships on its own branch and goes live only after David's "merge it".

**Order**

1. **Capture spike (first, small):** confirm on David's Chrome how the Meet tab capture has to start and that he still hears the call. This decides the Go live flow in Build 1.
2. **Builds 1 and 2 together:** Tape Sales mode and the relay. Milestone: labelled lines from a test Meet call land in the agent.
3. **Build 3 plus the replay harness,** in parallel with step 2 since it runs on recorded transcripts. Milestone: passes the replay test set.
4. **Build 4:** the Live block. Milestone: a mock call runs end to end from one screen.
5. **Mock calls, then shadow mode** on real calls.
6. **Build 5:** after-call automations.
7. **Build 6:** prospect brief and outcome report. The outbound copy switch to $99 doesn't depend on anything above and can happen any time.

**Risks**

| Risk | Mitigation |
| --- | --- |
| Chrome requires a click on the Meet tab to start capture | One-click Start button in the Tape dock on the Meet tab; confirmed in the spike |
| Chrome Web Store review delays the extension update | Install the test build directly on David's Chrome first; submit to the store early |
| Amazon terms (ACoS, TACoS, ASIN) misheard | Deepgram keyterm list with Amazon and Anata vocabulary |
| Coach ticks something too early | Rules in code, quotes shown, one-click undo, shadow mode first |
| Tape's single Render instance restarts mid-call | Extension resends, relay reconnects, text already at the agent is kept |
| Recording consent in two-party-consent states | Verbal statement (David's decision) with the Said it log on every call |
| Call transcripts are client data | Private to the agent only, never used for public content or the website chatbot |

**Open questions for David**

- [ ] Which recorded sales calls can go in the replay test set?
- [ ] ClickUp Sales CRM: confirm the custom column names to fill after the call.
- [ ] Abby recap: send automatically from day one, or as drafts for the first week?
- [ ] Trial start: where does Anata Intelligence record that a prospect connected Seller Central, so it can notify the agent?
- [ ] Day-10 hold: which Google calendar, and is the agent already connected to it?
- [ ] Retention: how long to keep call transcripts (for example 12 months)?

## Handoff to Claude Code

Two repos, each with its own deploy. Every build names its repo and the files to touch, so a Claude Code session can start from this doc alone.

**Repos and where they deploy**

| Repo | What it is | Deploys to | Read first |
| --- | --- | --- | --- |
| david-anata/tape | Tape: Chrome extension (`chrome-extension/`) and the Express server (`server.js`) | Render service srv-d8b08vml51nc7397fltg (tape.anatainc.com), auto-deploys from main; extension ships through the Chrome Web Store (`docs/CWS_SUBMISSION.md`) | `CLAUDE.md` |
| david-anata/Lead-scraper | The agent (FastAPI, `sales_support_agent/`), including the live Call Script | Vercel project anata-agent-staging (agent.anatainc.com), team david-narayans-projects, auto-deploys from main | `AGENTS.md`, `DESIGN.md`, `.claude/skills/rules` |

**Databases**

- Agent: Supabase project anata-agent-staging (vfcmljqakphwhslxtfzv). The Vercel runtime never creates tables, so new tables are applied as a Supabase migration (RLS on, revoke anon and authenticated, grant select, insert, update, delete to agent_app), and also added to `_ensure_sales_call_tables` in `sales_support_agent/models/database.py` for local and Render boots. The Phase 1 tables sales_call_trackers and sales_call_reviews show the pattern.
- Tape: Supabase project tape (owbbbyyzbgqhatujokbx). Sales mode needs no new Tape tables beyond usage counters.

**Files per build**

| Build | Repo | Files |
| --- | --- | --- |
| 1 Sales mode | tape | `chrome-extension/manifest.json` (add agent.anatainc.com to content scripts, version bump), new `chrome-extension/agent-bridge.js`, `background.js` (live session state, start and stop), `offscreen.js` (two recorders, 1 s slices, tab audio routed back to speakers), `dock.js` (Start live call button, consent prompt, End call) |
| 2 Live relay | tape | `server.js` (`/api/live/*` routes, Deepgram streams, forwarding to the agent, `/healthz` flags), `render.yaml` (NODE_VERSION 22, new env vars), `.env.example`, `docs/COSTS.md` (transcription cost) |
| 3 Live coach | Lead-scraper | `sales_support_agent/models/entities.py` and `models/database.py` (3 tables), new `services/sales/call_script/live.py` (sessions, signed tokens), new `services/sales/call_script/coach.py` (prompt, JSON schema, rules in code), `api/call_script_router.py` (live endpoints under /admin/sales/call), new `api/sales_call_live_router.py` for `/api/sales-call/live/*` (shared secret, registered in `sales_support_agent/main.py`), replay harness, tests in `tests/` |
| 4 Live block | Lead-scraper | `services/sales/call_script/assets/runtime_adapter.js` (live polling, applies updates through the script's existing `SS.applyCall`), `services/sales/call_script/assets/call_script.html` (Live block in the Talk flow pane, sidebar checkmarks; script text unchanged) |
| 5 After the call | Lead-scraper | new `services/sales/call_script/after_call.py`, using `integrations/clickup.py`, `integrations/gmail.py` and the Instantly client; check the Abby reply engine's current state in the Sales GTM build spec before wiring the recap |
| 6 GTM sync | Lead-scraper + Instantly | Instantly sequence copy and Abby's reply rules (`services/instantly_webhooks.py`, `services/reply_templates.py`), booking match and prospect brief in `services/sales/call_script/`, weekly outcome report |

**Source of truth for content:** the offer, pricing, proof and coach rules already live in `sales_support_agent/services/sales/call_script/assets/call_script.html` (Lead-scraper). The coach prompt is built from that file, not retyped.

**House rules for every session**

- One branch per build. Nothing merges to main without David's "merge it". Pull and rebase before pushing; never force-push.
- Tape: no new npm dependencies, film-room voice in UI copy, screenshot visual changes before calling them done.
- Agent: follow `DESIGN.md`; run the full test suite and compare against main, which has 44 known failing tests that predate this work.
- No em dashes in any copy.

**Kickoff prompt (paste into Claude Code)**

```
Read docs/LIVE_CALL_COACH_SPEC.md (in both david-anata/Lead-scraper and david-anata/tape), then CLAUDE.md in david-anata/tape and AGENTS.md + DESIGN.md in david-anata/Lead-scraper.
Start with step 1 of Build order: a capture spike in the Tape extension that proves the Meet tab audio and mic can be captured as two tracks while I still hear the call, and confirms what click Chrome requires to start it.
Work on a branch, report what you found, and wait for my "merge it" before anything reaches main.
```
