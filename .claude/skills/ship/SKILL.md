# /ship — Build, Verify & Report

**Description:** Build an approved implementation spec using a Workflow for maximum parallel execution, verify with tests and browser automation looping fixes until clean, then give an honest completion report. Use after /spec has been approved.

---

## GLOBAL OPERATING RULES

- Think before acting.
- Plan before building.
- Verify before declaring success.
- Never claim completion without evidence.
- Prefer parallel execution whenever tasks are independent.
- Preserve existing functionality unless explicitly instructed otherwise.
- If assumptions are required, state them clearly.
- If a better architecture is discovered during implementation, stop and present it before continuing.
- Optimize for maintainability, scalability, and clarity — not just speed.
- Treat every completed project as an opportunity to improve future workflows and skills.

---

## STEPS 3–5 — Build, Verify & Report

Using the approved implementation specification:

---

## Step 3 — Launch a Workflow

Do not write code directly. Do not call Agent one at a time. Invoke the `Workflow` tool with a script that orchestrates the entire build phase.

**How to structure the Workflow script:**

1. Decompose the spec into the maximum number of independent workstreams
2. Use `parallel()` for workstreams that touch different files and have no dependency on each other
3. Use `pipeline()` for workstreams that feed into each other (output of stage N is input of stage N+1)
4. Use a barrier (`await parallel(...)` before proceeding) only when a later stage genuinely needs ALL prior results together
5. Each `agent()` call gets a fully self-contained prompt — file paths, interfaces, expected inputs/outputs, and explicit ownership of exactly which files it may write

**Workflow script template:**

```javascript
export const meta = {
  name: 'ship-[project-name]',
  description: 'Build [project name] per approved spec',
  phases: [
    { title: 'Build', detail: 'parallel implementation agents' },
    { title: 'Verify', detail: 'tsc + tests + Chrome MCP' },
    { title: 'Fix', detail: 'parallel fix agents per issue found' },
  ],
}

// Phase 1: Build — fan out to maximum parallelism
phase('Build')
const results = await parallel([
  () => agent(`
    You are the backend agent. Implement [X].
    Files you own: [list exact paths]
    Do NOT touch any file outside this list.
    Interface contract: [inputs, outputs, API shape]
    Return: what you built, what you changed, any blockers.
  `, { label: 'backend:X', phase: 'Build' }),

  () => agent(`
    You are the frontend agent. Implement [Y].
    Files you own: [list exact paths]
    Do NOT touch any file outside this list.
    Interface contract: [props, API endpoint it calls, expected response shape]
    Return: what you built, what you changed, any blockers.
  `, { label: 'frontend:Y', phase: 'Build' }),

  () => agent(`
    You are the test agent. Write tests for [Z].
    Files you own: [list exact test file paths]
    Do NOT touch any file outside this list.
    Return: test names, what each covers, pass/fail.
  `, { label: 'tests:Z', phase: 'Build' }),
])

const built = results.filter(Boolean)
log(`Build complete: ${built.length} workstreams finished`)

// Phase 2: Verify
phase('Verify')
const verification = await agent(`
  Run full verification:
  1. cd frontend && npx tsc --noEmit — report exact errors
  2. Run affected pytest tests — report pass/fail
  3. Open dashboard.anatainc.com in Chrome MCP, navigate to [section], screenshot each tab
  4. Check console for JS errors (TypeError, ReferenceError, Uncaught only)
  Return: structured list of issues found, or "clean" if none.
`, { label: 'verify', phase: 'Verify' })

// Phase 3: Fix any issues found — fan out again
phase('Fix')
if (verification && verification !== 'clean') {
  const issues = verification.issues || []
  await parallel(issues.map(issue => () => agent(`
    Fix this specific issue: ${issue.description}
    File: ${issue.file}
    Do NOT touch any other file.
    After fixing, confirm the fix is correct.
    Return: what changed and why.
  `, { label: `fix:${issue.file}`, phase: 'Fix' })))
}

return { built, verification }
```

**Parallelism rules (learned in practice):**

Safe to parallelize:
- Multiple test files for disjoint widgets
- Backend endpoint + frontend component when they share only an API contract, not a file
- Read-only research agents alongside write agents on different files

Must serialize (use `pipeline()` or sequential `await`):
- Anything touching the same large file (`spec_performance_profit.py`, `app.js`, `index.tsx`) — conflicts are certain
- The section orchestrator (`index.tsx`) — only one agent owns it; others hand off file copies
- Any agent whose output is required input for the next agent

**Worktree agent output — always copy to main:**
Workflow agents run in isolated worktrees. After they complete, copy output files into the main repo:
```bash
cp .claude/worktrees/agent-<id>/path/to/file.tsx path/to/file.tsx
```
Do NOT re-run tests in main before copying — they won't be there yet.

**Extracting orphaned endpoint code from worktrees:**
When a worktree adds to a large shared file, use line count diff to find the new lines:
```bash
echo "$(wc -l < worktree/file.py) - $(wc -l < main/file.py)" | bc
```
Append only the new function(s) to main's file. Never overwrite the whole file.

---

## Step 4 — Continuous Verification & Optimization

After the Workflow completes, review what the verify phase found.

### THE THREE-PASS RULE (mandatory before anything goes live or to David for QA)

Nothing gets pushed live and nothing is handed to David for QA until the verification loop has passed **three consecutive times with zero findings**:

- **Pass 1 — Code gate:** `cd frontend && npx tsc --noEmit` (0 errors) + affected vitest/pytest suites green + a self-review of the full diff.
- **Pass 2 — Integration gate:** re-run the code gate from a clean read of the files on disk (not memory — iCloud/linters may have reverted edits), curl every new/changed endpoint and confirm response shape, confirm every changed widget mounts.
- **Pass 3 — Workflow gate (always the final pass):** walk the ACTUAL user workflow end-to-end in Chrome MCP the way David would click it — every entry point, every state (empty, loading, error, happy path), screenshots at each step, console clean. At every step ask: "does the content and direction make sense to the person seeing it?" A button that can't succeed, a label that lies, an approve with nothing to review — each is a FINDING even when all code gates are green.

Any finding at any pass → fix it → **the counter resets to zero**. Three greens in a row, then push and hand off with the evidence (screenshots + what was exercised).

Why this exists (2026-07-06): a "Brief saved · Approve" flow shipped where the brief was never visible in action mode — reviewers were asked to approve changes they couldn't see. tsc and tests were green; only walking the real workflow caught it.

If issues remain after the first fix pass, launch another Workflow (or targeted `Agent` calls) to fix them in parallel. Re-run verification after every fix pass — and restart the three-pass counter.

Never stop after one validation pass.

Continue until one of two conditions exists:

**A.** Everything requested has been successfully implemented and verified.

**OR**

**B.** A true blocker prevents completion — something that requires information not in the spec, a destructive infra change, or a decision with real tradeoffs. Surface it explicitly and stop.

Do not silently accept failures.

### TypeScript common gotchas

**`span` prop mismatch**: Before passing `span="span-full"` to any widget, check its TypeScript signature. Components with `(): JSX.Element` (no args) will cause tsc errors. Fix: remove the span attribute from the call site.

**ECharts in vitest**: Always mock EChart: `vi.mock("../../../lib/EChart", () => ({ EChart: () => null }))` — ECharts requires a canvas environment that jsdom doesn't provide.

**`useFilters: true` vs omitted**: The What-If Calculator and similar sandbox widgets intentionally omit `useFilters: true` because they are not filter-responsive. This emits a console warning in tests — it is correct behavior, not a bug.

### Chrome MCP verification rules

- KPI strip API errors on test/dev clients (e.g. "Could not load profit summary") are pre-existing data states, not regressions. Only chase console JS errors (TypeError, ReferenceError, Uncaught).
- Navigate to each sub-page tab and confirm the section title and at least one widget title renders before marking a tab as verified.
- Console tracking starts on first `read_console_messages` call. Refresh or navigate after connecting to capture load-time errors.

---

## Step 5 — Completion Report

Only declare completion after verification passes.

**If everything requested has been completed:**

Respond with:

> 💯 COMPLETE

Then summarize:
- What was built
- What was verified
- Improvements made during optimization
- Tests performed
- Recommendations

**If the project is NOT 100% complete:**

Do NOT claim success.

Instead provide:

**Remaining Work**

For every incomplete item explain:
- What remains
- Why it remains
- What prevented completion
- What would be required to finish it
- Estimated complexity

Be completely transparent. The goal is to always know exactly what percentage of the project is actually complete.

---

## Your QA Checklist

After changes are deployed to production, here is exactly where to go and what to check.

**Production app:** `dashboard.anatainc.com`
**Backend:** `amazon-sp-api-platform.onrender.com`
**Deploy trigger:** Push to `main` — Render auto-deploys; watch for `preDeployCommand: alembic upgrade head` to complete before testing

For every change shipped, the completion report will include a tailored QA checklist in this format:

> **Your QA Checklist**
>
> **Where to go:**
> `dashboard.anatainc.com` → [Section] → [Sub-page or tab]
>
> **What to do:**
> 1. [Specific action — click X, filter by Y, scroll to Z]
> 2. [What to look for — e.g. "table loads with data," "badge appears," "no console errors"]
> 3. [Edge case to exercise — e.g. "switch workspace and confirm it reloads correctly"]
>
> **What good looks like:**
> [Exact expected outcome]
>
> **Red flags to watch for:**
> - [Specific failure modes relevant to this change]
> - Console errors (open DevTools → Console tab before navigating)
> - Blank widgets or indefinite spinners
> - Data that looks stale or wrong after a filter change

**How to open DevTools:** Chrome → right-click anywhere → Inspect → Console tab. Keep it open while navigating so you catch errors on load.

**If something looks wrong:** Note the URL, the widget name, and what you saw vs. what you expected — that's enough to diagnose in the next session.

---

After the report, run /learn.

---

## Lessons Learned

### Lesson 1 — Workflow tool can't embed TypeScript (Image Optimizer build, 2026-07-07)
The Workflow JS script parser rejects embedded TS/TSX content that contains template literals with unicode escape sequences. If a build has `.tsx`/`.ts` file bodies to write, skip the Workflow entirely and use `Write`/`Edit` directly, or have the workflow agents write their own files without passing file content through the orchestrator script.

### Lesson 2 — `vi.stubGlobal` not `global.fetch` in vitest
`global.fetch = vi.fn()` causes `TS2304: Cannot find name 'global'` because this project's tsconfig targets DOM, not Node. Always use `vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) }))` instead.

### Lesson 3 — Render static site deploy lag requires bundle hash check
After pushing to `main`, the backend redeploys automatically but the frontend static bundle on `dashboard.anatainc.com` is a separate host. A hard refresh (Cmd+Shift+R) is required to bust the browser cache. During Chrome MCP verification, always confirm the bundle hash in the HTML source has changed before testing new UI — old bundles produce false negatives that look like code regressions.

### Lesson 4 — Drawer close button may be off-screen in Chrome MCP screenshots
Chrome MCP screenshots are scaled (1644x946 viewport → 1456x838 screenshot coords). A close button at the far right of a 600px drawer (positioned from the right viewport edge) lands at ~x=1437 in screenshot space — clicking x=1321 misses it. Use overlay-click (click the dimmed area to the left of the drawer panel) as the reliable close mechanism when testing drawers in Chrome MCP.

### Lesson 5 — Event-driven drawer pattern is the proven singleton approach
`window.dispatchEvent(new CustomEvent(EVENT_NAME, { detail: {...} }))` + `window.addEventListener` in `useEffect` with cleanup is the correct pattern for singleton drawers in this codebase. No context, no prop drilling. Confirmed working for: `AsinDrawer`, `ActionDrawer`, `ImageOptimizerDrawer`. Reuse this pattern for all future drawers.

### Lesson 6 — iCloud-sync reverts in-place edits mid-build; build multi-file work in a /tmp clone
The repo lives in an iCloud-synced folder that silently reverts tracked-file edits (and can flip HEAD) mid-session — see memory `project_synced_folder_git_hazard.md`. It recurred 2026-07-07: a ~10-file dayparting build had EVERY tracked-file edit reverted while only new untracked files survived. Signature: `git status` shows no `M` for files you just edited, and greps for your added markers return 0. **Remedy that works:** `git clone $(git remote get-url origin) /tmp/<name>`, `ln -sfn <synced>/frontend/node_modules /tmp/<name>/frontend/node_modules` (so tsc/vitest run without a fresh `npm ci`), re-apply edits there (a Python string-replace patcher with exact anchors is fastest and fails loudly on drift), run gates, commit + push from the clone. For ANY change touching more than ~2 files, prefer the clone from the start, or commit-fast after each small batch. After pushing, `rm` any orphaned untracked new files from the synced copy so David's next `git pull` isn't blocked.

### Lesson 7 — Backend tests live in the TOP-LEVEL `tests/` dir, not `amazon_sp_api_platform/tests/`
Both dirs exist. The main suite (`test_dayparting_optimizer.py`, `test_ads_automation_config.py`, `test_ads_budget_guardrails.py`, …) is in the repo-root `tests/`; `amazon_sp_api_platform/tests/` holds a smaller set. Grepping only the package dir for "is there an existing test?" gives a false negative and risks shipping a change that breaks the real suite. Check `ls tests/` before concluding a module is untested.

### Lesson 8 — Verify a migration's LIVE apply via the `/_rev` flip when no local Postgres exists (competitor-dedup ship, 2026-07-08)
This project has **no local Postgres and no Docker** — the backend test harness uses in-memory SQLite (`tests/conftest.py`). So a migration using Postgres-only SQL (`DELETE … USING`, `ALTER TABLE … ADD CONSTRAINT`) **cannot be applied or tested locally**, and prod Supabase is off-limits. Pass 1 can only prove the model/constraint creates cleanly on SQLite (via `Base.metadata.create_all`, which the passing suite exercises) and that `alembic heads` linearizes to one head — it CANNOT prove the migration runs. The migration's real integration test is Render's `preDeployCommand: alembic upgrade head`, which runs **before** the new version goes live. To turn that into an observable Pass-2 signal: bump `_REVISION_MARKER` in `amazon_sp_api_platform/main.py` (~line 1132) in the same push, then poll `https://amazon-sp-api-platform.onrender.com/_rev` until it flips to the new marker (~150s on starter plan). The flip proves preDeploy — hence the migration — succeeded; a broken migration stalls the deploy on the OLD marker. Poll in the background so you're not blocking. Always make migrations idempotent + guard `create_unique_constraint`/`create_table` with an inspector check (`_constraint_exists`) — on a fresh DB, `create_all` may have already stamped the object from the model, and a blind ADD raises DuplicateObject and stalls the deploy (see memory `project_deploy_hazard_createall_vs_alembic`).

### Lesson 9 — Verify a resumed handoff's "not built / remaining" claims against disk BEFORE building (same session)
A `/context-handoff` prompt (and the project memory it was built from) can be **stale** — a parallel session may have shipped the exact work the handoff sends you to build. This session's handoff said "Phase 3 adaptive milestones never built"; a `grep` of the import site + `git log -- <file>` showed it shipped hours earlier in commit `280571c3`, merged to main. **Before writing any code for a handoff's "next task": (1) `grep` for the target symbol/import, (2) `git log --oneline -- <expected file>`, (3) `git merge-base --is-ancestor <commit> HEAD` to confirm it's really merged.** Then correct the stale memory so the next session doesn't repeat the dead end. Same discipline as [Lesson: verify agent files after a Workflow] — trust disk state, not narrative.

### Lesson 10 — A "backend-only" change with a visible surface STILL needs the Pass-3 live walk; and match the fix to the observed symptom (strategy prod-readiness ship, 2026-07-08)
Two costly misses traced to one root cause: the earlier competitor-dedup ship (9b19b0ea) was declared done on code gates + a migration `/_rev` check — **but its output surface (the competitor table) was never live-walked**, because it looked "backend-only." The production-readiness walk a ship later found the fix was **necessary but insufficient**: David's original "same competitor appears twice" was *parent-variant* duplication (different child ASINs carrying identical `Parent Level Revenue`/BSR/units), which a `competitor_asin`-keyed dedup can't touch — the right key was a `parent_signature(brand,bsr,revenue,units)`. Takeaways: (1) if a change alters what a user-visible widget shows — even via pure backend/data logic — it is NOT exempt from Pass 3; walk the widget with real data. (2) When fixing "X appears twice / looks wrong," first reproduce the exact symptom and identify *what makes the rows duplicate* before choosing a dedup key — don't fix your model of the bug. (3) Data-cleaning fixes ripple downstream: deduping competitors changed the p75 goal (96.7K→33.7K) and rescaled the whole plan — verify the dependent chain, not just the table. (4) Derived $ figures shown to users (milestone budgets came out 20–40× too high while all tests passed) need **magnitude/plausibility assertions** in tests, not just structural ones, plus an eyeball at real scale — a green suite proved the strings formatted, not that $57,000/mo retargeting on a $49k/mo product was sane.

### Lesson 11 — A frozen tab is a FRONTEND loop, not a backend hang; suspect unmemoized hook returns in effect deps (VoC refinements ship, 2026-07-08)
The VoC page froze the browser on load. It *looked* like a backend hang: `/voc/asins` + several other endpoints sat "pending," Chrome MCP screenshots timed out ("script injection timed out — page busy"), and CDP `Runtime.evaluate` timed out ("renderer frozen"). But `curl` of the same endpoints returned in <0.5s and every candidate DB query EXPLAIN-ANALYZE'd sub-millisecond — so the backend was fine; the renderer's main thread was pinned by an infinite React render loop. Root cause: **`useWidgetData` returns a NEW `refetch` function on every render (it is not memoized — `refetch: () => { void query.refetch(); }` at useWidgetData.ts:228).** A `useCallback` that listed `refetch` in its deps therefore got a new identity every render, and a `useEffect` depending on that callback re-ran every render → `setState` → re-render → new `refetch` → loop. **Diagnosis discipline:** when a page hangs, first `curl` the endpoints and time the DB queries — if those are fast, it's frontend; a frozen renderer (CDP eval timing out) confirms it. **Fix pattern:** hold the unstable value in a ref (`const refetchRef = useRef(refetch); refetchRef.current = refetch;`) and keep the dependent `useCallback`'s deps `[]`. **Why tests missed it:** the vitest mock returns a *stable* `refetch` (same `vi.fn()`), so the loop only manifests with the real hook — add a regression test that returns a FRESH `refetch` each render (`mockImplementation(() => ({...makeWidgetReturn(), refetch: () => {}}))`) and asserts a bounded fetch count. This class of bug is invisible to tsc + unit tests + a fast backend — only the Pass-3 live walk catches it. Check every new widget's effect/callback deps for `refetch` or the whole `result` object from useWidgetData.
