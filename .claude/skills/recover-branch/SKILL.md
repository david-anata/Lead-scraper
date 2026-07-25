# /recover-branch — Extract Work from an Orphaned Git Branch

**Description:** Surgically recover completed work (widget, endpoint, tests) from a git branch that could not be cleanly merged — without cherry-pick, without resolving large file conflicts, without losing current main state.

---

## When to use

Use when:
- A PR was blocked by merge conflicts on a large shared file (`spec_performance_profit.py`, `app.js`, `index.tsx`)
- A worktree agent completed its work but its branch was never merged
- You want to salvage specific files/functions from a branch without taking everything it changed
- Cherry-pick was aborted due to conflicts

Do NOT use when:
- The branch has clean, non-conflicting changes — just merge/rebase normally
- You need the full branch (all files) — use `git merge` instead

---

## The core technique: manual file extraction

Never `git cherry-pick` branches that touched shared large files. Instead, extract only what you need:

### Step 1 — Identify what the branch added

```bash
# See all files changed in the branch vs main
git diff main...origin/<branch-name> --name-only

# See what was added to a specific large file
git diff main...origin/<branch-name> -- path/to/large/file.py | grep "^+" | grep -v "^+++" | wc -l
```

### Step 2 — Extract new files directly

For files that are entirely new (new widget, new test file):
```bash
git show origin/<branch-name>:frontend/src/react-app/sections/profit/widgets/MyWidget.tsx \
  > frontend/src/react-app/sections/profit/widgets/MyWidget.tsx

git show origin/<branch-name>:tests/test_my_feature.py \
  > tests/test_my_feature.py
```

### Step 3 — Extract appended content from large files

For a function appended to the end of a large file:
```bash
# Find line counts
WORKTREE_LINES=$(git show origin/<branch-name>:path/file.py | wc -l)
MAIN_LINES=$(wc -l < path/file.py)
NEW_LINES=$((WORKTREE_LINES - MAIN_LINES))

# Read and append just the new portion
git show origin/<branch-name>:path/file.py | tail -n $NEW_LINES >> path/file.py
```

Or: read the last N lines of the branch's file and visually verify before appending (safer):
```bash
git show origin/<branch-name>:path/file.py | tail -200
# Review the output, then append what's relevant
```

### Step 4 — DO NOT touch

- `frontend/public/app.js` — build artifact; always take main's version
- Section orchestrator (`index.tsx`) — if it was already rewritten in current session, do NOT overwrite from the branch; add only the new widget import/mount manually
- Any file the current session already modified

### Step 5 — Verify extraction

```bash
cd frontend && npx tsc --noEmit        # must be 0 errors
pytest tests/test_recovered_file.py -v # backend tests must pass
```

---

## Lessons learned (from profit section recovery, 2026-07-04)

**Cherry-pick fails on 5000-line spec files.** `spec_performance_profit.py` is too large and diverges too fast across PRs. Multiple agents all append to it concurrently; cherry-picking almost always conflicts. Extraction is always faster.

**`app.js` is a build artifact.** Never take it from a branch — it's always stale. Rebuild from source (`npm run build`) or take main's version.

**Worktree agent branches are ephemeral.** `origin/worktree-agent-<id>` branches exist as long as the worktree is up. Retrieve files before the worktree is cleaned up.

**`git show branch:path > local/path` is the fastest extraction method.** No checkout, no stash, no conflict. Output is the exact file content at that branch's HEAD.

**Trust the agent's test count, not a local re-run.** After copying a test file from a worktree, the local vitest run will fail with "No test files found" if the test file wasn't in the main repo before — but that just means `vitest`'s watch glob missed it on first scan. The test itself is correct if the agent reported it passing.

---

## Common mistakes to avoid

- **Overwriting a large shared file** — never `cp worktree/spec_performance_profit.py main/` — this wipes 5000 lines of other people's work
- **Forgetting to add the import** — after extracting `MyWidget.tsx`, manually add `import { MyWidget } from "./widgets/MyWidget"` to the section orchestrator
- **Running git cherry-pick on `main`** — it will conflict immediately on any file other agents touched; use extraction instead
- **Trusting `git diff main...branch` line count via `grep "^+"` alone** — the count includes context lines that grep might mis-attribute; always visually read the tail before appending

---

## Automation potential

**Human Assisted.** The extraction steps are mechanical, but which files to extract (vs which to skip) requires human judgment — especially when the section orchestrator has been independently rewritten. An agent can do the extraction if given precise file paths and told exactly what NOT to touch.
