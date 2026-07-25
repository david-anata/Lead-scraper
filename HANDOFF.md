# CONTEXT HANDOFF — Lead-scraper / Sales Deck fix — 2026-07-22

## MISSION
Fix wrong headline numbers (review count, rating, price, sales rank, revenue) for the
**target product** in Sales Decks generated from an Amazon product link. The reliable data
is already fetched but ignored; the deck currently eyeballs those numbers off the Amazon
web page instead. This handoff carries an approved spec into the build.

## YOUR WORKING POSTURE
You are aggressively building. You do not wait or ask "should I continue?" — you continue.
After each task you identify the next and start it. You narrate clearly, self-validate by
running the code and reading real output, and keep git clean with descriptive commits.
Deploy is push to the branch → Render auto-deploys. No secrets in code — the user sets env
on Render.

## THE BUG (one paragraph)
In Amazon-link ("digital-shelf") mode, `build_xray_report()` fetches the target's full,
reliable Rainforest record and returns it as `rainforest_target_raw`, but it excludes the
target's ASIN from the competitor list. That raw record is then used only to set the category
label — it is never turned into a "target row." So `_find_target_row()` finds no target in
the competitor set, `target_row` ends up empty, and every target metric falls back to the
fragile HTML scrape (`hero_product`). The scrape's review-count reader grabs the first
"N,NNN ratings" string on the page, which is frequently the wrong number. Competitors are
unaffected because they already flow through the reliable converter.

## THE FIX (approved spec)
Reuse the existing converter that competitors already use, for the target too.

1. In `sales_support_agent/services/deck/service.py`, inside the `if rainforest_asin:` block
   (where the `rf` client and `rainforest_target_raw` are in scope), convert the target once:
   `rainforest_target_row = rf._product_to_xray(rainforest_target_raw, display_order=0)`
   — guard for `None` (converter returns `None` on a malformed record).
2. In the target-row precedence chain (currently around service.py:650-656), insert the
   Rainforest target row ABOVE the fuzzy competitor match. Final precedence, highest first:
   - Uploaded Target Xray CSV (`target_xray_row`) — explicit human override, stays highest.
   - **Rainforest target row (NEW)** — the reliable source for digital-shelf mode.
   - Fuzzy competitor match (`target_match.product`) — existing fallback.
   - Scraped `hero_product` — last-resort fallback, UNCHANGED (still covers website/non-Amazon
     targets and the case where Rainforest returns nothing).

Because target review count, price, BSR, rating, and revenue all read from `target_row`
first (service.py ~764-798, and target_review_count at ~774), fixing the row fixes all five
numbers at once. Do NOT change rendering, the CSV path, competitor handling, or the scrape.

## KEY FILES & LINE ANCHORS (verify before editing — lines drift)
- `sales_support_agent/services/deck/service.py`
  - digital-shelf branch: ~542-563 (sets `rainforest_target_raw`, `rf` client at ~545)
  - target-row resolution: ~642-656 (`_find_target_row` → `target_row`; CSV override at ~655)
  - target metric reads: ~764-798 (`target_review_count` at ~774)
- `sales_support_agent/services/rainforest.py`
  - `_product_to_xray()` at line 188 — the converter to reuse (maps `ratings_total` →
    `review_count` at ~257)
  - `build_xray_report()` at line 266 — returns `(xray_report, target_data)`; `target_data`
    is the full `{"product": {...}}` response, exactly the shape `_product_to_xray` expects
  - target excluded from competitors at lines 150 and 180 (`asin != target_asin`)
- `sales_support_agent/services/product_research.py`
  - the fragile scrape fallback; review-count regex at ~564 (`([\d,]+)\s+ratings`) — leave as-is

## DEPLOYMENT — RENDER
- Service: `sales-support-agent` (single service; routers mount in-process)
- Live URL: https://agent.anatainc.com  (health check: /health)
- Start command: `uvicorn sales_support_agent.main:app --host 0.0.0.0 --port $PORT`
- Build: `pip install -r requirements.txt`
- Auto-deploys on push. A brief 502 during a deploy restart is normal — refresh past it.
- No env vars change for this fix.

## TECH STACK
Python, FastAPI, SQLAlchemy, uvicorn. Deck data from Rainforest API (target + competitors)
with a Helium 10 CSV path as the alternative. Amazon page scrape is the last-resort fallback.

## CONSTRAINTS TO HONOR
- This edits the Generate Sales Deck feature deliberately, per the approved spec. Keep the
  change surgical — one branch of one function plus a test. Do not touch the deck viewer,
  the CSV path, competitor logic, rendering, or `deck_generator.py`.
- Every fix ships with a test in the same commit.
- If on the default branch, branch first before committing. Current work branch:
  `consolidate-agent-single-service`.

## TASK QUEUE

### RESUME IMMEDIATELY
1. Open `sales_support_agent/services/deck/service.py`, re-locate the digital-shelf branch and
   the target-row precedence chain (anchors above), and apply the two-part fix.

### THEN
2. Add an automated test in the same commit (`tests/test_deck_generator.py`): feed the
   digital-shelf builder a known Rainforest target record whose `ratings_total`, rating,
   price, and rank DIFFER from what the scrape would produce; assert the deck's target fields
   come from the Rainforest record. Add a `None`-conversion case to prove the safe fallback.
3. Confirm an existing CSV-path deck test still passes unchanged (regression guard).

### VERIFY (three-pass rule — 3 clean passes in a row; any finding resets the count)
- Pass 1 — automated: new tests + existing deck suite green.
- Pass 2 — data check: generate a real Just Ingredients Amazon-link deck; confirm each target
  number matches the reliable source and Amazon.
- Pass 3 — end-to-end Chrome walkthrough of the live Generate Sales Deck page: paste the Just
  Ingredients Amazon link, generate, open the finished deck, and read it as the salesperson
  would. Check every place the target's numbers appear (product summary, target-vs-competitor
  comparison table, headline stats) for correct, self-consistent numbers. Also generate one
  CSV-path deck to confirm that flow still looks right.

### THEN
4. Commit with a descriptive message and push the branch. Confirm Render redeploys and
   /health returns healthy.

## KEY DECISIONS ALREADY MADE (honor these)
- Reuse `_product_to_xray()` instead of hand-mapping fields, so the target row is built
  identically to competitor rows.
- Uploaded Target Xray CSV stays the highest-authority source; Rainforest target row beats the
  fuzzy match; scrape remains the final fallback.
- Fix the data source, NOT the scrape regex — the scrape is guesswork and we already hold
  reliable data for this product.

## COMMANDS TO KNOW
```bash
# Run the deck tests (set a throwaway DB URL so imports don't fail)
SALES_AGENT_DB_URL="sqlite:///test_deck.db" python3 -m pytest tests/test_deck_generator.py -q

# Confirm the app imports cleanly before pushing
python3 -c "from sales_support_agent.main import app; print('startup OK')"

# Check the live site is healthy
curl -s -o /dev/null -w "%{http_code}\n" https://agent.anatainc.com/health
```

## HOW TO BEHAVE
- Re-read this file, then run `git status` and `git log --oneline -5` to reorient.
- Begin the RESUME IMMEDIATELY task. Build, test in the same commit, verify with the three-pass
  rule, then push.
- Speak to the user (David) in plain English — he is not technical. No jargon, no em dashes.
  Only surface what needs his attention or a decision.
