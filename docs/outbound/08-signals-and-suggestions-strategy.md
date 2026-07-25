# 08 - Signals, Suggestions, and Success Tracking

How the app decides who to contact, learns what works, and gets sharper over time.

Right now the app uses a flat gate: Shopify, revenue band, country, niche, has an
email. That keeps out junk, but it treats a stale store the same as a brand that
just doubled ad spend last month. StoreLeads actually exposes the timing and
buying signals that separate "fits the profile" from "ready to take our call."
This is the plan to use them.

Our pitch is performance marketing: find wasted ad spend, move it, close the CVR
holes, about 40% lift in month one. So the best prospect is a brand that (1) is
already spending on ads (there is waste to find), (2) already cares about
conversion (they will talk CVR), and (3) just changed something (budget is moving
right now). Every signal below maps to one of those three.

---

## Part 1 - Suggestions (score, rank, and give a reason)

Instead of pass/fail, every brand gets a **fit score** and a **"why now" reason**.
The reason is the trigger that fired, and it doubles as the opening line Clay uses
to personalize the email.

### The v1 signal rubric (a hypothesis we will test)

Points add up. This is the starting weighting; Part 3 tunes it with real reply data.

**Spend is happening (there is waste to audit)**
- Runs Meta + Google pixels together: **+3** (`f:tech = Facebook Pixel, Google Ads Pixel`)
- Adds a third+ paid channel (TikTok / Pinterest / Bing / Criteo pixel): **+2**
- Has ads or affiliate apps installed (Criteo GO, impact.com, CJ, Aspire): **+2**
- Healthy monthly app spend, roughly $300 to $3,000: **+1** (`f:masmin` / `f:masmax`)

**They already believe in CVR (easy conversation)**
- Runs CRO / testing / analytics tooling: **+3**
  (Intelligems, Rebuy, Triple Whale, Polar, Lucky Orange, Hotjar, Microsoft
  Clarity, KnoCommerce, Octane AI, Boost AI Search) via `f:an` on those app tokens

**Why now - the trigger (also the email hook)**
- Installed an ads / analytics / CRO app in the last ~45 days: **+3**
  (`f:app_installed_at:min`) - active project, budget just moved
- Uninstalled a growth app recently: **+2** (`f:app_uninstalled_at`) - shopping for a fix
- Upgraded plan, e.g. moved to Shopify Plus, recently: **+2** (`f:last_plan_change_at:min`)
- Replatformed recently: **+1** (`f:last_platform_change_at`) - rebuild moment
- Fast social follower growth in last 30 days: **+1** (`f:tiktokfollowers30dpmin`, etc.)
- Trending tag (TikTok / Pinterest / etc.): **+1** (`f:tags`)

**Anti-signals (subtract or drop)**
- "Public Company" feature: **exclude** - too big, already has an agency
- Enterprise analytics stack (Monetate, Ometria, Klevu top tiers): **-2**
- No ad pixel at all: **-2** - the "wasted spend" pitch has nothing to grab
- Dropshipper / Print on Demand tags: **exclude** - out of ICP

**Hard gates (unchanged, must pass first)**
- Platform Shopify, revenue ~$1M to $15M/yr, country US/UK/CA/AU, niche match,
  has a contact email.

### Tiers
- **A (hot): 8+** - fresh trigger plus strong fit. Contact first.
- **B (warm): 4 to 7** - strong fit, no fresh trigger. Steady volume.
- **C (base): 1 to 3** - fits ICP, thin signals. Backfill only.

Each suggestion carries its fired signals, so a lead reads like:
"apparel, $4M, runs Meta+Google+TikTok, added Intelligems 3 weeks ago" - Tier A,
and Clay opens with the Intelligems install.

### One architecture change this needs
Today we pull a generic top-ranked page and filter in our code. To use these
signals we pull with StoreLeads' real filters (`f:tech`, `f:an`,
`f:app_installed_at`, `f:cc`, `f:ermin`/`f:ermax`, `f:masmin`) and request the
`apps`, `technologies`, `created_at`, `last_plan_change_at`, and follower fields.
Bonus: targeted queries return high-fit stores on every page, so we scan far fewer
pages. That is also the real cure for the rate-limit (429) errors, not just the
retry backoff we already added.

---

## Part 2 - Tracking success rate (close the loop)

A score is only a guess until we see who actually books. So we record what we bet
on, then join it to what happened in Instantly.

**When a brand is suggested/pushed, store one row keyed by domain:**
`domain, tier, signals[], niche, pushed_at, campaign`.
(This same store is the never-email-twice memory. One table, two jobs.)

**When Instantly reports outcomes**, match back by domain / email and attach:
`sent, replied, positive_reply, booked_call, bounced`.

**Then the scoreboard reports rates by signal and by tier, not just overall:**
- Booked-call rate for Tier A vs B vs C
- Positive-reply rate for each signal (e.g. "installed CRO app <45d" vs "trending tag")
- Best niche, best channel-mix

That is the difference between "we sent 4,555 emails" and "the CRO-install trigger
books calls at 3x our average, apparel beats supplements, TikTok-trending is
noise."

---

## Part 3 - Determining the best signals (the learning loop)

Every two weeks the scoreboard computes positive-reply-rate per signal against the
overall baseline:
- Signal beats baseline by a clear margin: **raise its weight.**
- Signal at or below baseline after enough volume: **lower it or retire it.**
- Not enough data yet: **leave it, keep gathering.**

Over a few cycles the fit score stops being a guess and becomes what the data says
books calls. New signals from StoreLeads (a new app category, a new trigger) get
added at a low weight and earn their way up the same way.

Guardrails: never let one great-looking signal collapse volume below the weekly
booked-call target; keep a slice of Tier B/C going so we keep learning; a signal
needs a minimum sample before we act on it, so we do not overreact to a lucky week.

---

## Build order
1. Switch the pull to StoreLeads server-side filters + richer fields (also fixes 429).
2. Add the fit score + tier + reason to each suggestion; expose Tier A/B/C in the app.
3. Add the domain outcome store (signals + pushed_at); wire it as the dedup memory.
4. Extend the scoreboard to report rates by signal and tier.
5. Turn on the two-week reweight review.

Nothing here sends email or changes what Clay/Instantly do. It changes who we pick
and how we learn, upstream of the copy David approves.
