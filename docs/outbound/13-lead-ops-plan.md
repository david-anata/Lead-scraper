# 13 - Lead Ops: what we pull, what triggers it, and how we track it

The point: stop pulling a static "brands that fit" list and start pulling brands
**at the moment something changed**. StoreLeads exposes date-windowed filters, which is
what turns a list into a trigger. Every pull carries the reason it fired, and that reason
becomes both the opening line and the thing we measure.

---

## The six recipes

| Tier | Recipe | What it looks for | Why it converts | Runs | Cap |
|---|---|---|---|---|---|
| A | Just installed a growth or CRO app | installed Triple Whale, Intelligems, Polar, Lucky Orange, Clarity, Kno, Octane, Rebuy, Criteo or Impact in the last **14 days** | budget moved this month and a project is live | Tue/Wed | 40 |
| A | Just dropped a growth tool | uninstalled one of those in the last **30 days** | they had the problem, tried a tool, still have the problem | Tue/Wed | 30 |
| B | Upgraded to Shopify Plus | plan changed in the last **60 days** to Plus | upgrades follow growth, and growth follows budget | Tue/Wed | 30 |
| B | Recently replatformed | platform changed in the last **90 days** | everything is in flux and vendors are being chosen | Tue/Wed | 25 |
| B | Social following spiking | TikTok followers **+25% in 30 days** | attention is spiking and spend usually follows | Tue/Wed | 25 |
| C | Core ICP | the plain profile, best revenue first | keeps volume steady when triggers are thin | Weekdays | 25 |

Every recipe also carries the ICP floor, enforced server-side **and** re-checked in our own
code so a StoreLeads filter quirk can never widen the target: Shopify, active store,
US/GB/CA/AU, $1M-$15M a year, has a contact route, never a dropshipper or print-on-demand.

## Why the timing is what it is
StoreLeads refreshes weekly, normally Monday. Trigger recipes therefore run **Tuesday and
Wednesday**, once the newly changed rows have actually landed. The core ICP pull runs every
weekday to keep the top of funnel steady. **Nothing pulls on weekends** because nothing
sends on weekends.

Caps are deliberately small (25 to 40). Frequent and low beats one big blast: it protects
the sending domains, it keeps reply volume inside what two people can work, and it means a
bad week costs us 30 brands instead of 3,000.

A full trigger week plans about **175 fresh brands**, and a quiet week about **125**.

## What gets tracked
- **Every pull** is logged: when, which recipe, scanned, how many fit ICP, how many were
  genuinely fresh, how many were skipped as already-seen, and whether the pull was cut
  short by a rate limit.
- **Every lead** is stored with the recipe that sourced it and the signals that fired.
- Because the recipe rides through the CSV into Clay and Instantly, the scoreboard can
  eventually answer the only question that matters: **which recipe books calls**. Recipes
  that do not earn their place get retired.

## Where to see it
`agent.anatainc.com` -> Sales -> **Lead Ops**. It shows today's scheduled pulls, all six
recipes with the reason each one fires, a Pull now button per recipe, and the history of
every pull with its real numbers.

## Next, once Clay is on a plan with API access
Today the loop is: pull here, download the CSV, import to Clay. With Clay's API we replace
the middle by hand-off: the app pushes each pull straight into Clay, Clay enriches and
verifies, and pushes qualified rows into Instantly. The recipe tag survives the whole way,
so per-recipe booked calls keep working. Nothing about the recipes or the tracking changes
- only the transport does.

---

## Live results, first real runs (25 Jul 2026)

Every recipe was run against the real StoreLeads API. These are the actual numbers.

| Recipe | Scanned | Fresh brands | Notes |
|---|---|---|---|
| Core ICP | 45 | **25** | very efficient, reliable volume filler |
| Upgraded to Shopify Plus | - | **30** | filled its cap |
| Recently replatformed | 49 | **25** | very efficient |
| Social following spiking | 39 | **25** | very efficient |
| Just installed a growth/CRO app | 550 | **4** | hottest signal, but expensive and scarce |
| Just dropped a growth tool | 0 | **0** | nothing matched today's rotated tool |

Sample of what comes back: `Vitamin A` (vitaminaswim.com, apparel, US, Tier A) from the
just-installed trigger; `Billy Reid` (billyreid.com, apparel, US, ~$14.9M/yr) from core ICP.

### What we learned by running it
1. **StoreLeads' app install/uninstall filters take ONE app id, not a list.** Given a comma
   list they return zero rows and no error. This is the single most important gotcha here,
   and it is why two recipes originally returned nothing. Fixed: the install window is now
   re-checked in our own code, and the uninstall filter sends one tool at a time, rotating
   daily so a few weeks covers the whole list.
2. **The hottest trigger is the scarcest.** "Just installed a growth app" scanned 550 rows
   to find 4 brands, because most stores installed their tools long ago. Expect a handful
   of very warm leads from it, not volume. It is also the only recipe that gets cut short
   by pagination, so it should not be relied on for daily numbers.
3. **The steady earners are replatformed, social surge and plan upgrade** - each returned a
   full cap from under 50 scanned rows. These carry the weekly volume.
4. **Realistic weekly volume is roughly 110 to 135 fresh brands**, not the 175 originally
   planned, because the just-installed trigger delivers far fewer than its cap.

### Tuning to consider next
- Widen the just-installed window from 14 to 30 days to lift its yield.
- Add a second churn recipe so two tools are checked per day instead of one.
- Once per-recipe booked calls exist on the scoreboard, drop whichever recipe is not
  earning its place rather than guessing.

---

## Tuning and trackable segmentation

Everything worth retuning is now editable on the Lead Ops page. No code change, no deploy:

- how many days back a "just installed" still counts (default 14)
- how many churn tools we check per day (default 1)
- the lookback window for plan upgrade, replatformed and churn
- the minimum follower growth percent for social surge
- the per-recipe cap, and whether a recipe is live at all

### Why a date and a note is not enough
A note tells you *that* something changed. It cannot tell you whether last week's booked
calls came from the old settings or the new ones, because pulls straddle the change.

So every change **bumps a settings version**, and **every pull records the version it ran
under**. That turns "did widening the window help?" into a question you can answer by
comparing v3 pulls against v4 pulls, instead of arguing from memory.

The change log records, for each change: the version, when, which setting, the old and new
value, the note explaining why, and who made it. A change that does not actually change a
value is ignored, so the log never fills with noise.

### How to use it
1. Change a number on Lead Ops, write **why** in the note, save. The version bumps.
2. Let it run for a week.
3. Compare the runs at the new version against the old one. When per-recipe booked calls
   land on the scoreboard, this becomes a straight before-and-after read.

### The two tuning moves we already know we want
- **Widen "just installed" from 14 to 30 days.** It returned only 4 brands from 550 scanned.
- **Check 2 churn tools a day instead of 1.** One tool a day found nothing on its first run.

Both are now one edit each, with the note explaining the reasoning, rather than a code change.
