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
