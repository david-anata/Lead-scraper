# Sales Deck System Logic

## Purpose

The Anata sales deck turns marketplace evidence into a commercial point of view. It is not just a presentation and it is not a promise that a brand will reach a specific revenue number. Its job is to help a prospect understand:

1. where the brand sits today;
2. what comparable competitors are already achieving;
3. what is preventing the brand from closing that gap;
4. which actions should happen first;
5. how much qualified traffic each channel must eventually contribute; and
6. how Anata can coordinate marketing, commerce, and fulfillment around that plan.

The deck is the sales-facing bridge between marketplace research and the broader Brand Analysis system.

## Why This Matters in Sales

Most sales presentations describe services. The Anata deck should diagnose the business first and make the proposed services feel like a direct response to visible evidence.

This approach improves the sales conversation because it:

- replaces generic capability claims with prospect-specific evidence;
- gives the salesperson a defensible narrative from current state to target state;
- makes the scale of the opportunity understandable in sessions, units, revenue, and time;
- exposes dependencies so the prospect does not expect every channel to scale immediately;
- distinguishes observed facts from modeled assumptions;
- connects marketing ambition to conversion readiness and fulfillment capacity; and
- creates a measurable baseline that can be revisited after the engagement starts.

The most important commercial outcome is shared understanding. The prospect and Anata should leave the conversation aligned on the problem, the benchmark, the first phase, and the evidence required to continue investing.

## System Workflow

The system follows this sequence:

```text
Source files and listing data
        ↓
Normalize the target and competitor set
        ↓
Calculate category, listing, search, and traffic benchmarks
        ↓
Identify conversion and positioning gaps
        ↓
Translate the traffic gap into a phased channel plan
        ↓
Connect the plan to Anata services and proposed engagement
        ↓
Generate a stored, shareable sales deck
```

Each generated deck is a persisted HTML snapshot. A later code deployment does not rewrite an existing deck. When the generator logic changes, the deck must be regenerated to receive the new logic and presentation.

## Primary Inputs

The strongest version of the deck uses:

- the target Amazon ASIN or product URL;
- a competitor Helium 10 Xray export;
- a separate target Xray export when the target is absent from the competitor file;
- keyword exports;
- Cerebro ranking data;
- word-frequency data;
- the intended service channels;
- optional creative and case-study references;
- proposed offer details;
- growth assumptions such as conversion rate, COGS, shipping, and average order value.

If an input is missing, the system may use listing enrichment or a documented fallback. Missing evidence must not be presented as a verified fact.

## Data and Calculation Logic

### 1. Product identity and price

The system resolves the target product, listing title, brand, image, ASIN, price, rating, reviews, dimensions, BSR, and available market metrics.

For Amazon multipacks, the purchase or buy-box total is the relevant list price. A per-count price must not replace the amount the customer actually pays at checkout. Structured Amazon price data is preferred when the visible page contains both totals and unit-price language.

### 2. Market benchmark

The market section summarizes the uploaded comparable set:

- 30-day revenue;
- 30-day units;
- average price;
- average rating;
- BSR;
- review density;
- revenue share;
- seller country;
- size tier; and
- fulfillment method.

The competitor set must be relevant to the target product. A large but poorly matched category produces an impressive number and a weak sales argument.

### 3. Estimated sessions

The standard planning conversion rate is 15%.

When reported units are available:

```text
Estimated monthly sessions = monthly units ÷ 0.15
```

When units are unavailable but revenue and price are available:

```text
Estimated monthly sessions = (monthly revenue ÷ price) ÷ 0.15
```

Reported units are preferred because they avoid compounding a price error into the traffic estimate.

These session values are estimates, not observed analytics. They provide a consistent comparison across the target and competitors until first-party traffic and conversion data are available.

### 4. Current target sessions

The target baseline uses the same logic:

```text
Current monthly sessions = target monthly units ÷ 0.15
```

If target units are missing, the system may estimate units from available listing signals. That fallback should be treated as lower-confidence evidence.

### 5. Session goal

An explicit user-entered session goal takes precedence.

Without an explicit goal, the default benchmark is the top-quartile session level among comparable competitors with usable unit data. This is more credible than using the largest competitor and more ambitious than using the category average.

If no usable competitor benchmark exists, the system falls back to a multiplier of the target's current sessions.

```text
Session gap = benchmark sessions − current sessions
```

The result is bounded at zero. The deck should not invent a negative acquisition requirement when the target already exceeds the benchmark.

### 6. Default channel allocation

The session gap is allocated across five demand channels:

| Channel | Default share | Primary role |
| --- | ---: | --- |
| Organic | 30% | Improve indexing, conversion content, and durable non-paid demand |
| On-channel paid | 25% | Capture existing Amazon purchase intent and defend search presence |
| Off-channel paid | 20% | Create incremental traffic and external-demand signals through Meta and TikTok |
| Affiliate and creators | 15% | Build trust and shoppable demand through creator distribution |
| Retargeting and LTV | 10% | Convert accumulated audiences and past purchasers more efficiently |

The mix must total 100%. It is a planning default, not a permanent budget allocation. Real performance data should change the mix.

### 7. Channel economics

The deck estimates channel sessions, units, revenue, and directional cost using the supplied inputs and documented defaults.

Examples include:

- on-channel paid sessions multiplied by the modeled Amazon CPC;
- off-channel sessions multiplied by the storefront-link CPC;
- affiliate volume translated into required impressions, videos, commissions, COGS, and shipping;
- retargeting volume based on audience window, frequency, CPM, CTR, and repeat-conversion assumptions.

Organic is shown without media spend, but it is not free. It requires strategy, copy, creative, merchandising, and operational labor.

Affiliate, creator, retargeting, and repeat-purchase outputs are directional until calibrated with first-party data.

## The 24-Month Operating Plan

The roadmap describes end-of-phase operating states, not guaranteed achievement dates.

### Phase 1 — Foundation, months 1–3

Primary channels: organic and on-channel paid.

Core actions:

- validate Brand Registry and listing ownership;
- correct product identity and pricing;
- rewrite the title, bullets, backend terms, and image sequence;
- publish A+ Content and Storefront foundations;
- launch controlled Sponsored Products and early retargeting;
- establish the baseline for conversion, search visibility, and paid efficiency.

Sales meaning: fix the conversion surface before buying large amounts of traffic.

### Phase 2 — Acceleration, months 4–8

New emphasis: off-channel paid.

Core actions:

- defend proven brand and category terms;
- implement Amazon Attribution;
- test Meta and TikTok traffic against Storefront and PDP destinations;
- begin creator recruitment and pilot content;
- reallocate only after conversion and inventory gates are met.

Sales meaning: prove that the listing can absorb incremental demand without wasting spend.

### Phase 3 — Scale, months 9–15

New emphasis: affiliate and creator scale, with controlled prospecting.

Core actions:

- scale repeatable creator cohorts;
- introduce performance tiers;
- evaluate DSP only after PDP and PPC evidence is strong;
- compare assisted new-to-brand traffic with paid-search performance;
- expand the sources that produce qualified sessions, not merely clicks.

Sales meaning: scale repeatable acquisition systems rather than isolated campaigns.

### Phase 4 — LTV, months 16–24

New emphasis: retargeting, repeat purchase, and audience compounding.

Core actions:

- validate high-intent viewer and purchaser pools;
- layer PDP-viewer, cart-abandoner, and category retargeting;
- use tailored promotions where audience thresholds support them;
- refresh creative and creator cohorts;
- set the next benchmark based on observed performance.

Sales meaning: turn acquired demand into a more efficient, durable growth system.

For every phase, the deck shows:

- the actions being implemented;
- the channels active by that point;
- the monthly traffic expected from each channel at the end of the phase; and
- the cumulative session level relative to the competitor benchmark.

## Relationship to Brand Analysis

Brand Analysis explains the brand's overall commercial position. The sales deck converts that diagnosis into an acquisition and operating thesis.

The two systems should work together as follows:

| Brand Analysis question | Sales deck response |
| --- | --- |
| What does the brand stand for? | Positioning, offer, listing hierarchy, and creative recommendations |
| Who is the customer? | Search intent, use cases, keyword language, and creator audiences |
| Where does the brand win or lose? | Target-versus-competitor evidence and conversion gaps |
| How large is the opportunity? | Comparable revenue, units, reviews, share, and estimated sessions |
| What should happen first? | Phase 1 conversion and measurement actions |
| How will demand be created? | Channel-level actions, traffic targets, and economics |
| Can operations support growth? | Fulfillment, inventory, Shipping OS, and service dependencies |
| How will progress be measured? | Baseline, phase targets, and first-party calibration requirements |

The sales deck should feed useful findings back into Brand Analysis:

- competitor positioning and price bands;
- high-intent and missing keyword clusters;
- review and trust gaps;
- creative proof patterns;
- channel readiness;
- traffic and conversion benchmarks;
- fulfillment constraints;
- offer and service-fit hypotheses.

Over time, Brand Analysis should become the reusable strategic record, while each sales deck becomes a prospect-specific commercial application of that record.

## Recommended Sales Conversation

The salesperson should present the deck in this order:

1. Confirm product and category accuracy.
2. Establish the target's current position.
3. Show the relevant competitor benchmark.
4. Explain the conversion and positioning gaps.
5. Translate the gap into sessions rather than jumping directly to revenue.
6. Walk through the phases and their dependencies.
7. Explain what Anata owns in each channel.
8. Confirm the first 90-day scope and measurement plan.
9. Treat later phases as conditional on evidence, inventory, and economics.

This keeps the conversation diagnostic. The offer follows the reasoning instead of appearing before the prospect understands the problem.

## Evidence Labels and Guardrails

Every important output belongs to one of three evidence classes:

- **Observed:** directly supplied by Amazon, Helium 10, the prospect, or another identified source.
- **Estimated:** calculated from observed inputs, such as units divided by 15% conversion.
- **Directional:** based on planning defaults that require validation, such as creator impressions or repeat-conversion lift.

The deck must not:

- present estimated sessions as first-party traffic;
- use a per-unit multipack price as the total purchase price;
- treat the largest category outlier as the automatic goal;
- promise the phase-end numbers as guaranteed outcomes;
- recommend scaling traffic before conversion and inventory readiness;
- hide missing inputs or substitute unsupported precision;
- imply that every channel begins at full scale on day one.

## Operating and Regeneration Rules

- Preserve the original source exports for every deck.
- Record the target identifier, filenames, assumptions, and generation date.
- Regenerate a deck after material logic or design changes.
- Expect a regenerated deck to receive a new URL under the current implementation.
- Revalidate live price and product identity during regeneration.
- Review the competitor set before presenting the resulting benchmark.
- Replace directional defaults with first-party data after the first 30 days of execution.
- Reforecast the channel mix at each phase gate.

Vercel toolbar comments are review evidence, not the durable product specification. Important decisions from comments should be transferred into repository documentation, tests, or tracked work before the associated deployment changes.

## Future System Improvements

The highest-value next improvements are:

1. store source uploads or durable source references with each deck run;
2. add a **Regenerate deck** action that preserves inputs and creates a clearly linked revision;
3. show observed, estimated, and directional labels directly beside every modeled metric;
4. connect Brand Analysis records to deck runs so findings can be reused instead of re-entered;
5. track phase targets against actual sessions, conversion, revenue, inventory, and CAC;
6. maintain a revision history showing which generator version created each deck; and
7. convert unresolved Vercel comments into durable implementation tasks before deployment aliases move.

## Success Definition

The sales deck system succeeds when a prospect can answer five questions without interpretation:

1. Where are we now?
2. What are relevant competitors achieving?
3. What must change before we scale?
4. How much qualified traffic must each channel contribute over time?
5. What will Anata own, measure, and adjust to help us reach the next benchmark?

When those answers are clear, the deck supports both a stronger sale and a better-operated engagement.
