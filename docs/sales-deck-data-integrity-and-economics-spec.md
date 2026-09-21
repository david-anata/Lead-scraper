# Sales Deck Data Integrity, Forecasting, and Prospect-Readiness Specification

Status: proposed
Audience: product, engineering, sales leadership, Account Executives, QA
Primary workflow: Sales → Generate Sales Deck → Digital Shelf Auto
Last updated: September 2, 2026

## 1. Executive summary

The sales-deck system has a strong presentation structure, but an automated deck can currently look complete while being built from an irrelevant competitor set. In the reviewed Dixie Belle example, the target was furniture paint, while the comparison set included gym chalk, climbing chalk, classroom chalk, and tailor's chalk. The generated deck then treated those products as evidence for market size, price positioning, keyword opportunities, the session benchmark, channel traffic targets, and revenue projections.

This is a sales-critical failure. A polished deck amplifies the apparent certainty of the wrong data. An Account Executive cannot safely present claims such as “$63k monthly opportunity,” “add rock climbing to the title,” or “4,000 sessions to match competitors” when the compared products serve different buyers and use cases.

The system must become evidence-gated rather than render-gated. A deck is not Ready merely because HTML was produced. It is Ready only when the target identity is verified, the competitor set clears relevance and coverage thresholds, the benchmark is based on sufficiently reliable observations, and the financial plan is supportable—or visibly framed as a scenario that requires review.

This specification introduces:

1. canonical target identity and variation verification;
2. category-aware competitor discovery using the operator's category input;
3. multi-signal competitor relevance scoring and exclusion reasons;
4. hard quality gates that prevent unsupported prospect claims;
5. evidence classes and provenance throughout the generated deck;
6. benchmark and session-goal calculations using only qualified competitors;
7. a contribution-economics model beyond TACOS;
8. confidence-aware keyword recommendations;
9. an operator review step before a deck can be treated as prospect-ready;
10. observability, regression fixtures, rollout controls, and recovery behavior.

## 2. Verified current behavior

### 2.1 Current automated flow

The current Digital Shelf flow is approximately:

1. The operator submits an ASIN or Amazon URL from `/admin/sales-decks`.
2. The form may also submit a free-text `category_label`, verified price/AOV, conversion rate, TACOS target, channel mix, and cost assumptions.
3. `sales_support_agent/api/router.py` passes these values to `DeckGenerationService.generate_deck`.
4. `sales_support_agent/services/deck/service.py` calls `RainforestClient.build_xray_report(rainforest_asin)`.
5. `sales_support_agent/services/rainforest.py` fetches the target product and tries competitor discovery using a BSR bestseller-category URL.
6. If that route cannot produce a usable list, it constructs a search query from the first three to five title words longer than three characters.
7. Competitor products are fetched and converted into Xray-style rows.
8. The deck dataset, keyword fallbacks, benchmark, growth plan, and HTML are built from those rows.

### 2.2 Confirmed root cause

The operator's `category_label` is accepted and used as a user-facing label, but it is not passed into `RainforestClient.build_xray_report`. It therefore does not constrain discovery. In the reviewed example, changing the label from “chalk mineral furniture paint” to “furniture paint for cabinets wood metal home decor” changed headings while the comparison set remained dominated by gym and classroom chalk.

The fallback query is also vulnerable because it uses the opening title tokens rather than a product-type representation. A title beginning with “Dixie Belle Paint Company Chalk…” can over-weight brand and “chalk” while under-weighting “furniture paint,” “cabinet paint,” surfaces, and finish type.

### 2.3 Downstream propagation

Once an irrelevant competitor enters the Xray-style report, the system currently allows it to influence:

- category revenue and units;
- average price and price-positioning claims;
- best-seller selection;
- average and top-three estimated sessions;
- target-vs-benchmark rating and review comparisons;
- observed buyer language and missing keyword recommendations;
- current-to-goal session delta;
- phase-level channel targets;
- modeled units and revenue;
- “why now” sales copy;
- LLM-generated CRO, creative, and positioning recommendations.

### 2.4 Reviewed example

The reviewed deck correctly showed the chosen target price of $31.45 and a 15% TACOS goal. However, it also:

- compared furniture paint to Rage Fitness gym chalk;
- recommended “rock climbing,” “gym,” and “chalk ball” as missing terms;
- reported a $63k category opportunity from the contaminated set;
- derived a 4,000-session goal from that set;
- reported 4.5 stars and 87 reviews while the inspected Amazon listing displayed a materially different ratings signal;
- modeled $18,870 in monthly revenue, $3,162 in media, and 16.8% implied TACOS before complete contribution costs.

The growth arithmetic is internally consistent, but its inputs are not prospect-safe.

## 3. Product goal and success condition

### Goal

Produce a sales deck that an Account Executive can present without needing to apologize for or reverse-engineer its data. Every major claim must be traceable to the correct target, a qualified comparison set, an explicit assumption, or a clearly labeled scenario.

### Success condition

An operator can enter an ASIN, confirm the target and category, review the discovered competitor set and data confidence, then generate a deck whose market, keyword, benchmark, growth, and economics sections are all built from the same approved evidence set.

### Primary users

- Account Executive generating and presenting the deck;
- sales leader reviewing the quality of the recommendation;
- prospect evaluating Anata's competence and commercial proposal;
- operator or analyst correcting ambiguous Amazon data;
- engineer diagnosing data-source and model failures.

## 4. Scope

### In scope

- Digital Shelf Auto intake and generation;
- Rainforest target and competitor acquisition;
- optional Amazon SP-API enrichment where configured;
- category resolution and competitor qualification;
- target identity, variation, price, ratings, BSR, and freshness handling;
- automated keyword evidence derived from qualified competitors;
- market aggregation and brand rollups;
- session benchmarking and phase/channel planning;
- TACOS and contribution-economics presentation;
- deck readiness states, warnings, operator review, audit metadata, and analytics;
- automated/manual data-path consistency;
- test coverage, rollout, monitoring, and recovery.

### Non-goals

- replacing Helium 10 as the source for exact search-volume data;
- guaranteeing Amazon sales or traffic outcomes;
- inventing COGS, FBA fees, conversion rates, or historical sessions;
- treating BSR-derived revenue as exact revenue;
- building a general-purpose product taxonomy for all commerce platforms;
- redesigning the entire sales deck or changing approved Anata brand primitives;
- automatically publishing or sending a deck that has not cleared readiness checks.

## 5. Product principles

1. Wrong data is worse than missing data.
2. A label must not imply that the underlying evidence changed when it did not.
3. Unsupported sections degrade gracefully; they do not fabricate certainty.
4. Operators see the source, freshness, confidence, and next corrective action.
5. Manual overrides are explicit, attributed, timestamped, and auditable.
6. Financial outcomes separate revenue, media, marketplace costs, fulfillment, product cost, and Anata fees.
7. Prospect-ready and generated are different states.

## 6. Target workflow

### Step 1: Enter target

The operator supplies an ASIN or Amazon product URL. Optional inputs include an intended product category, target marketplace, verified AOV, target TACOS, known CVR, current sessions, COGS, FBA/fulfillment cost, and shipping cost.

The primary action at this stage becomes `Analyze product`, not `Generate deck`.

### Step 2: Verify canonical target identity

The system fetches and displays:

- ASIN;
- canonical title;
- brand;
- selected variation attributes where available;
- parent ASIN and child ASIN where available;
- primary image;
- current buy-box/list price and source;
- rating and rating count;
- BSR path, category identifiers, and category names;
- marketplace and fetched-at timestamp.

The operator must see one target-confirmation panel before generation. If the fetched product does not match the intended product, the operator can replace the ASIN without creating a deck run.

Target identity becomes `Confirmed` automatically only when the returned ASIN matches the normalized submitted ASIN and a non-empty title, brand, image, and product-type/category signal exist. Otherwise it is `Needs review`.

### Step 3: Resolve product type and category

Create a `CategoryResolution` record with distinct fields:

- `amazon_category_id` and `amazon_category_path` from the target;
- `canonical_product_type`, such as `furniture_paint`;
- `operator_category_label`, such as “Chalk mineral furniture paint”;
- `discovery_query`, used for fallback search;
- `display_category_label`, used in the deck;
- `source` (`amazon_bsr`, `amazon_browse_node`, `operator`, `classifier`, or `manual`);
- `confidence` from 0 to 1;
- `reason_codes` explaining the choice.

The display label must never silently substitute for the discovery category. The UI must show both when they differ.

Recommended default resolution order:

1. specific Amazon browse node/category identifier;
2. specific BSR leaf category that semantically matches the target title and attributes;
3. operator category input;
4. deterministic product-type classifier based on title, bullets, category path, and attributes;
5. safe search-query builder;
6. manual operator selection.

An LLM may suggest a product type only after deterministic signals are collected. It may not directly approve competitors or overwrite source facts.

### Step 4: Discover candidate competitors

Run candidate discovery in tiers and retain provenance for every candidate:

1. same leaf browse node or specific BSR category;
2. adjacent browse nodes sharing the canonical product type;
3. exact product-type search query;
4. operator-supplied competitor ASINs;
5. manual Helium 10 upload.

The fallback query must not use “first five long words.” Build it from normalized product-type and use-case tokens. Remove brand, size, color, pack count, marketing adjectives, punctuation artifacts, and known ambiguous head terms unless supported by category context.

For the reviewed target, a safe query would resemble `furniture paint cabinets wood` or `chalk finish furniture paint`, not `Dixie Belle Paint Company Chalk`.

### Step 5: Score and qualify candidates

Every candidate receives a deterministic relevance score and reason codes before its metrics can influence the deck.

Recommended score components:

| Signal | Weight | Notes |
| --- | ---: | --- |
| Exact browse node/category ID match | 30 | Strongest available structured signal |
| Canonical product-type match | 25 | Must distinguish furniture paint from consumable chalk |
| Title/bullet semantic overlap | 15 | Use normalized product terms, not brand tokens |
| Shared use-case/surface attributes | 10 | Cabinets, wood, metal, furniture, home décor |
| Compatible form factor | 10 | Paint/liquid coating vs powder, stick, ball, or block |
| Comparable price/size family | 5 | Supporting signal only; never the primary classifier |
| Brand/category consistency | 5 | Penalize known mismatches and unrelated departments |

Hard exclusions override the numeric score:

- a conflicting product form (`gym_chalk`, `chalk_stick`, `tailor_chalk`) when the target is `furniture_paint`;
- conflicting department or incompatible browse node;
- accessory/refill/ingredient when the target is a finished product;
- duplicate ASIN, duplicate parent variation, or the target itself;
- missing ASIN or title;
- sponsored/non-product records that cannot be resolved to a product.

Recommended thresholds:

- `qualified`: score ≥ 0.75 and no hard exclusion;
- `review`: score 0.55–0.74 and no hard exclusion;
- `excluded`: score < 0.55 or any hard exclusion.

Only qualified competitors may feed prospect-facing market and benchmark metrics by default. Review candidates are visible to the operator but excluded until approved.

### Step 6: Apply data-quality gates

Define a `DeckEvidenceAssessment` before rendering:

- target identity status;
- target metric coverage;
- qualified competitor count;
- competitor relevance distribution;
- source freshness;
- real-vs-estimated unit coverage;
- price coverage;
- BSR coverage;
- ratings coverage;
- keyword source class;
- economics-input coverage;
- blocker, warning, and informational reason codes.

Default readiness rules:

- target identity must be Confirmed;
- at least five qualified competitors for full market claims;
- at least three qualified competitors for a limited benchmark;
- median competitor relevance ≥ 0.80;
- no known cross-product-type competitor may be included;
- current price must have a source and timestamp or be an explicit operator override;
- the benchmark must state whether units are observed floors, BSR estimates, or mixed;
- automated keyword recommendations require at least five qualified competitor titles;
- a growth benchmark cannot be labeled “competitor benchmark” with fewer than three qualified competitors;
- a deck with a blocking condition cannot enter Ready or Delivered.

Behavior by coverage:

- 5+ qualified competitors: full market analysis allowed;
- 3–4: limited benchmark with a prominent lower-confidence label;
- 0–2: suppress market size, best-seller comparison, keyword gaps, and competitor-derived traffic goal; show `Needs review` with corrective actions.

### Step 7: Operator competitor review

Before generation, show a compact table with image, title, brand, price, category, relevance score, inclusion status, source, and exclusion reason. The operator can:

- approve a review candidate;
- exclude a qualified candidate with a reason;
- add an ASIN;
- edit the canonical category/product type;
- rerun discovery;
- switch to manual Helium 10 data.

Manual changes must be captured in the run summary with operator identity, timestamp, old value, new value, and reason. No silent CRM or dataset mutation is permitted.

### Step 8: Generate deck

Generation consumes an immutable evidence snapshot. Data cannot be refetched halfway through rendering. The same approved competitor set must feed the market, brand rollup, keywords, benchmark, growth plan, and narrative recommendations.

The created run enters one of these states:

- `Ready`: all prospect-facing quality gates pass;
- `Needs review`: HTML exists but one or more nonfatal data-quality checks failed;
- `Blocked`: target or market evidence is insufficient for a meaningful deck;
- `Failed`: technical generation error;
- `Stale`: evidence exceeded the configured freshness window.

Only Ready decks should expose the normal copy/share/deliver actions. Internal preview remains available for Needs review and Blocked decks with an unmistakable banner.

## 7. Backend architecture

### 7.1 Proposed modules

Keep the existing FastAPI and service architecture. Add small deterministic services rather than embedding additional rules in the renderer.

Suggested modules:

- `sales_support_agent/services/deck/target_identity.py`
  - normalize ASIN and marketplace;
  - resolve parent/child variation;
  - reconcile Rainforest and optional SP-API facts;
  - emit canonical target plus conflicts.
- `sales_support_agent/services/deck/category_resolution.py`
  - normalize category paths;
  - map ambiguous phrases to canonical product types;
  - construct safe discovery queries;
  - emit confidence and reason codes.
- `sales_support_agent/services/deck/competitor_discovery.py`
  - orchestrate candidate sources;
  - deduplicate ASINs and parent variations;
  - retain provenance.
- `sales_support_agent/services/deck/competitor_relevance.py`
  - deterministic scoring;
  - hard-exclusion rules;
  - operator overrides.
- `sales_support_agent/services/deck/evidence_quality.py`
  - coverage and readiness gates;
  - section-level eligibility;
  - warning/blocker messages.
- `sales_support_agent/services/deck/economics.py`
  - revenue, TACOS, contribution profit, break-even, and scenario calculations.

`sales_support_agent/services/rainforest.py` remains an integration client and raw product mapper. It should not own sales-deck readiness decisions.

### 7.2 Rainforest interface change

Replace the ASIN-only deck call with an explicit discovery request:

```python
@dataclass(frozen=True)
class CompetitorDiscoveryRequest:
    target_asin: str
    amazon_domain: str = "amazon.com"
    category_id: str | None = None
    category_path: tuple[str, ...] = ()
    canonical_product_type: str = ""
    operator_category_label: str = ""
    discovery_query: str = ""
    competitor_limit: int = 12
```

The result must include candidates and provenance instead of returning only a pre-aggregated Xray report:

```python
@dataclass(frozen=True)
class CompetitorCandidate:
    product: XrayProduct
    discovery_source: str
    source_query_or_category: str
    relevance_score: float
    relevance_status: str
    reason_codes: tuple[str, ...]
```

The Xray-style aggregate is built only after qualification.

### 7.3 Category-label wiring correction

At minimum, `DeckGenerationService._build_dataset` must pass the resolved discovery context into competitor discovery before the report is built. The current pattern—building the report, then resolving a display category—must be inverted:

1. fetch target;
2. build canonical target identity;
3. resolve category/product type;
4. discover and qualify competitors;
5. build aggregate report;
6. build keywords, benchmark, growth plan, and deck copy.

This prevents a display-only override from creating a false impression of corrected sourcing.

### 7.4 Source precedence and conflict handling

Recommended precedence by field:

| Field | Preferred source | Fallback | Conflict behavior |
| --- | --- | --- | --- |
| ASIN | submitted normalized ASIN | none | block on mismatch |
| Title/brand/image | Amazon/Rainforest product | SP-API | flag material conflicts |
| Variation | SP-API relationship | Rainforest attributes | Needs review if unresolved |
| Price | operator verified override | current buy box | show both and attribution |
| Rating/count | current product response | SP-API | use freshest; retain conflict |
| BSR/category | structured Amazon category data | operator category | never conflate display and discovery |
| Units | Amazon recent-sales floor | BSR estimate | label evidence class per row |
| Revenue | units × price | none | inherit unit evidence class |
| Search volume | Helium 10 | none | competitor-title language is not volume |
| Sessions | first-party sessions | units ÷ assumed CVR | label as derived estimate |

When two current sources conflict materially, do not silently choose the value that makes the deck look stronger. Display the conflict internally and require confirmation for the affected claim.

### 7.5 Persistence and audit contract

Version the evidence payload stored in `AutomationRun.summary_json`:

```json
{
  "deck_evidence_version": 2,
  "target_identity": {},
  "category_resolution": {},
  "competitor_candidates": [],
  "qualified_competitor_asins": [],
  "excluded_competitors": [],
  "evidence_assessment": {},
  "economics_scenarios": {},
  "operator_overrides": [],
  "source_requests": [],
  "generated_at": "ISO-8601"
}
```

For the first release, this can remain JSON-backed without a schema migration if current payload sizes remain acceptable. If operators need cross-run reporting on exclusions, confidence, or source failures, add normalized tables in a later migration. Do not store secrets, full API responses, or unnecessary customer data.

Each external fetch audit record should include provider, request type, non-secret request fingerprint, response status, latency, fetched-at time, cache status, and error class.

## 8. Competitor and market calculations

### Qualified aggregation

Every market calculation must operate on `qualified_competitors`, never raw candidates.

- category revenue: sum qualified competitor revenue estimates;
- category units: sum qualified competitor units;
- average price: arithmetic mean of qualified ASIN prices with price coverage disclosed;
- by-brand price: average price across qualified ASINs for that brand;
- by-brand sessions: sum qualified ASIN sessions for that brand;
- by-brand revenue/units: sums across qualified ASINs;
- best seller: highest-evidence revenue candidate, with BSR as a secondary ranking signal;
- top-three benchmark: compute from the strongest comparable candidates, not merely the first three BSR rows.

The deck must show comparison count and evidence quality, for example: `8 qualified listings · 6 observed unit floors · 2 BSR estimates · fetched Sep 2, 2026`.

### Outlier handling

Flag rather than silently remove outliers. Suggested rules:

- price outside 0.25×–4× the target's comparable unit price;
- size/quantity not normalizable;
- revenue more than 5 median absolute deviations from the qualified set;
- missing essential category/product-type evidence;
- variant duplicates.

An approved outlier may remain in the landscape but should not necessarily define the benchmark. Record whether each row is `landscape_only` or `benchmark_eligible`.

## 9. Keyword and buyer-language logic

Define three distinct evidence classes:

1. `Search volume`: supplied by Helium 10 or another approved search-volume source.
2. `Observed competitor language`: extracted from qualified competitor titles and copy; no volume claim.
3. `Suggested positioning language`: generated recommendation; never presented as observed demand.

Automated title-language extraction must use only qualified competitors. Generic stop terms, brand names, colors, pack sizes, and irrelevant product-type terms are removed. Terms that conflict with the canonical product type are excluded even if frequent.

If fewer than five qualified competitor titles are available, suppress the automated keyword-gap table and show: `Insufficient relevant competitor coverage. Add competitor ASINs or upload Helium 10 data.`

The system must never recommend a term such as “rock climbing” to furniture paint because it was common in excluded results.

LLM recommendations receive the canonical target, qualified competitors, exclusions, and evidence classes. The prompt must explicitly prohibit treating observed title frequency as search volume and prohibit recommendations based on excluded candidates.

## 10. Session benchmark and growth-plan logic

### Current sessions

Use this precedence:

1. first-party current monthly sessions entered or integrated by the operator;
2. first-party units divided by verified CVR;
3. observed recent-sales floor divided by assumed CVR;
4. BSR-estimated units divided by assumed CVR;
5. unknown.

Unknown must remain unknown. It must not render as `0 sessions` unless zero is an observed value. In the reviewed deck, unavailable target units became a visual baseline of zero, which makes the opportunity look larger than the evidence supports.

### Goal sessions

Use this precedence:

1. operator-approved explicit goal;
2. comparable benchmark percentile from at least three benchmark-eligible competitors;
3. current sessions × an operator-approved multiplier;
4. unavailable.

Recommended default comparator is the median or 60th percentile of benchmark-eligible sessions, not an unqualified top-three mean. A high-growth scenario may use the 75th percentile, but must be labeled as an upside scenario.

### Scenario model

Provide three scenarios when enough evidence exists:

- Conservative: lower qualified benchmark, lower CVR, higher CPC;
- Base: median/60th-percentile benchmark and current input assumptions;
- Upside: upper benchmark, improved CVR/AOV, and validated channel efficiency.

The operator chooses which scenario appears as the recommended plan. The deck shows a range when input uncertainty is material.

### Phase model

Retain four phases across up to 24 months:

- Foundation: months 1–3;
- Acceleration: months 4–8;
- Scale: months 9–15;
- LTV: months 16–24.

Each channel must have:

- first active phase;
- phase-specific maturity curve;
- end-of-phase monthly sessions;
- cost driver and unit;
- expected units/revenue using the channel-specific CVR;
- calibration gate before the next phase;
- evidence class.

Phase totals must equal the sum of displayed channel run rates. The final phase must reconcile to the selected scenario goal within rounding tolerance. If economics cannot support the full traffic plan, the system must create an approved-budget plan rather than displaying the unaffordable traffic target as the operating commitment.

## 11. Financial and contribution-economics model

TACOS is necessary but insufficient. The prospect needs to see what remains after acquiring and fulfilling the sale.

### Required inputs

- AOV/list price;
- conversion rate by channel or documented fallback;
- target TACOS;
- COGS per unit;
- Amazon referral fee rate;
- estimated FBA/fulfillment fee per unit;
- inbound or customer shipping cost where applicable;
- creator/affiliate commission;
- TikTok/platform commission where applicable;
- media cost by channel;
- Anata retainer;
- Anata growth commission and baseline;
- return/refund allowance when material.

### Calculations

For each scenario and channel:

```text
orders = sessions × channel conversion rate
gross revenue = orders × AOV
media TACOS = media spend ÷ gross revenue
marketplace costs = referral + FBA/platform fees
product costs = COGS + fulfillment/shipping + returns allowance
affiliate costs = creator + TikTok/platform commissions + applicable product costs
Anata fees = retainer + commission above baseline
contribution profit = revenue - discounts - media - marketplace costs - product costs - affiliate costs - Anata fees
contribution margin = contribution profit ÷ revenue
break-even TACOS = pre-ad contribution margin after non-media variable costs
```

### Required presentation behavior

Show at least:

- steady-state revenue;
- media spend;
- implied TACOS;
- 15% TACOS budget;
- non-media variable costs;
- Anata fees;
- contribution profit/margin;
- missing inputs;
- budget or traffic adjustment required to reach the target.

If COGS or fulfillment costs are missing, do not show a green profitability conclusion. Show `Contribution profit unavailable` and list the exact inputs required.

If the full traffic scenario requires 16.8% TACOS and the target is 15%, calculate and present one or more reconciliations:

- traffic supportable at the approved budget;
- CPC required to fund the full goal;
- CVR or AOV required to fund the full goal;
- timeline extension or organic-mix shift required.

The recommended operating plan must not exceed the approved media budget without a visible exception.

## 12. Sales-deck content behavior

### Executive summary

Replace unconditional opportunity claims with evidence-aware claims:

- Ready: `A $X monthly category benchmark across N qualified listings.`
- Limited: `A directional benchmark from N relevant listings; validate before forecasting.`
- Blocked: suppress the number and say `Market comparison needs refinement.`

Do not use “opportunity” as a synonym for total competitor revenue. Explain whether the number is total observed competitor revenue, addressable share, or modeled target revenue.

### Target benchmark

Show the exact comparator rationale. Never label an incompatible product as “best seller.” When target metrics are missing, show `Unavailable` rather than converting absence into zero.

### Search behavior

Show the evidence class in the section title or subtitle. Search-volume claims require an approved volume source. Competitor-language claims must say they are title/copy observations.

### Growth plan

Lead with the selected scenario, benchmark source, and critical assumptions. Keep the phase/action presentation, but add phase gates such as:

- launch paid scale only after PDP CVR reaches the agreed threshold;
- expand off-channel only after attribution and inventory gates pass;
- expand creator spend only after qualified-session and contribution thresholds pass;
- activate LTV tactics only after audience-size eligibility is met.

### Economics

Place the contribution summary before the engagement offer. A prospect should understand the likely economics before seeing Anata's price.

### Offer section

Show the financial relationship between the modeled plan and the proposed engagement. If the $3,000 retainer is not included in the contribution model, state that clearly. Prefer including it.

## 13. Admin UI requirements

Follow `DESIGN.md` operator-mode patterns and shared status vocabulary.

### Page sequence

1. Page header and source readiness;
2. target input;
3. target verification;
4. category resolution;
5. competitor review and evidence assessment;
6. growth/economics assumptions;
7. generation action;
8. resulting status and preview.

### Status and alerts

- Teal `Ready`: all required evidence gates passed.
- Amber `Needs review`: operator action can correct the evidence.
- Red `Blocked`: no prospect-ready output is possible with current evidence.
- Neutral `Stale`: refresh required before delivery.

Color is never the only indicator. Every non-Ready state names the reason and ends with an action such as `Review 7 excluded products`, `Confirm target variation`, `Add COGS`, or `Upload Helium 10 CSV`.

### Loading

Show named stages instead of a single indefinite message:

1. Fetching target;
2. Resolving product category;
3. Discovering candidates;
4. Checking relevance;
5. Building economics;
6. Rendering preview.

The UI must remain stable while stages update and expose a safe retry after provider failure.

### Empty and error states

- No competitors: explain that no reliable market set was found and offer manual ASIN entry or CSV upload.
- Provider timeout: retain confirmed form inputs and allow retry.
- Partial product data: show available fields and identify missing fields.
- Conflicting identity: block generation until confirmed.
- Invalid economics: point to the exact input and preserve other values.
- Stale run: allow refresh into a new immutable run; do not silently mutate a delivered deck.

### Responsive/accessibility

- Competitor tables use contained horizontal overflow and retain row actions.
- All fields have programmatic labels and inline errors tied with `aria-describedby`.
- Progress updates use an appropriate live region without repeatedly stealing focus.
- Keyboard users can include/exclude competitors and reach the generation action.
- Focus moves to the error summary when generation is blocked.
- At 1280px and 1440px, no critical controls or evidence columns are clipped.

## 14. API contract

The current synchronous generate endpoint should be split conceptually into analysis and generation, even if both remain in the same router initially.

### Analyze

`POST /admin/api/deck-analysis`

Input:

```json
{
  "asin_or_url": "B075WZVF7W",
  "amazon_domain": "amazon.com",
  "operator_category_label": "chalk mineral furniture paint"
}
```

Output includes target identity, category resolution, candidates, assessment, and an expiring `analysis_id` referencing the immutable input snapshot.

### Update review

`PATCH /admin/api/deck-analysis/{analysis_id}`

Allows category correction, manual inclusion/exclusion, explicit competitor ASINs, and verified target values. Every change requires a reason code or note.

### Generate

`POST /admin/api/generate-deck`

Accepts `analysis_id`, approved assumption set, selected scenario, offers, and deck options. The server revalidates analysis freshness and readiness. A client cannot bypass blocking gates by omitting assessment data.

### Status

Return structured status rather than only message text:

```json
{
  "status": "needs_review",
  "stage": "competitor_relevance",
  "progress_pct": 55,
  "blockers": [],
  "warnings": [
    {
      "code": "INSUFFICIENT_QUALIFIED_COMPETITORS",
      "message": "2 of 12 candidates match furniture paint.",
      "action": "review_competitors"
    }
  ]
}
```

## 15. Analytics and observability

Track:

- target fetch success/failure and latency;
- discovery path used;
- candidate and qualified competitor counts;
- relevance score distribution;
- hard-exclusion reason counts;
- percentage of runs requiring operator correction;
- category-label/discovery-category disagreements;
- observed vs BSR-estimated unit coverage;
- target metric conflicts;
- decks generated by readiness state;
- Needs review → Ready conversion;
- delivered decks later marked invalid;
- prospect engagement segmented by evidence quality;
- forecast vs realized performance when first-party data becomes available.

Log external writes and operator overrides. Do not log API keys or complete raw provider responses.

Alert when:

- cross-product-type exclusions exceed 30% of candidates;
- fewer than three qualified competitors are found;
- the category label is being ignored or does not match the discovery query;
- rating/review counts conflict materially across sources;
- a Ready deck contains an excluded term in its keyword recommendations;
- a displayed operating plan exceeds the configured TACOS budget.

## 16. Rollout and compatibility

### Phase A: shadow assessment

- Keep current generated output unchanged.
- Run the new category resolution and relevance scorer in shadow mode.
- Persist assessment metadata and compare old vs qualified market sets.
- Build regression fixtures from known ambiguous products, including furniture paint vs gym/classroom chalk.

### Phase B: internal warnings

- Show evidence status and competitor review to internal operators.
- Mark questionable decks Needs review, but retain internal preview.
- Do not change external delivered decks retroactively.

### Phase C: prospect-readiness enforcement

- Disable copy/share/deliver for Blocked decks.
- Require explicit operator review for limited benchmarks.
- Use only qualified competitors throughout downstream calculations.

### Phase D: contribution economics

- Add required margin inputs and scenarios.
- Make approved-budget plan the default sales recommendation.
- Begin forecast-vs-actual calibration.

### Rollback

Gate the new qualification and readiness enforcement behind configuration flags. Rollback may restore the previous internal workflow, but must not silently mark known invalid decks Ready. Existing immutable deck links continue to render with their original evidence version.

## 17. Acceptance criteria

### Target identity

1. Given a submitted ASIN, the normalized returned ASIN must match or generation is blocked.
2. The operator sees title, brand, image, variation, price, rating count, category, source, and freshness before generation.
3. Missing target metrics render as unavailable, never as observed zero.
4. Material source conflicts create a Needs review state with both values shown internally.

### Competitor discovery

5. The operator category/product type affects the discovery request, not only deck headings.
6. A furniture-paint target excludes gym chalk, climbing chalk, classroom chalk, sidewalk chalk, and tailor's chalk.
7. Every candidate has source, relevance score, status, and reason codes.
8. Duplicate variations do not inflate market totals.
9. Fewer than three qualified competitors suppresses competitor-derived market, keyword, and traffic claims.
10. Five or more qualified competitors permits full market claims when other coverage thresholds pass.

### Keywords

11. Excluded competitors cannot contribute keyword recommendations.
12. Automated competitor-language tables never display search-volume numbers without a volume source.
13. Insufficient qualified titles produces a corrective empty state.
14. The furniture-paint fixture never recommends “rock climbing,” “gym,” or “chalk ball.”

### Market and brand aggregation

15. By-brand average price equals the mean of qualified ASIN prices for that brand.
16. By-brand sessions, units, and revenue equal the sum of qualified ASIN rows.
17. Every aggregate displays comparison count and evidence composition.
18. Landscape-only outliers cannot define the benchmark.

### Growth plan

19. Goal sessions use an explicit operator goal or at least three benchmark-eligible competitors.
20. Unknown current sessions display as unknown and do not create an artificial zero baseline.
21. Phase totals equal displayed channel totals within rounding tolerance.
22. Phase session targets increase according to configured maturity curves and reconcile to the final goal.
23. The selected plan identifies the benchmark, CVR, AOV, CPCs, and confidence class.
24. An unaffordable full-traffic plan is not labeled as the approved operating plan.

### Economics

25. Implied TACOS equals media spend divided by modeled revenue.
26. The target TACOS media budget equals modeled revenue multiplied by target TACOS.
27. Affiliate/product/platform costs remain separated from media TACOS and appear in contribution economics.
28. Anata retainer and growth commission are included or explicitly marked excluded.
29. Missing COGS/FBA inputs prevent a positive contribution-profit conclusion.
30. When modeled spend exceeds the TACOS budget, the system shows the budget, gap, and at least one reconciliation lever.

### Operator and delivery states

31. Generated HTML can exist while the run remains Needs review or Blocked.
32. Only Ready runs expose standard external delivery actions.
33. Manual overrides are auditable with actor, timestamp, field, old/new values, and reason.
34. Provider failure preserves form and confirmed analysis state for retry.
35. A stale analysis cannot be generated without server-side revalidation.

## 18. Validation plan

### Unit tests

- ASIN and variation normalization;
- canonical product-type classification;
- safe discovery-query construction;
- score weights, thresholds, hard exclusions, and reason codes;
- candidate deduplication;
- evidence coverage and readiness states;
- qualified market and brand aggregations;
- keyword extraction using qualified rows only;
- current/goal session precedence;
- phase reconciliation;
- TACOS, contribution, break-even, and scenario math.

### Integration tests

- mock Rainforest leaf-category success;
- mock bestseller failure followed by safe search fallback;
- confirm operator category reaches the discovery service;
- mock ambiguous furniture-paint results containing gym chalk and verify exclusion;
- target metric conflicts between provider responses;
- partial provider timeout with retained analysis;
- manual competitor approval/exclusion audit;
- generate from immutable analysis and reject stale/tampered analysis;
- ensure downstream sections receive the same qualified ASIN set.

### Golden deck fixtures

Maintain representative fixtures for:

- ambiguous “chalk” furniture paint;
- supplement with neighboring but distinct formats;
- apparel parent/child variation;
- target missing from competitor search;
- sparse niche with fewer than three competitors;
- mixed observed-sales floors and BSR estimates;
- complete Helium 10 manual path;
- unknown current sessions and missing COGS;
- TACOS-compliant and TACOS-exceeding plans.

Golden assertions should inspect claims and evidence labels, not brittle full-HTML snapshots alone.

### Browser verification

For desktop widths 1280px and 1440px:

1. enter ASIN;
2. analyze;
3. confirm target identity;
4. review competitor inclusions/exclusions;
5. correct category and rerun discovery;
6. enter economics inputs;
7. generate;
8. verify status and internal preview;
9. verify delivery actions only when Ready;
10. confirm deck market, keywords, benchmark, phases, and economics all reference the approved evidence.

## 19. Recommended implementation sequence

1. Add canonical target, category resolution, candidate, and assessment data contracts.
2. Separate target fetch from competitor discovery.
3. Wire operator category/product type into discovery.
4. Replace title-prefix fallback with safe product-type query construction.
5. Add relevance scoring, hard exclusions, deduplication, and regression fixtures.
6. Gate market, keyword, and benchmark calculations on qualified competitors.
7. Add analysis/review state to the admin flow and persist audit metadata.
8. Correct unknown-session handling and benchmark eligibility.
9. Add scenario-based growth planning and approved-budget reconciliation.
10. Add contribution economics including Anata fees.
11. Add prospect-readiness states and delivery gating.
12. Run shadow mode, inspect mismatches, tune thresholds, then enforce.

## 20. Open decisions and recommended defaults

### How many competitors are enough?

Recommended: five for full market claims, three for a limited benchmark, and fewer than three as Blocked for competitor-derived claims.

### Can an operator override relevance?

Recommended: yes, with a required reason and audit entry. A manual inclusion should carry a visible `operator approved` evidence label.

### Should an LLM classify product type?

Recommended: suggestion only. Deterministic category, attribute, and exclusion rules remain authoritative for qualification.

### Should existing deck links update when data is corrected?

Recommended: no. Decks remain immutable and versioned. Generate a new run and optionally mark the old run internally as invalid or superseded.

### Should the $3,000 retainer be included in prospect economics?

Recommended: yes. Show operating contribution before and after Anata fees so the commercial proposal is defensible.

### What is the default benchmark statistic?

Recommended: the median or 60th percentile of benchmark-eligible qualified competitors. Avoid top-three mean as the default because it is sensitive to outliers and estimated data.

### What happens when exact search-volume data is absent?

Recommended: show qualified competitor language as observed language, never as demand volume, and invite the operator to add Helium 10 data for forecasting.

## 21. Definition of done

This initiative is complete when an ambiguous target such as Dixie Belle furniture paint can be analyzed without admitting gym/classroom chalk into prospect-facing evidence; the operator can see and correct the evidence before generation; all market, keyword, benchmark, phase, and economics sections consume one qualified immutable dataset; unsupported claims are suppressed; and the final deck communicates both growth potential and contribution economics in a form an Account Executive can confidently defend.

## 22. Client-facing experience objective

The public deck is not an internal data dump. It is a guided sales conversation that a prospect may also read without an Account Executive present.

### Client goal

Within the first two minutes, the client should understand:

1. that Anata analyzed the correct product and relevant competitive market;
2. the most important commercial gap;
3. why the evidence is credible;
4. what Anata recommends doing first;
5. what a supportable result could look like;
6. what assumptions still require the client's data;
7. what decision or next step is being requested.

### Entry point

The client enters through the tokenized deck URL on desktop or mobile, typically from an Account Executive email or a live screen-share. The deck must work in both presenter-led and self-guided modes without exposing internal controls, debug language, system warnings, or implementation terminology.

### Primary path

`Executive decision → market evidence → current position → demand language → recommended plan → economics → Anata scope → next step`

### Client-facing success condition

After viewing the deck, a qualified prospect can accurately summarize the opportunity, distinguish facts from estimates, identify the first 90-day actions, understand the budget and profitability assumptions, and choose a next step without asking the salesperson to explain how the deck itself works.

## 23. Client-facing UX audit findings

These findings are ranked by sales impact, frequency, confidence, and implementation effort. P0 findings prevent a credible presentation. P1 findings materially weaken comprehension or conversion. P2 findings improve polish, accessibility, or self-guided use.

### P0 — The visual certainty exceeds the evidence certainty

**Evidence:** The reviewed deck leads with `A $63k monthly opportunity` and displays complete metric cards, charts, benchmark tables, and traffic plans even though the target was compared with gym and classroom chalk. The qualification caveat appears later and cannot neutralize the confidence established by the opening.

**Root cause:** Readiness and evidence quality are not first-class rendering inputs. All populated numbers receive the same visual authority.

**Desired behavior:** The visual hierarchy must track the evidence hierarchy. A weak or incomplete benchmark may not receive a definitive opportunity headline, positive status styling, or a complete operating forecast.

**Acceptance condition:** A deck with fewer than three qualified competitors opens with a `Market comparison needs review` state, suppresses unsupported totals, and cannot visually resemble a Ready deck.

### P0 — Incorrect recommendations are presented as actionable advice

**Evidence:** The reviewed deck recommends adding `rock climbing`, `gym`, and `chalk ball` to furniture-paint copy. Those terms are presented in normal recommendation components rather than as rejected evidence.

**Root cause:** Keyword extraction and recommendation rendering do not inherit competitor qualification status.

**Desired behavior:** Recommendations must carry evidence lineage. Excluded competitor language cannot reach a recommendation component. Low-confidence suggestions must be visually and verbally distinct from validated actions.

**Acceptance condition:** Every recommendation has a source class and confidence state; no recommendation can be rendered if its source set fails section eligibility.

### P0 — The deck does not answer the prospect's profitability question

**Evidence:** The deck shows $18,870 modeled revenue, $3,162 paid media, a $2,830 TACOS-constrained budget, approximately $481 of affiliate costs, and a $3,000 Anata retainer in different areas. COGS and FBA remain unknown. The client must combine these values mentally and still cannot determine contribution profit.

**Root cause:** Media efficiency, channel costs, product costs, and Anata commercial terms are presented as separate stories instead of one reconciled decision.

**Desired behavior:** Before the offer, show one contribution bridge from gross revenue to estimated contribution after known costs, with missing inputs explicitly blocking a profitability conclusion.

**Acceptance condition:** A client never sees a positive profitability implication when COGS or fulfillment is missing. The economics view shows included and excluded costs and reconciles to the recommended budget.

### P1 — The opening prioritizes a large number over the decision

**Evidence:** The opening headline emphasizes total category revenue. Total visible competitor revenue is not necessarily the client's addressable opportunity or achievable revenue.

**Root cause:** The headline is generated from the largest available market aggregate instead of the sales decision the evidence supports.

**Desired behavior:** Lead with a defensible decision statement, such as `The listing has a positioning and conversion gap before paid scale`, followed by the strongest qualified proof.

**Acceptance condition:** The headline specifies what the number means. Words such as `opportunity`, `addressable`, and `forecast` cannot be used interchangeably.

### P1 — Important caveats appear after the claim they qualify

**Evidence:** Source/methodology text and estimation warnings appear below major tables and growth calculations. A live presenter or scanning client may never reach them before forming a conclusion.

**Root cause:** Provenance is treated as a footnote rather than part of the metric component.

**Desired behavior:** Every high-impact number carries a concise adjacent evidence label. Detailed methodology remains available through progressive disclosure.

**Acceptance condition:** Price, revenue, sessions, search terms, and forecasts each show `Observed`, `Estimated`, `Operator input`, or `Scenario` at the point of use.

### P1 — Unknown data is visually converted into a poor baseline

**Evidence:** The reviewed deck displays `Current sessions: 0` because target sales/sessions were unavailable. This visually implies the product has no traffic rather than unknown traffic.

**Root cause:** Missing numeric data falls through to zero in the growth-plan presentation.

**Desired behavior:** Use `Unknown` with a specific request for Seller Central sessions or units. Show scenario uplift from an assumed baseline only after the assumption is explicitly approved.

**Acceptance condition:** Unknown values have a distinct neutral treatment and never participate in a subtraction, percentage, progress bar, or “gap” without an approved assumption.

### P1 — The middle of the deck is too repetitive for a sales conversation

**Evidence:** Section title and content headings repeat phrases such as `Who owns the page-one real estate` and `Where the listing needs to improve`. The client moves through multiple tables and grids before reaching the plan.

**Root cause:** Section-divider language and content-card language are generated independently, producing duplicate framing rather than narrative progression.

**Desired behavior:** Each section has one job and advances the argument. A divider may orient the client, but the following slide must add a conclusion, not repeat the title.

**Acceptance condition:** No adjacent section/divider pair repeats the same heading. Every section begins with a one-sentence takeaway and ends with its implication for the plan.

### P1 — The 24-month plan is informative but too dense to present live

**Evidence:** The growth section includes a five-step ramp, four detailed phase cards, five channel cards, numerous assumptions, costs, sessions, units, revenue values, and methodology text in one long section.

**Root cause:** The design attempts to satisfy executive, operator, and methodology needs simultaneously.

**Desired behavior:** Use a layered plan: executive roadmap first, selected phase detail second, assumptions/methodology last. The client should be able to understand the plan without reading every action.

**Acceptance condition:** The main roadmap communicates phase, time window, objective, milestone, traffic target, and investment in one scan. Detailed actions are expandable or placed on supporting slides.

### P1 — The plan lacks explicit decision gates

**Evidence:** Phase actions mention validation in prose, but the visual model emphasizes scheduled activation. A client may interpret every later channel as committed regardless of performance.

**Root cause:** Phase timing is modeled more strongly than progression criteria.

**Desired behavior:** Every phase ends with a measurable gate: conversion, contribution margin, attribution quality, inventory readiness, creator efficiency, or audience eligibility.

**Acceptance condition:** Later-phase spend is labeled conditional and the gate is displayed next to the transition.

### P1 — The offer appears disconnected from modeled economics

**Evidence:** The recommended $3,000 retainer and 5% growth commission appear after the growth economics without showing their effect on contribution.

**Root cause:** Offer cards use static commercial copy and are not reconciled with the selected forecast scenario.

**Desired behavior:** The recommended offer states what is included in the model and shows the financial effect of Anata fees.

**Acceptance condition:** The selected offer and economics summary reconcile to the same scenario, baseline, term, and fee assumptions.

### P2 — Evidence density needs a clearer visual grammar

**Evidence:** Observed data, estimated values, assumptions, and recommendations currently share similar typography and component treatment.

**Root cause:** The design system has status colors but the deck lacks a compact client-facing evidence-label component.

**Desired behavior:** Introduce an `EvidenceLabel` variant for public deliverables using text plus restrained semantic color:

- `Observed` — direct/current source;
- `Estimated` — derived from observed inputs;
- `Client input` — supplied or confirmed by the prospect/operator;
- `Scenario` — modeled outcome;
- `Needs validation` — not safe for commitment.

**Acceptance condition:** Evidence meaning remains clear in grayscale/print and does not rely on color alone.

### P2 — Self-guided navigation needs stronger orientation

**Evidence:** The opening has jump links, but a long scrolling deck makes it difficult to know progress, return to the summary, or distinguish core narrative from methodology.

**Desired behavior:** Provide a restrained sticky progress/navigation control on desktop and a compact section menu on mobile. Include `Back to summary` at major transitions.

**Acceptance condition:** Navigation is keyboard accessible, does not obscure content, preserves tokenized URLs, and is excluded or adapted in print.

## 24. Revised client narrative architecture

The deck should be shorter in its core path and move detailed evidence into supporting layers.

### 1. Decision summary

Purpose: establish relevance, trust, and the recommendation.

Show:

- exact product identity and image;
- one decision-led headline;
- three to four high-confidence metrics;
- one evidence-quality line;
- recommended first action;
- section navigation.

Do not show an opportunity total unless its meaning and evidence are defensible.

### 2. Market position

Purpose: show the relevant competitive context.

Show:

- comparison-set definition;
- qualified competitor count and freshness;
- category/benchmark metrics with evidence labels;
- by-brand view using average price and summed sessions/revenue;
- one takeaway explaining where the client sits.

Move the full ASIN table into expandable evidence or an appendix.

### 3. Listing gap

Purpose: compare the target with the right benchmark.

Show:

- target vs representative benchmark or benchmark range;
- verified price, ratings, review count, BSR, and content coverage;
- two strengths and two prioritized gaps;
- explicit missing-data requests.

Avoid framing every difference as a defect. Explain whether the gap affects discoverability, conversion, trust, or margin.

### 4. Buyer discovery

Purpose: explain how relevant buyers discover this product type.

Show separate blocks for:

- verified search-volume data, when available;
- observed language from qualified competitors;
- suggested positioning language.

Each block states its source and allowed use. Only verified volume may support demand forecasts.

### 5. Recommended growth path

Purpose: connect the evidence to a phased operating plan.

Show one roadmap with four phases. Each phase displays:

- time window;
- business objective;
- two to four core actions;
- end-of-phase traffic range;
- expected investment range;
- success gate;
- newly activated channels.

Place channel-level math in a supporting view. Use ranges when data uncertainty would make a single number falsely precise.

### 6. Economics and decision

Purpose: answer whether the plan can be profitable and what must be true.

Show:

- selected scenario;
- gross revenue;
- media budget and TACOS;
- marketplace/product costs;
- Anata fees;
- contribution outcome or missing-data state;
- the constraint and recommended adjustment;
- sensitivity levers for CVR, CPC, AOV, and organic mix.

### 7. Anata engagement

Purpose: make the purchase decision concrete.

Show the recommended engagement first, alternatives second. Connect scope to Phase 1 deliverables, ownership, timing, fees, and the selected economics scenario.

### 8. Next step

Purpose: ask for one clear commitment.

Show:

- exact next action;
- what the client should provide;
- what Anata will deliver next;
- expected timing;
- scheduling/contact action.

### Appendix

Include methodology, full competitor table, source list, assumptions, excluded competitors, detailed channel model, and definitions. The appendix preserves rigor without slowing the main sales story.

## 25. Client-facing component contracts

### DecisionHeader

Contains the product identity, decision headline, concise recommendation, evidence status, and presentation date. One `h1` only. The product image must have meaningful alternative text.

### EvidenceLabel

Always contains visible text and optional source/freshness detail. It cannot be represented by color alone. On hover/focus or disclosure, it explains the derivation in plain language.

### MetricCard

Required fields:

- label;
- value or `Unknown`;
- evidence class;
- source/freshness;
- comparison context;
- optional caution.

A MetricCard may not use green/positive treatment solely because the number is large.

### InsightCallout

Contains `What we see`, `Why it matters`, and `Recommended response`. This replaces unsupported declarative copy with a traceable sales argument.

### ComparisonTable

Must identify the comparison basis, allow brand/ASIN views where relevant, preserve row labels on mobile, and disclose which rows are benchmark-eligible. Long titles are truncated visually but available to keyboard and assistive-technology users.

### PhaseRoadmap

Shows four ordered phases and conditional transitions. Each transition includes a success gate. Current/goal traffic may display as a range or unknown; no progress percentage is calculated from unknown data.

### EconomicsBridge

Shows the flow from gross revenue to contribution outcome. Missing values remain visible as gaps. The component must distinguish media TACOS from affiliate commissions, fulfillment, product costs, and service fees.

### AssumptionPanel

Lists scenario inputs with their evidence class and owner. Client-confirmable assumptions have a clear follow-up action in the internal/operator version; the external deck shows which inputs require validation without exposing editing controls.

### OfferCard

Shows recommended status, scope, owner, fee structure, minimum term, and relationship to the modeled economics. Avoid presenting two visually equal choices when one is recommended for a specific reason.

### NextStepPanel

Contains one primary action, optional secondary contact path, what happens after the action, and expected response timing. Links must describe their destination rather than using repeated generic text.

## 26. Visual hierarchy and content-density rules

- One primary takeaway per viewport or printed page.
- One dominant metric at most; supporting metrics may not compete at the same scale.
- Use client language before platform terminology. Define TACOS, BSR, DSP, and BTP on first use.
- Keep body copy to short explanatory paragraphs; convert operational detail into structured rows or appendix content.
- Avoid adjacent grids of visually identical cards when the information has a clear sequence or priority.
- Use status color only for evidence and decision meaning, consistent with `DESIGN.md`.
- Preserve Montserrat for concise headings/labels and Inter/Segoe UI for reading.
- Reuse the Anata token system; do not introduce a second visual language or raw colors.
- Keep decorative treatments subordinate to data credibility. Spotlight effects may support focus but cannot imply confidence or quality.
- Every chart has a textual takeaway and accessible data alternative.
- Every abbreviation and estimate marker remains legible in projected, mobile, and print contexts.

## 27. Responsive, presentation, and print behavior

### Desktop presentation mode

- Optimize for 1280×720, 1440×900, and common screen-share dimensions.
- Major section starts should fit without clipping the headline, takeaway, and key evidence.
- Sticky navigation must not cover headings when jump links are used.
- Tables use contained overflow and keep the first identifying column visible where practical.
- Presenter mode may reveal speaker notes or methodology prompts internally, but never in the public URL.

### Mobile self-guided mode

- Stack metric content in priority order, not source order.
- Replace wide comparison tables with labeled comparison rows or cards when horizontal scrolling would hide meaning.
- Use a compact section menu rather than a persistent wide navigation rail.
- Maintain minimum 44×44px interactive targets.
- Avoid charts whose labels require hover.
- Keep the primary scheduling action reachable after the summary and at the final section without duplicating competing CTAs throughout every section.

### Print/PDF mode

- Preserve section boundaries and avoid splitting a phase, table row, or offer card across pages.
- Remove sticky UI, animation, and unnecessary navigation.
- Repeat table headers across pages.
- Include full URLs or useful link labels where clickable links may be lost.
- Ensure evidence labels and statuses remain distinguishable in grayscale.
- Add generation date, evidence version, and page number in a restrained footer.

## 28. Accessibility requirements

- Maintain logical heading order with one page `h1` and sequential section headings.
- Provide a skip link to the decision summary and another route to the next-step section.
- Use semantic tables only for actual tabular comparisons; cards and layout grids must not masquerade as tables.
- Tabs such as `By ASIN` and `By Brand` implement correct tablist, tab, and tabpanel semantics with arrow-key behavior.
- All expandable methodology and evidence controls expose name, state, and focus behavior.
- Product and competitor images use meaningful alt text; decorative imagery uses empty alt text.
- Charts have concise summaries and accessible underlying values.
- Never rely on teal, amber, red, or sky color alone.
- Meet WCAG AA contrast for text, controls, evidence labels, and focus indicators.
- Honor `prefers-reduced-motion`; no metric animation is required for comprehension.
- Focus follows navigation predictably and never jumps merely because analytics or lazy loading updates.
- Public tracking must not interfere with keyboard interaction, announcements, or reading order.

## 29. Client-view states and recovery behavior

### Ready

The standard client deck. Evidence checks passed. Source/freshness labels remain visible but do not interrupt the narrative.

### Directional

Allowed only when sales leadership approves the limited-benchmark policy. The opening and affected sections say `Directional analysis`, specify the limitation, and avoid definitive opportunity or forecast language.

### Stale

The public link may continue to show the immutable deck, but a visible date and restrained stale notice explain that market data should be refreshed before making a new decision. Internal users see `Generate refreshed version`.

### Superseded

An old deck remains accessible for auditability but displays a non-obstructive notice linking internal users to the replacement. Public behavior requires a product decision: recommended default is to keep the immutable historical view without exposing internal run details.

### Invalidated

If a delivered deck is later found to contain materially wrong data, internal access must show a strong invalidation banner and disable reuse. The external policy must be defined before launch; recommended default is a neutral `This analysis is being refreshed—contact Anata for the current version` page rather than continuing to present known false claims.

### Partial/blocked

Never exposed as a normal client deck. Internal preview shows which sections are suppressed and the exact recovery action.

## 30. Client-facing UI acceptance criteria

36. The first viewport identifies the exact product, analysis date, evidence status, main conclusion, and recommended next action.
37. A market-size number cannot appear in the primary headline unless the comparison set passes full-market readiness.
38. Every high-impact number displays its evidence class adjacent to the value.
39. Unknown values render as `Unknown`, never zero, and do not produce progress percentages or opportunity deltas.
40. The core narrative contains no adjacent duplicate section headings.
41. The core deck can be presented without opening the methodology appendix.
42. The comparison-set definition and qualified listing count appear before competitive conclusions.
43. Excluded competitors and their terms cannot appear in client recommendations.
44. Search volume, observed competitor language, and suggested positioning are visually and verbally distinct.
45. The four-phase roadmap fits in one executive overview and exposes detailed actions separately.
46. Every phase transition displays a measurable success gate.
47. Traffic and financial outputs use ranges when the input confidence does not support single-number precision.
48. The economics section reconciles gross revenue, media, non-media variable costs, Anata fees, and contribution outcome.
49. Missing COGS or fulfillment produces a visible incomplete-economics state before the offer.
50. The recommended offer states whether its fees are included in the displayed contribution outcome.
51. The final section contains one primary next action and explains what happens afterward.
52. Public content contains no internal run IDs, provider errors, debug wording, operator controls, or raw confidence-score mechanics.
53. At 1280×720 and 1440×900, headings, metrics, roadmap labels, and CTAs are not clipped or obscured.
54. At 375px and 430px widths, the primary narrative is readable without page-level horizontal scrolling.
55. Keyboard users can traverse navigation, disclosures, tabs, and CTAs in logical order with visible focus.
56. Charts, statuses, and evidence labels remain understandable without color and in print/PDF output.
57. Reduced-motion users receive no nonessential animation.
58. An invalidated deck cannot continue to display known false claims as a normal Ready experience.

## 31. UI validation plan

### Narrative comprehension test

Give a Ready deck to a sales representative and a person unfamiliar with the account. Without coaching, each should answer:

- What product was analyzed?
- What is the primary growth constraint?
- Which evidence is observed versus estimated?
- What happens in the first 90 days?
- What budget is recommended and why?
- Is the plan profitable with current information?
- What should the client do next?

Failure to answer any of these indicates a hierarchy or copy problem, even if every component renders correctly.

### Sales role-play test

The Account Executive presents the deck to an internal challenger who asks:

- Why are these the right competitors?
- Where did this number come from?
- Why is this session goal achievable?
- How does 15% TACOS constrain the plan?
- Are your fees included?
- What needs to be true before Phase 2?

The presenter must answer from visible deck evidence without consulting source code or an external spreadsheet.

### Visual QA matrix

Verify:

- desktop presentation at 1280×720, 1440×900, and 1920×1080;
- mobile at 375×812 and 430×932;
- browser zoom at 200%;
- keyboard-only navigation;
- reduced-motion preference;
- grayscale/print preview;
- long product/brand names;
- missing image, price, ratings, sessions, COGS, and competitors;
- Ready, Directional, Stale, Superseded, Invalidated, and internal Blocked states.

### Analytics validation

Measure section reach, time to economics, CTA engagement, navigation use, and abandonment. Do not treat longer time as automatically positive; combine behavioral data with comprehension and sales-conversion outcomes.

## 32. UI implementation priorities

### Priority 1 — credibility gate

Implement evidence-aware rendering, suppress invalid claims, fix unknown values, and prevent non-Ready external delivery. This has the highest sales impact and must precede visual polish.

### Priority 2 — decision narrative

Reorder the client path, rewrite the opening around a defensible decision, simplify repeated sections, and move detailed tables/methodology into supporting layers.

### Priority 3 — economics bridge

Add contribution economics and reconcile the recommended offer with the plan.

### Priority 4 — roadmap clarity

Create the executive roadmap with phase gates and supporting channel detail.

### Priority 5 — responsive and accessible refinement

Complete mobile comparison behavior, presentation sizing, print rules, keyboard interaction, chart alternatives, and reduced motion.

## 33. Additional open UI decisions

### Should clients see confidence scores?

Recommended: show plain-language evidence states, not numeric internal scores. Numeric relevance/confidence values belong in the operator review and appendix only when useful.

### Should the core deck be paginated or remain one long page?

Recommended: retain scroll-based delivery for link sharing, but structure it as clear presentation-length sections with sticky progress and print-safe boundaries. Avoid artificial carousel controls that make comparison and deep linking harder.

### Should methodology be hidden?

Recommended: no. Keep it available through an appendix or disclosure, while placing concise provenance beside each important number.

### Should a Directional deck be shareable?

Recommended: only under a documented sales policy and with unmistakable directional language. The safer default is internal preview until full readiness passes.

### How should an invalidated public link behave?

Recommended: replace known false analysis with a neutral refresh notice and contact action while preserving the original evidence internally for audit. Legal/sales leadership should approve the external wording.
