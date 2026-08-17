# Clay Table Blueprint — the enrichment handoff

Goal: take a bare list of ecommerce store domains from StoreLeads and turn each one into a
real decision-maker with a verified email and one personalized line, then push only the good,
qualified leads into Instantly. Clay handles contact enrichment and qualification
and personalization.

Build this as one Clay table, left to right. Each numbered item is one column.

---

## Column order

1. **Import** — the StoreLeads list.
   - Source: CSV import from StoreLeads, or point StoreLeads' webhook at this table.
   - Minimum fields to bring in: `domain`, `brand_name`, `niche`/`category`, and any
     StoreLeads signals you want to reference (apps installed, estimated revenue, etc.).

2. **Find the decision-maker** (Find People at company).
   - Search the domain for job titles: owner, founder, co-founder, CEO, CMO, head of
     marketing, ecommerce director.
   - Keep it to decision-makers. Skip junior/marketing-coordinator roles.

3. **Find work email** (waterfall).
   - Use Clay's waterfall across 2-3 email providers so you only pay for the one that returns
     a result. Inputs: full name + company domain + LinkedIn.

4. **Verify email** (bulk verification).
   - Run every found email through a bulk verifier. Keep only "good/valid".

5. **Catch-all recovery** (only for the risky/catch-all ones).
   - Run anything that came back "catch-all" or "unknown" through a catch-all-specific verifier
     (e.g. Findmail). About half of catch-alls are real and worth recovering.
   - Gate this so it only runs on non-good rows (saves credits).

6. **Qualify** (AI, run BEFORE personalization to save credits).
   - Prompt below. Output must be a single word: `yes` or `no`.
   - Only rows that pass continue. Expect this to remove 30-50% of the raw list. That is the
     point and it is what keeps reply rates high and spam complaints low.

7. **Personalized line** (AI, only on qualified + valid rows).
   - Prompt below. Output is ONE short sentence (roughly 8-15 words) that becomes `[[clay_line]]`
     in Email 1.

8. **Push to Instantly** (only rows that are: qualified = yes, email = valid, line = written).
   - Map: email, first name, `{{brand}}`, `{{niche}}`, and the personalized line as a custom
     field named `clay_line`.
   - Point at the correct Instantly campaign (see doc 04).

---

## Prompt A — Qualification (column 6)

Paste into the AI column. Feed it the brand's domain, name, niche, and (if available) their
StoreLeads signals and a scrape of their site.

```
You are a sales qualifier for a performance-marketing agency that runs paid ads for
direct-to-consumer ecommerce brands.

Decide if THIS company is a good fit to pitch. A good fit is:
- A real direct-to-consumer ecommerce brand that sells its OWN products.
- Roughly $1M to $15M in annual sales.
- In beauty/wellness/supplements, food & beverage, apparel/accessories, home/lifestyle,
  pets, or baby/kids/toys/gifts.
- Almost certainly already running paid ads (Meta, Google, TikTok) or clearly could.

NOT a fit (answer no):
- Agencies, consultancies, software, wholesalers, distributors, manufacturers, B2B,
  dropshippers, print-on-demand, Etsy/Amazon-only resellers, marketplaces.
- Tiny stores with almost no products or traffic, or clearly inactive stores.

Company: {{brand}} ({{domain}})
Niche: {{niche}}
Notes: {{any StoreLeads signals or site summary}}

Answer with ONE word only: yes or no. No explanation.
```

---

## Prompt B — Personalized line (column 7)

This is the single sentence that opens Email 1. It must sound like real research, not a
template. Base it on something NOT obvious from a plain company description.

```
Write ONE short sentence (8 to 15 words) I can open a cold email with, addressed to the
founder of a direct-to-consumer ecommerce brand.

The sentence must reference something SPECIFIC and real about THIS brand that shows I looked:
a product line or bestseller, a recent launch or collection, a promotion or bundle, an
obvious site or ad detail, or a category dynamic. Do NOT reference anything generic that
applies to any brand (like "I love your mission" or "great website").

Do NOT pitch, compliment vaguely, or mention ads/marketing. Just the specific observation.
Keep it casual and human. No emojis. No exclamation points.

Brand: {{brand}} ({{domain}})
Niche: {{niche}}
What we found: {{site scrape / StoreLeads signals}}

Return only the sentence.
```

Quality check before you run the whole list: run steps 6 and 7 on ~100 rows and read them.
If even 1 in 10 personalized lines sounds off or is factually wrong, tighten Prompt B before
running the full list. A wrong line is worse than no line.

---

## Cost note
Clay's $300+/month tier is the one that unlocks bringing your own provider keys and webhooks,
which is what makes this cost-effective at real volume. Qualifying (step 6) before enriching
and personalizing is the single biggest credit saver.
