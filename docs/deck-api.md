# Private deck API

A key-protected way to generate a sales deck without the admin login. It is
the admin **Manual** upload behind an API key, nothing more: it calls the same
code (`_run_generate_deck`) with the same trigger, so every deck gets the same
`automation_runs` record and output and appears in **Admin > Sales > Sales
assets** exactly like a manual upload.

## Turn it on

Set the environment variable `DECK_API_KEY` (Vercel project settings) to a long
random value. While it is empty the route answers **404** (disabled).

## Request

`POST /api/decks/generate`, `multipart/form-data`, header `X-Deck-Api-Key: <key>`.

| Field | Same as Manual form |
| --- | --- |
| `target_product_input` | Product URL or ASIN (optional if a target Xray is uploaded) |
| `category_label` | Category label, e.g. `Beaded Earrings` |
| `case_study_url` | Case study link |
| `csv_files` (repeat) | The 5 Helium 10 CSVs (target Xray, competitor Xray, Keyword, Cerebro, Word Frequency); types are auto-detected |
| `brand_name` | Optional. When set, this is the deck title, slug, and narrative brand. Use it when a homepage scrape would otherwise name the deck from the first tokens of the product title (Habibi NY was rendering as `H HABIBI White x anata`). |
| `target_price` | Optional. Positive number, with or without `$`. On a website / DTC target this is the price used in DTC pricing commentary, ahead of a homepage scrape (HENOSIS was priced at `$99` from the homepage instead of the real `$20`). On an Amazon target it corrects a scrape only when no ASIN-matched Helium 10 price is present. `growth_aov` still wins when both are sent. |
| optional | `channels`, `creative_mockup_url`, `offers`, `offer_payload_json`, `include_recommended_plan`, `include_growth_plan`, `template_version`, growth-plan fields |

Existing fields stay optional in the same way. Omitting `brand_name` and `target_price` still derives the brand from the page and the price from the page or the Amazon listing. The title is cleaned as described below.

## Website targets and Amazon presence

When `target_product_input` is a website URL rather than an Amazon ASIN or URL:

- Competitor Xray rows are not candidate-matched or fuzzy-matched to the prospect. A page-one product is never narrated as the brand.
- A multi-row file sent as `target_xray_csv` uses the row whose ASIN matches `target_product_input`. The first row is used only for a one-row export, or for a multi-row file when no ASIN was provided. If an ASIN was provided and it is not in the file, no row is treated as the prospect.
- Before any sentence about Amazon, the generator searches Amazon with the Rainforest client (`RAINFOREST_API_KEY`). `details.amazon_presence` is `present`, `absent`, or `unknown`.
- `present` builds an existing-seller plan from the matched listings (ASIN, title, rating, review count, price, and image count when Amazon returned them). `details.amazon_listings` carries those rows.
- `absent` is a completed search with no brand store and no listing. Only then may the deck say the brand is not an Amazon seller.
- `unknown` (no key, a failed call, or listings that could not be attributed) makes no claim that the brand is on Amazon or off it.
- The deck title is `{Brand} x anata strategy deck`. A repeated brand token, a trailing product word, a region suffix, "Home Page", and a scraped "Contact Us ..." nav fragment are removed.
- The follow-up date is three days out, then the next day that is not a weekend or a US federal holiday.

```bash
curl -X POST https://agent.anatainc.com/api/decks/generate \
  -H "X-Deck-Api-Key: $DECK_API_KEY" \
  -F target_product_input=https://habibiny.com \
  -F brand_name="Habibi NY" \
  -F target_price=20 \
  -F category_label="Eau de Parfum" \
  -F csv_files=@competitor_xray.csv
```

```bash
curl -X POST https://agent.anatainc.com/api/decks/generate \
  -H "X-Deck-Api-Key: $DECK_API_KEY" \
  -F target_product_input=https://www.amazon.com/dp/B0XXXXXXXX \
  -F category_label="Beaded Earrings" \
  -F case_study_url=https://anatainc.com/case-studies/sonrei \
  -F csv_files=@target_xray.csv -F csv_files=@competitor_xray.csv \
  -F csv_files=@keyword.csv -F csv_files=@cerebro.csv -F csv_files=@word_frequency.csv
```

Response: the same JSON as the Manual upload (`details.run_id`, `details.view_url`, warnings, ...).

Errors: `404` route disabled, `401` missing/wrong key (constant-time compare),
`400` no target product, `500` generation failed.

## Content gate

`details.content_gate_passed` is false when the HTML still contains a zero
dollar amount, the placeholder "this category", "Top 0", a zero or empty
metric, a coverage row of "0 covered" or "0 missing", product advice from
the wrong category, a claim about an image, review, or price the scrape
did not support, a retired client name, or a claim that the brand is not on
Amazon without `amazon_presence` of `absent`. `details.market_mode` is `dtc`
when the target is a store URL and no Amazon market export was supplied.
That deck is an Amazon launch plan from the store unless Amazon search
confirms the brand already sells there, in which case the deck uses the
live listings. Competitor and market metrics are left out when no export
was supplied.

`details.evidence_status` is `missing` when no usable competitor export was
supplied. The public page returns 200 for a DTC deck that passes the content
gate. It returns 409 when the market export was expected and is missing, or
when the content gate fails. Abby's yes path uses that same rule and holds
the send, with the reason logged, whenever the public page would not return
200. An unconfirmed "not on Amazon" claim fails the gate, so it is held too.
