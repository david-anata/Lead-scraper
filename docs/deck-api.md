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
| optional | `channels`, `creative_mockup_url`, `offers`, `offer_payload_json`, `include_recommended_plan`, `include_growth_plan`, `template_version`, growth-plan fields |

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
