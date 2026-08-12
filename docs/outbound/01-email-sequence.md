# Outbound Email Sequence — Amazon / marketplace management

Lead offer: we run Amazon for consumer brands. Proof point: Zantrex, ROAS 2x to 3.5x.

This doc describes the live Instantly campaign `Anata // Claude`
(id `56a13f93-a364-40f9-ab83-5b19a93f8eb1`). If you change one, change the other.

Previous versions of this doc described an ad-spend / wasted-budget pitch. That was never
what the live campaign said and it is not what Anata sells. Do not rebuild from it.

Rules baked in (from the cold-email playbook):
- 2 emails. Email 1 does the heavy lifting, Email 2 handles the obvious objection.
- Text only: no links, no images, no unsubscribe language (these send you to spam).
- The call to action is answerable with one word. No calendar link.
- Spintax is mandatory so every send is worded a little differently. Instantly's syntax is
  `{{RANDOM | option a | option b}}`, not `{a|b}`.
- The opener asks, it does not assert. No numbers in the opener.

## Variables

Instantly native fields, populated by the Clay column `Add Lead to Campaign`:

| Variable | Source in Clay | Example |
|---|---|---|
| `{{firstName}}` | `First Name` | David |
| `{{personalization}}` | lookup `record.personalization` | the one opening line our app writes |
| `{{sendingAccountFirstName}}` | Instantly sending account | |

Custom field:

| Variable | Source in Clay | Example |
|---|---|---|
| `{{amz_product}}` | `Formula` column, which reads `record.amz_product` | Huppy Toothpaste Tablets |

Always give custom fields a fallback (`{{amz_product | your products}}`) so a missing value
never renders as a gap in the sentence.

`clay_line`, `brand` and `niche` are gone. They duplicated `personalization` and
`companyName` and the campaign never referenced them.

---

## Email 1 — the pitch (day 0)

**Subject** (spintax): `{{RANDOM | one thing i noticed | quick one | worth a look?}}`

**Body:**
```
{{RANDOM | Hey | Hi}} {{firstName | there}},

{{personalization}}

We run Amazon for consumer brands. Last one was Zantrex, ROAS 2x to 3.5x.

{{RANDOM | Happy to | I can}} map out who's on your listings, what they're charging, and
who's bidding on your brand name.

{{RANDOM | Want it? | Should I send it?}}

{{RANDOM | Best | Cheers}},
{{sendingAccountFirstName}}
```

Why it works: `{{personalization}}` is the real, specific observation our Amazon check found
(earns trust), the middle names the offer and the proof, and the ask is a soft yes/no.

---

## Email 2 — kill the objection (reply in thread)

**Subject:** leave as `<Previous email's subject>` so it threads under Email 1.

**Body:**
```
{{firstName | there}} - {{RANDOM | worth saying | should have said}}: none of this needs
access to your Amazon account. It's all public.

What you'd get:

1. Who else is listing {{amz_product | your products}}, and at what price
2. Which competitors are bidding on your brand name
3. Where your listings sit against theirs

{{RANDOM | Want it? | Should I send it over?}}
```

Why it works: the first line removes the "you want my account access" objection before it
forms, then the numbered list makes the deliverable concrete. Bullet 1 names the actual
product our Amazon check found, so the email references the research rather than gesturing
at it.

---

## Where the research actually shows up

The Amazon check writes eight `amz_*` fields per brand. Only some reach the email:

| Field | Used where |
|---|---|
| `amz_situation` | gates the send, and decides which opening line the app writes |
| `amz_product` | Email 2, bullet 1 |
| `amz_rivals_on_name` | not used in copy |
| `amz_marketplace` | not used in copy |
| `amz_undercut`, `amz_sellers_band`, `amz_confidence` | not used in copy |

The opener deliberately holds back specifics because it asks rather than asserts. The
specifics belong in Email 2, where we are already being concrete.

---

## Pending change to the live campaign

Not yet applied in Instantly. Two edits to Email 1:

1. Delete the sentence `Takes me a day, costs nothing, we're building case studies.` It is
   reassurance, and "costs nothing, building case studies" makes the offer sound like we
   need the logo more than they need the audit.
2. Nothing else in Email 1 changes.

And one edit to Email 2, bullet 1:

- `Who else is listing your products, and at what price`
- becomes `Who else is listing {{amz_product | your products}}, and at what price`
