# The Clay prompt that writes the opening line

Lives here so it is versioned. The live copy is in the Clay column; this is the
source of truth when they disagree.

**Where it goes:** Clay > Anata // Claude Table > Found Contacts > the AI column
that feeds `{{personalization}}` in Instantly.

**Run condition:** only when `amz_situation` is not empty. A brand we could not
confidently match sends an empty situation, and running the column anyway pays
for a sentence we must not send.

**Why the facts are words and not numbers:** the app buckets them before Clay
ever sees them ("a handful of other sellers", never 18). Seller counts move
daily. A brand that reads "19 sellers" and counts 14 stops reading there, so
there is deliberately no number available for the model to quote.

---

## Prompt

```
#CONTEXT#
You write the first line of a cold email to the owner of an ecommerce brand.
It sits directly under "Hey [first name]," and is the only line guaranteed to
be read. The rest of the email is already written and offers to map out what is
happening to them on Amazon.

#INPUT#
situation: /amz_situation
their product: /amz_product
other sellers: /amz_sellers_band
someone below their price: /amz_undercut
competitors on their name: /amz_rivals_on_name

What each situation means:
- resellers      other sellers are sitting on their own product listing
- undercut       one of those sellers is priced below what the brand charges
- rivals_on_name competitors appear in Amazon search for the brand's own name
- absent         nothing of theirs comes up on Amazon under their own name

#TASK#
Write ONE sentence about that situation, ending in a short question.

#RULES#
- One sentence. A second is allowed only if it is the question.
- It must end in a question. Questions get replies. Statements get ignored.
- Never write a number, a count, a price or a percentage. Use the exact wording
  given in "other sellers" and "competitors on their name" and nothing more
  precise. If you are tempted to be specific, you are wrong.
- Shorten their product to what a person would actually call it. "Rho Nutrition
  Liposomal NAD+ Liquid Supplement" becomes "the NAD+".
- Plain conversational English. No marketing language, no exclamation marks, no
  emoji, no greeting, no sign off, no quotes around the sentence.
- Do not compliment them. Do not open with "I noticed" or "I came across".
- Do not mention ACoS, TACoS, ad spend, fees, or what we sell. The next
  paragraph of the email does that.
- Do not state the obvious (that they sell supplements, that they are on
  Shopify, that Amazon is competitive).
- If situation is empty, output exactly: SKIP

#EXAMPLES#
situation=resellers | product=Rho Nutrition Liposomal NAD+ Liquid Supplement | other sellers=a handful of other sellers
There are a handful of other sellers sitting on your NAD+ listing. Are those all authorized?

situation=undercut | product=Huppy Plastic-Free Toothpaste Tablets | other sellers=a couple of other sellers
Someone is listing your toothpaste tablets below your own price. Is that an authorized seller?

situation=rivals_on_name | competitors on their name=a few competitors
A few competitors come up on Amazon when you search your own brand name. Is that something you are watching?

situation=absent
I could not find anything of yours on Amazon under your own name. Is that deliberate?

#OUTPUT#
The sentence only.
```

---

## After it runs

Read ten of them cold before sending any. The test from the copywriting
playbook: would a friend reading over your shoulder think it was written to one
person, or think it was a template? Optimize for the former.

Anything that comes back as `SKIP` must not be sent. Filter those rows out
rather than falling back to a generic line, because the playbook is explicit
that generic personalization hurts reply rates more than none at all.
