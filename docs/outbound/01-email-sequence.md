# Outbound Email Sequence — Performance Marketing

Lead offer: performance marketing. Proof: ~40% sales lift in month one by finding wasted
ad spend, reallocating it, and closing conversion-rate (CVR) holes.

Rules baked in (from the cold-email playbook):
- 3 emails max. Email 1 does the heavy lifting.
- Email 1 is text only: no links, no images, no unsubscribe language (these send you to spam).
- The call to action is answerable with one word. No calendar link in email 1.
- Spintax (the `{a|b|c}` options) is mandatory so every send is worded a little differently.
- First name is the only personalization token we always use. Company name is used carefully.
- The `[[clay_line]]` slot is the one personalized sentence Clay writes per brand (see doc 02).

Variables: `{{first_name}}`, `{{brand}}`, `{{niche}}` (e.g. "beauty", "pet"), `[[clay_line]]`.

---

## Email 1 — the pitch (Day 0, text only, no links)

**Subject** (spintax):
`{{{first_name}} - quick one|{{brand}} + ad spend|question on {{brand}}'s ads}`

**Body:**
```
{Hey|Hi} {{first_name}},

[[clay_line]]

{Most|A lot of} {{niche}} brands we look at are quietly {burning|wasting} 20-30% of their
ad budget on campaigns that aren't pulling, with a couple of conversion leaks on the site
{costing|draining} the rest.

We find that wasted spend, move it to what's actually working, and patch the leaks. In the
first month, the brands we do this for {average|typically see} about a 40% lift in sales.

{Worth|Want} me to take a quick look at {{brand}} and show you where the money's leaking?

{Best|Cheers},
{Sender name}
```

Why it works: the first line is Clay's real, specific observation (earns trust), the middle
names the problem and the exact mechanism, the 40% is the proof, and the ask is a soft yes/no
with zero friction.

---

## Email 2 — the bump (Day 3, same thread, short)

**Subject:** leave blank so it threads under Email 1.

**Body:**
```
{Hey|Hi} {{first_name}}, {floating this back up|bumping this once}.

We just did this for another {{niche}} brand and found a little over {$4k|$5k}/mo in wasted
spend in the first week, before we even touched the site.

{Open to a quick look at {{brand}}?|Want me to send the 3 things we'd check first?}
```

Why it works: adds one concrete number we cut from Email 1, keeps it short, same easy ask.

---

## Email 3 — lower the friction (Day 7, thread ok to include one trusted link)

**Subject:** leave blank (threads) or `{last one on this|quick idea for {{brand}}}`.

**Body:**
```
{{first_name}}, last one from me on this.

{If ads aren't the priority right now, totally fair|No stress if the timing's off}.

If it's useful either way, I can send a 2-minute breakdown of the 3 conversion leaks we see
most in {{niche}} stores. {Want it?|Should I send it over?}
```

Why it works: stops selling, offers free value (a handraiser), and gives a clean exit. This
is the email where a trusted link (a Loom, a short doc) is safe because the thread is now warm.

---

## Notes for setup
- Space emails 3 to 5 days apart. One day apart reads as needy and hurts replies.
- Pick the winner by POSITIVE reply rate, not raw replies (a high reply rate full of "no
  thanks" is a false win).
- When someone replies interested, reply from inside Instantly (never forward to another
  mailbox), and only then send the calendar link.
- After ~3 months, the un-replied list can be re-run with a fresh angle (e.g. lead with the
  CVR leaks instead of wasted spend), not the same copy.

## A/B variant to test if replies lag (from the discovery)
Swap Email 1's ask for the free-teardown hook you already have on your site:
`{Worth|Want} me to run {{brand}}'s numbers and send back where the spend is leaking?`
This is a lower-friction "free value" open and often out-pulls a direct pitch.
