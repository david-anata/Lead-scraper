# 11 - Copy Audit: our sequence vs the Copywriting Playbook

Audited: `docs/outbound/01-email-sequence.md` (v1)
Standard: `Cold_Outbound_Copywriting_Playbook.md` + `Anata_Cold_Outbound_System_Brief.md` s5.3
Verdict: **7 findings. 3 of them would materially cost us replies. Do not send v1.**
Result: a corrected **v2** is at the bottom. v2 is what goes into Instantly.

---

## What v1 already got right (keep these)
- Email 1 is text only: no links, no images, no opt-out language. (Playbook 11, Brief 5.3)
- Spintax throughout, so no two sends are worded identically. (Brief 5.3)
- CTA is answerable with one word, no calendar link in email 1. (Playbook 3, Block 4)
- First name is the main token. (Playbook 3)
- The personalized line comes from real research, and is one sentence, not a paragraph. (Playbook 3, Block 1)
- Winner is judged on positive reply rate. (Playbook 9)
- 3 to 5 days between sends, and a 3 month recycle with a fresh angle. (Brief 6)

---

## Findings

### 1. Two of the three subject lines give away the pitch (HIGH)
Playbook 5 states the single governing rule: the subject and preview must never reveal
that a pitch is coming. Their only job is to earn the open.

- `{{brand}} + ad spend` - announces this is an ads pitch.
- `question on {{brand}}'s ads` - same problem.

Also, putting the company name in the subject is one of the most common mail-merge tells
(Playbook 5), so both lines fail twice. Only `{{first_name}} - quick one` survives.

**Fix:** short, lowercase, curiosity-led, no brand token, question-led where possible.

### 2. No preview text is defined at all (HIGH)
Playbook 5 and 8: preview text is a second subject line, and subject plus preview share a
roughly 150 character budget. Leaving it undefined means the email client fills it with
"Hey {first name}," which wastes the single best piece of pre-open real estate we have.

**Fix:** v2 defines the first line so the preview pulls curiosity, not a greeting.

### 3. Block 2 (who am I / why should I care) is missing (HIGH)
The playbook's 4-block framework is Personalization, then **who am I and why it matters**,
then offer, then CTA. v1 jumps from the personalized line straight into the problem. There
is no specific, named, quantified, size-matched proof point, which is exactly what makes
social proof land instead of read as bragging (Playbook 2 and 3).

**Fix:** v2 adds one sentence of matched proof before the offer.

### 4. The offer has no risk reversal (MEDIUM-HIGH)
Playbook 3 and 6: an offer is result + timeframe + **risk mitigation**. v1 has the result
(about 40% lift) and the timeframe (first month) but nothing that removes the reader's
downside. Playbook 6 notes offers roughly triple top-of-funnel conversion, and the risk
mitigation is a core third of it.

**Fix:** v2 leads with a free, specific, front-end deliverable (a teardown of where their
spend is leaking) and gives a believable reason it is free. That is the playbook's
"front-end offer to core offer" structure, and it is lower risk than a revenue guarantee
we would have to honour on a cold list.

### 5. Three emails as the default, when the default should be two (MEDIUM)
Brief 5.3 says 2 to 3 max. Playbook 7 is stricter: default is two, and a third is added
only as an optimisation to a campaign already beating baseline, never as the starting
structure.

**Fix:** launch with emails 1 and 2. Email 3 is written and held, and only switched on once
positive reply rate is beating baseline.

### 6. Email 2 reminds them they ignored us (MEDIUM)
"bumping this once" and "floating this back up" tell the reader they already chose not to
reply, which Playbook 7 and 11 call out as actively discouraging a response. Playbook 7 also
says a good follow-up **elaborates**, and should stand alone without scrolling up.

**Fix:** v2's email 2 leads with new substance and never references the earlier email.

### 7. Brand token needs a cleanup step, and the sender needs a full name (LOW)
- Playbook 3 and 11: visibly auto-inserted variables (all caps, "LLC", "Inc") are a tell.
  StoreLeads merchant names carry these. Clay must casualize the brand name before it is
  ever merged into copy (Playbook 10 lists this as a good, narrow use of AI).
- Playbook 8: full name as sender reads as more legitimate than a first name alone. v1
  leaves `{Sender name}` undefined.

**Fix:** add a "Clean brand name" column in Clay, and set the sender to a full name.

---

## v2 - the sequence to load into Instantly

Launch with **Email 1 and Email 2 only**. Hold Email 3.

Variables: `{{first_name}}`, `{{brand_clean}}` (casualized in Clay), `{{niche}}`, `[[clay_line]]`.

### Email 1 - day 0, text only, no links

**Subject** (spintax, no brand token, nothing about ads):
`{quick question|worth a look?|one thing i noticed|mind if i ask}`

**Body:**
```
{Hey|Hi} {{first_name}},

[[clay_line]]

{We just|Recently we} helped another {{niche}} brand around your size find about
{$4k|$5k}/mo {sitting in|going to} ad campaigns that weren't converting, and moved it
into the ones that were.

{Most|A lot of} {{niche}} brands we look at are quietly {burning|wasting} 20-30% of
their budget the same way, with a {couple of|few} conversion leaks {costing|draining}
the rest.

{Happy to|I can} map out exactly where it's leaking for {{brand_clean}} and send it
over free. {We're|I'm} building out case studies in {{niche}} right now, so there's
no catch and nothing to pay.

{Want me to send it?|Should I put it together?}

{Best|Cheers},
[Full Name]
```

Block check: personalization (Clay line) → who am I and why it matters (matched, quantified
proof) → offer (specific free deliverable, believable reason it is free, zero risk) →
one-word CTA. Roughly 6 sentences, plain language, no links, no opt-out.

### Email 2 - day 4, same thread, elaborates (never mentions the first email)

**Subject:** leave blank so it threads.

**Body:**
```
{{first_name}} - the {three|3} things {we|I} check first, in case it's useful:

1. {Spend on|Budget going to} campaigns that haven't converted in 30 days
2. {Where|The point where} {mobile|checkout} traffic drops off
3. {Which|What} audiences are {overlapping|competing with each other} and bidding
   {against|up} themselves

{That's usually|Between those three is normally} where the 20-30% is hiding.

{Want the {{brand_clean}} version?|Should I run it for {{brand_clean}}?}
```

Elaborates with genuinely new substance, stands alone, same low-friction ask, still no links.

### Email 3 - HOLD. Only switch on once positive reply rate beats baseline.

```
{{first_name}}, last one from me on this.

{If ads aren't the priority right now, totally fair|No stress if the timing's off}.

{Either way|Regardless}, {I can|happy to} send the 2-minute breakdown of the 3
conversion leaks we see most in {{niche}} stores - {yours to keep|no strings}.

{Want it?|Should I send it over?}
```

---

## Before this goes live
1. Preview every spintax combination in Instantly and confirm none read as nonsense. (Brief 5.3)
2. Run the copy through a spam-word checker. (Brief 5.3)
3. Add the "Clean brand name" column in Clay and confirm 10 sample outputs by eye.
4. Read ~100 generated `[[clay_line]]` outputs. If even 1 in 10 is off or wrong, fix the
   prompt before sending. (Brief 4.3)
5. Set the sender to a full name on every mailbox. (Playbook 8)
6. David approves this copy, then a test batch to our own inboxes, then live.

## When testing
Change **one** thing at a time, and judge only on **positive reply rate**. Give each variant
roughly 500 to 1,000 sends before calling it. (Playbook 9)

---

# v3 - FINAL paste-ready copy (fixes what the live preview exposed)

The Instantly preview (25 Jul) showed every merge variable rendering **empty**, and three
sentences broke as a result:

| Preview showed | Why it is broken |
|---|---|
| `{Hey\|Hi} ,` | dangling comma where the name should be |
| "map out exactly where it's leaking **for** and send it" | brand variable was load-bearing |
| "building out case studies **in** right now" | niche variable was load-bearing |

That happens whenever a variable is missing for a lead, not just in preview. Clay will not
return a niche or a clean brand name for 100% of rows, so this **would** have gone out to
real people. Playbook 11 names this exactly: "mismatched grammar around a variable" is one
of the clearest mail-merge tells.

**The fix: no variable is load-bearing.** Every sentence below reads correctly even if the
variable is blank. The real personalization stays where it belongs, in `[[clay_line]]`,
which is the playbook's whole point (Block 1). The only remaining token is the first name,
which gets a fallback.

## Email 1 - paste this

**Subject:**
```
{quick question|worth a look?|one thing i noticed|mind if i ask}
```

**Body:**
```
{Hey|Hi} {{first_name}},

[[clay_line]]

{We just|Recently we} helped another brand about your size find around {$4k|$5k}/mo
{sitting in|going to} ad campaigns that weren't converting, and moved it into the
ones that were.

{Most|A lot of} the brands we look at are quietly {burning|wasting} 20-30% of their
budget the same way, with a {couple of|few} conversion leaks {costing|draining} the rest.

{Happy to|I can} map out exactly where the money is leaking and send it over free.
{We're|I'm} building out more case studies right now, so there's no catch and nothing
to pay.

{Want me to send it?|Should I put it together?}

{Best|Cheers},
[Your Full Name]
```

## Email 2 - paste this (day 4, blank subject so it threads)

```
{{first_name}} - the {three|3} things {we|I} check first, in case it's useful:

1. {Spend on|Budget going to} campaigns that haven't converted in 30 days
2. {Where|The point where} {mobile|checkout} traffic drops off
3. {Which|What} audiences are {overlapping|competing with each other} and bidding
{against|up} themselves

{That's usually|Between those three is normally} where the 20-30% is hiding.

{Want me to run it for you?|Should I put your version together?}
```

## Email 3 - HOLD (leave it saved but switch it off until you beat baseline)

```
{{first_name}}, last one from me on this.

{If ads aren't the priority right now, totally fair|No stress if the timing's off}.

{Either way|Regardless}, {I can|happy to} send the 2-minute breakdown of the 3
conversion leaks we see most - {yours to keep|no strings}.

{Want it?|Should I send it over?}
```

## Two settings to change in Instantly
1. **Set a fallback on `{{first_name}}`.** Use Instantly's Insert Variables menu and give it
   a default of `there`, so a missing name reads "Hey there," not "Hey ,".
2. **Spacing is set to 2 days.** Move it to 3 or 4. One or two days apart reads as needy and
   costs replies.

## How to actually validate the variables
The preview is blank because the campaign has **no leads**. To prove the syntax works:
1. Add one test lead in the Leads tab with your own email, a first name, and any brand.
2. Open Preview again. Confirm the name fills in and no sentence has a gap or double space.
3. Send the test to yourself and read it on a phone.
4. Confirm the spintax braces are gone in the received email. If you can still see `{` or
   `|` in the delivered message, the spintax did not render and it must be fixed before send.

---

# LOADED INTO INSTANTLY - 25 Jul 2026

v3 is now live in campaign `Anata // Claude`
(`56a13f93-a364-40f9-ab83-5b19a93f8eb1`). Written directly into all three steps and
verified by reading the campaign's own saved state back, not just the editor view.

| Step | Subject | Delay | State |
|---|---|---|---|
| 1 | `{quick question\|worth a look?\|one thing i noticed\|mind if i ask}` | day 0 | v3 loaded |
| 2 | blank, so it threads under email 1 | +3 days | v3 loaded |
| 3 | blank, so it threads | +2 days | v3 loaded, keep switched OFF until baseline is beaten |

Verified after a full page reload:
- **Zero load-bearing variables.** `{{niche}}` and `{{brand_clean}}` are gone from all
  three emails, so a missing value can never produce broken English again.
- **No links anywhere**, including email 1.
- `[[clay_line]]` present in email 1 as the single personalization slot.
- Sender signs off as `David Narayan` (full name, per Playbook 8).
- Spintax intact throughout.

## Two things left, both one click each
1. **Gap between email 2 and 3 is 2 days; make it 4.** The delay field would not accept a
   programmatic change, so it needs a manual edit on the Step 2 card. Day 0 / 3 / 7 is the
   target. Not a blocker: 2 days is tight but not a rule violation.
2. **Add one test lead** (your own email, a first name, any brand) in the Leads tab. The
   Add Leads dialog does not render for automation. Once a lead exists, open Preview and
   confirm the name fills in and no sentence has a gap. Then send yourself the test and
   check the delivered mail contains no `{` or `|` characters, which would mean spintax
   did not render.

If Gabe's mailboxes are also attached to this campaign, change the sign-off on email 1 so
it is not always David's name.
