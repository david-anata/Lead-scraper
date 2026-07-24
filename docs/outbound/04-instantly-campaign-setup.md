# Instantly Campaign Setup Checklist

Your warmed sending domains are already live, so this is just configuring the campaign
correctly. Get these settings right or good copy still lands in spam.

## Mailboxes
- Route all your warmed sending mailboxes into the campaign by tag (not hand-picked).
- Leave warmup ON, permanently, on every mailbox. It offsets spam complaints.
- Keep daily sending per mailbox low: start ~10-20/day, ramp slowly, cap well under 50.
  Rough mature target: ~5 mailboxes per domain, ~100/day per domain across them.
- Turn ON "slow ramp" so mailboxes don't jump from 0 to full volume.

## Campaign options (the ones that matter)
- **Open tracking:** OFF.
- **Link tracking:** OFF. (Tracking code is a top spam trigger.)
- **Send first email as text only:** ON. (No links or images in Email 1.)
- **Winning metric:** POSITIVE reply rate (or opportunities), not open or click.
- **Stop on reply:** ON.
- **Allow risky emails:** OFF. Let Instantly skip likely bounces/spam-traps.
- **AI skip hostile / unlikely-to-reply:** ON where offered.
- **Unsubscribe link:** OFF in Email 1 (and skip it entirely; handle opt-outs manually).

## Sequence
- Paste the three emails from doc 01, with the spintax intact.
- Map the Clay custom field `clay_line` into Email 1 where `[[clay_line]]` sits.
- Space steps 3-5 days apart.
- Run Instantly's built-in spam-word checker on the copy before launching.

## Inbox placement (do this)
- Turn on daily inbox-placement testing.
- Set the automation: if placement drops below ~70-80%, pause that mailbox from sending and
  hold it in warmup for ~2-4 weeks, then slow-ramp it back when it recovers.

## Deliverability guardrails
- Never forward replies to another mailbox. Reply from inside Instantly's inbox.
- Maintain a block list (competitors, current clients, hostile repliers). A shared Google
  sheet of emails-to-skip is the easy way to feed it.
- Watch bounce rate. High bounce is as damaging as spam complaints; the Clay verification
  step keeps this low.

## What the app's scoreboard will read from here (next code phase)
Sent, reply rate, positive-reply rate, bounce, and inbox placement, per campaign and per niche.
That is why the winning metric and niche tagging above matter: they feed the scoreboard.
