# Anata Studios Event Center — Agreement Business Terms

Version: 1  
Status: Counsel-approved 2026-07-31, subject to two conditions, both now met  
Venue: Anata Studios Event Center, 1657 N. State Street, Lehi, Utah 84043

> This versioned schedule consolidates owner-approved commercial and operating
> rules. Counsel approved it on 2026-07-31 on two conditions: that it follows
> the policies published at anatabuilding.com/events/the-arena/policies, and
> that its dynamic portions are viable.
>
> Both conditions are enforced by
> `tests/test_building_agreement_dynamic_fields.py`, which pins the published
> commercial figures against this text and renders every merge token against a
> complete booking. Change a price on one side without the other and the test
> names the number.
>
> This remains a schedule, not a signed agreement. It must not be sent to a
> customer until the approval is recorded through the governed action in Agent.

## 1. Parties and event

- Customer: `{{customer_name}}`
- Customer email: `{{customer_email}}`
- Event space: `{{event_space}}`
- Setup access begins: `{{setup_starts_at}}`
- Guest event begins: `{{guest_starts_at}}`
- Guest event ends: `{{guest_ends_at}}`
- Teardown access ends: `{{teardown_ends_at}}`
- Estimated attendance: `{{attendance}}`
- Agreement signer must be at least 21.

Agent remains the authoritative source for the event window, availability,
commercial terms, agreement status, cleared payment, and booking status. A
calendar entry is only an operational projection.

## 2. Price and payment schedule

- Subtotal before discount: `{{subtotal_before_discount}} {{currency}}`
- Discount applied: `{{discount_amount}} {{currency}}`
- Reason for the discount: `{{discount_reason}}`
- Quote total: `{{quote_total}} {{currency}}`
- Required booking deposit: `{{deposit_amount}}`
- Deposit type: `{{deposit_type}}`
- Pricing inclusions: `{{included}}`
- Selected add-ons: `{{addons}}`
- Tax treatment: `{{tax_terms}}`

Where a discount is shown above, the quote total is already net of it and the
booking deposit is calculated from that discounted total. A discount applies
only to this booking and sets no rate for any future one. Where no discount is
shown, the amount reads zero and the subtotal equals the quote total.

The current owner-approved commercial baseline, effective November 10, 2026,
is a half day (up to 6 hours) at $1,500, a full day at $2,000, a full day plus
a second-day half day at $3,000, and two full days at $3,500. There is no
hourly rate. There is no security deposit and no upfront cleaning fee. Weekday
business events are not priced from this card; the team puts that pricing
together and can offer a tour.

50% of the venue rental is due to book the date. The remaining 50% is due
before the event. The 50% is calculated on venue rental, confirmed add-ons,
and estimated tax, after any discount. ACH and checks have to clear before
the deadline. No setup access is allowed until the second 50% has cleared.

Payment and signature must both be completed on the same day. Neither one alone
secures the date. A date is confirmed only when Agent records the approved
agreement, cleared required deposit, completed conflict check, and confirmed
reservation.

## 3. Cancellation and transfer

The frozen cancellation policy for this booking is:

`{{cancellation_policy}}`

Owner-approved policy:

- If the customer cancels within 7 days of a payment, that payment is refunded
  in full.
- If the customer cancels more than 7 days after payment, the paid 50% is
  forfeited and the unpaid 50% is not owed.
- A customer may request a date transfer at least 14 days before the event, but
  Anata may approve or deny the request in its sole discretion based on date
  availability and operational feasibility.
- An approved transfer may be used only once, must move the event to an
  available date within six months, and is subject to pricing current for the
  replacement date. Paid amounts are credited to the approved replacement
  event.
- A qualifying force-majeure event may receive a no-fee transfer within six
  months, subject to availability.
- If Anata cancels for a reason within its control, customer payments are
  refunded.

## 4. Access, hours, setup, teardown, and overtime

Booked time includes setup and teardown. There are no complimentary hours
outside that window. No Anata setup, teardown, or event labor is included
unless it is purchased.

Weekdays are for business events only. Large events and parties are weekends
only. New Year's Eve is allowed on any day of the week; the team confirms
those hours.

- Saturday: guest events may run 8:00 a.m.–midnight; teardown must finish by
  2:00 a.m.
- Sunday: guest events may run 8:00 a.m.–10:00 p.m.; teardown must finish by
  midnight.

Extensions beyond the booked window require Anata approval and are quoted
separately. Do not invent an extension price.

Without purchased setup/reset service, the customer configures the room and
returns tables and chairs to the designated staging area. The customer or
caterer removes all trash, decorations, food, property, and vendor equipment.

## 5. Included amenities and optional services

Included amenities are up to 30 tables, 200 chairs, built-in stage, available
microphones/speakers/projector/screens, guest Wi-Fi, lounge area and
refreshments, shared onsite parking, and ground-floor access. A kitchen is not
an included amenity and may not be used by vendors.

Optional services:

- Table/chair setup and reset: $250 for up to 75 guests; $400 for 76–150
  guests; $550 for 151–200 guests.
- A/V technician: $75 per hour with a two-hour minimum.
- Premium Anata event labor: $125 per hour per staff member with a two-hour
  minimum.
- Add-ons ordered fewer than seven days before the event carry a 20% rush fee.
- Linens, décor, event lighting, additional equipment, catering, and specialty
  staging are custom quoted and itemized.

## 6. Cleaning, damage, and property

There is no upfront cleaning fee and no security deposit. Clients bag trash
into the building's cans and remove decorations and food they brought. If
cleanup rules are not followed, or for damage, missing equipment, false alarms,
or overtime, extra charges may apply. Anata provides an itemized notice within
five business days.

Property left behind is held for seven days and may then be donated or disposed
of.

## 7. Vendors, food, and alcohol

All vendors require prior Anata approval. Outside caterers must be licensed and
insured. Food may be assembled onsite, but cooking and kitchen use are
prohibited. The customer is responsible for vendor conduct, damage, cleanup,
and compliance.

Alcohol requires prior written approval and must be supplied and served by an
approved, licensed, and insured provider. Customer-supplied alcohol is not
allowed. Alcohol service ends 30 minutes before the event ends. Alcohol events
require customer-paid security.

Alcohol plan, required security, and event schedule are due 14 days before the
event. Material changes to controlled items after that deadline require written
approval.

## 8. Insurance and security

Anata may require a certificate of insurance based on event risk. The
owner-approved starting requirement is $1 million per occurrence naming Anata
as an additional insured, subject to the approved agreement language.

Default security staffing:

- 1–75 attendees: one guard.
- 76–150 attendees: two guards.
- 151–200 attendees: three guards.

Anata may require more security based on alcohol, format, performers, ticketing,
age mix, or prior incidents. The customer pays provider cost plus a 20% Anata
coordination fee.

## 9. Decorations, effects, conduct, and safety

Customers may not move Anata furniture or equipment without consent. Painter’s
tape and removable adhesive strips are allowed. Balloons, loose confetti,
confetti cannons, glitter, flower petals, rice, and artificial snow are allowed
only if completely removed; residue results in an extraordinary-cleaning
charge.

Open flames are prohibited. Smoke and haze effects require prior approval.
Smoking and vaping are prohibited inside and within 20 feet of entrances.
Cannabis and illegal substances are prohibited.

Anata may stop unsafe, illegal, destructive, or materially disruptive activity.
Teen events require the signer or another approved responsible adult onsite
throughout. Children must remain supervised.

## 10. Parking, media, privacy, and records

Onsite parking is shared, not exclusive, and not guaranteed. Approximately 50
spaces may be available subject to other building use. No overnight parking is
permitted. A nearby North Lehi park-and-ride is available; customers and guests
park at their own risk subject to the legally approved allocation of risk.

Anata may photograph or record an event for marketing only with separate written
consent. Security cameras may operate in common and event areas, never in
restrooms or private areas. Signed agreements, payment records, incident
reports, inspection evidence, consent records, and material audit history are
retained for seven years.

## 11. Legal clauses covered by the 2026-07-31 approval

This section is retained as the record of what counsel approved on 2026-07-31,
not as a list of what is outstanding. The November 10, 2026 owner policy in
sections 2 through 4 replaces the July non-refundable-deposit wording. Where
they differ, sections 2 through 4 control. The July approval covers:

- non-refundable deposit, cancellation balances, transfers, and force majeure;
- tax and card-fee disclosures;
- late fees and collection remedies;
- post-event charges and saved-payment-method authorization;
- liability limitation, indemnity, assumption of risk, minors, ladder use,
  lawful gambling, weapons, and immediate termination;
- insurance and additional-insured wording;
- parking enforcement;
- mediation, Utah governing law, Utah County venue, attorney fees, and emergency
  relief;
- signer representations, notices, counterparts, severability, entire
  agreement, amendment, and electronic-signature consent.

The approved reusable agreement must reference this schedule’s version and
checksum or incorporate its approved terms without changing their meaning.
