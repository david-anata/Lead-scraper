# Target Roles — who we contact and why

What Anata sells: Amazon and marketplace management for consumer brands, with an owned
3PL. Target brand: roughly $1M to $20M a year, Shopify, US/UK/CA/AU, in beauty and
wellness, food and beverage, apparel, home and lifestyle, pets, or baby/kids/toys/gifts.

That size band is the single most important thing on this page. **At $1M to $5M most of
the roles below do not exist.** The founder is the ecommerce manager, the marketplace
manager and the ops lead. Only above roughly $8M do you reliably find a dedicated
marketplace or ecommerce hire. Treat the list as "who to accept when they exist", not
"who to go looking for".

Priority key:
- **A — signs the contract.** Contact these first.
- **B — owns the problem, can champion it internally.** Worth contacting.
- **C — feels the pain, cannot buy alone.** Contact only if A and B are unreachable.
- **D — do not contact.** Wrong function, or will route you to procurement.

---

## 1. Founder and owner — priority A

The core target. At our size band this person decides, and usually within one email.

- Founder
- Co-founder
- Founding Partner
- Owner
- Co-owner
- Proprietor
- Managing Director (UK and AU usage — often the owner)
- Managing Partner
- Director (UK usage — often the owner, NOT the US middle-manager sense)
- President
- Principal

**UK and Australia caution.** "Director" in the UK is frequently a company officer and a
real decision maker. In the US it is usually mid-level. Same word, two very different
people. Country has to be read alongside title.

## 2. C-suite — priority A

- Chief Executive Officer / CEO
- Chief Operating Officer / COO
- Chief Marketing Officer / CMO
- Chief Revenue Officer / CRO
- Chief Commercial Officer / CCO
- Chief Growth Officer
- Chief Digital Officer
- Chief Ecommerce Officer
- Chief Brand Officer
- Chief Strategy Officer
- Chief Customer Officer
- Chief Supply Chain Officer
- Chief Financial Officer / CFO — priority B, see finance below

## 3. Ecommerce and marketplace — priority A

The closest match to what we sell. If this person exists, they are often a better
contact than the CEO because the pain is theirs daily.

- VP of Ecommerce
- Head of Ecommerce
- Director of Ecommerce
- Ecommerce Manager
- Ecommerce Lead
- Digital Commerce Manager
- Online Sales Manager
- DTC Manager / Head of DTC
- Head of Marketplaces
- Marketplace Manager
- Marketplace Specialist
- Amazon Manager
- Amazon Specialist
- Amazon Account Manager
- Head of Amazon
- Channel Manager
- Head of Channel / Channel Development
- Retail Media Manager
- Head of Online Retail

**Anyone with "Amazon" or "Marketplace" in the title is the highest-intent contact in
this entire document**, regardless of seniority. They already own the problem the opener
describes. Do not deprioritise them for being a "Manager".

## 4. Marketing — priority B

Owns budget and often owns the channel by default when no ecommerce hire exists.

- VP of Marketing
- Head of Marketing
- Marketing Director
- Director of Marketing
- Marketing Manager
- Senior Marketing Manager
- Brand Manager
- Head of Brand
- Head of Growth
- Growth Manager / Growth Lead
- Head of Performance Marketing
- Performance Marketing Manager
- Paid Media Manager
- Digital Marketing Manager
- Head of Digital
- Digital Director
- Acquisition Manager
- Head of Customer Acquisition
- CRM Manager — priority C
- Content Manager — priority D
- Social Media Manager — priority D

## 5. Sales and revenue — priority B

- VP of Sales
- Head of Sales
- Sales Director
- Director of Sales
- National Sales Manager
- Head of Wholesale
- Wholesale Manager
- Key Account Manager
- Head of Business Development
- Business Development Manager
- Head of Partnerships
- Commercial Director
- Commercial Manager
- Head of Revenue

## 6. Operations, supply chain and fulfilment — priority B

Relevant because of the owned 3PL. These people feel warehousing and fulfilment pain
directly, and a marketplace conversation often opens through them.

- COO (see C-suite)
- VP of Operations
- Head of Operations
- Operations Director
- Operations Manager
- Head of Supply Chain
- Supply Chain Manager
- Head of Logistics
- Logistics Manager
- Fulfilment Manager
- Head of Fulfilment
- Warehouse Manager — priority C
- Inventory Manager
- Demand Planner / Head of Planning
- Procurement Manager — priority C
- Head of Product Operations

## 7. Finance — priority B, and only for the right message

Buys on margin and cost recovery, not growth. The Amazon opener will not land. Lead with
leaked margin, unauthorised sellers eroding price, or fulfilment cost.

- CFO
- Finance Director
- Head of Finance
- Financial Controller
- VP of Finance

## 8. Agency, consultant and fractional — priority C

Often the person actually running the channel at our size band. Can be a fast route in,
or a blocker protecting their retainer. Read the situation before pitching.

- Fractional CMO
- Fractional COO
- Ecommerce Consultant
- Amazon Consultant
- Agency Account Director
- Outsourced Marketing Manager

## 9. Do not contact — priority D

Wrong function, or guarantees a procurement detour.

- Customer Service / Support roles
- HR, People, Talent, Recruiting
- Software Engineer, Developer, IT
- Designer, Creative, Photographer, Copywriter
- Office Manager, Executive Assistant, Receptionist
- Legal, Compliance
- Warehouse Associate, Picker, Packer, Driver
- Intern, Assistant, Coordinator (any function)
- Investor, Board Member, Advisor, Non-Executive Director
- Anyone at an agency we would compete with

---

## How this maps to the system today

The Clay contact finder returns a `title` and a `department` for each person. The send
gate currently accepts `department === "executive"` and nothing else.

That is narrower than this document. It admits sections 1 and 2 and some of 3, and
excludes marketing, sales, operations and finance entirely. It also has a known flaw:
department does not distinguish a founder from a VP, so two colleagues at the same brand
can both pass.

Two known gaps if we widen it:

1. **Nothing enforces one contact per brand.** Clay formulas cannot see sibling rows, so
   a brand with three qualifying people yields three. Today that is handled by the human
   approving the push. Automating it needs the app to rank a brand's contacts and mark a
   primary, because the app sees all of a brand's contacts at once and Clay does not.

2. **Priority is not encoded anywhere.** A Marketplace Manager and a Content Manager both
   read as "marketing" to Clay. Until title priority lives in code, the A/B/C ordering
   above is guidance for a human, not a rule the system enforces.

---

## Message fit by group

The opening line our app writes is about Amazon: competitors on your listings, resellers,
undercutting, or absence from the marketplace. That lands differently by role.

| Group | Does the Amazon opener land? |
|---|---|
| Founder, owner, CEO | Yes. Brand control is emotive to them |
| Ecommerce, marketplace, Amazon roles | Yes, strongest fit in the list |
| COO, operations, supply chain | Partly. Lead with fulfilment and control |
| Marketing | Partly. Lead with brand protection and paid search waste |
| Sales, wholesale | Partly. Lead with channel conflict and unauthorised sellers |
| Finance | No. Rewrite around margin leak before contacting |

Do not send the standard opener to finance or operations without changing it. A line
about competitors on your listings reads as a marketing problem to a CFO, and the email
gets deleted.
