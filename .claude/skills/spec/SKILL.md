# /spec — Implementation Specification

**Description:** Write a full implementation specification from a completed project interview, before any code is written. Use after /discover is done and approved.

---

## GLOBAL OPERATING RULES

- Think before acting.
- Plan before building.
- Verify before declaring success.
- Never claim completion without evidence.
- Prefer parallel execution whenever tasks are independent.
- Preserve existing functionality unless explicitly instructed otherwise.
- If assumptions are required, state them clearly.
- If a better architecture is discovered during implementation, stop and present it before continuing.
- Optimize for maintainability, scalability, and clarity — not just speed.
- Treat every completed project as an opportunity to improve future workflows and skills.

---

## STEP 2 — Create the Implementation Specification

Using everything we've discussed, create a detailed implementation specification.

Include:
- Overall architecture
- Major phases
- Dependencies
- Technical decisions
- Alternative approaches
- Tradeoffs
- Risks
- Success criteria
- Verification strategy — MUST plan for the THREE-PASS RULE: the build phase cannot push live or hand off to David until the verification loop passes three consecutive times with zero findings, where the final pass is always an end-to-end Chrome MCP walkthrough of the real user workflow (every entry point, every state, "does the content make sense to the person seeing it?"). Any finding resets the counter. The spec must name the exact workflows that pass 3 will walk.
- Testing strategy

For every major decision explain:
- Why it was chosen
- Alternatives considered
- Potential drawbacks

The goal is to eliminate ambiguity before implementation.

Do not build anything yet.

---

After writing the full spec, close with:

> "Does this spec look correct? Once approved, run /ship to begin building."

Do not begin building. Do not proceed. Wait for explicit approval.
