# Von HR reference-to-Anata parity audit

**Reference inspected:** `anatavon/anata-hr` at commit `703cf739362abbe42f134df2ae868889e63fcd3b`

**Destination:** Anata Agent HR at `agent.anatainc.com` / Lead-scraper repository
**Boundary:** Von's repository is a read-only product reference. Anata does not depend on it, connect to it, modify it, or treat its data or calculations as authoritative.
## Plain-English conclusion

Agent already duplicates nearly all useful behavior from the reference app and adds stronger approval, evidence, privacy, compliance, and recovery controls. We should copy the product ideas—not Von's tax math, payment behavior, Base44 dependency, or broad permissions.

The remaining useful parity gap is employer-managed handbook publishing with version-specific employee acknowledgement. That capability is included in the next Anata release.

## Feature-by-feature decision

| Von reference capability | Anata status | Decision |
|---|---|---|
| Employee profile and onboarding | Implemented and stronger | Keep Anata's secure invitation, employee-owned profile, W-4, I-9 status, correction, and activation flow. |
| W-4 fields | Implemented and stronger | Keep encrypted, effective-dated Anata records. Never copy Von's automatically defaulted filing choice. |
| Clock in/out | Implemented | Keep simple daily clocking, as David requested. |
| Time project and tag fields | Data-compatible but intentionally hidden | Do not add project coding to the employee clock. The approved need is simple clock in/out. |
| Timesheet review and freeze | Implemented and stronger | Keep employee submission, independent manager approval, corrections, and stale-approval invalidation. |
| Teams and manager assignment | Implemented | Keep least-privilege assigned-employee scope. |
| Pay schedules | Implemented for Anata's approved schedule | Keep 1st–15th paid 20th and 16th–month-end paid following 5th, with Saturday/Friday and Sunday/Monday adjustment. Do not add unused multi-company schedule complexity. |
| Hourly, salaried, and contractor types | Implemented | Keep W-2 payroll separate from Wise contractor approvals and evidence. |
| Bonus and custom pay | Implemented and expanded | Keep bonuses, commissions, reimbursements, holiday pay, PTO, voluntary deductions, garnishments, and corrections as reviewed inputs. |
| Payroll preview/run | Implemented and stronger | Keep immutable versions, readiness blockers, separate preparer/approver, cash impact, and audit trail. |
| Tax calculation | Rebuilt | Reject Von's intentionally simplified tax engine. Anata preserves rule versions, opening balances, W-4 elections, employer taxes, and independent-review gates. Calculations remain estimates until qualified reconciliation. |
| Paychecks and printed checks | Implemented and stronger | Keep unique check numbers, void/reissue history, employee statements, and reconciliation evidence. |
| Stripe Connect transfer | Intentionally not copied | Stripe transfer is money movement, not payroll tax calculation or filing. Do not enable automatic transfer from an unverified estimate. |
| Tax reports | Implemented and expanded | Keep payroll registers, liability evidence, quarterly/YTD accountant exports, compliance calendar, and recovery ZIP. |
| Employee handbook | Partial before this audit | Add employer publishing, one active version, immutable history, secure HTTPS link, per-version acknowledgement, and reminder coverage. |
| Organization/multi-company model | Not needed | Anata is one small Utah employer. Avoid unnecessary tenant complexity. |
| Admin roles | Rebuilt and stronger | Reject broad employee/manager/owner/admin access. Keep separate people, compensation, time, payroll, settings, and audit permissions. |
| Base44 backend/data dependency | Rejected | Agent's database and recovery exports are Anata-controlled. Von/Base44 data is not required at runtime. |

## Unsafe reference behavior explicitly rejected

1. Von's `runPayroll` source says its federal/state tax model is simplified and not tax-accurate.
2. The reference can label a submitted run `completed` even when it has only persisted line items.
3. The reference can send Stripe transfers using those simplified net-pay calculations.
4. Stripe does not deposit or file federal or Utah payroll taxes.
5. Reference roles allow broad manager/owner/admin payroll execution without Anata's independent approval and recent-authentication controls.

## Proof required before Anata's first real payroll

- Complete all three employee masters and current W-4s.
- Reconcile opening wages and taxes through the cutover date.
- Record the actual Utah unemployment rate from evidence.
- Obtain an independent qualified review of the calculation package.
- Approve all time, PTO, holiday, and variable-pay inputs.
- Compare one full payroll against an independent calculation with no unexplained differences.
- Keep transfers disabled until the approved net amounts and payment controls are independently verified.
