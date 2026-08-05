"""Plain-language, source-derived Arena launch status.

This module only reads already-loaded records and non-secret configuration
flags. It never calls a provider, changes a decision, or treats configuration
as proof that an external action succeeded.
"""

from __future__ import annotations

from typing import Any, Iterable


OWNER_DECISION_KEYS = {
    "cancellation_policy",
    "tax_treatment",
    "setup_price",
    "teardown_price",
    "overtime_rate",
    "payment_workflow",
    "transactional_sender",
}


def _decision_complete(decisions: dict[str, dict[str, Any]], key: str) -> bool:
    required = {
        "transactional_sender": "owner_confirmed",
    }.get(key, "accepted_policy")
    return str(decisions.get(key, {}).get("status") or "") == required


def _item(
    *,
    key: str,
    label: str,
    state: str,
    summary: str,
    next_action: str,
    owner: str,
    href: str = "",
    action_label: str = "",
) -> dict[str, str]:
    return {
        "key": key,
        "label": label,
        "state": state,
        "summary": summary,
        "next_action": next_action,
        "owner": owner,
        "href": href,
        "action_label": action_label,
    }


def build_arena_launch_status(
    *,
    launch_decisions: Iterable[dict[str, Any]],
    rate_plans: Iterable[dict[str, Any]],
    agreement_templates: Iterable[dict[str, Any]],
    provider_readiness: dict[str, Any],
) -> dict[str, Any]:
    """Return one truthful staff checklist derived from authoritative evidence."""

    decisions = {
        str(row.get("decision_key") or ""): dict(row)
        for row in launch_decisions
    }
    plans = [
        dict(row)
        for row in rate_plans
        if str(row.get("offering_id") or "") == "arena-events"
    ]
    plans.sort(key=lambda row: int(row.get("version") or 0), reverse=True)
    approved_plan = next(
        (row for row in plans if row.get("status") == "approved"),
        None,
    )
    current_plan = approved_plan or (plans[0] if plans else None)
    templates = [
        dict(row)
        for row in agreement_templates
        if row.get("template_key") == "arena-event-agreement"
    ]
    approved_template = next(
        (row for row in templates if row.get("status") == "approved"),
        None,
    )
    current_template = (
        approved_template
        or max(templates, key=lambda row: int(row.get("version") or 0), default=None)
    )

    owner_complete = sum(
        1 for key in OWNER_DECISION_KEYS if _decision_complete(decisions, key)
    )
    items: list[dict[str, str]] = [
        _item(
            key="business_rules",
            label="Business rules",
            state="complete" if owner_complete == len(OWNER_DECISION_KEYS) else "blocked",
            summary=(
                "Your cancellation, access, overtime, payment, and customer-email "
                "rules are saved."
                if owner_complete == len(OWNER_DECISION_KEYS)
                else f"{owner_complete} of {len(OWNER_DECISION_KEYS)} owner-approved rules are saved."
            ),
            next_action=(
                "Nothing to answer. Change a saved rule only when the policy changes."
                if owner_complete == len(OWNER_DECISION_KEYS)
                else "Record only the missing owner-approved business rules."
            ),
            owner="Anata",
            href="/admin/building/settings#arena-launch-readiness",
            action_label="Review saved rules",
        )
    ]

    if approved_plan:
        pricing_state = "complete"
        pricing_summary = "The current Arena rate plan is approved and locked."
        pricing_next = "Agent uses its frozen terms for new estimates and quotes."
    elif current_plan:
        pricing_state = "needs_review"
        pricing_summary = "The owner-approved Arena pricing is prepared as a private draft."
        pricing_next = "Finish the listed tax and stale-copy checks, then review and approve the plan."
    else:
        pricing_state = "blocked"
        pricing_summary = "No Arena pricing draft is available."
        pricing_next = "Prepare the owner-reconciled private Arena rate plan."
    items.append(_item(
        key="pricing",
        label="Arena pricing",
        state=pricing_state,
        summary=pricing_summary,
        next_action=pricing_next,
        owner="Anata",
        href="/admin/building/settings#commercial-rate-plans",
        action_label="Open pricing",
    ))

    tax_ready = bool(
        current_plan
        and current_plan.get("tax_status") in {"taxable", "non_taxable"}
        and (
            current_plan.get("tax_status") == "non_taxable"
            or int(current_plan.get("tax_rate_bps") or 0) > 0
        )
    )
    items.append(_item(
        key="tax",
        label="Tax determination",
        state="complete" if tax_ready else "external",
        summary=(
            "Accountant-reviewed tax treatment is recorded."
            if tax_ready
            else "Quotes still require tax review; no taxable categories or numeric rate are approved."
        ),
        next_action=(
            "No action unless the accountant changes the determination."
            if tax_ready
            else "Ask the accountant which Arena charges are taxable and the rate to apply."
        ),
        owner="Accountant",
        href="/admin/building/settings#commercial-rate-plans",
        action_label="Review tax field",
    ))

    conflicts = list((current_plan or {}).get("conflicts") or [])
    provider_conflicts = [
        row
        for row in conflicts
        if row.get("blocks_rate_plan_approval")
        and row.get("status") not in set(row.get("approval_resolution_statuses") or [])
    ]
    booking_copy_ready = bool(current_plan) and not provider_conflicts
    items.append(_item(
        key="booking_copy",
        label="Old booking-page copy",
        state=(
            "complete"
            if booking_copy_ready
            else ("external" if current_plan else "blocked")
        ),
        summary=(
            "All stale booking-page conflicts have verified remediation evidence."
            if booking_copy_ready
            else (
                f"{len(provider_conflicts)} TidyCal conflict{'s' if len(provider_conflicts) != 1 else ''} still show outdated deposit, balance, or payment-link information."
                if current_plan
                else "Pricing reconciliation has not yet identified the booking-page conflicts."
            )
        ),
        next_action=(
            "No action."
            if booking_copy_ready
            else (
                "Correct each item in TidyCal, then record provider-remediation evidence beside that item."
                if current_plan
                else "Prepare the Arena pricing draft before reviewing booking-page conflicts."
            )
        ),
        owner="Booking-page administrator",
        href="/admin/building/settings#commercial-rate-plans",
        action_label="Review conflicts",
    ))

    items.append(_item(
        key="agreement",
        label="Reusable event agreement",
        state="complete" if approved_template else "external",
        summary=(
            "A reusable Arena agreement has legal approval and a locked version."
            if approved_template
            else (
                "The Arena agreement is prepared for legal review."
                if current_template
                else "No reusable Arena agreement has been prepared."
            )
        ),
        next_action=(
            "Use the approved version for new agreement packages."
            if approved_template
            else "Have the designated legal reviewer approve the complete reusable agreement."
        ),
        owner="Legal reviewer",
        href="/admin/building/contracts/templates",
        action_label="Open agreement",
    ))

    esign_ready = bool(provider_readiness.get("esign_verified"))
    items.append(_item(
        key="esign",
        label="Electronic signatures",
        state="complete" if esign_ready else "external",
        summary=(
            "The approved e-sign connection is verified."
            if esign_ready
            else "Agent can prepare agreements, but no approved e-sign provider is connected."
        ),
        next_action=(
            "Monitor provider delivery and signed-document evidence."
            if esign_ready
            else "Select the e-sign provider and authorize its production credentials and callback workflow."
        ),
        owner="Owner and platform administrator",
        href="/admin/building/contracts",
        action_label="Open contracts",
    ))

    # QuickBooks is the rail Anata actually bills on: it issues the Arena
    # invoice and holds the payment of record. Stripe exists only as a webhook
    # that hears about a payment automatically. Requiring Stripe made this row
    # ask for a provider the business does not use, so either rail satisfies it
    # and the wording says which one is connected.
    quickbooks_billing = bool(provider_readiness.get("quickbooks_connected"))
    stripe_confirmation = bool(
        provider_readiness.get("payment_credentials")
        and provider_readiness.get("payment_webhook")
    )
    payment_ready = quickbooks_billing or stripe_confirmation
    items.append(_item(
        key="payment",
        label="Customer payments",
        state="complete" if payment_ready else "external",
        summary=(
            "QuickBooks issues the invoice and holds the payment of record. "
            "A cleared payment is recorded on the booking by hand."
            if quickbooks_billing and not stripe_confirmation
            else "The payment account and confirmation webhook are configured."
            if payment_ready
            else "Payment requests can be prepared, but no billing account is connected."
        ),
        next_action=(
            "Nothing to connect. Record each cleared payment on its booking."
            if quickbooks_billing and not stripe_confirmation
            else "Complete a controlled provider verification before accepting a live booking."
            if payment_ready
            else "Connect QuickBooks, or authorize a payment account and its confirmation webhook."
        ),
        owner="Finance and platform administrator",
        href="/admin/building/billing",
        action_label="Open billing",
    ))

    calendar_configured = bool(provider_readiness.get("calendar_configured"))
    calendar_writes = bool(provider_readiness.get("calendar_writes_enabled"))
    calendar_verified = (
        decisions.get("event_calendar", {}).get("status") == "provider_verified"
    )
    calendar_ready = calendar_configured and calendar_writes and calendar_verified
    # "Outside setup" told an operator nothing about which of the three is
    # missing, so somebody asked to finish it had no way to see what to do.
    calendar_error = str(provider_readiness.get("calendar_readiness_error") or "")
    calendar_target = str(provider_readiness.get("calendar_target_id") or "")
    if calendar_ready:
        calendar_summary = (
            "The dedicated calendar is verified and production projection is enabled."
        )
        calendar_next = "Monitor projection errors and reconciliation."
    elif calendar_error:
        calendar_summary = f"Agent cannot reach a calendar: {calendar_error}"
        calendar_next = "Fix the connection above before recording it as verified."
    elif not calendar_writes:
        calendar_summary = (
            f"Connected to {calendar_target}, but writes are switched off."
            if calendar_target
            else "Connected, but writes are switched off."
        )
        calendar_next = "Set BUILDING_GOOGLE_CALENDAR_WRITES_ENABLED=true, then record the calendar as verified."
    elif not calendar_verified:
        calendar_summary = (
            f"Connected to {calendar_target} and writes are on. Not yet recorded as verified."
            if calendar_target
            else "Connected and writes are on. Not yet recorded as verified."
        )
        calendar_next = "Record the dedicated event calendar decision to finish this."
    else:
        calendar_summary = (
            "Calendar projection remains dry-run only; no verified dedicated "
            "Arena destination is active."
        )
        calendar_next = (
            "Provide an Anata-owned calendar, grant service-account access, "
            "verify it, then deliberately enable writes."
        )
    items.append(_item(
        key="calendar",
        label="Dedicated Arena calendar",
        state="complete" if calendar_ready else "external",
        summary=calendar_summary,
        next_action=calendar_next,
        owner="Google Workspace and platform administrator",
        # This pointed at a section that lives on the Operations tab, so the
        # link did nothing from Settings. Send it to the card that records it.
        href="/admin/building/settings#decision-event_calendar",
        action_label="Record the calendar",
    ))

    sender_ready = bool(
        provider_readiness.get("sender_credentials")
        and provider_readiness.get("sender_webhook")
        and provider_readiness.get("sender_matches_owner_choice")
    )
    items.append(_item(
        key="sender",
        label="Customer email",
        state="complete" if sender_ready else "external",
        summary=(
            "building@anatainc.com and delivery feedback are configured."
            if sender_ready
            else "building@anatainc.com is the approved inbox, but production sender/domain verification is incomplete."
        ),
        next_action=(
            "Complete a controlled delivery test before automated customer use."
            if sender_ready
            else "Verify the anatainc.com sender with the email provider and configure delivery feedback."
        ),
        owner="Google Workspace and platform administrator",
        href="/admin/building/contacts",
        action_label="Open customer communication",
    ))

    catalog_evidence_present = any(
        key in provider_readiness
        for key in ("arena_space_public_available", "arena_offering_published")
    )
    catalog_ready = bool(
        provider_readiness.get("arena_space_public_available")
        and provider_readiness.get("arena_offering_published")
    ) if catalog_evidence_present else True
    if catalog_evidence_present:
        items.append(_item(
            key="public_catalog",
            label="Public Arena catalog",
            state="complete" if catalog_ready else "blocked",
            summary=(
                "The canonical Arena space is public and available, and its offering is published."
                if catalog_ready
                else "The verified Arena records remain private, unavailable, or unpublished. The website therefore uses its safe marketing fallback."
            ),
            next_action=(
                "Keep Agent inventory and the dedicated calendar authoritative."
                if catalog_ready
                else "After the final rehearsal, deliberately mark The Arena public and available and publish only the canonical arena-events offering."
            ),
            owner="Building administrator",
            href="/admin/building/catalog",
            action_label="Review catalog publication",
        ))

    customer_launch_ready = bool(
        approved_plan
        and tax_ready
        and approved_template
        and esign_ready
        and payment_ready
        and calendar_ready
        and sender_ready
        and catalog_ready
        and not provider_conflicts
    )
    items.append(_item(
        key="customer_launch",
        label="Customer booking launch",
        state="complete" if customer_launch_ready else "automatic",
        summary=(
            "The governed booking path has all required authoritative evidence."
            if customer_launch_ready
            else "Public estimates remain non-binding and booking confirmation remains locked."
        ),
        next_action=(
            "Run the final controlled booking rehearsal and production approval."
            if customer_launch_ready
            else "No separate answer is needed. Agent unlocks this only after the required evidence above is complete."
        ),
        owner="Agent (automatic)",
        href="/admin/building/bookings",
        action_label="Open bookings",
    ))

    external_count = sum(1 for row in items if row["state"] == "external")
    blocked_count = sum(1 for row in items if row["state"] == "blocked")
    complete_count = sum(1 for row in items if row["state"] == "complete")
    return {
        "items": items,
        "owner_complete": owner_complete,
        "owner_total": len(OWNER_DECISION_KEYS),
        "external_count": external_count,
        "blocked_count": blocked_count,
        "complete_count": complete_count,
        "customer_launch_ready": customer_launch_ready,
        "headline": (
            "Ready for a controlled launch rehearsal"
            if customer_launch_ready
            else "Your business answers are saved; outside approvals and connections remain"
        ),
    }
