"""QuickBooks draft invoices for approved Building billing schedules."""

from __future__ import annotations

from datetime import date
import hashlib
from typing import Any

import requests

from sales_support_agent.api.qbo_auth_router import _load_tokens, get_valid_access_token

QBO_BASE_URL = "https://quickbooks.api.intuit.com/v3/company"
VERIFIED_QBO_REALM_ID = "9130357569555476"
BUILDING_ITEM_IDS = {"event": "77", "deposit": "79"}


class BuildingQuickBooksError(RuntimeError):
    """QuickBooks rejected or could not verify a Building operation."""


class BuildingQuickBooksClient:
    """Purpose-limited QBO client that creates drafts but never sends them."""

    def __init__(self) -> None:
        self.realm_id = str((_load_tokens() or {}).get("realm_id") or "").strip()

    @property
    def is_configured(self) -> bool:
        return bool(self.realm_id and get_valid_access_token())

    def _headers(self) -> dict[str, str]:
        token = get_valid_access_token()
        if not token or not self.realm_id:
            raise BuildingQuickBooksError(
                "QuickBooks is not connected. Reconnect anata LLC in Finance settings."
            )
        if self.realm_id != VERIFIED_QBO_REALM_ID:
            raise BuildingQuickBooksError(
                "Building billing is connected to an unverified QuickBooks company."
            )
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{QBO_BASE_URL}/{self.realm_id}/{path}",
            headers=self._headers(),
            params=params,
            json=payload,
            timeout=30,
        )
        if response.status_code >= 400:
            message = ""
            try:
                fault = (response.json() or {}).get("Fault") or {}
                error = next(iter(fault.get("Error") or []), {})
                message = str(
                    error.get("Detail") or error.get("Message") or ""
                ).strip()
            except (TypeError, ValueError):
                message = ""
            safe_message = message[:300] if message else "Review the invoice fields in QuickBooks."
            raise BuildingQuickBooksError(
                f"QuickBooks rejected the Building draft ({response.status_code}): {safe_message}"
            )
        return response.json()

    @staticmethod
    def _quoted(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def ensure_customer(self, *, name: str, email: str) -> dict[str, Any]:
        normalized_email = email.strip().lower()
        query = (
            "SELECT * FROM Customer WHERE PrimaryEmailAddr = "
            f"'{self._quoted(normalized_email)}' MAXRESULTS 2"
        )
        data = self._request(
            "GET", "query", params={"query": query, "minorversion": "70"}
        )
        rows = data.get("QueryResponse", {}).get("Customer", [])
        if len(rows) > 1:
            raise BuildingQuickBooksError(
                "QuickBooks has duplicate customers for this billing email."
            )
        if rows:
            return rows[0]
        data = self._request(
            "POST",
            "customer",
            params={"minorversion": "70"},
            payload={
                "DisplayName": name.strip(),
                "PrimaryEmailAddr": {"Address": normalized_email},
            },
        )
        customer = data.get("Customer") or {}
        if not customer.get("Id"):
            raise BuildingQuickBooksError("QuickBooks returned no customer ID.")
        return customer

    def create_draft_invoice(
        self,
        *,
        customer_id: str,
        description: str,
        amount_cents: int,
        schedule_type: str,
        due_date: date,
        idempotency_key: str,
        line_items: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if schedule_type not in {
            "one_time",
            "deposit",
            "final_balance",
            "security_deposit",
            "event_invoice",
        }:
            raise BuildingQuickBooksError(
                "This Building schedule is not an event charge and has no verified QuickBooks item."
            )
        item_id = (
            BUILDING_ITEM_IDS["deposit"]
            if schedule_type == "security_deposit"
            else BUILDING_ITEM_IDS["event"]
        )
        amount = round(amount_cents / 100, 2)
        sales_detail: dict[str, Any] = {
            "ItemRef": {"value": item_id},
            "Qty": 1,
            "UnitPrice": amount,
            # Agent freezes the legally reviewed tax in the approved quote.
            # Prevent QuickBooks from calculating tax again on that gross amount.
            "TaxCodeRef": {"value": "NON"},
        }
        qbo_lines: list[dict[str, Any]] = []
        for item in line_items or []:
            item_amount = round(int(item["amount_cents"]) / 100, 2)
            detail: dict[str, Any] = {
                "ItemRef": {"value": BUILDING_ITEM_IDS["deposit"] if item.get("type") == "security_deposit" else BUILDING_ITEM_IDS["event"]},
                "Qty": 1,
                "UnitPrice": item_amount,
                # Event line amounts are gross of Agent-calculated sales tax;
                # the refundable deposit is independently non-taxable.
                "TaxCodeRef": {"value": "NON"},
            }
            qbo_lines.append({
                "Amount": item_amount,
                "Description": str(item["description"]),
                "DetailType": "SalesItemLineDetail",
                "SalesItemLineDetail": detail,
            })
        provider_request_id = "building-" + hashlib.sha256(
            idempotency_key.encode("utf-8")
        ).hexdigest()[:40]
        data = self._request(
            "POST",
            "invoice",
            params={"minorversion": "70", "requestid": provider_request_id},
            payload={
                "CustomerRef": {"value": customer_id},
                "DueDate": due_date.isoformat(),
                "PrivateNote": f"Agent Building schedule {idempotency_key}",
                "CustomerMemo": {"value": description.strip()},
                "Line": qbo_lines or [{
                    "Amount": amount,
                    "Description": description.strip(),
                    "DetailType": "SalesItemLineDetail",
                    "SalesItemLineDetail": sales_detail,
                }],
            },
        )
        invoice = data.get("Invoice") or {}
        if not invoice.get("Id"):
            raise BuildingQuickBooksError("QuickBooks returned no invoice ID.")
        return invoice

    def get_invoice(self, invoice_id: str) -> dict[str, Any]:
        """Read one authoritative QuickBooks invoice for reconciliation."""

        data = self._request(
            "GET",
            f"invoice/{self._quoted(invoice_id)}",
            params={"minorversion": "70"},
        )
        invoice = data.get("Invoice") or {}
        if not invoice.get("Id"):
            raise BuildingQuickBooksError("QuickBooks returned no invoice evidence.")
        return invoice
