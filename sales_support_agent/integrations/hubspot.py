"""Thin HubSpot CRM v3/v4 API client.

HubSpot is the sales system source of truth (replacing ClickUp for sales).
This client is read-first for Phase 0 (deals, companies, contacts, line items,
owners); write/engagement methods are added in later phases. It mirrors the
shape of ``integrations/clickup.py`` — a small ``requests`` wrapper keyed off
``Settings`` — and reuses the ``HUBSPOT_API_TOKEN`` private-app token that the
legacy ``services/fulfillment_deck/hubspot_sync`` already relied on.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Sequence

import requests

from sales_support_agent.config import Settings


class HubSpotAPIError(RuntimeError):
    def __init__(self, *, status_code: int, method: str, path: str, message: str):
        super().__init__(message)
        self.status_code = int(status_code)
        self.method = method
        self.path = path


# Default deal properties we mirror locally. HubSpot returns only requested
# properties on search, so this list is the contract for the deal board.
DEAL_PROPERTIES: tuple[str, ...] = (
    "dealname",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "hubspot_owner_id",
    "createdate",
    "hs_lastmodifieddate",
    "hs_is_closed",
    "hs_is_closed_won",
    "description",
)

CONTACT_PROPERTIES: tuple[str, ...] = (
    "firstname",
    "lastname",
    "email",
    "phone",
    "jobtitle",
    "company",
    "associatedcompanyid",
    "hs_lastmodifieddate",
)

COMPANY_PROPERTIES: tuple[str, ...] = (
    "name",
    "domain",
    "industry",
    "city",
    "state",
    "hs_lastmodifieddate",
)

LINE_ITEM_PROPERTIES: tuple[str, ...] = (
    "name",
    "quantity",
    "price",
    "amount",
    "hs_product_id",
    "hs_sku",
)


class HubSpotClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    @property
    def is_configured(self) -> bool:
        return bool((self.settings.hubspot_api_token or "").strip())

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        response = requests.request(
            method=method,
            url=f"{self.settings.hubspot_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": f"Bearer {self.settings.hubspot_api_token}",
                "Content-Type": "application/json",
            },
            params=params,
            json=json_body,
            timeout=self.settings.hubspot_request_timeout_seconds,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            detail = ""
            try:
                payload = response.json() or {}
                if isinstance(payload, dict):
                    detail = str(payload.get("message") or payload.get("error") or "").strip()
            except ValueError:
                detail = (response.text or "").strip()
            message = f"HubSpot API {method.upper()} {path} failed with {response.status_code}."
            if detail:
                message = f"{message} {detail}"
            raise HubSpotAPIError(
                status_code=response.status_code,
                method=method.upper(),
                path=path,
                message=message,
            ) from exc
        if not response.content:
            return {}
        return response.json()

    # ------------------------------------------------------------------
    # Generic CRM object reads
    # ------------------------------------------------------------------
    def list_objects(
        self,
        object_type: str,
        *,
        properties: Sequence[str],
        limit: int = 100,
        after: str | None = None,
        filter_groups: list[dict[str, Any]] | None = None,
        sorts: list[dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], Optional[str]]:
        """One page of a CRM search. Returns (results, next_after_cursor)."""
        body: dict[str, Any] = {
            "limit": max(1, min(int(limit), 100)),
            "properties": list(properties),
        }
        if after:
            body["after"] = after
        if filter_groups:
            body["filterGroups"] = filter_groups
        if sorts:
            body["sorts"] = sorts
        payload = self._request("POST", f"/crm/v3/objects/{object_type}/search", json_body=body)
        results = list(payload.get("results", []) or [])
        next_after = (
            ((payload.get("paging") or {}).get("next") or {}).get("after") or None
        )
        return results, next_after

    def iter_objects(
        self,
        object_type: str,
        *,
        properties: Sequence[str],
        filter_groups: list[dict[str, Any]] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        max_records: int | None = None,
        start_after: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate every matching object, following pagination cursors."""
        after = start_after
        seen = 0
        while True:
            results, after = self.list_objects(
                object_type,
                properties=properties,
                after=after,
                filter_groups=filter_groups,
                sorts=sorts,
            )
            for row in results:
                yield row
                seen += 1
                if max_records is not None and seen >= max_records:
                    return
            if not after:
                return

    def get_object(
        self,
        object_type: str,
        object_id: str,
        *,
        properties: Sequence[str],
        associations: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"properties": ",".join(properties)}
        if associations:
            params["associations"] = ",".join(associations)
        return self._request("GET", f"/crm/v3/objects/{object_type}/{object_id}", params=params)

    def batch_read(
        self,
        object_type: str,
        ids: Sequence[str],
        *,
        properties: Sequence[str],
    ) -> list[dict[str, Any]]:
        """Read up to 100 objects by id in one call."""
        if not ids:
            return []
        body = {
            "properties": list(properties),
            "inputs": [{"id": str(i)} for i in ids if i],
        }
        payload = self._request(
            "POST", f"/crm/v3/objects/{object_type}/batch/read", json_body=body
        )
        return list(payload.get("results", []) or [])

    def list_associations(
        self, from_type: str, from_id: str, to_type: str
    ) -> list[str]:
        """Return associated object ids (CRM v4 associations)."""
        payload = self._request(
            "GET", f"/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}"
        )
        ids: list[str] = []
        for row in payload.get("results", []) or []:
            obj_id = str(row.get("toObjectId") or row.get("id") or "").strip()
            if obj_id:
                ids.append(obj_id)
        return ids

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------
    def iter_deals(self, *, max_records: int | None = None) -> Iterator[dict[str, Any]]:
        filter_groups = None
        pipeline = (self.settings.hubspot_sales_pipeline_id or "").strip()
        if pipeline:
            filter_groups = [
                {"filters": [{"propertyName": "pipeline", "operator": "EQ", "value": pipeline}]}
            ]
        return self.iter_objects(
            "deals",
            properties=DEAL_PROPERTIES,
            filter_groups=filter_groups,
            max_records=max_records,
        )

    def iter_contacts(self, *, max_records: int | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_objects("contacts", properties=CONTACT_PROPERTIES, max_records=max_records)

    def iter_companies(self, *, max_records: int | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_objects("companies", properties=COMPANY_PROPERTIES, max_records=max_records)

    def get_line_items(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        return self.batch_read("line_items", ids, properties=LINE_ITEM_PROPERTIES)

    def list_owners(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/crm/v3/owners", params={"limit": 100})
        return list(payload.get("results", []) or [])

    def list_deal_pipelines(self) -> list[dict[str, Any]]:
        """Deal pipelines with their stages (each stage has id + label)."""
        payload = self._request("GET", "/crm/v3/pipelines/deals")
        return list(payload.get("results", []) or [])

    def deal_stage_labels(self) -> dict[str, str]:
        """Flat map of stage_id -> human label across all deal pipelines."""
        labels: dict[str, str] = {}
        for pipeline in self.list_deal_pipelines():
            for stage in pipeline.get("stages", []) or []:
                sid = str(stage.get("id") or "").strip()
                label = str(stage.get("label") or "").strip()
                if sid and label:
                    labels[sid] = label
        return labels

    # ------------------------------------------------------------------
    # Write methods (Phase 2+)
    # ------------------------------------------------------------------

    def update_deal(self, deal_id: str, properties: dict[str, str]) -> dict[str, Any]:
        """PATCH a deal's properties in HubSpot."""
        return self._request(
            "PATCH",
            f"/crm/v3/objects/deals/{deal_id}",
            json_body={"properties": properties},
        )

    def update_contact(self, contact_id: str, properties: dict[str, str]) -> dict[str, Any]:
        """PATCH a contact's properties in HubSpot."""
        return self._request(
            "PATCH",
            f"/crm/v3/objects/contacts/{contact_id}",
            json_body={"properties": properties},
        )

    def log_email_engagement(
        self,
        *,
        deal_id: str,
        contact_ids: list[str],
        from_email: str,
        to_emails: list[str],
        subject: str,
        body_html: str,
        sent_at_ms: int,
    ) -> dict[str, Any]:
        """Log a sent email as a HubSpot CRM engagement (POST /crm/v3/objects/emails).

        Association type IDs are HUBSPOT_DEFINED: deal=210, contact=198.
        Phase 5 will call this after Gmail send to keep HubSpot current.
        """
        properties: dict[str, Any] = {
            "hs_timestamp": str(sent_at_ms),
            "hs_email_direction": "EMAIL",
            "hs_email_status": "SENT",
            "hs_email_subject": subject,
            "hs_email_text": body_html,
            "hs_email_from_email": from_email,
            "hs_email_to_email": ",".join(to_emails),
        }
        associations: list[dict[str, Any]] = []
        if deal_id:
            associations.append({
                "to": {"id": deal_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 210}],
            })
        for cid in contact_ids or []:
            associations.append({
                "to": {"id": cid},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 198}],
            })
        body: dict[str, Any] = {"properties": properties}
        if associations:
            body["associations"] = associations
        return self._request("POST", "/crm/v3/objects/emails", json_body=body)
