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
    "service_type",
    "agency",
    "fulfillment",
    "shipping_os",
    "hs_next_step",
    # Native HubSpot email/activity signals — used as fallback for touch timestamps
    # when Gmail matching has not yet populated last_outbound_at / last_inbound_at.
    "hs_email_last_send_date",
    "hs_email_last_replied",
    "notes_last_updated",
    "hs_last_sales_activity_date",
    "hs_sales_email_last_opened",
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

NOTE_PROPERTIES: tuple[str, ...] = (
    "hs_note_body",
    "hs_timestamp",
    "hs_lastmodifieddate",
    "hubspot_owner_id",
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

    def get_notes(self, ids: Sequence[str]) -> list[dict[str, Any]]:
        return self.batch_read("notes", ids, properties=NOTE_PROPERTIES)

    def get_recent_deal_notes(self, deal_id: str, *, limit: int = 8) -> list[dict[str, Any]]:
        note_ids = self.list_associations("deals", deal_id, "notes")[: max(int(limit), 1)]
        notes = self.get_notes(note_ids)
        def _sort_key(row: dict[str, Any]) -> str:
            props = row.get("properties") or {}
            return str(props.get("hs_timestamp") or props.get("hs_lastmodifieddate") or "")
        return sorted(notes, key=_sort_key, reverse=True)

    def list_owners(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/crm/v3/owners", params={"limit": 100})
        return list(payload.get("results", []) or [])

    def list_deal_pipelines(self) -> list[dict[str, Any]]:
        """Deal pipelines with their stages (each stage has id + label)."""
        payload = self._request("GET", "/crm/v3/pipelines/deals")
        return list(payload.get("results", []) or [])

    def get_deal_pipeline(self, pipeline_id: str) -> dict[str, Any]:
        return self._request("GET", f"/crm/v3/pipelines/deals/{pipeline_id}")

    def list_properties(self, object_type: str) -> list[dict[str, Any]]:
        payload = self._request("GET", f"/crm/v3/properties/{object_type}")
        return list(payload.get("results", []) or [])

    def list_association_labels(self, from_type: str, to_type: str) -> list[dict[str, Any]]:
        payload = self._request("GET", f"/crm/v4/associations/{from_type}/{to_type}/labels")
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

    def create_deal(
        self,
        properties: dict[str, str],
        *,
        associations: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Create a HubSpot deal with optional CRM associations."""
        body: dict[str, Any] = {"properties": properties}
        if associations:
            body["associations"] = associations
        return self._request("POST", "/crm/v3/objects/deals", json_body=body)

    def update_contact(self, contact_id: str, properties: dict[str, str]) -> dict[str, Any]:
        """PATCH a contact's properties in HubSpot."""
        return self._request(
            "PATCH",
            f"/crm/v3/objects/contacts/{contact_id}",
            json_body={"properties": properties},
        )

    def find_contact_by_email(self, email: str) -> dict[str, Any] | None:
        """Return the existing HubSpot contact for an exact normalized email."""

        results, _ = self.list_objects(
            "contacts",
            properties=CONTACT_PROPERTIES,
            limit=1,
            filter_groups=[{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": str(email or "").strip().lower(),
                }]
            }],
        )
        return results[0] if results else None

    def create_contact(self, properties: dict[str, str]) -> dict[str, Any]:
        """Create a new contact in HubSpot. Returns the created contact object."""
        return self._request(
            "POST",
            "/crm/v3/objects/contacts",
            json_body={"properties": properties},
        )

    def create_company(self, properties: dict[str, str]) -> dict[str, Any]:
        """Create a new company in HubSpot. Returns the created company object."""
        return self._request(
            "POST",
            "/crm/v3/objects/companies",
            json_body={"properties": properties},
        )

    def create_association(
        self,
        from_type: str,
        from_id: str,
        to_type: str,
        to_id: str,
        *,
        association_type_id: int,
    ) -> dict[str, Any]:
        """Associate two CRM objects (CRM v4 associations PUT).

        Common HUBSPOT_DEFINED typeIds: contact→deal=4, deal→contact=3,
        company→deal=341, deal→company=342.
        """
        return self._request(
            "PUT",
            f"/crm/v4/associations/{from_type}/{from_id}/{to_type}/{to_id}",
            json_body=[{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": association_type_id}],
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

    def create_note(
        self,
        *,
        deal_id: str,
        body: str,
        timestamp_ms: int | None = None,
    ) -> dict[str, Any]:
        """Create a CRM note associated with a deal (POST /crm/v3/objects/notes).

        Association typeId 214 = note → deal (HUBSPOT_DEFINED).
        """
        import time as _time
        return self._request(
            "POST",
            "/crm/v3/objects/notes",
            json_body={
                "properties": {
                    "hs_note_body": body,
                    "hs_timestamp": str(timestamp_ms or int(_time.time() * 1000)),
                },
                "associations": [{
                    "to": {"id": deal_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 214}],
                }],
            },
        )

    def create_contact_note(
        self,
        *,
        contact_id: str,
        body: str,
        timestamp_ms: int | None = None,
    ) -> dict[str, Any]:
        """Create a CRM note associated with a contact (POST /crm/v3/objects/notes).

        Association typeId 202 = note → contact (HUBSPOT_DEFINED). Mirrors
        create_note above, which targets deals (typeId 214).
        """
        import time as _time
        return self._request(
            "POST",
            "/crm/v3/objects/notes",
            json_body={
                "properties": {
                    "hs_note_body": body,
                    "hs_timestamp": str(timestamp_ms or int(_time.time() * 1000)),
                },
                "associations": [{
                    "to": {"id": contact_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 202}],
                }],
            },
        )

    def create_task(
        self,
        *,
        deal_id: str,
        subject: str,
        body: str,
        due_at_ms: int,
        owner_id: str = "",
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "hs_task_subject": subject,
            "hs_task_body": body,
            "hs_timestamp": str(due_at_ms),
            "hs_task_status": "NOT_STARTED",
            "hs_task_priority": "HIGH",
            "hs_task_type": "TODO",
        }
        if owner_id.strip():
            properties["hubspot_owner_id"] = owner_id.strip()
        return self._request(
            "POST",
            "/crm/v3/objects/tasks",
            json_body={
                "properties": properties,
                "associations": [{
                    "to": {"id": deal_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 216}],
                }],
            },
        )
