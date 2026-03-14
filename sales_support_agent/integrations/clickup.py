"""Thin ClickUp API client."""

from __future__ import annotations

from typing import Any

import requests

from sales_support_agent.config import Settings


class ClickUpClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None) -> Any:
        response = requests.request(
            method=method,
            url=f"{self.settings.clickup_base_url.rstrip('/')}/{path.lstrip('/')}",
            headers={
                "Authorization": self.settings.clickup_api_token,
                "Content-Type": "application/json",
            },
            params=params,
            json=json_body,
            timeout=self.settings.clickup_request_timeout_seconds,
        )
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    def get_list(self, list_id: str) -> dict[str, Any]:
        return self._request("GET", f"list/{list_id}")

    def get_accessible_custom_fields(self, list_id: str) -> list[dict[str, Any]]:
        try:
            payload = self._request("GET", f"list/{list_id}/field")
        except requests.HTTPError:
            return []
        return list(payload.get("fields", []) or payload.get("custom_fields", []) or [])

    def get_tasks(self, list_id: str, *, include_closed: bool = True, page: int = 0) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"list/{list_id}/task",
            params={
                "page": page,
                "include_closed": str(include_closed).lower(),
                "subtasks": "false",
            },
        )
        return list(payload.get("tasks", []) or [])

    def get_task(self, task_id: str) -> dict[str, Any]:
        return self._request("GET", f"task/{task_id}")

    def get_task_comments(self, task_id: str) -> list[dict[str, Any]]:
        try:
            payload = self._request("GET", f"task/{task_id}/comment")
        except requests.HTTPError:
            return []
        return list(payload.get("comments", []) or [])

    def create_task_comment(self, task_id: str, comment_text: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"task/{task_id}/comment",
            json_body={"comment_text": comment_text, "notify_all": False},
        )

    def update_task(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("PUT", f"task/{task_id}", json_body=payload)

    def set_custom_field_value(self, task_id: str, field_id: str, value: Any) -> dict[str, Any]:
        return self._request("POST", f"task/{task_id}/field/{field_id}", json_body={"value": value})

