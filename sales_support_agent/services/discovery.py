"""Read-only ClickUp schema discovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sales_support_agent.config import Settings
from sales_support_agent.integrations.clickup import ClickUpClient


class ClickUpDiscoveryService:
    def __init__(self, settings: Settings, clickup_client: ClickUpClient):
        self.settings = settings
        self.clickup_client = clickup_client

    def run(self, *, sample_size: int | None = None) -> dict[str, Any]:
        list_id = self.settings.clickup_list_id
        sample_limit = sample_size or self.settings.clickup_discovery_sample_size

        list_metadata = self.clickup_client.get_list(list_id)
        accessible_fields = self.clickup_client.get_accessible_custom_fields(list_id)
        tasks = self.clickup_client.get_tasks(list_id, include_closed=True, page=0)[:sample_limit]

        sample_payloads: list[dict[str, Any]] = []
        discovered_statuses: set[str] = set()
        for task in tasks:
            task_id = str(task.get("id") or "")
            comments = self.clickup_client.get_task_comments(task_id) if task_id else []
            status_name = str(((task.get("status") or {}).get("status")) or "")
            if status_name:
                discovered_statuses.add(status_name)
            sample_payloads.append(
                {
                    "id": task_id,
                    "name": task.get("name"),
                    "status": status_name,
                    "assignees": task.get("assignees", []),
                    "custom_fields": task.get("custom_fields", []),
                    "recent_comments": comments[:5],
                }
            )

        snapshot = {
            "list_id": list_id,
            "list_name": list_metadata.get("name", ""),
            "list_metadata": list_metadata,
            "accessible_custom_fields": accessible_fields,
            "discovered_statuses": sorted(discovered_statuses),
            "sample_tasks": sample_payloads,
        }
        self._write_snapshot(snapshot)
        return snapshot

    def _write_snapshot(self, snapshot: dict[str, Any]) -> None:
        path: Path = self.settings.discovery_snapshot_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")

