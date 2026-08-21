"""Slack notification client."""

from __future__ import annotations

from typing import Any

import requests

from sales_support_agent.config import Settings


SLACK_CHAT_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_UPLOAD_URL_REQUEST = "https://slack.com/api/files.getUploadURLExternal"
SLACK_UPLOAD_COMPLETE = "https://slack.com/api/files.completeUploadExternal"


class SlackClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def is_configured(self, channel: str = "") -> bool:
        return bool(self.settings.slack_bot_token and (channel or self.settings.slack_channel_id))

    def post_message(self, *, text: str, blocks: list[dict[str, Any]] | None = None,
                     channel: str = "") -> dict[str, Any]:
        if not self.is_configured(channel):
            return {"ok": False, "skipped": True, "reason": "slack_not_configured"}

        response = requests.post(
            SLACK_CHAT_POST_MESSAGE_URL,
            headers={
                "Authorization": f"Bearer {self.settings.slack_bot_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "channel": channel or self.settings.slack_channel_id,
                "text": text,
                "blocks": blocks or [],
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Slack API error: {payload}")
        return payload

    def upload_file(self, *, content: bytes, filename: str, title: str,
                    initial_comment: str, channel: str = "") -> dict[str, Any]:
        """Upload one CSV through Slack's external upload flow."""
        if not self.is_configured(channel):
            return {"ok": False, "skipped": True, "reason": "slack_not_configured"}
        headers = {"Authorization": f"Bearer {self.settings.slack_bot_token}"}
        request = requests.post(
            SLACK_UPLOAD_URL_REQUEST, headers=headers,
            data={"filename": filename, "length": str(len(content))}, timeout=30,
        )
        request.raise_for_status()
        upload = request.json()
        if not upload.get("ok"):
            raise RuntimeError(f"Slack upload URL error: {upload}")
        binary = requests.post(upload["upload_url"], files={"file": (filename, content, "text/csv")}, timeout=60)
        binary.raise_for_status()
        complete = requests.post(
            SLACK_UPLOAD_COMPLETE, headers={**headers, "Content-Type": "application/json; charset=utf-8"},
            json={"files": [{"id": upload["file_id"], "title": title}],
                  "channel_id": channel or self.settings.slack_channel_id,
                  "initial_comment": initial_comment}, timeout=30,
        )
        complete.raise_for_status()
        payload = complete.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Slack upload completion error: {payload}")
        return payload

