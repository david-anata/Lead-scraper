"""Slack notification client."""

from __future__ import annotations

from typing import Any

import requests

from sales_support_agent.config import Settings


SLACK_CHAT_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"


class SlackClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def is_configured(self) -> bool:
        return bool(self.settings.slack_bot_token and self.settings.slack_channel_id)

    def post_message(self, *, text: str, blocks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        if not self.is_configured():
            return {"ok": False, "skipped": True, "reason": "slack_not_configured"}

        response = requests.post(
            SLACK_CHAT_POST_MESSAGE_URL,
            headers={
                "Authorization": f"Bearer {self.settings.slack_bot_token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            json={
                "channel": self.settings.slack_channel_id,
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

