"""Riverside Business API v3 ingestion adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class RiversideEpisode:
    """One ready Riverside recording normalized for Agent ingestion."""

    episode_id: str
    assets: list[dict[str, Any]]


class RiversideClient:
    """Read ready recordings and transcripts from Riverside's supported v3 API."""

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://platform.riverside.fm",
        timeout_seconds: float = 20.0,
    ) -> None:
        self.api_key = api_key.strip()
        if not self.api_key:
            raise ValueError("Riverside API key is required.")
        self.base_url = base_url.rstrip("/")
        if self.base_url != "https://platform.riverside.fm":
            raise ValueError("Riverside API base URL is not allowlisted.")
        self.timeout_seconds = max(1.0, min(float(timeout_seconds), 30.0))

    def _get(self, url: str) -> bytes:
        request = Request(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json, text/plain",
                "User-Agent": "Anata-Agent-Content/1.0",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310
            return response.read(2_000_000)

    def list_ready_recordings(
        self,
        *,
        studio_id: str = "",
        start_date: str = "",
    ) -> list[RiversideEpisode]:
        """Return ready recording assets, including downloaded TXT transcripts."""

        params = {"page": 0}
        if studio_id:
            params["studioId"] = studio_id
        if start_date:
            params["start_date"] = start_date
        url = f"{self.base_url}/api/v3/recordings?{urlencode(params)}"
        payload = json.loads(self._get(url) or b"{}")
        episodes: list[RiversideEpisode] = []
        for recording in payload.get("data") or []:
            if str(recording.get("status") or "").lower() != "ready":
                continue
            episode_id = str(
                recording.get("recording_id") or recording.get("id") or ""
            ).strip()
            if not episode_id:
                continue
            title = str(recording.get("name") or "")
            assets: list[dict[str, Any]] = []
            for track in recording.get("tracks") or []:
                for file_item in track.get("files") or []:
                    file_type = str(file_item.get("type") or "")
                    download_url = str(file_item.get("download_url") or "")
                    if not download_url:
                        continue
                    assets.append(
                        {
                            "asset_id": str(file_item.get("id") or download_url),
                            "asset_type": (
                                "audio" if "audio" in file_type else "video"
                            ),
                            "status": str(track.get("status") or "ready"),
                            "title": title,
                            "speaker": str(track.get("name") or ""),
                            "source_url": download_url,
                            "metadata": {"riverside_file_type": file_type},
                        }
                    )
            transcription = recording.get("transcription") or {}
            if str(transcription.get("status") or "").lower() == "done":
                text_file = next(
                    (
                        item
                        for item in transcription.get("files") or []
                        if str(item.get("type") or "").lower() == "txt"
                    ),
                    None,
                )
                if text_file and text_file.get("download_url"):
                    transcript_url = str(text_file["download_url"])
                    transcript_text = self._get(transcript_url).decode(
                        "utf-8", errors="replace"
                    )
                    assets.append(
                        {
                            "asset_id": f"{episode_id}:transcript:txt",
                            "asset_type": "transcript",
                            "status": "ready",
                            "title": title,
                            "transcript_text": transcript_text[:200_000],
                            "source_url": transcript_url,
                            "metadata": {"riverside_file_type": "txt"},
                        }
                    )
            if assets:
                episodes.append(RiversideEpisode(episode_id, assets))
        return episodes
