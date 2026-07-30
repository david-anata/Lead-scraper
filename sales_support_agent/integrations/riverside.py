"""Riverside Business API v3 ingestion adapter."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urlsplit
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

    def resolve_download_url(self, source_url: str) -> str:
        """Resolve a Riverside bearer URL to its short-lived signed media URL."""

        parsed = urlsplit(source_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "platform.riverside.fm"
            or not parsed.path.startswith("/api/v3/download/")
        ):
            raise ValueError("Riverside media URL is not allowlisted.")
        request = Request(
            source_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "Anata-Agent-Content/1.0",
            },
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310
            resolved = response.geturl()
        final = urlsplit(resolved)
        if final.scheme != "https" or not final.hostname:
            raise RuntimeError("Riverside did not return a safe media URL.")
        return resolved

    def list_ready_recordings(
        self,
        *,
        studio_id: str = "",
        start_date: str = "",
        completed_episode_ids: set[str] | None = None,
    ) -> list[RiversideEpisode]:
        """Return ready recording assets, including downloaded TXT transcripts."""

        params = {"page": 0}
        if studio_id:
            params["studioId"] = studio_id
        if start_date:
            params["start_date"] = start_date
        url = f"{self.base_url}/api/v3/recordings?{urlencode(params)}"
        episodes: list[RiversideEpisode] = []
        recordings: list[dict[str, Any]] = []
        seen_pages: set[str] = set()
        for _ in range(50):
            if url in seen_pages:
                raise RuntimeError("Riverside pagination repeated a page.")
            seen_pages.add(url)
            payload = json.loads(self._get(url) or b"{}")
            recordings.extend(payload.get("data") or [])
            next_url = str(payload.get("next_page_url") or "").strip()
            if not next_url:
                break
            parsed = urlsplit(next_url)
            if (
                parsed.scheme != "https"
                or parsed.hostname != "platform.riverside.fm"
                or not parsed.path.startswith("/api/v3/recordings")
            ):
                raise RuntimeError("Riverside returned an unsafe pagination URL.")
            url = next_url
        else:
            raise RuntimeError("Riverside pagination exceeded the safety limit.")

        for recording in recordings:
            if str(recording.get("status") or "").lower() != "ready":
                continue
            episode_id = str(
                recording.get("recording_id") or recording.get("id") or ""
            ).strip()
            if not episode_id:
                continue
            if episode_id in (completed_episode_ids or set()):
                continue
            title = str(recording.get("name") or "")
            assets: list[dict[str, Any]] = []
            for track in recording.get("tracks") or []:
                for file_item in track.get("files") or []:
                    file_type = str(file_item.get("type") or "")
                    download_url = str(file_item.get("download_url") or "")
                    if not download_url:
                        continue
                    stable_file_id = str(file_item.get("id") or "").strip()
                    if not stable_file_id:
                        stable_file_id = hashlib.sha256(
                            urlsplit(download_url).path.encode("utf-8")
                        ).hexdigest()[:32]
                    assets.append(
                        {
                            "asset_id": f"{episode_id}:{stable_file_id}",
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
