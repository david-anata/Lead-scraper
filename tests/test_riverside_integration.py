from __future__ import annotations

import json

from sales_support_agent.integrations.riverside import RiversideClient


class FakeRiversideClient(RiversideClient):
    def __init__(self, responses: dict[str, bytes]) -> None:
        super().__init__(api_key="test-key")
        self.responses = responses
        self.requested: list[str] = []

    def _get(self, url: str) -> bytes:
        self.requested.append(url)
        return self.responses[url]


def _recording(recording_id: str, *, status: str = "ready") -> dict:
    return {
        "recording_id": recording_id,
        "name": f"Episode {recording_id}",
        "status": status,
        "tracks": [
            {
                "id": f"track-{recording_id}",
                "status": "done",
                "files": [
                    {
                        "type": "raw_video",
                        "download_url": (
                            "https://platform.riverside.fm/api/v3/download/file/"
                            f"video-{recording_id}"
                        ),
                    }
                ],
            }
        ],
        "transcription": {
            "status": "done",
            "files": [
                {
                    "type": "txt",
                    "download_url": (
                        "https://platform.riverside.fm/api/v3/download/transcription/"
                        f"{recording_id}?type=txt"
                    ),
                }
            ],
        },
    }


def test_riverside_paginates_and_ingests_every_ready_recording() -> None:
    first = "https://platform.riverside.fm/api/v3/recordings?page=0"
    second = "https://platform.riverside.fm/api/v3/recordings?page=1"
    t1 = (
        "https://platform.riverside.fm/api/v3/download/transcription/"
        "one?type=txt"
    )
    t2 = (
        "https://platform.riverside.fm/api/v3/download/transcription/"
        "two?type=txt"
    )
    client = FakeRiversideClient(
        {
            first: json.dumps(
                {
                    "data": [_recording("one"), _recording("processing", status="processing")],
                    "next_page_url": second,
                }
            ).encode(),
            second: json.dumps(
                {"data": [_recording("two")], "next_page_url": None}
            ).encode(),
            t1: b"One complete source-backed transcript for operators.",
            t2: b"Two complete source-backed transcript for operators.",
        }
    )
    episodes = client.list_ready_recordings()
    assert [item.episode_id for item in episodes] == ["one", "two"]
    assert all(len(item.assets) == 2 for item in episodes)
    assert len({asset["asset_id"] for item in episodes for asset in item.assets}) == 4
    assert second in client.requested
